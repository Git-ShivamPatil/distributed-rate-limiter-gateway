#!/usr/bin/env bash
#
# Starts a real gateway process and exercises the decision endpoint over a real
# socket.
#
# The unit tests drive the same handler in-process with a fake clock. This runs
# the binary the way a reader runs it -- built, flagged and listening -- because
# a suite that only calls the handler proves nothing about the binary's
# arguments, its config file, or its listener. The first time it ran it caught
# exactly that class of bug: an unset config field became a key budget of zero,
# and every request failed closed.
#
# Two phases, deliberately:
#
#   1. the shipped configs/local.yaml, reproducing the case study's own numbers
#   2. scripts/testdata/smoke.yaml, whose limits refill over an hour, so the
#      header and refusal assertions cannot depend on how fast this machine is
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

PORT="${PORT:-18080}"
ADDR="127.0.0.1:${PORT}"
URL="http://${ADDR}"
BIN="./bin/gateway"
LOG="$(mktemp -t gateway-smoke-XXXXXX.log)"
GATEWAY_PID=""
fail=0

stop_gateway() {
  if [ -n "${GATEWAY_PID}" ] && kill -0 "${GATEWAY_PID}" 2>/dev/null; then
    kill "${GATEWAY_PID}" 2>/dev/null
    wait "${GATEWAY_PID}" 2>/dev/null
  fi
  GATEWAY_PID=""
}

cleanup() {
  stop_gateway
  if [ "${KEEP_LOG:-0}" = "1" ]; then echo "gateway log: ${LOG}"; else rm -f "${LOG}"; fi
}
trap cleanup EXIT

start_gateway() { # $1 = config path, $2 = node id
  local config="$1" node="$2"
  # --flag=value on purpose: this is the form compose and Kubernetes write.
  "${BIN}" --node="${node}" --config="${config}" --http-addr="${ADDR}" >>"${LOG}" 2>&1 &
  GATEWAY_PID=$!
  for _ in $(seq 1 50); do
    if curl -fsS --max-time 1 "${URL}/healthz" >/dev/null 2>&1; then
      echo "gateway up on ${ADDR} (pid ${GATEWAY_PID}, config ${config})"
      return 0
    fi
    if ! kill -0 "${GATEWAY_PID}" 2>/dev/null; then
      echo "FAIL: the gateway exited during startup with ${config}" >&2
      cat "${LOG}" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "FAIL: /healthz never answered on ${ADDR}" >&2
  cat "${LOG}" >&2
  return 1
}

header_value() { # $1 = header name, $2 = response headers
  echo "$2" | grep -i "^$1:" | tr -d '\r' | awk '{print $2}'
}

echo "=== build ==="
go build -o "${BIN}" ./cmd/gateway || exit 1

# ---------------------------------------------------------------- phase 1 ---
# The numbers the case study puts on the page, against the config it names.
echo
echo "=== phase 1: the shipped config ==="
start_gateway ./configs/local.yaml smoke-shipped || exit 1

# 20 tokens per minute is one token every 3 seconds, so an exact 20/10 split is
# only valid while the loop stays inside that interval.
scripts/burst-check.sh --url "${URL}" --tenant acme \
  --requests 30 --expect-allowed 20 --expect-denied 10 --max-seconds 2 || fail=1

# A different tenant on the same policy is untouched by that flood.
scripts/burst-check.sh --url "${URL}" --tenant quiet-neighbour \
  --requests 20 --expect-allowed 20 --expect-denied 0 --max-seconds 2 || fail=1

# The pro policy's 25-per-second window refuses before its per-minute bucket
# does. How many get through depends on how long the loop takes, so the
# assertion is a floor and a refusal, not an exact count.
scripts/burst-check.sh --url "${URL}" --tenant globex \
  --requests 40 --min-allowed 25 --min-denied 1 || fail=1

stop_gateway

# ---------------------------------------------------------------- phase 2 ---
# Semantics that must not depend on machine speed.
echo
echo "=== phase 2: fixed limits, no refill during the run ==="
start_gateway ./scripts/testdata/smoke.yaml smoke-fixed || exit 1

echo "--- an allowed response carries the headers and no Retry-After ---"
allowed_headers=$(curl -s -D - -o /dev/null --max-time 5 -H 'X-Tenant-ID: header-check' "${URL}/v1/check")
if [ "$(header_value X-RateLimit-Limit "${allowed_headers}")" != "20" ]; then
  echo "FAIL: X-RateLimit-Limit was '$(header_value X-RateLimit-Limit "${allowed_headers}")', expected 20" >&2
  fail=1
fi
if [ "$(header_value X-RateLimit-Remaining "${allowed_headers}")" != "19" ]; then
  echo "FAIL: X-RateLimit-Remaining was '$(header_value X-RateLimit-Remaining "${allowed_headers}")', expected 19" >&2
  fail=1
fi
if [ -n "$(header_value Retry-After "${allowed_headers}")" ]; then
  echo "FAIL: an allowed response carried Retry-After" >&2
  fail=1
fi

echo "--- a refusal carries a whole-second Retry-After ---"
for _ in $(seq 1 19); do
  curl -s -o /dev/null -H 'X-Tenant-ID: header-check' "${URL}/v1/check"
done
denied_headers=$(curl -s -D - -o /dev/null --max-time 5 -H 'X-Tenant-ID: header-check' "${URL}/v1/check")
# 20 per hour is one token every 180s, and the bucket is empty.
retry=$(header_value Retry-After "${denied_headers}")
if [ "${retry}" != "180" ]; then
  echo "FAIL: Retry-After on a refusal was '${retry}', expected 180 (one token per 180s at 20/hour)" >&2
  fail=1
fi
if [ "$(header_value X-RateLimit-Remaining "${denied_headers}")" != "0" ]; then
  echo "FAIL: a refusal did not report 0 remaining" >&2
  fail=1
fi

echo "--- every limit is reported, and the tighter one sets the headers ---"
dual=$(curl -s --max-time 5 -H 'X-Tenant-ID: dual-tenant' "${URL}/v1/check")
dual_headers=$(curl -s -D - -o /dev/null --max-time 5 -H 'X-Tenant-ID: dual-tenant' "${URL}/v1/check")
limit_count=$(echo "${dual}" | grep -o '"name":' | wc -l | tr -d ' ')
if [ "${limit_count}" -ne 2 ]; then
  echo "FAIL: the response named ${limit_count} limits, expected 2: ${dual}" >&2
  fail=1
fi
if [ "$(header_value X-RateLimit-Limit "${dual_headers}")" != "5" ]; then
  echo "FAIL: the headers describe the looser limit, not the tighter one: ${dual_headers}" >&2
  fail=1
fi

echo "--- a refused request consumes from nothing ---"
# The strict limit allows 5. Spend them, then confirm the 1000-wide bucket has
# only been charged for those 5 and not for the refusals.
for _ in $(seq 1 3); do
  curl -s -o /dev/null -H 'X-Tenant-ID: dual-tenant' "${URL}/v1/check"
done
for _ in $(seq 1 10); do
  curl -s -o /dev/null -H 'X-Tenant-ID: dual-tenant' "${URL}/v1/check" # all refused
done
body=$(curl -s --max-time 5 -H 'X-Tenant-ID: dual-tenant' "${URL}/v1/check")
hourly_remaining=$(echo "${body}" | sed 's/.*"name":"per-hour","limit":1000,"remaining":\([0-9]*\).*/\1/')
if [ "${hourly_remaining}" != "995" ]; then
  echo "FAIL: the per-hour bucket shows ${hourly_remaining} remaining, expected 995." >&2
  echo "  Ten refused requests charged it anyway: a refusal is consuming quota." >&2
  echo "  body: ${body}" >&2
  fail=1
fi

echo "--- a request with no tenant is refused ---"
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${URL}/v1/check")
if [ "${code}" != "400" ]; then
  echo "FAIL: a request with no tenant header answered ${code}, expected 400" >&2
  fail=1
fi

echo "--- an impossible cost is a client error, not a refusal ---"
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
  -H 'X-Tenant-ID: header-check' -H 'X-RateLimit-Cost: 21' "${URL}/v1/check")
if [ "${code}" != "400" ]; then
  echo "FAIL: a cost larger than the bucket answered ${code}, expected 400" >&2
  fail=1
fi

if [ "${fail}" -ne 0 ]; then
  echo
  echo "SMOKE FAILED" >&2
  echo "--- gateway log ---" >&2
  cat "${LOG}" >&2
  exit 1
fi
echo
echo "SMOKE OK"
