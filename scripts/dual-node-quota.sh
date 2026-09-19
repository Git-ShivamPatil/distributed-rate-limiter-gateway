#!/usr/bin/env bash
#
# Two gateway processes, one Redis, one quota.
#
# This is the check that the shared backend is actually shared. Fire N requests
# split across two independent gateway processes and require the combined
# number admitted to equal the configured quota EXACTLY -- not "about right",
# and emphatically not 2x, which is what per-process counters produce and what
# every single-node test in this repository would still pass with.
#
# It also runs the same scenario against the memory backend as a control. That
# run is REQUIRED to over-admit: a test that cannot fail when the mechanism is
# removed is not evidence that the mechanism works.
#
#   scripts/dual-node-quota.sh [--requests 2000] [--redis 127.0.0.1:6379]
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

REQUESTS=2000
REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
PORT_A="${PORT_A:-18090}"
PORT_B="${PORT_B:-18091}"
# A quota that does not refill during the run, so "exactly the quota" means
# exactly the quota rather than "the quota plus whatever refilled".
QUOTA=500

while [ $# -gt 0 ]; do
  case "$1" in
    --*=*) flag="${1%%=*}"; value="${1#*=}"; shift; set -- "${flag}" "${value}" "$@"; continue ;;
  esac
  case "$1" in
    --requests) REQUESTS="$2"; shift 2 ;;
    --redis)    REDIS_ADDR="$2"; shift 2 ;;
    -h|--help)  sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "dual-node-quota: unknown argument $1" >&2; exit 2 ;;
  esac
done

WORK="$(mktemp -d -t dual-node-XXXXXX)"
PID_A=""
PID_B=""

cleanup() {
  for pid in "${PID_A}" "${PID_B}"; do
    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null
      wait "${pid}" 2>/dev/null
    fi
  done
  rm -rf "${WORK}"
}
trap cleanup EXIT

write_config() { # $1 = path, $2 = backend
  cat >"$1" <<YAML
node:
  id: dual-node
  http_addr: "127.0.0.1:18090"
limiter:
  backend: $2
redis:
  addr: "${REDIS_ADDR}"
  db: 14
policies:
  default: shared
  named:
    shared:
      failure_mode: closed
      limits:
        - name: quota
          algorithm: token_bucket
          count: ${QUOTA}
          period: 1h
          burst: ${QUOTA}
check_api:
  trust_tenant_header: true
YAML
}

start_pair() { # $1 = config
  "${BIN}" --node=node-a --config="$1" --http-addr="127.0.0.1:${PORT_A}" >>"${WORK}/a.log" 2>&1 &
  PID_A=$!
  "${BIN}" --node=node-b --config="$1" --http-addr="127.0.0.1:${PORT_B}" >>"${WORK}/b.log" 2>&1 &
  PID_B=$!

  for port in "${PORT_A}" "${PORT_B}"; do
    local up=0
    for _ in $(seq 1 50); do
      if curl -fsS --max-time 1 "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then up=1; break; fi
      sleep 0.2
    done
    if [ "${up}" -ne 1 ]; then
      echo "FAIL: no gateway on port ${port}" >&2
      cat "${WORK}"/*.log >&2
      return 1
    fi
  done
  return 0
}

# Fires REQUESTS requests alternating between the two nodes and prints how many
# were admitted in total.
run_split() { # $1 = tenant
  local tenant="$1" allowed=0 code port
  for i in $(seq 1 "${REQUESTS}"); do
    if [ $((i % 2)) -eq 0 ]; then port="${PORT_A}"; else port="${PORT_B}"; fi
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
      -H "X-Tenant-ID: ${tenant}" "http://127.0.0.1:${port}/v1/check")
    if [ "${code}" = "200" ]; then allowed=$((allowed + 1)); fi
  done
  echo "${allowed}"
}

BIN="${WORK}/gateway"
echo "=== build ==="
go build -o "${BIN}" ./cmd/gateway || exit 1

fail=0

# ------------------------------------------------------------------ redis ---
echo
echo "=== shared backend: two nodes, one Redis, one quota ==="
write_config "${WORK}/redis.yaml" redis
if ! start_pair "${WORK}/redis.yaml"; then
  echo "  (is Redis reachable at ${REDIS_ADDR}?)" >&2
  exit 1
fi

# Clear any state from an earlier run; the config pins db 14.
redis-cli -u "redis://${REDIS_ADDR}/14" FLUSHDB >/dev/null 2>&1 || \
  echo "note: could not flush db 14 (redis-cli missing?); using a unique tenant instead"
TENANT="dual-$(date +%s)-$$"

shared_allowed=$(run_split "${TENANT}")
echo "two nodes admitted ${shared_allowed} of ${REQUESTS} against a quota of ${QUOTA}"
if [ "${shared_allowed}" -ne "${QUOTA}" ]; then
  echo "FAIL: expected exactly ${QUOTA}." >&2
  if [ "${shared_allowed}" -gt "${QUOTA}" ]; then
    echo "  More than the quota means the two nodes are NOT sharing state." >&2
  else
    echo "  Fewer than the quota means requests were lost or refused wrongly." >&2
  fi
  fail=1
fi
stop_a="${PID_A}"; stop_b="${PID_B}"
kill "${stop_a}" "${stop_b}" 2>/dev/null; wait "${stop_a}" "${stop_b}" 2>/dev/null
PID_A=""; PID_B=""

# ---------------------------------------------------------------- control ---
# The same scenario with per-process counters. This run MUST over-admit; if it
# does not, the test above is not measuring what it claims to.
echo
echo "=== control: the same two nodes with per-process counters ==="
write_config "${WORK}/memory.yaml" memory
start_pair "${WORK}/memory.yaml" || exit 1

control_allowed=$(run_split "control-${TENANT}")
echo "two nodes with private counters admitted ${control_allowed} against a quota of ${QUOTA}"
if [ "${control_allowed}" -le "${QUOTA}" ]; then
  echo "FAIL: the control admitted ${control_allowed}, which is not more than the quota." >&2
  echo "  Per-process counters must over-admit here. If they do not, the shared-quota" >&2
  echo "  result above cannot be read as evidence that Redis is doing anything." >&2
  fail=1
fi

echo
if [ "${fail}" -ne 0 ]; then
  echo "DUAL-NODE QUOTA FAILED" >&2
  exit 1
fi
echo "DUAL-NODE QUOTA OK: shared=${shared_allowed} (exactly the quota), control=${control_allowed} (over-admits, as it must)"
