#!/usr/bin/env bash
#
# A policy edited on one node takes effect on another node.
#
# Two gateways share one Postgres and one Redis. Both cache policies for an
# hour, so nothing here can be explained by a TTL lapsing: if node B starts
# enforcing an edit node A made, it is because the change was pushed to it.
#
# The control is the same scenario with change notifications switched off on
# node B, which MUST keep enforcing the old policy. Without that half, a
# passing run would only show that something changed, not why.
#
# How a phase tells the two apart: editing a limit's parameters starts a fresh
# counter, because the parameters are part of the storage key.
#
#   node B saw the edit    -> a fresh bucket of the NEW size, so N admitted
#   node B did not see it  -> the OLD bucket, already spent, so 0 admitted
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

PORT_A="${PORT_A:-18092}"
PORT_B="${PORT_B:-18093}"
GRPC_A="${GRPC_A:-19192}"
GRPC_B="${GRPC_B:-19193}"
URL_A="http://127.0.0.1:${PORT_A}"
URL_B="http://127.0.0.1:${PORT_B}"
ADMIN_TOKEN="policy-reload-test-token"
DSN="${POSTGRES_DSN:-postgres://gateway:gateway@127.0.0.1:5432/gateway?sslmode=disable}"
REDIS="${REDIS_ADDR:-127.0.0.1:6379}"

WORK="$(mktemp -d -t policy-reload-XXXXXX)"
PID_A=""
PID_B=""
fail=0

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

write_config() { # $1 = path, $2 = listen_for_changes
  cat >"$1" <<YAML
node:
  id: reload-node
  http_addr: "127.0.0.1:${PORT_A}"
limiter:
  backend: redis
redis:
  addr: "${REDIS}"
  db: 13
  # Generous on purpose. The production default is 250ms, which is right for a
  # limiter: a check that has not answered by then should fail rather than hold
  # a request up. This test runs two gateways, a Postgres and a curl loop on one
  # development box, where a scheduling stall longer than that says nothing
  # about the thing being tested.
  timeout: 3s
postgres:
  dsn: "${DSN}"
policy:
  store: postgres
  # An hour, so that nothing in this test can be explained by a TTL lapsing.
  cache_ttl: 1h
  stale_for: 1h
  listen_for_changes: $2
auth:
  api_keys: true
  admin_token_env: GATEWAY_ADMIN_TOKEN
check_api:
  trust_tenant_header: true
YAML
}

start_node() { # $1 = config, $2 = port, $3 = node id, $4 = grpc port -> echoes pid
  # Each node needs its own gRPC port as well as its own HTTP one: the config
  # names a single address and two processes on one host cannot share it.
  # Without this the second node exits with "address already in use", which is
  # the right behaviour and a broken test.
  GATEWAY_ADMIN_TOKEN="${ADMIN_TOKEN}" \
    "${BIN}" --node="$3" --config="$1" --http-addr="127.0.0.1:$2" \
    --grpc-addr="127.0.0.1:$4" >>"${WORK}/$3.log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 1 "http://127.0.0.1:$2/healthz" >/dev/null 2>&1; then
      echo "${pid}"
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "FAIL: node $3 exited during startup" >&2
      cat "${WORK}/$3.log" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "FAIL: node $3 never became healthy" >&2
  cat "${WORK}/$3.log" >&2
  return 1
}

admin() { # $1 = method, $2 = path, $3 = body (may be empty)
  if [ -n "${3:-}" ]; then
    curl -s -o "${WORK}/admin.out" -w '%{http_code}' -X "$1" \
      -H "X-Admin-Token: ${ADMIN_TOKEN}" -H 'Content-Type: application/json' \
      -d "$3" "${URL_A}$2"
  else
    curl -s -o "${WORK}/admin.out" -w '%{http_code}' -X "$1" \
      -H "X-Admin-Token: ${ADMIN_TOKEN}" "${URL_A}$2"
  fi
}

burst() { # $1 = base url, $2 = tenant, $3 = how many -> echoes the allowed count
  local allowed=0 other=0 codes="" code
  for _ in $(seq 1 "$3"); do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
      -H "X-Tenant-ID: $2" "$1/v1/check")
    case "${code}" in
      200) allowed=$((allowed + 1)) ;;
      429) ;;
      *)   other=$((other + 1)); codes="${codes} ${code}" ;;
    esac
  done
  if [ "${other}" -ne 0 ]; then
    # A 503 here is the counter store failing closed. Counted as "not allowed"
    # it would read as a limit doing its job, which is how an infrastructure
    # problem gets mistaken for a passing test.
    echo "burst against $2 got ${other} answers that were neither 200 nor 429:${codes}" >&2
    echo "-1"
    return
  fi
  echo "${allowed}"
}

set_policy() { # $1 = policy name, $2 = count
  local body
  body=$(printf '{"failure_mode":"closed","limits":[{"name":"per-hour","algorithm":"token_bucket","count":%s,"period_ms":3600000,"burst":%s}]}' "$2" "$2")
  local code
  code=$(admin PUT "/admin/v1/policies/$1" "${body}")
  if [ "${code}" != "200" ]; then
    echo "FAIL: writing policy $1 answered ${code}: $(cat "${WORK}/admin.out")" >&2
    return 1
  fi
}

BIN="${WORK}/gateway"
echo "=== build ==="
go build -o "${BIN}" ./cmd/gateway || exit 1
go build -o "${WORK}/gatewayctl" ./cmd/gatewayctl || exit 1

echo "=== migrate ==="
"${WORK}/gatewayctl" migrate up --dsn "${DSN}" || exit 1

run_phase() { # $1 = listen_for_changes on node B, $2 = expected admitted after the edit
  local listen="$1" expect="$2"
  local suffix
  suffix="$(date +%s)$$${RANDOM}"
  local policy="reload-${suffix}"
  local tenant="reload-${suffix}"

  write_config "${WORK}/a.yaml" true
  write_config "${WORK}/b.yaml" "${listen}"

  PID_A=$(start_node "${WORK}/a.yaml" "${PORT_A}" node-a "${GRPC_A}") || return 1
  PID_B=$(start_node "${WORK}/b.yaml" "${PORT_B}" node-b "${GRPC_B}") || return 1

  set_policy "${policy}" 5 || return 1
  local code
  code=$(admin POST "/admin/v1/tenants" "{\"id\":\"${tenant}\",\"name\":\"Reload\",\"policy\":\"${policy}\"}")
  if [ "${code}" != "200" ]; then
    echo "FAIL: creating the tenant answered ${code}: $(cat "${WORK}/admin.out")" >&2
    return 1
  fi

  # Node B learns the tenant and spends its whole allowance.
  local before
  before=$(burst "${URL_B}" "${tenant}" 8)
  if [ "${before}" -ne 5 ]; then
    echo "FAIL: node B admitted ${before} of 8 against a limit of 5 before the edit" >&2
    return 1
  fi

  # Node A edits the policy. Node B's TTL is an hour.
  set_policy "${policy}" 2 || return 1
  sleep 1

  local after
  after=$(burst "${URL_B}" "${tenant}" 8)
  echo "node B admitted ${after} of 8 after the edit (expected ${expect})"
  if [ "${after}" -ne "${expect}" ]; then
    return 1
  fi
  return 0
}

echo
echo "=== notifications on: the edit reaches the other node ==="
if run_phase true 2; then
  echo "OK: node B enforced the new limit of 2 without its TTL lapsing"
else
  echo "FAIL: node B did not pick up the edit" >&2
  fail=1
fi
kill "${PID_A}" "${PID_B}" 2>/dev/null; wait "${PID_A}" "${PID_B}" 2>/dev/null
PID_A=""; PID_B=""

echo
echo "=== control: notifications off, the edit must NOT reach it ==="
if run_phase false 0; then
  echo "OK: with notifications off node B kept enforcing the old policy, so the phase above measured something real"
else
  echo "FAIL: the control did not behave as required." >&2
  echo "  With change notifications off and an hour-long TTL, node B must still be" >&2
  echo "  enforcing the OLD policy, whose bucket is already spent -- so it must admit 0." >&2
  echo "  If it admitted the new limit, something other than notifications is refreshing" >&2
  echo "  the cache and the test above proves nothing." >&2
  fail=1
fi

echo
if [ "${fail}" -ne 0 ]; then
  echo "POLICY RELOAD FAILED" >&2
  echo "--- node A ---" >&2; cat "${WORK}/node-a.log" >&2
  echo "--- node B ---" >&2; cat "${WORK}/node-b.log" >&2
  exit 1
fi
echo "POLICY RELOAD OK"
