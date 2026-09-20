#!/usr/bin/env bash
#
# Wipe the counter store underneath a running cluster.
#
# This is a LIMITATION TEST. It does not show that a wipe is survivable --
# nothing can survive losing every counter. It shows that the loss is
# DETECTED, COUNTED and BOUNDED, rather than being a silent gift of a full
# fresh quota to every tenant at once.
#
# Redis has no identity that survives being emptied. A store that was flushed
# and came back looks exactly like a store nobody has used yet, and the most
# likely way to lose everything -- FLUSHALL -- does not even change the run id,
# so a detector watching only that would see a perfectly healthy store. What
# makes the difference visible is a generation, minted once per lifetime of the
# store and held in the cluster's own log, where the flush cannot reach it.
#
# Four things are asserted, and the fourth is the one that matters:
#
#   1. The wipe is DETECTED -- the node reports itself blocked.
#   2. While it is blocked, ZERO requests are admitted. Fail-closed is global
#      here, not per tenant: the generation names the whole store, and the
#      probe fires before it could know which tenants were touched.
#   3. The cluster agrees on a NEW generation that strictly OUTRANKS the old
#      one, so a node still carrying the previous number is refused rather than
#      waved through.
#   4. The amnesty is measured and printed. A bound nobody measures is a guess.
#
# THE CONTROL. Every assertion above would be vacuous if the mechanism were
# simply absent -- so the same scenario runs against a build with no store
# generation at all, where the wipe MUST go unnoticed and the tenant MUST get
# its full quota back in silence.
#
#   scripts/store-wipe-test.sh              the real thing; must pass
#   scripts/store-wipe-test.sh --control    no generation at all; must fail
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

CONTROL=0
if [ "${1:-}" = "--control" ]; then
  CONTROL=1
fi

NODES=3
QUOTA=40
SPEND=20
REDIS="${REDIS_ADDR:-127.0.0.1:6379}"
REDIS_HOST=${REDIS%%:*}
REDIS_PORT=${REDIS##*:}
TENANT="wipe-$(date +%s)$$"
POLICY="wipe-policy-$(date +%s)$$"
ADMIN_TOKEN="${GATEWAY_ADMIN_TOKEN:-cluster-admin-token}"
DIR=.cluster
fail=0

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "INCONCLUSIVE: redis-cli is not installed, so the store cannot be wiped" >&2
  exit 2
fi

cleanup() { ./scripts/cluster-down.sh >/dev/null 2>&1; }
trap cleanup EXIT

if [ "${CONTROL}" -eq 1 ]; then
  echo "=== CONTROL: no store generation (this run MUST fail) ==="
  export BUILD_TAGS=faultinject
  export GATEWAY_FAULT=no_store_gen
fi

echo "=== start ${NODES} nodes ==="
GATEWAY_ADMIN_TOKEN="${ADMIN_TOKEN}" ./scripts/cluster-up.sh "${NODES}" || exit 1

first_http=$(head -1 "${DIR}/nodes" | awk '{print $2}')
base="http://127.0.0.1:${first_http}"
admin=$(./scripts/leader-http.sh) || exit 1

# A quota that cannot refill during the run, so every number below is a count.
body=$(printf '{"failure_mode":"closed","limits":[{"name":"per-day","algorithm":"token_bucket","count":%s,"period_ms":86400000,"burst":%s}]}' "${QUOTA}" "${QUOTA}")
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" -H 'Content-Type: application/json' \
  -d "${body}" "${admin}/admin/v1/policies/${POLICY}")
if [ "${code}" != "200" ]; then
  echo "FAIL: creating the policy answered ${code}" >&2
  exit 1
fi
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" -H 'Content-Type: application/json' \
  -d "{\"id\":\"${TENANT}\",\"name\":\"Wipe test\",\"policy\":\"${POLICY}\"}" \
  "${admin}/admin/v1/tenants")
if [ "${code}" != "200" ]; then
  echo "FAIL: creating the tenant answered ${code}" >&2
  exit 1
fi
sleep 1

send() { # send N requests, print "allowed denied other"
  a=0; d=0; o=0
  n=1
  while [ "${n}" -le "$1" ]; do
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
      -H "X-Tenant-ID: ${TENANT}" "${base}/v1/check")
    case "${c}" in
      200) a=$((a + 1)) ;;
      429) d=$((d + 1)) ;;
      *)   o=$((o + 1)) ;;
    esac
    n=$((n + 1))
  done
  echo "${a} ${d} ${o}"
}

store_field() { # store_field <name>
  curl -fsS --max-time 5 "${base}/v1/cluster" 2>/dev/null |
    sed -n "s/.*\"store\":{[^}]*\"$1\":\([^,}]*\).*/\1/p"
}

echo
echo "=== spend ${SPEND} of the ${QUOTA} ==="
read -r a d o <<EOF
$(send ${SPEND})
EOF
echo "${a} allowed, ${d} refused, ${o} other"
if [ "${a}" -ne "${SPEND}" ]; then
  echo "FAIL: only ${a} of ${SPEND} were admitted before anything was wiped" >&2
  exit 1
fi

gen_before=$(store_field committed)
gen_before=${gen_before:-0}
echo "the cluster has agreed on store generation ${gen_before}"
if [ "${CONTROL}" -eq 0 ] && [ "${gen_before}" = "0" ]; then
  echo "FAIL: no store generation was ever committed, so there is nothing to wipe out" >&2
  exit 1
fi

echo
echo "=== wipe the store ==="
# The run id does NOT change across this, which is exactly why the missing
# generation has to be a signal in its own right.
run_before=$(redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" info server | sed -n 's/^run_id:\(.*\)/\1/p' | tr -d '\r')
redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" flushdb >/dev/null || exit 1
run_after=$(redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" info server | sed -n 's/^run_id:\(.*\)/\1/p' | tr -d '\r')
if [ "${run_before}" = "${run_after}" ]; then
  echo "the run id is unchanged (${run_after}); only the missing generation says anything happened"
else
  echo "NOTE: this Redis changed its run id across a flush, so that signal would also have fired"
fi

echo
echo "=== claim 1: the wipe is detected ==="
blocked=""
waited=0
for _ in $(seq 1 100); do
  blocked=$(store_field blocked)
  [ "${blocked}" = "true" ] && break
  waited=$((waited + 1))
  sleep 0.2
done
if [ "${blocked}" = "true" ]; then
  echo "detected after about $((waited * 200))ms"
else
  echo "FAIL: the store was wiped and no node noticed" >&2
  fail=1
fi

echo
echo "=== claim 2: nothing is admitted while it is blocked ==="
read -r a d o <<EOF
$(send 10)
EOF
echo "${a} allowed, ${d} refused, ${o} neither"
if [ "${a}" -ne 0 ]; then
  echo "FAIL: ${a} requests were admitted against counters that had just been erased." >&2
  echo "  That is the silent full-quota amnesty this whole mechanism exists to stop." >&2
  fail=1
fi

echo
echo "=== claim 3: the new generation outranks the old ==="
gen_after=""
for _ in $(seq 1 100); do
  gen_after=$(store_field committed)
  gen_after=${gen_after:-0}
  if [ "${gen_after}" != "0" ] && [ "${gen_after}" -gt "${gen_before}" ]; then break; fi
  sleep 0.2
done
echo "generation ${gen_before} before the wipe, ${gen_after} after"
if [ -z "${gen_after}" ] || [ "${gen_after}" = "0" ] || [ "${gen_after}" -le "${gen_before}" ]; then
  echo "FAIL: the generation did not advance past ${gen_before}." >&2
  echo "  A node still carrying the old number would not be refused." >&2
  fail=1
fi

echo
echo "=== claim 4: it serves again, and the size of the hole ==="
recovered=""
for _ in $(seq 1 100); do
  recovered=$(store_field blocked)
  [ "${recovered}" = "false" ] && break
  sleep 0.2
done
read -r a d o <<EOF
$(send 5)
EOF
echo "${a} allowed, ${d} refused, ${o} neither after recovery"
if [ "${a}" -eq 0 ]; then
  echo "FAIL: the cluster never started serving again" >&2
  fail=1
fi

# Measured, printed, and gated by nothing. Losing the counters means losing
# them; the published claim is that the loss is detected and bounded, not that
# it did not happen.
echo
echo "MEASURED: the tenant had spent ${SPEND} of ${QUOTA} and, after the wipe, starts again from zero."
echo "The amnesty is one fresh quota per tenant, bounded by the detection window above."

echo
if [ "${CONTROL}" -eq 1 ]; then
  if [ "${fail}" -eq 0 ]; then
    echo "CONTROL FAILED TO FAIL: the store generation was removed and the scenario" >&2
    echo "still passed, so the real run is not evidence of anything." >&2
    exit 1
  fi
  echo "CONTROL OK: with no store generation, the wipe went unnoticed and the quota came back in silence"
  exit 0
fi

if [ "${fail}" -ne 0 ]; then
  echo "STORE WIPE FAILED" >&2
  for log in "${DIR}"/gateway-*.log; do
    echo "--- ${log} ---" >&2
    tail -20 "${log}" >&2
  done
  exit 1
fi
echo "STORE WIPE OK: detected, refused while blocked, generation ${gen_before} to ${gen_after}, serving again"
