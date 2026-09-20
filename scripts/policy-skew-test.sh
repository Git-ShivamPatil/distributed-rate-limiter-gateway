#!/usr/bin/env bash
#
# Tighten a limit while one node is still holding the old one.
#
# This is the one place consensus is load-bearing for a CORRECTNESS property
# rather than for coordination. Postgres LISTEN/NOTIFY tells every node that a
# policy changed, but it gives no ordering across them: a node whose listener
# is down, or whose cache has not lapsed, carries the old limit and has no way
# to know it. It supplies that limit to a script which trusts its caller, and
# for the whole skew window the cluster enforces max(old, new) -- which for a
# TIGHTENING is an over-admission nobody sees.
#
# A totally ordered generation, minted in the log and compared inside the
# store, makes the widening un-appliable: the stale node's check is refused by
# Redis itself, and the node refreshes rather than enforcing what it holds.
#
# THE CONTROL is the whole point. With no generation minted, the same stale
# node MUST keep enforcing the old, looser limit -- and it does, by a wide
# margin, because the counter key carries a fingerprint of the limit's
# parameters, so the tightened limit starts a fresh counter while the stale
# node keeps drawing on the old one.
#
#   scripts/policy-skew-test.sh              the real thing; must pass
#   scripts/policy-skew-test.sh --control    no generation minted; must fail
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

CONTROL=0
if [ "${1:-}" = "--control" ]; then
  CONTROL=1
fi

NODES=3
WIDE=1000
TIGHT=100
PROBES=300
# Node 2 is the one pinned behind: no change notifications and an hour-long
# cache, so it holds whatever it last read until something refuses it.
STALE_N=2
STAMP="$(date +%s)$$"
POLICY="skew-policy-$(date +%s)$$"
ADMIN_TOKEN="${GATEWAY_ADMIN_TOKEN:-cluster-admin-token}"
DIR=.cluster
fail=0

cleanup() { ./scripts/cluster-down.sh >/dev/null 2>&1; }
trap cleanup EXIT

if [ "${CONTROL}" -eq 1 ]; then
  echo "=== CONTROL: policy edits mint no generation (this run MUST fail) ==="
  export BUILD_TAGS=faultinject
  export GATEWAY_FAULT=no_policy_gen
fi

echo "=== start ${NODES} nodes, with gateway-${STALE_N} pinned behind ==="
STALE_NODE="${STALE_N}" GATEWAY_ADMIN_TOKEN="${ADMIN_TOKEN}" ./scripts/cluster-up.sh "${NODES}" || exit 1

admin=$(./scripts/leader-http.sh) || exit 1
stale_http=$(sed -n "${STALE_N}p" "${DIR}/nodes" | awk '{print $2}')
stale="http://127.0.0.1:${stale_http}"

# A fresh node to make the edit visible: any node that is NOT the pinned one.
fresh_http=""
while read -r id http _; do
  if [ "${id}" != "gateway-${STALE_N}" ]; then
    fresh_http="${http}"
    break
  fi
done <"${DIR}/nodes"
fresh="http://127.0.0.1:${fresh_http}"
echo "stale node: ${stale}    fresh node: ${fresh}"

# The pinned node has to OWN the tenant. A node that does not own one forwards
# its checks to the node that does -- which is not stale -- so the stale copy of
# the policy would never be consulted and the scenario would pass while testing
# nothing. That is not a flaw in the ring; it is the ring working, and it is
# why the tenant is chosen rather than named.
TENANT=""
n=1
while [ "${n}" -le 200 ]; do
  candidate="skew-${STAMP}-${n}"
  owner=$(curl -fsS --max-time 5 "${stale}/v1/cluster?tenant=${candidate}" |
    sed -n 's/.*"owner":"\([^"]*\)".*/\1/p')
  if [ "${owner}" = "gateway-${STALE_N}" ]; then
    TENANT="${candidate}"
    break
  fi
  n=$((n + 1))
done
if [ -z "${TENANT}" ]; then
  echo "FAIL: no tenant out of 200 is owned by gateway-${STALE_N}" >&2
  exit 1
fi
echo "using ${TENANT}, which gateway-${STALE_N} owns, so its own policy decides"

writePolicy() { # writePolicy <count>
  b=$(printf '{"failure_mode":"closed","limits":[{"name":"per-day","algorithm":"token_bucket","count":%s,"period_ms":86400000,"burst":%s}]}' "$1" "$1")
  curl -s -o /dev/null -w '%{http_code}' -X PUT \
    -H "X-Admin-Token: ${ADMIN_TOKEN}" -H 'Content-Type: application/json' \
    -d "${b}" "${admin}/admin/v1/policies/${POLICY}"
}

send() { # send <base> <n>  -> "allowed denied other"
  a=0; d=0; o=0
  n=1
  while [ "${n}" -le "$2" ]; do
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
      -H "X-Tenant-ID: ${TENANT}" "$1/v1/check")
    case "${c}" in
      200) a=$((a + 1)) ;;
      429) d=$((d + 1)) ;;
      *)   o=$((o + 1)) ;;
    esac
    n=$((n + 1))
  done
  echo "${a} ${d} ${o}"
}

code=$(writePolicy "${WIDE}")
if [ "${code}" != "200" ]; then
  echo "FAIL: creating the wide policy answered ${code}" >&2
  exit 1
fi
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" -H 'Content-Type: application/json' \
  -d "{\"id\":\"${TENANT}\",\"name\":\"Skew test\",\"policy\":\"${POLICY}\"}" \
  "${admin}/admin/v1/tenants")
if [ "${code}" != "200" ]; then
  echo "FAIL: creating the tenant answered ${code}" >&2
  exit 1
fi
sleep 1

echo
echo "=== the stale node reads the wide limit of ${WIDE} ==="
read -r a d o <<EOF
$(send "${stale}" 1)
EOF
if [ "${a}" -ne 1 ]; then
  echo "FAIL: the stale node could not serve the tenant at all (${a} allowed, ${d} refused, ${o} other)" >&2
  exit 1
fi
echo "cached"

echo
echo "=== tighten to ${TIGHT}, everywhere except that node ==="
code=$(writePolicy "${TIGHT}")
if [ "${code}" != "200" ]; then
  echo "FAIL: tightening the policy answered ${code}" >&2
  exit 1
fi
sleep 1

# A fresh node uses the new limit, which is what advances the generation the
# store has seen. Without this the stale node would be the only one that ever
# spoke and would have nothing to be behind.
read -r a d o <<EOF
$(send "${fresh}" 1)
EOF
echo "the fresh node admitted ${a} under the new limit"

echo
echo "=== ${PROBES} requests at the node that still holds ${WIDE} ==="
read -r a d o <<EOF
$(send "${stale}" ${PROBES})
EOF
echo "${a} allowed, ${d} refused, ${o} neither"

stale_id="gateway-${STALE_N}"
policy_stale=$(curl -fsS --max-time 5 "${stale}/v1/cluster" 2>/dev/null |
  sed -n 's/.*"policy_stale":\([0-9]*\).*/\1/p')
policy_stale=${policy_stale:-0}
echo "${stale_id} was refused by the store's policy fence ${policy_stale} times"

# The ceiling. The tightened limit starts a fresh counter -- the counter key
# carries a fingerprint of the limit's parameters -- so the honest expectation
# is the new limit plus the one request the fresh node already spent, with a
# little room for the request that triggered the fence.
ceiling=$((TIGHT + 10))
if [ "${a}" -gt "${ceiling}" ]; then
  echo "FAIL: ${a} admitted at the stale node against a tightened limit of ${TIGHT}." >&2
  echo "  It is still enforcing the ${WIDE} it was holding, which is the whole" >&2
  echo "  point of the generation." >&2
  fail=1
fi
if [ "${CONTROL}" -eq 0 ] && [ "${policy_stale}" -eq 0 ]; then
  echo "FAIL: nothing was ever refused by the policy fence." >&2
  echo "  The count above may be low for some other reason; without a fence" >&2
  echo "  having fired, this run shows nothing." >&2
  fail=1
fi

echo
if [ "${CONTROL}" -eq 1 ]; then
  if [ "${fail}" -eq 0 ]; then
    echo "CONTROL FAILED TO FAIL: no generation was minted and the stale node still" >&2
    echo "stopped enforcing the old limit, so the real run proves nothing." >&2
    exit 1
  fi
  echo "CONTROL OK: with no generation, the stale node kept enforcing ${WIDE} and admitted ${a}"
  exit 0
fi

if [ "${fail}" -ne 0 ]; then
  echo "POLICY SKEW FAILED" >&2
  for log in "${DIR}"/gateway-*.log; do
    echo "--- ${log} ---" >&2
    tail -20 "${log}" >&2
  done
  exit 1
fi
echo "POLICY SKEW OK: the stale node was fenced ${policy_stale} times and admitted ${a}, not ${WIDE}"
