#!/usr/bin/env bash
#
# Three nodes, one ring: a request that lands on the wrong node is decided by
# the right one, and the tenant still has exactly one quota.
#
# There are two claims here and they need separate evidence, because in this
# design they are independent:
#
#   1. ONE QUOTA. This would hold even if forwarding did nothing at all --
#      counters live in Redis and every admission is backed by an atomic script
#      there. That is the design's safety property, and it is why a rebalance
#      cannot over-admit.
#   2. THE RING IS DOING SOMETHING. Because (1) does not depend on it, the only
#      way to know forwarding happened is to look: the answer names the owner
#      and says it was forwarded, and the owner's own decision counter moves.
#
# A test that only checked (1) would pass with the ring ripped out.
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

NODES=3
TENANT="cluster-$(date +%s)$$"
POLICY="cluster-policy-$(date +%s)$$"
QUOTA=40
ADMIN_TOKEN="${GATEWAY_ADMIN_TOKEN:-cluster-admin-token}"
DIR=.cluster
fail=0

cleanup() { ./scripts/cluster-down.sh >/dev/null 2>&1; }
trap cleanup EXIT

echo "=== start ${NODES} nodes ==="
GATEWAY_ADMIN_TOKEN="${ADMIN_TOKEN}" ./scripts/cluster-up.sh "${NODES}" || exit 1

first_http=$(head -1 "${DIR}/nodes" | awk '{print $2}')
base="http://127.0.0.1:${first_http}"

# Policy edits are minted through the leader, so they go there. Reading and
# checking still happen on whichever node the test wants; this is only for the
# writes below.
admin=$(./scripts/leader-http.sh) || exit 1
echo "admin writes go to the leader at ${admin}"

echo
echo "=== every node names the same owner ==="
./scripts/show-ownership.sh "${TENANT}" || fail=1

# A policy that does not refill during the run, so an exact count stays exact.
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
  -d "{\"id\":\"${TENANT}\",\"name\":\"Cluster\",\"policy\":\"${POLICY}\"}" \
  "${admin}/admin/v1/tenants")
if [ "${code}" != "200" ]; then
  echo "FAIL: creating the tenant answered ${code}" >&2
  exit 1
fi
sleep 1 # let the policy reach every node's cache

owner=$(curl -fsS --max-time 5 "${base}/v1/cluster?tenant=${TENANT}" | sed 's/.*"owner":"\([^"]*\)".*/\1/')
echo
echo "=== ${TENANT} is owned by ${owner} ==="

# Find a node that is NOT the owner: that is the interesting one to ask.
non_owner_http=""
non_owner_id=""
while read -r id http _; do
  if [ "${id}" != "${owner}" ]; then
    non_owner_http="${http}"
    non_owner_id="${id}"
    break
  fi
done <"${DIR}/nodes"
if [ -z "${non_owner_http}" ]; then
  echo "FAIL: could not find a node that is not the owner" >&2
  exit 1
fi
echo "asking ${non_owner_id}, which does not own it"

echo
echo "=== claim 2: the answer comes from the owner ==="
answer=$(curl -fsS --max-time 5 -H "X-Tenant-ID: ${TENANT}" "http://127.0.0.1:${non_owner_http}/v1/check")
echo "${answer}"
if ! echo "${answer}" | grep -q "\"owner\":\"${owner}\""; then
  echo "FAIL: the answer does not name ${owner} as the owner" >&2
  fail=1
fi
if ! echo "${answer}" | grep -q '"forwarded":true'; then
  echo "FAIL: the request was not forwarded." >&2
  echo "  The quota assertion below would still pass, because correctness comes from" >&2
  echo "  Redis rather than from the ring -- which is exactly why this has to be" >&2
  echo "  checked separately." >&2
  fail=1
fi
if ! echo "${answer}" | grep -q "\"node\":\"${non_owner_id}\""; then
  echo "FAIL: the answering node is not the one that was asked" >&2
  fail=1
fi

echo
echo "=== claim 1: one quota, whichever node is asked ==="
allowed=0
denied=0
other=0
i=0
while [ "${i}" -lt $((QUOTA + 20)) ]; do
  # Round-robin across every node, so most requests land on a non-owner.
  n=$(( (i % NODES) + 1 ))
  http=$(sed -n "${n}p" "${DIR}/nodes" | awk '{print $2}')
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
    -H "X-Tenant-ID: ${TENANT}" "http://127.0.0.1:${http}/v1/check")
  case "${code}" in
    200) allowed=$((allowed + 1)) ;;
    429) denied=$((denied + 1)) ;;
    *)   other=$((other + 1)) ;;
  esac
  i=$((i + 1))
done

# One request was already spent proving claim 2.
expected=$((QUOTA - 1))
echo "across ${NODES} nodes: ${allowed} allowed, ${denied} refused, ${other} other (expected ${expected} allowed)"
if [ "${other}" -ne 0 ]; then
  echo "FAIL: ${other} requests answered with neither 200 nor 429" >&2
  fail=1
fi
if [ "${allowed}" -ne "${expected}" ]; then
  echo "FAIL: ${allowed} admitted against a quota of ${QUOTA} with one already spent." >&2
  if [ "${allowed}" -gt "${expected}" ]; then
    echo "  More than the quota means the nodes are not sharing counters." >&2
  else
    echo "  Fewer means requests were lost or refused wrongly." >&2
  fi
  fail=1
fi

echo
echo "=== forwarding actually happened ==="
# The owner decided for everyone, so its local count should dominate, and the
# non-owners should have forwarded rather than decided.
while read -r id http _; do
  stats=$(curl -fsS --max-time 5 "http://127.0.0.1:${http}/v1/cluster" |
    sed 's/.*"decisions":{\([^}]*\)}.*/\1/')
  echo "${id}: ${stats}"
  if [ "${id}" != "${owner}" ]; then
    fwd=$(echo "${stats}" | sed 's/.*"forwarded":\([0-9]*\).*/\1/')
    if [ -z "${fwd}" ] || [ "${fwd}" -eq 0 ]; then
      echo "FAIL: ${id} forwarded nothing, although it does not own ${TENANT}" >&2
      fail=1
    fi
  fi
done <"${DIR}/nodes"

echo
if [ "${fail}" -ne 0 ]; then
  echo "CLUSTER QUOTA FAILED" >&2
  for log in "${DIR}"/gateway-*.log; do
    echo "--- ${log} ---" >&2
    tail -20 "${log}" >&2
  done
  exit 1
fi
echo "CLUSTER QUOTA OK: one quota across ${NODES} nodes, and the forwarding is real"
