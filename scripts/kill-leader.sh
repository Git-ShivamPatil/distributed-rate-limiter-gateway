#!/usr/bin/env bash
#
# Three nodes under continuous traffic; kill the Raft leader mid-flight.
#
# The claim is that consensus governs membership and nothing else, so losing
# the leader is an election and not an outage. Two things have to be shown
# separately, because neither implies the other:
#
#   1. A NEW LEADER IS ELECTED, and it is not the node that was killed. How
#      long that took is REPORTED and asserted on by nothing: election timing
#      is a property of raft's timers and of the machine, and a test that gates
#      on it fails on a busy runner for reasons that have nothing to do with
#      the gateway.
#   2. ENFORCEMENT IS UNAFFECTED. Every request to a surviving node is answered
#      with a decision -- never an error -- across the transition, and the
#      total admitted equals the quota EXACTLY. Not "about the quota": the
#      counters are in Redis and the script that debits them never learns there
#      was an election, so the number has to come out on the nose.
#
# THE CONTROL. Claim 2 would also hold if this cluster had no consensus at all,
# which means the clean run alone proves nothing about the design decision. So
# the same scenario runs again against a deliberately broken build in which
# consensus IS on the request path -- every decision first commits an entry --
# and that run is REQUIRED to fail. It is the difference between "consensus is
# off the request path" and "consensus happened to be fast today".
#
#   scripts/kill-leader.sh              the real thing; must pass
#   scripts/kill-leader.sh --control    the broken build; must fail
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

CONTROL=0
if [ "${1:-}" = "--control" ]; then
  CONTROL=1
fi

NODES=3
QUOTA=60
REQUESTS=100
KILL_AT=30
TENANT="killtest-$(date +%s)$$"
POLICY="killtest-policy-$(date +%s)$$"
ADMIN_TOKEN="${GATEWAY_ADMIN_TOKEN:-cluster-admin-token}"
DIR=.cluster
fail=0

cleanup() { ./scripts/cluster-down.sh >/dev/null 2>&1; }
trap cleanup EXIT

if [ "${CONTROL}" -eq 1 ]; then
  echo "=== CONTROL: consensus on the request path (this run MUST fail) ==="
  export BUILD_TAGS=faultinject
  export GATEWAY_FAULT=raft_counters
fi

echo "=== start ${NODES} nodes ==="
GATEWAY_ADMIN_TOKEN="${ADMIN_TOKEN}" ./scripts/cluster-up.sh "${NODES}" || exit 1

first_http=$(head -1 "${DIR}/nodes" | awk '{print $2}')
base="http://127.0.0.1:${first_http}"

# The policy below is minted through the leader, so it is written there. This
# happens BEFORE the kill; afterwards there is a different leader, which is the
# whole point of the test.
admin=$(./scripts/leader-http.sh) || exit 1

# A quota that cannot refill during the run. At one token per 24h/60 = 24
# minutes, nothing this loop does can be mistaken for a refill -- which is the
# only way an exact-count assertion stays exact.
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
  -d "{\"id\":\"${TENANT}\",\"name\":\"Kill test\",\"policy\":\"${POLICY}\"}" \
  "${admin}/admin/v1/tenants")
if [ "${code}" != "200" ]; then
  echo "FAIL: creating the tenant answered ${code}" >&2
  exit 1
fi
sleep 1 # let the policy reach every node's cache

leader=$(curl -fsS --max-time 5 "${base}/v1/cluster" | sed -n 's/.*"leader_id":"\([^"]*\)".*/\1/p')
if [ -z "${leader}" ]; then
  echo "FAIL: no leader to kill" >&2
  exit 1
fi
leader_n=${leader#gateway-}
leader_pid=$(sed -n "${leader_n}p" "${DIR}/pids")
echo
echo "=== the leader is ${leader} (pid ${leader_pid}) ==="

# Everything below would look identical if consensus were decorating a
# configuration file rather than governing it: every node would still be
# routing on the ring its own file gave it, and the quota would still come out
# exactly. So check that the membership actually committed.
committed=$(curl -fsS --max-time 5 -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  "${admin}/admin/v1/cluster/members" | grep -o '"id":"gateway-[0-9]*"' | wc -l)
echo "the log holds ${committed} members"
if [ "${committed}" -ne "${NODES}" ]; then
  echo "FAIL: the log holds ${committed} members, not ${NODES}." >&2
  echo "  Nothing below would notice: the nodes would be routing on their" >&2
  echo "  configuration files and consensus would be decorating them." >&2
  exit 1
fi

# Traffic goes to the nodes that will survive. A dead process serves nothing,
# and that is not what is being tested: the question is whether the REST of the
# cluster keeps deciding while the leader is gone.
survivors=""
while read -r id http _; do
  if [ "${id}" != "${leader}" ]; then
    survivors="${survivors} ${http}"
  fi
done <"${DIR}/nodes"
# shellcheck disable=SC2086
set -- ${survivors}
s1=$1
s2=$2
echo "traffic goes to the survivors on ${s1} and ${s2}"

echo
echo "=== ${REQUESTS} requests, killing the leader at request ${KILL_AT} ==="
allowed=0
denied=0
other=0
allowed_after=0
answered_after=0
started=$(date +%s)
i=1
while [ "${i}" -le "${REQUESTS}" ]; do
  if [ "${i}" -eq "${KILL_AT}" ]; then
    # A deterministic trigger, not a timer: the kill lands between request
    # 29 and request 30 on every machine, however fast or slow it is.
    kill -9 "${leader_pid}" 2>/dev/null
    echo "--- killed ${leader} after ${allowed} admissions ---"
  fi

  if [ $((i % 2)) -eq 0 ]; then http=${s1}; else http=${s2}; fi
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
    -H "X-Tenant-ID: ${TENANT}" "http://127.0.0.1:${http}/v1/check")
  case "${code}" in
    200) allowed=$((allowed + 1)); [ "${i}" -gt "${KILL_AT}" ] && allowed_after=$((allowed_after + 1)) ;;
    429) denied=$((denied + 1)) ;;
    *)   other=$((other + 1)) ;;
  esac
  case "${code}" in
    200|429) [ "${i}" -gt "${KILL_AT}" ] && answered_after=$((answered_after + 1)) ;;
  esac
  i=$((i + 1))
done
elapsed=$(( $(date +%s) - started ))

echo "${allowed} allowed, ${denied} refused, ${other} neither, in ${elapsed}s"
echo "after the kill: ${answered_after} answered, of which ${allowed_after} admitted"

# An exact-count assertion is only exact if nothing refilled underneath it.
if [ "${elapsed}" -ge 1440 ]; then
  echo "INCONCLUSIVE: the run took ${elapsed}s, long enough for the bucket to refill a token" >&2
  exit 2
fi

echo
echo "=== claim 1: a new leader, and not the dead one ==="
new_leader=""
waited=0
for _ in $(seq 1 150); do
  new_leader=$(curl -fsS --max-time 1 "http://127.0.0.1:${s1}/v1/cluster" 2>/dev/null |
    sed -n 's/.*"leader_id":"\([^"]*\)".*/\1/p')
  if [ -n "${new_leader}" ] && [ "${new_leader}" != "${leader}" ]; then break; fi
  waited=$((waited + 1))
  sleep 0.2
done
if [ -z "${new_leader}" ] || [ "${new_leader}" = "${leader}" ]; then
  echo "FAIL: no new leader within 30s (still reporting '${new_leader:-none}')" >&2
  fail=1
else
  # Reported, not asserted. Nothing here is a threshold. A wait of zero is the
  # usual answer and the interesting one: the election finished while the
  # traffic above was still running, which is the whole point.
  if [ "${waited}" -eq 0 ]; then
    echo "elected ${new_leader}, already in place by the time the traffic finished"
  else
    echo "elected ${new_leader}, about $((waited * 200))ms after the traffic finished"
  fi
fi

echo
echo "=== claim 2: enforcement was unaffected ==="
if [ "${other}" -ne 0 ]; then
  echo "FAIL: ${other} requests answered with neither a decision nor a refusal." >&2
  echo "  An election is not supposed to be visible on the request path at all." >&2
  fail=1
fi
if [ "${answered_after}" -eq 0 ]; then
  echo "FAIL: nothing was answered after the kill, so nothing was tested" >&2
  fail=1
fi
if [ "${allowed}" -ne "${QUOTA}" ]; then
  echo "FAIL: ${allowed} admitted against a quota of ${QUOTA}." >&2
  if [ "${allowed}" -gt "${QUOTA}" ]; then
    echo "  More than the quota across a leader change is over-admission -- the thing" >&2
    echo "  this whole design exists to make impossible." >&2
  else
    echo "  Fewer means requests were refused that should have been admitted." >&2
  fi
  fail=1
fi

echo
echo "=== the ring did not move ==="
# Nobody committed a membership change, so no tenant may have moved. A ring
# that shifted here would mean an election had rearranged the keyspace.
owner1=$(curl -fsS --max-time 5 "http://127.0.0.1:${s1}/v1/cluster?tenant=${TENANT}" |
  sed -n 's/.*"owner":"\([^"]*\)".*/\1/p')
owner2=$(curl -fsS --max-time 5 "http://127.0.0.1:${s2}/v1/cluster?tenant=${TENANT}" |
  sed -n 's/.*"owner":"\([^"]*\)".*/\1/p')
echo "${TENANT} is owned by ${owner1} and ${owner2}"
if [ -z "${owner1}" ] || [ "${owner1}" != "${owner2}" ]; then
  echo "FAIL: the survivors disagree about who owns ${TENANT}" >&2
  fail=1
fi

echo
if [ "${CONTROL}" -eq 1 ]; then
  if [ "${fail}" -eq 0 ]; then
    echo "CONTROL FAILED TO FAIL: consensus was put on the request path and the" >&2
    echo "scenario still passed, so the clean run is not evidence of anything." >&2
    exit 1
  fi
  echo "CONTROL OK: with consensus on the request path, killing the leader broke traffic"
  exit 0
fi

if [ "${fail}" -ne 0 ]; then
  echo "KILL LEADER FAILED" >&2
  for log in "${DIR}"/gateway-*.log; do
    echo "--- ${log} ---" >&2
    tail -20 "${log}" >&2
  done
  exit 1
fi
echo "KILL LEADER OK: ${leader} died mid-flight, ${new_leader} took over, ${allowed}/${QUOTA} admitted exactly"
