#!/usr/bin/env bash
#
# Prints the HTTP base URL of the node that is currently the Raft leader.
#
#   base=$(scripts/leader-http.sh)
#   curl -X PUT "${base}/admin/v1/policies/free" ...
#
# Policy and membership edits are minted through the leader, so a script that
# writes to whichever node happens to be first in the list works until the day
# an election has moved leadership -- and then fails in a way that looks like
# the edit was rejected rather than misdirected. This removes the luck.
#
# With no cluster running, or no leader yet, it prints nothing and exits 1.
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
DIR=.cluster

if [ ! -f "${DIR}/nodes" ]; then
  echo "leader-http: no cluster is running (${DIR}/nodes is missing)" >&2
  exit 1
fi

# Ask any node: every node knows who the leader is, and a node that does not
# is not one whose answer we would want anyway.
first_http=$(head -1 "${DIR}/nodes" | awk '{print $2}')

leader=""
for _ in $(seq 1 50); do
  leader=$(curl -fsS --max-time 1 "http://127.0.0.1:${first_http}/v1/cluster" 2>/dev/null |
    sed -n 's/.*"leader_id":"\([^"]*\)".*/\1/p')
  [ -n "${leader}" ] && break
  sleep 0.2
done

if [ -z "${leader}" ]; then
  echo "leader-http: no leader after 10s" >&2
  exit 1
fi

while read -r id http _; do
  if [ "${id}" = "${leader}" ]; then
    echo "http://127.0.0.1:${http}"
    exit 0
  fi
done <"${DIR}/nodes"

echo "leader-http: the leader is ${leader}, which is not in ${DIR}/nodes" >&2
exit 1
