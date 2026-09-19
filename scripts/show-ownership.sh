#!/usr/bin/env bash
#
# Asks every node in the local cluster who owns a tenant.
#
#   scripts/show-ownership.sh acme
#
# They have to agree. The ring is derived from the member ids alone, so two
# nodes naming different owners is a bug, not a configuration difference -- and
# this is the cheapest way to see it. Exits non-zero when they disagree, so it
# works as an assertion as well as a thing to look at.
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

TENANT="${1:-acme}"
DIR=.cluster

if [ ! -f "${DIR}/nodes" ]; then
  echo "show-ownership: no cluster is running; start one with scripts/cluster-up.sh 3" >&2
  exit 2
fi

printf '%-12s %-12s %-24s %s\n' ASKED OWNER 'OWNER ADDR' SELF
first=""
disagree=0

while read -r id http _; do
  body=$(curl -fsS --max-time 5 "http://127.0.0.1:${http}/v1/cluster?tenant=${TENANT}" 2>/dev/null)
  if [ -z "${body}" ]; then
    printf '%-12s %s\n' "${id}" "(no answer)"
    disagree=1
    continue
  fi

  owner=$(echo "${body}" | sed 's/.*"owner":"\([^"]*\)".*/\1/')
  addr=$(echo "${body}" | sed 's/.*"owner_addr":"\([^"]*\)".*/\1/')
  self=$(echo "${body}" | grep -o '"owner_is_self":[a-z]*' | cut -d: -f2)

  printf '%-12s %-12s %-24s %s\n' "${id}" "${owner}" "${addr}" "${self}"

  if [ -z "${first}" ]; then
    first="${owner}"
  elif [ "${owner}" != "${first}" ]; then
    disagree=1
  fi
done <"${DIR}/nodes"

echo
if [ "${disagree}" -ne 0 ]; then
  echo "DISAGREEMENT: the nodes do not name the same owner for ${TENANT}." >&2
  echo "  The ring is computed from the member ids alone, so this is a bug rather" >&2
  echo "  than a difference of configuration." >&2
  exit 1
fi
echo "all nodes agree: ${TENANT} is owned by ${first}"
