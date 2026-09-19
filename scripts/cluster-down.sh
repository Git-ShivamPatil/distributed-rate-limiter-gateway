#!/usr/bin/env bash
#
# Stops the cluster scripts/cluster-up.sh started and removes its state.
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
DIR=.cluster

if [ ! -f "${DIR}/pids" ]; then
  echo "no cluster is running"
  rm -rf "${DIR}"
  exit 0
fi

while read -r pid; do
  if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null
    wait "${pid}" 2>/dev/null
  fi
done <"${DIR}/pids"

if [ "${KEEP_LOGS:-0}" = "1" ]; then
  rm -f "${DIR}/pids" "${DIR}/nodes"
  echo "stopped; logs kept in ${DIR}"
else
  rm -rf "${DIR}"
  echo "stopped"
fi
