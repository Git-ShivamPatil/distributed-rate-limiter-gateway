#!/usr/bin/env bash
#
# Starts a local cluster of N gateway nodes sharing one Redis and one Postgres.
#
#   scripts/cluster-up.sh 3
#   scripts/show-ownership.sh acme
#   scripts/cluster-down.sh
#
# Every node gets the same member list, so they compute the same ring: the
# ring is derived from node ids alone, and any node that disagreed would be a
# bug rather than a configuration difference.
#
# State goes in .cluster/ -- configs, logs, pids and a table of the nodes --
# which is gitignored and removed by cluster-down.sh.
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

NODES="${1:-3}"
BASE_HTTP="${BASE_HTTP:-18100}"
BASE_GRPC="${BASE_GRPC:-19100}"
REDIS="${REDIS_ADDR:-127.0.0.1:6379}"
DSN="${POSTGRES_DSN:-postgres://gateway:gateway@127.0.0.1:5432/gateway?sslmode=disable}"
ADMIN_TOKEN="${GATEWAY_ADMIN_TOKEN:-cluster-admin-token}"
DIR=.cluster

case "${NODES}" in
  ''|*[!0-9]*) echo "cluster-up: node count must be a number, got ${NODES}" >&2; exit 2 ;;
esac
if [ "${NODES}" -lt 1 ] || [ "${NODES}" -gt 9 ]; then
  echo "cluster-up: between 1 and 9 nodes, got ${NODES}" >&2
  exit 2
fi

if [ -f "${DIR}/pids" ]; then
  echo "cluster-up: a cluster is already running (see ${DIR}/pids); run scripts/cluster-down.sh first" >&2
  exit 2
fi

mkdir -p "${DIR}"
BIN="${DIR}/gateway"

echo "=== build ==="
go build -o "${BIN}" ./cmd/gateway || exit 1
go build -o "${DIR}/gatewayctl" ./cmd/gatewayctl || exit 1

echo "=== migrate ==="
"${DIR}/gatewayctl" migrate up --dsn "${DSN}" || exit 1

# The member list every node shares.
members=""
for i in $(seq 1 "${NODES}"); do
  members="${members}    - id: gateway-${i}
      addr: \"127.0.0.1:$((BASE_GRPC + i))\"
"
done

: >"${DIR}/nodes"
: >"${DIR}/pids"

for i in $(seq 1 "${NODES}"); do
  http=$((BASE_HTTP + i))
  grpc=$((BASE_GRPC + i))
  cfg="${DIR}/gateway-${i}.yaml"

  cat >"${cfg}" <<YAML
node:
  id: gateway-${i}
  http_addr: "127.0.0.1:${http}"
  grpc_addr: "127.0.0.1:${grpc}"
limiter:
  backend: redis
redis:
  addr: "${REDIS}"
  # A development box runs every node, Redis, Postgres and the load on two
  # cores; the production default of 250ms measures that rather than the code.
  timeout: 3s
postgres:
  dsn: "${DSN}"
policy:
  store: postgres
  cache_ttl: 5s
  stale_for: 5m
  listen_for_changes: true
auth:
  api_keys: true
  admin_token_env: GATEWAY_ADMIN_TOKEN
check_api:
  trust_tenant_header: true
cluster:
  vnodes: 256
  forward_timeout: 3s
  members:
${members}
YAML

  GATEWAY_ADMIN_TOKEN="${ADMIN_TOKEN}" "${BIN}" --config="${cfg}" >"${DIR}/gateway-${i}.log" 2>&1 &
  pid=$!
  echo "${pid}" >>"${DIR}/pids"
  echo "gateway-${i} ${http} ${grpc}" >>"${DIR}/nodes"
done

# Wait for all of them, and fail loudly rather than leaving half a cluster up.
failed=0
while read -r id http _; do
  up=0
  for _ in $(seq 1 100); do
    if curl -fsS --max-time 1 "http://127.0.0.1:${http}/healthz" >/dev/null 2>&1; then up=1; break; fi
    sleep 0.2
  done
  if [ "${up}" -ne 1 ]; then
    echo "FAIL: ${id} never became healthy" >&2
    tail -20 "${DIR}/${id}.log" >&2
    failed=1
  fi
done <"${DIR}/nodes"

if [ "${failed}" -ne 0 ]; then
  ./scripts/cluster-down.sh >/dev/null 2>&1
  exit 1
fi

echo
printf '%-12s %-22s %s\n' NODE HTTP GRPC
while read -r id http grpc; do
  printf '%-12s %-22s %s\n' "${id}" "http://127.0.0.1:${http}" "127.0.0.1:${grpc}"
done <"${DIR}/nodes"
echo
echo "${NODES} nodes up. Try:"
echo "  scripts/show-ownership.sh acme"
echo "  scripts/cluster-down.sh"
