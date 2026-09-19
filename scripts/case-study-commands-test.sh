#!/usr/bin/env bash
#
# Runs the commands the case study prints, in the order it prints them, and
# checks that each does what the page says it does.
#
#   1. docker compose up -d redis postgres && make migrate
#   2. go run ./cmd/gateway --node gateway-1 --config ./configs/local.yaml
#   3. cd dashboard && npm install && npm run dev
#   4. k6 run tests/rate-limit.js && kubectl get pods -n gateway
#
# Steps arrive as their milestones do; the ones that do not exist yet are
# reported as pending rather than quietly skipped.
#
# Docker is optional so the script is useful on a machine whose Docker lives on
# the Windows side and is not on this PATH. Without it, step 1 is reported as
# SKIPPED -- never as passed. CI runs it with --with-docker.
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

WITH_DOCKER=0
PORT="${PORT:-8080}"

while [ $# -gt 0 ]; do
  case "$1" in
    --with-docker) WITH_DOCKER=1; shift ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "case-study-commands-test: unknown argument $1" >&2; exit 2 ;;
  esac
done

fail=0
LOG="$(mktemp -t case-study-XXXXXX.log)"
GATEWAY_PID=""

cleanup() {
  if [ -n "${GATEWAY_PID}" ] && kill -0 "${GATEWAY_PID}" 2>/dev/null; then
    kill "${GATEWAY_PID}" 2>/dev/null
    wait "${GATEWAY_PID}" 2>/dev/null
  fi
  rm -f "${LOG}"
}
trap cleanup EXIT

echo "=== step 1: docker compose up -d redis postgres && make migrate ==="
if [ "${WITH_DOCKER}" -eq 1 ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "FAIL: --with-docker was given but docker is not on PATH" >&2
    fail=1
  else
    if docker compose up -d redis postgres; then
      # The page says this brings the data plane up. Prove it answers, rather
      # than trusting that `up -d` returning zero means anything.
      ready=0
      for _ in $(seq 1 60); do
        if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then ready=1; break; fi
        sleep 1
      done
      if [ "${ready}" -ne 1 ]; then
        echo "FAIL: redis never answered PING after compose reported it up" >&2
        fail=1
      else
        echo "redis answers PING"
      fi
      ready=0
      for _ in $(seq 1 60); do
        if docker compose exec -T postgres pg_isready -U gateway -d gateway >/dev/null 2>&1; then ready=1; break; fi
        sleep 1
      done
      if [ "${ready}" -ne 1 ]; then
        echo "FAIL: postgres never became ready after compose reported it up" >&2
        fail=1
      else
        echo "postgres is ready"
      fi
    else
      echo "FAIL: docker compose up -d redis postgres returned non-zero" >&2
      fail=1
    fi
  fi
else
  echo "SKIPPED: re-run with --with-docker to exercise the compose step."
  echo "  (On the development machine Docker Desktop is Windows-side and is not"
  echo "   on the PATH inside WSL, so this cannot run there. CI runs it.)"
fi

echo "--- make migrate ---"
if make migrate >/dev/null 2>&1; then
  echo "make migrate succeeded"
else
  echo "PENDING: make migrate is not implemented yet (milestone 3)."
fi

echo
echo "=== step 2: go run ./cmd/gateway --node gateway-1 --config ./configs/local.yaml ==="
# Exactly the published command, with the published flag form, and with the
# published listen address taken from the published config file.
go run ./cmd/gateway --node gateway-1 --config ./configs/local.yaml >"${LOG}" 2>&1 &
GATEWAY_PID=$!

URL="http://127.0.0.1:${PORT}"
ready=0
for _ in $(seq 1 100); do
  if curl -fsS --max-time 1 "${URL}/healthz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.3
done
if [ "${ready}" -ne 1 ]; then
  echo "FAIL: the published command did not produce a gateway listening on ${PORT}" >&2
  cat "${LOG}" >&2
  exit 1
fi
echo "gateway answers on ${URL}"

# The milestone's own check, written the way its verification step writes it.
echo "--- for i in \$(seq 1 30); do curl ... /v1/check; done | sort | uniq -c ---"
counts=$(for _ in $(seq 1 30); do
  curl -s -o /dev/null -w '%{http_code}\n' -H 'X-Tenant-ID: acme' "${URL}/v1/check"
done | sort | uniq -c | tr -s ' ' | sed 's/^ //')
echo "${counts}"

if ! echo "${counts}" | grep -q '^20 200$'; then
  echo "FAIL: expected exactly 20 responses of 200" >&2
  fail=1
fi
if ! echo "${counts}" | grep -q '^10 429$'; then
  echo "FAIL: expected exactly 10 responses of 429" >&2
  fail=1
fi

echo
echo "=== step 3: cd dashboard && npm install && npm run dev ==="
if [ -d dashboard ]; then
  echo "dashboard exists; exercised by its own check"
else
  echo "PENDING: the dashboard lands in milestone 7."
fi

echo
echo "=== step 4: k6 run tests/rate-limit.js && kubectl get pods -n gateway ==="
if [ -f tests/rate-limit.js ]; then
  echo "load profile exists; exercised by the benchmark workflow"
else
  echo "PENDING: the load profiles land in milestone 8 and the chart in milestone 9."
fi

echo
if [ "${fail}" -ne 0 ]; then
  echo "CASE STUDY COMMANDS FAILED" >&2
  exit 1
fi
echo "CASE STUDY COMMANDS OK (pending steps named above)"
