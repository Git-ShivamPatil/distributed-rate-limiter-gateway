#!/usr/bin/env bash
#
# Fires N requests at the decision endpoint as one tenant and asserts the split
# of 200s and 429s.
#
# This is the milestone-1 verification step, mechanised: a bucket of 20 tokens
# must admit exactly 20 of 30 requests and refuse exactly 10.
#
#   scripts/burst-check.sh --url http://localhost:8080 --tenant acme \
#       --requests 30 --expect-allowed 20 --expect-denied 10 --max-seconds 2
#
# --max-seconds exists because an exact count is only meaningful while the
# quota has not refilled underneath it. A loop that takes longer than the
# refill interval has measured something else, and this fails saying so rather
# than reporting a wrong number or passing by luck.
#
set -uo pipefail

URL="http://localhost:8080"
TENANT="acme"
REQUESTS=30
EXPECT_ALLOWED=""
EXPECT_DENIED=""
MIN_ALLOWED=""
MIN_DENIED=""
MAX_SECONDS=""
COST=""

usage() { sed -n '2,18p' "$0"; }

while [ $# -gt 0 ]; do
  # Accept --flag=value as well as --flag value: everything declarative writes
  # the first form, and a script that only takes the second breaks in compose.
  case "$1" in
    --*=*)
      flag="${1%%=*}"; value="${1#*=}"; shift
      set -- "${flag}" "${value}" "$@"
      continue
      ;;
  esac
  case "$1" in
    --url)            URL="$2"; shift 2 ;;
    --tenant)         TENANT="$2"; shift 2 ;;
    --requests)       REQUESTS="$2"; shift 2 ;;
    --expect-allowed) EXPECT_ALLOWED="$2"; shift 2 ;;
    --expect-denied)  EXPECT_DENIED="$2"; shift 2 ;;
    --min-allowed)    MIN_ALLOWED="$2"; shift 2 ;;
    --min-denied)     MIN_DENIED="$2"; shift 2 ;;
    --max-seconds)    MAX_SECONDS="$2"; shift 2 ;;
    --cost)           COST="$2"; shift 2 ;;
    -h|--help)        usage; exit 0 ;;
    *) echo "burst-check: unknown argument $1" >&2; usage >&2; exit 2 ;;
  esac
done

headers=(-H "X-Tenant-ID: ${TENANT}")
if [ -n "${COST}" ]; then headers+=(-H "X-RateLimit-Cost: ${COST}"); fi

allowed=0
denied=0
other=0
other_codes=""

started=${SECONDS}
for _ in $(seq 1 "${REQUESTS}"); do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${headers[@]}" "${URL}/v1/check")
  case "${code}" in
    200) allowed=$((allowed + 1)) ;;
    429) denied=$((denied + 1)) ;;
    *)   other=$((other + 1)); other_codes="${other_codes} ${code}" ;;
  esac
done
elapsed=$((SECONDS - started))

echo "tenant=${TENANT} requests=${REQUESTS} allowed=${allowed} denied=${denied} other=${other} elapsed=${elapsed}s"

fail=0

if [ -n "${MAX_SECONDS}" ] && [ "${elapsed}" -gt "${MAX_SECONDS}" ]; then
  echo "INCONCLUSIVE: the loop took ${elapsed}s, longer than the ${MAX_SECONDS}s this assertion is valid for." >&2
  echo "  Quota refilled while it ran, so an exact count no longer means anything." >&2
  fail=1
fi

if [ -n "${EXPECT_ALLOWED}" ] && [ "${allowed}" -ne "${EXPECT_ALLOWED}" ]; then
  echo "FAIL: ${allowed} requests were allowed, expected exactly ${EXPECT_ALLOWED}" >&2
  fail=1
fi
if [ -n "${EXPECT_DENIED}" ] && [ "${denied}" -ne "${EXPECT_DENIED}" ]; then
  echo "FAIL: ${denied} requests were refused, expected exactly ${EXPECT_DENIED}" >&2
  fail=1
fi
if [ -n "${MIN_ALLOWED}" ] && [ "${allowed}" -lt "${MIN_ALLOWED}" ]; then
  echo "FAIL: ${allowed} requests were allowed, expected at least ${MIN_ALLOWED}" >&2
  fail=1
fi
if [ -n "${MIN_DENIED}" ] && [ "${denied}" -lt "${MIN_DENIED}" ]; then
  echo "FAIL: ${denied} requests were refused, expected at least ${MIN_DENIED}" >&2
  fail=1
fi
if [ "${other}" -ne 0 ]; then
  echo "FAIL: ${other} requests answered with neither 200 nor 429:${other_codes}" >&2
  fail=1
fi

if [ "${fail}" -ne 0 ]; then
  echo "burst-check FAILED" >&2
  exit 1
fi
echo "burst-check OK"
