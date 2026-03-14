#!/usr/bin/env bash
set -euo pipefail

# ── Load Test for stophammer ──────────────────────────────────────────────────
#
# Sends concurrent ingest and search requests using `hey` (HTTP load generator)
# and reports latency percentiles and throughput.
#
# Prerequisites:
#   - `hey` installed: go install github.com/rakyll/hey@latest
#     or: brew install hey
#   - stophammer running at PRIMARY_URL (default: http://localhost:8008)
#   - A feed already ingested (run e2e_docker_compose_tests.sh first)
#
# Usage:
#   ./tests/load_test.sh
#   PRIMARY_URL=http://localhost:8008 ./tests/load_test.sh

PRIMARY_URL="${PRIMARY_URL:-http://localhost:8008}"
CONCURRENCY="${CONCURRENCY:-20}"
TOTAL_REQUESTS="${TOTAL_REQUESTS:-100}"
P99_THRESHOLD_MS="${P99_THRESHOLD_MS:-2000}"

PASSED=0
FAILED=0
TESTS=()

log()  { printf '\033[1;34m[load]\033[0m %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); TESTS+=("PASS: $1"); printf '\033[1;32m  PASS\033[0m %s\n' "$1"; }
fail() { FAILED=$((FAILED + 1)); TESTS+=("FAIL: $1"); printf '\033[1;31m  FAIL\033[0m %s\n' "$1"; }

# ── Check hey is installed ───────────────────────────────────────────────────

if ! command -v hey &>/dev/null; then
    log "ERROR: 'hey' is not installed."
    log "Install with: brew install hey  OR  go install github.com/rakyll/hey@latest"
    exit 1
fi

# ── Verify target is reachable ───────────────────────────────────────────────

log "Checking target is reachable at ${PRIMARY_URL}/health..."
if ! curl -sf "${PRIMARY_URL}/health" > /dev/null 2>&1; then
    log "ERROR: ${PRIMARY_URL}/health is not reachable. Start the server first."
    exit 1
fi

# ── Test 1: Ingest endpoint under concurrent load ───────────────────────────

log "Running ingest load test: ${TOTAL_REQUESTS} requests, concurrency ${CONCURRENCY}..."

# Build a batch of unique ingest payloads (each with a unique content_hash).
INGEST_PAYLOAD=$(cat <<'PAYLOAD'
{
  "canonical_url": "http://mock-rss/load-test-feed.xml",
  "source_url": "http://mock-rss/load-test-feed.xml",
  "crawl_token": "test-token",
  "http_status": 200,
  "content_hash": "load-test-hash-UNIQUE",
  "feed_data": {
    "feed_guid": "load-test-guid-aaaa-bbbb-ccccddddeeee",
    "title": "Load Test Album",
    "description": "Feed for load testing.",
    "image_url": null,
    "language": "en",
    "explicit": false,
    "itunes_type": null,
    "raw_medium": "music",
    "author_name": "Load Test Artist",
    "owner_name": "Load Test Artist",
    "pub_date": 1772524800,
    "feed_payment_routes": [
      {
        "recipient_name": "Load Test Artist",
        "route_type": "node",
        "address": "02abababababababababababababababababababababababababababababababababab",
        "custom_key": null,
        "custom_value": null,
        "split": 100,
        "fee": false
      }
    ],
    "tracks": [
      {
        "track_guid": "load-track-one",
        "title": "Load Track",
        "pub_date": 1772524800,
        "duration_secs": 180,
        "enclosure_url": "http://mock-rss/load-track.mp3",
        "enclosure_type": "audio/mpeg",
        "enclosure_bytes": 4500000,
        "track_number": 1,
        "season": null,
        "explicit": false,
        "description": null,
        "author_name": null,
        "payment_routes": [],
        "value_time_splits": []
      }
    ]
  }
}
PAYLOAD
)

# Write payload to a temp file for hey
PAYLOAD_FILE=$(mktemp)
echo "$INGEST_PAYLOAD" > "$PAYLOAD_FILE"

test_name="Ingest endpoint: ${TOTAL_REQUESTS} requests @ concurrency ${CONCURRENCY}"
hey_output=$(hey -n "$TOTAL_REQUESTS" -c "$CONCURRENCY" \
    -m POST \
    -H "Content-Type: application/json" \
    -D "$PAYLOAD_FILE" \
    "${PRIMARY_URL}/ingest/feed" 2>&1)

rm -f "$PAYLOAD_FILE"

# Parse hey output for latency percentiles
ingest_p50=$(echo "$hey_output" | grep "50% in" | awk '{print $3}' | sed 's/secs//')
ingest_p95=$(echo "$hey_output" | grep "95% in" | awk '{print $3}' | sed 's/secs//')
ingest_p99=$(echo "$hey_output" | grep "99% in" | awk '{print $3}' | sed 's/secs//')
ingest_rps=$(echo "$hey_output" | grep "Requests/sec:" | awk '{print $2}')
ingest_errors=$(echo "$hey_output" | grep -c '\[5[0-9][0-9]\]' || echo "0")

log "  Ingest p50: ${ingest_p50:-N/A}s  p95: ${ingest_p95:-N/A}s  p99: ${ingest_p99:-N/A}s"
log "  Ingest throughput: ${ingest_rps:-N/A} req/s"

# Check p99 threshold (convert seconds to ms)
if [ -n "$ingest_p99" ]; then
    p99_ms=$(echo "$ingest_p99" | awk "{printf \"%.0f\", \$1 * 1000}")
    if [ "$p99_ms" -le "$P99_THRESHOLD_MS" ]; then
        pass "$test_name (p99=${p99_ms}ms, threshold=${P99_THRESHOLD_MS}ms)"
    else
        fail "$test_name (p99=${p99_ms}ms exceeds threshold=${P99_THRESHOLD_MS}ms)"
    fi
else
    fail "$test_name (could not parse p99 latency)"
fi

# ── Test 2: Search endpoint under concurrent load ───────────────────────────

log "Running search load test: ${TOTAL_REQUESTS} requests, concurrency ${CONCURRENCY}..."

test_name="Search endpoint: ${TOTAL_REQUESTS} requests @ concurrency ${CONCURRENCY}"
hey_output=$(hey -n "$TOTAL_REQUESTS" -c "$CONCURRENCY" \
    "${PRIMARY_URL}/v1/search?q=Test" 2>&1)

search_p50=$(echo "$hey_output" | grep "50% in" | awk '{print $3}' | sed 's/secs//')
search_p95=$(echo "$hey_output" | grep "95% in" | awk '{print $3}' | sed 's/secs//')
search_p99=$(echo "$hey_output" | grep "99% in" | awk '{print $3}' | sed 's/secs//')
search_rps=$(echo "$hey_output" | grep "Requests/sec:" | awk '{print $2}')

log "  Search p50: ${search_p50:-N/A}s  p95: ${search_p95:-N/A}s  p99: ${search_p99:-N/A}s"
log "  Search throughput: ${search_rps:-N/A} req/s"

if [ -n "$search_p99" ]; then
    p99_ms=$(echo "$search_p99" | awk "{printf \"%.0f\", \$1 * 1000}")
    if [ "$p99_ms" -le "$P99_THRESHOLD_MS" ]; then
        pass "$test_name (p99=${p99_ms}ms, threshold=${P99_THRESHOLD_MS}ms)"
    else
        fail "$test_name (p99=${p99_ms}ms exceeds threshold=${P99_THRESHOLD_MS}ms)"
    fi
else
    fail "$test_name (could not parse p99 latency)"
fi

# ── Test 3: Health endpoint under high load (baseline) ───────────────────────

log "Running health endpoint baseline: 200 requests, concurrency 50..."

test_name="Health endpoint baseline: 200 requests @ concurrency 50"
hey_output=$(hey -n 200 -c 50 \
    "${PRIMARY_URL}/health" 2>&1)

health_p99=$(echo "$hey_output" | grep "99% in" | awk '{print $3}' | sed 's/secs//')
health_rps=$(echo "$hey_output" | grep "Requests/sec:" | awk '{print $2}')

log "  Health p99: ${health_p99:-N/A}s  throughput: ${health_rps:-N/A} req/s"

if [ -n "$health_p99" ]; then
    p99_ms=$(echo "$health_p99" | awk "{printf \"%.0f\", \$1 * 1000}")
    if [ "$p99_ms" -le 500 ]; then
        pass "$test_name (p99=${p99_ms}ms)"
    else
        fail "$test_name (p99=${p99_ms}ms exceeds 500ms for health endpoint)"
    fi
else
    fail "$test_name (could not parse p99 latency)"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

echo ""
log "=========================================="
log "  Load Test Results"
log "=========================================="
echo ""
log "  Ingest:  p50=${ingest_p50:-N/A}s  p95=${ingest_p95:-N/A}s  p99=${ingest_p99:-N/A}s  rps=${ingest_rps:-N/A}"
log "  Search:  p50=${search_p50:-N/A}s  p95=${search_p95:-N/A}s  p99=${search_p99:-N/A}s  rps=${search_rps:-N/A}"
log "  Health:  p99=${health_p99:-N/A}s  rps=${health_rps:-N/A}"
echo ""
for t in "${TESTS[@]}"; do
    if echo "$t" | grep -q "^PASS"; then
        printf '  \033[1;32m%s\033[0m\n' "$t"
    else
        printf '  \033[1;31m%s\033[0m\n' "$t"
    fi
done
echo ""
log "Total: $((PASSED + FAILED))  Passed: ${PASSED}  Failed: ${FAILED}"
log "=========================================="

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi

log "All load tests passed."
exit 0
