// k6 load test for stophammer
//
// Usage:
//   k6 run tests/load_test.js
//   k6 run --env PRIMARY_URL=http://localhost:8008 tests/load_test.js

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const PRIMARY_URL = __ENV.PRIMARY_URL || "http://localhost:8008";
const CRAWL_TOKEN = __ENV.CRAWL_TOKEN || "test-token";

// Custom metrics
const ingestErrors = new Rate("ingest_errors");
const searchErrors = new Rate("search_errors");
const ingestLatency = new Trend("ingest_latency", true);
const searchLatency = new Trend("search_latency", true);

export const options = {
  scenarios: {
    // Scenario 1: Concurrent ingest requests
    ingest_load: {
      executor: "constant-vus",
      vus: 20,
      duration: "30s",
      exec: "ingestTest",
      tags: { scenario: "ingest" },
    },
    // Scenario 2: Concurrent search requests
    search_load: {
      executor: "constant-vus",
      vus: 20,
      duration: "30s",
      exec: "searchTest",
      startTime: "5s", // slight offset to let some data be ingested first
      tags: { scenario: "search" },
    },
  },
  thresholds: {
    // Fail if p99 latency exceeds 2000ms
    "http_req_duration{scenario:ingest}": ["p(99)<2000"],
    "http_req_duration{scenario:search}": ["p(99)<2000"],
    // Fail if error rate exceeds 5%
    ingest_errors: ["rate<0.05"],
    search_errors: ["rate<0.05"],
  },
};

const ingestPayload = JSON.stringify({
  canonical_url: "http://mock-rss/load-test-feed.xml",
  source_url: "http://mock-rss/load-test-feed.xml",
  crawl_token: CRAWL_TOKEN,
  http_status: 200,
  content_hash: "k6-load-test-hash",
  feed_data: {
    feed_guid: "k6-load-test-guid-aaaa-bbbb-ccccddddeeee",
    title: "k6 Load Test Album",
    description: "Feed for k6 load testing.",
    image_url: null,
    language: "en",
    explicit: false,
    itunes_type: null,
    raw_medium: "music",
    author_name: "k6 Load Test Artist",
    owner_name: "k6 Load Test Artist",
    pub_date: 1772524800,
    feed_payment_routes: [
      {
        recipient_name: "k6 Load Test Artist",
        route_type: "node",
        address:
          "02cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
        custom_key: null,
        custom_value: null,
        split: 100,
        fee: false,
      },
    ],
    tracks: [
      {
        track_guid: "k6-track-one",
        title: "k6 Track",
        pub_date: 1772524800,
        duration_secs: 180,
        enclosure_url: "http://mock-rss/k6-track.mp3",
        enclosure_type: "audio/mpeg",
        enclosure_bytes: 4500000,
        track_number: 1,
        season: null,
        explicit: false,
        description: null,
        author_name: null,
        payment_routes: [],
        value_time_splits: [],
      },
    ],
  },
});

const headers = { "Content-Type": "application/json" };

export function ingestTest() {
  // Vary the content_hash per iteration to avoid content_hash dedup
  const uniquePayload = ingestPayload.replace(
    "k6-load-test-hash",
    `k6-hash-${__VU}-${__ITER}-${Date.now()}`
  );

  const res = http.post(`${PRIMARY_URL}/ingest/feed`, uniquePayload, {
    headers,
    tags: { scenario: "ingest" },
  });

  const ok = check(res, {
    "ingest status is 200": (r) => r.status === 200,
    "ingest accepted": (r) => {
      try {
        return JSON.parse(r.body).accepted === true;
      } catch {
        return false;
      }
    },
  });

  ingestErrors.add(!ok);
  ingestLatency.add(res.timings.duration);

  sleep(0.1);
}

export function searchTest() {
  const queries = ["Test", "Album", "Artist", "Track", "k6"];
  const q = queries[Math.floor(Math.random() * queries.length)];

  const res = http.get(`${PRIMARY_URL}/v1/search?q=${q}`, {
    tags: { scenario: "search" },
  });

  const ok = check(res, {
    "search status is 200": (r) => r.status === 200,
    "search has data array": (r) => {
      try {
        return Array.isArray(JSON.parse(r.body).data);
      } catch {
        return false;
      }
    },
  });

  searchErrors.add(!ok);
  searchLatency.add(res.timings.duration);

  sleep(0.1);
}
