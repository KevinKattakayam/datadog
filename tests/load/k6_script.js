// k6 Load Test — Enterprise Observability Pipeline
// Run: k6 run tests/load/k6_script.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const publishLatency = new Trend('publish_latency_ms');
const publishErrors = new Counter('publish_errors');
const publishSuccess = new Rate('publish_success_rate');

// Test configuration
export const options = {
  scenarios: {
    // Ramp-up phase
    ramp_up: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 10 },    // Warm up
        { duration: '2m', target: 50 },      // Ramp to moderate load
        { duration: '3m', target: 100 },     // Sustained high load
        { duration: '1m', target: 200 },     // Peak burst
        { duration: '30s', target: 0 },      // Cool down
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<1000'],
    publish_success_rate: ['rate>0.99'],
    publish_latency_ms: ['p(99)<200'],
  },
};

const BASE_URL = __ENV.INGESTOR_URL || 'http://localhost:8080';
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || '100');

const services = ['checkout', 'payments', 'inventory', 'search', 'auth', 'gateway'];
const endpoints = ['/api/v1/order', '/api/v1/pay', '/api/v1/search', '/health'];
const hosts = ['prod-api-01', 'prod-api-02', 'prod-api-03', 'prod-api-04', 'prod-api-05'];
const metricNames = [
  'api.request.duration_ms',
  'api.request.count',
  'api.error.count',
  'db.query.duration_ms',
  'cache.hit.ratio',
  'cpu.usage.percent',
  'memory.usage.bytes',
];

function randomElement(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateBatch(size) {
  const metrics = [];
  const now = Math.floor(Date.now() / 1000);

  for (let i = 0; i < size; i++) {
    const name = randomElement(metricNames);
    let value = Math.random() * 200 + 50;
    let unit = 'ms';

    if (name.includes('count')) {
      value = Math.floor(Math.random() * 1000);
      unit = 'count';
    } else if (name.includes('percent') || name.includes('ratio')) {
      value = Math.random() * 100;
      unit = 'percent';
    } else if (name.includes('bytes')) {
      value = Math.random() * 1e9;
      unit = 'bytes';
    }

    // 5% chance of anomaly
    if (Math.random() < 0.05) {
      value *= 10;
    }

    metrics.push({
      name: name,
      value: value,
      unit: unit,
      tags: {
        service: randomElement(services),
        endpoint: randomElement(endpoints),
        region: 'us-east-1',
      },
      timestamp: now,
      host: randomElement(hosts),
    });
  }

  return metrics;
}

export default function () {
  const batch = generateBatch(BATCH_SIZE);
  const payload = JSON.stringify({ metrics: batch });

  const start = Date.now();
  const res = http.post(`${BASE_URL}/ingest/batch`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  const latency = Date.now() - start;

  publishLatency.add(latency);

  const success = check(res, {
    'status is 202': (r) => r.status === 202,
    'response has accepted count': (r) => JSON.parse(r.body).accepted > 0,
  });

  if (success) {
    publishSuccess.add(1);
  } else {
    publishErrors.add(1);
    publishSuccess.add(0);
  }

  sleep(0.01); // 10ms between requests per VU
}

export function handleSummary(data) {
  const totalMetrics = data.metrics.http_reqs.values.count * BATCH_SIZE;
  const duration = data.state.testRunDurationMs / 1000;
  const throughput = totalMetrics / duration;

  console.log(`\n📊 Load Test Summary`);
  console.log(`   Total Metrics:  ${totalMetrics.toLocaleString()}`);
  console.log(`   Duration:       ${duration.toFixed(1)}s`);
  console.log(`   Throughput:     ${throughput.toFixed(0)} metrics/sec`);
  console.log(`   p95 Latency:    ${data.metrics.http_req_duration.values['p(95)'].toFixed(1)}ms`);
  console.log(`   p99 Latency:    ${data.metrics.http_req_duration.values['p(99)'].toFixed(1)}ms`);
  console.log(`   Success Rate:   ${(data.metrics.publish_success_rate.values.rate * 100).toFixed(2)}%`);

  return {};
}
