import test from 'node:test';
import assert from 'node:assert/strict';

import {
  Metrics,
  MetricsError,
  createMetrics
} from '../src/metrics.js';

test('metrics counter increments', () => {
  const metrics = new Metrics();

  assert.equal(
    metrics.counter('events'),
    1
  );

  assert.equal(
    metrics.increment('events', 4),
    5
  );

  assert.equal(
    metrics.getCounter('events'),
    5
  );
});

test('metrics gauges set and add', () => {
  const metrics = new Metrics();

  assert.equal(
    metrics.gauge('queue', 3),
    3
  );

  assert.equal(
    metrics.addGauge('queue', 2),
    5
  );

  assert.equal(
    metrics.getGauge('queue'),
    5
  );
});

test('metrics histogram records aggregate values', () => {
  const metrics = new Metrics();

  metrics.observe('latency', 10);
  metrics.observe('latency', 20);
  metrics.observe('latency', 5);

  assert.deepEqual(
    metrics.getHistogram('latency'),
    {
      count: 3,
      sum: 35,
      min: 5,
      max: 20
    }
  );
});

test('metrics timer records duration', async () => {
  const metrics = new Metrics();

  const stop =
    metrics.timer('operation');

  await Promise.resolve();

  const elapsed = stop();

  assert.equal(
    typeof elapsed,
    'number'
  );

  assert.equal(
    metrics.getHistogram(
      'operation'
    ).count,
    1
  );

  assert.equal(
    stop(),
    null
  );
});

test('metrics time supports async functions', async () => {
  const metrics = new Metrics();

  const result =
    await metrics.time(
      'operation',
      async () => 'ok'
    );

  assert.equal(result, 'ok');

  assert.equal(
    metrics.getHistogram(
      'operation'
    ).count,
    1
  );
});

test('metrics time rejects non-functions', async () => {
  const metrics = new Metrics();

  await assert.rejects(
    () =>
      metrics.time(
        'operation',
        null
      ),
    /fn must be a function/
  );
});

test('metrics snapshot is detached', () => {
  const metrics = new Metrics();

  metrics.counter(
    'events',
    3
  );

  metrics.gauge(
    'queue',
    7
  );

  metrics.observe(
    'latency',
    12
  );

  const snapshot =
    metrics.snapshot();

  snapshot.counters.events = 999;
  snapshot.gauges.queue = 999;
  snapshot.histograms.latency.count = 999;

  assert.equal(
    metrics.getCounter('events'),
    3
  );

  assert.equal(
    metrics.getGauge('queue'),
    7
  );

  assert.equal(
    metrics.getHistogram(
      'latency'
    ).count,
    1
  );
});

test('metrics exports Prometheus text', () => {
  const metrics = new Metrics();

  metrics.counter(
    'rpc_requests',
    4
  );

  metrics.gauge(
    'queue_depth',
    2
  );

  metrics.observe(
    'request_duration_ms',
    10
  );

  metrics.observe(
    'request_duration_ms',
    20
  );

  const output =
    metrics.toPrometheus();

  assert.match(
    output,
    /# TYPE soroban_events_rpc_requests counter/
  );

  assert.match(
    output,
    /soroban_events_rpc_requests 4/
  );

  assert.match(
    output,
    /# TYPE soroban_events_queue_depth gauge/
  );

  assert.match(
    output,
    /soroban_events_queue_depth 2/
  );

  assert.match(
    output,
    /# TYPE soroban_events_request_duration_ms summary/
  );

  assert.match(
    output,
    /soroban_events_request_duration_ms_count 2/
  );

  assert.match(
    output,
    /soroban_events_request_duration_ms_sum 30/
  );

  assert.match(
    output,
    /soroban_events_request_duration_ms_min 10/
  );

  assert.match(
    output,
    /soroban_events_request_duration_ms_max 20/
  );
});

test('metrics supports a custom Prometheus prefix', () => {
  const metrics = new Metrics();

  metrics.counter(
    'events_processed',
    3
  );

  const output =
    metrics.toPrometheus({
      prefix: 'app'
    });

  assert.match(
    output,
    /# TYPE app_events_processed counter/
  );

  assert.match(
    output,
    /app_events_processed 3/
  );
});

test('metrics rejects invalid Prometheus prefixes', () => {
  const metrics = new Metrics();

  assert.throws(
    () =>
      metrics.toPrometheus({
        prefix: '123-invalid'
      }),
    /prefix must be a valid Prometheus metric prefix/
  );
});

test('empty metrics export empty Prometheus output', () => {
  const metrics = new Metrics();

  assert.equal(
    metrics.toPrometheus(),
    ''
  );
});

test('metrics reset clears state', () => {
  const metrics = new Metrics();

  metrics.counter('events');
  metrics.gauge('queue', 2);
  metrics.observe('latency', 3);

  metrics.reset();

  assert.deepEqual(
    metrics.snapshot(),
    {
      counters: {},
      gauges: {},
      histograms: {}
    }
  );
});

test('metrics merge combines values', () => {
  const left = new Metrics();
  const right = new Metrics();

  left.counter('events', 2);
  left.gauge('queue', 3);
  left.observe('latency', 10);

  right.counter('events', 5);
  right.gauge('queue', 8);
  right.observe('latency', 20);

  left.merge(right);

  assert.equal(
    left.getCounter('events'),
    7
  );

  assert.equal(
    left.getGauge('queue'),
    8
  );

  assert.deepEqual(
    left.getHistogram('latency'),
    {
      count: 2,
      sum: 30,
      min: 10,
      max: 20
    }
  );
});

test('createMetrics preserves Metrics instances', () => {
  const metrics = new Metrics();

  assert.equal(
    createMetrics(metrics),
    metrics
  );
});

test('invalid names are rejected', () => {
  const metrics = new Metrics();

  assert.throws(
    () => metrics.counter(''),
    /metric name must be a non-empty string/
  );
});

test('invalid values are rejected', () => {
  const metrics = new Metrics();

  assert.throws(
    () => metrics.counter(
      'events',
      NaN
    ),
    /metric value must be a finite number/
  );

  assert.throws(
    () => metrics.gauge(
      'queue',
      Infinity
    ),
    /metric value must be a finite number/
  );
});

test('MetricsError extends Error', () => {
  assert.equal(
    new MetricsError('test')
      instanceof Error,
    true
  );
});
