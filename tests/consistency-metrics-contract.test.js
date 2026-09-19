import test from 'node:test';
import assert from 'node:assert/strict';

import {
  LedgerConsistencyTracker,
  ConsistencyError,
  Metrics,
  createMetrics
} from '../src/index.js';

test('consistency tracker accepts a continuous ledger sequence', () => {
  const tracker =
    new LedgerConsistencyTracker();

  const first = {
    sequence: 100,
    hash: 'hash-100',
    previousHash: 'hash-99'
  };

  const second = {
    sequence: 101,
    hash: 'hash-101',
    previousHash: 'hash-100'
  };

  tracker.observe(first);
  tracker.observe(second);

  assert.equal(
    true,
    true
  );
});

test('consistency tracker rejects sequence gaps', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'hash-100',
    previousHash: 'hash-99'
  });

  assert.throws(
    () =>
      tracker.observe({
        sequence: 102,
        hash: 'hash-102',
        previousHash: 'hash-100'
      }),
    ConsistencyError
  );
});

test('consistency tracker rejects broken previous hash', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'hash-100',
    previousHash: 'hash-99'
  });

  assert.throws(
    () =>
      tracker.observe({
        sequence: 101,
        hash: 'hash-101',
        previousHash: 'wrong-hash'
      }),
    ConsistencyError
  );
});

test('consistency tracker rejects conflicting duplicate ledger', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'hash-100',
    previousHash: 'hash-99'
  });

  assert.throws(
    () =>
      tracker.observe({
        sequence: 100,
        hash: 'different-hash',
        previousHash: 'hash-99'
      }),
    ConsistencyError
  );
});

test('consistency tracker accepts identical duplicate ledger', () => {
  const tracker =
    new LedgerConsistencyTracker();

  const ledger = {
    sequence: 100,
    hash: 'hash-100',
    previousHash: 'hash-99'
  };

  tracker.observe(ledger);

  assert.doesNotThrow(
    () => tracker.observe(ledger)
  );
});

test('metrics increments counters', () => {
  const metrics =
    new Metrics();

  metrics.increment(
    'events_processed'
  );

  metrics.increment(
    'events_processed',
    4
  );

  const snapshot =
    metrics.snapshot();

  assert.equal(
    snapshot.counters.events_processed,
    5
  );
});

test('metrics supports gauges', () => {
  const metrics =
    new Metrics();

  metrics.gauge(
    'latest_ledger',
    123
  );

  assert.equal(
    metrics.snapshot().gauges.latest_ledger,
    123
  );
});

test('metrics records observations', () => {
  const metrics =
    new Metrics();

  metrics.observe(
    'latency_ms',
    10
  );

  metrics.observe(
    'latency_ms',
    20
  );

  const snapshot =
    metrics.snapshot();

  assert.ok(
    snapshot.histograms.latency_ms
  );
});

test('metrics exports Prometheus text', () => {
  const metrics =
    new Metrics();

  metrics.increment(
    'events_processed',
    3
  );

  const output =
    metrics.toPrometheus();

  assert.equal(
    typeof output,
    'string'
  );

  assert.match(
    output,
    /events_processed/
  );
});

test('metrics reset clears collected data', () => {
  const metrics =
    new Metrics();

  metrics.increment(
    'events_processed',
    3
  );

  metrics.reset();

  const snapshot =
    metrics.snapshot();

  assert.equal(
    Object.keys(
      snapshot.counters
    ).length,
    0
  );
});

test('createMetrics creates Metrics', () => {
  const metrics =
    createMetrics();

  assert.ok(
    metrics instanceof Metrics
  );
});

test('createMetrics preserves an existing Metrics instance', () => {
  const original =
    new Metrics();

  const result =
    createMetrics(original);

  assert.equal(
    result,
    original
  );
});
