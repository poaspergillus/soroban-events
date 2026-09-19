import test from 'node:test';
import assert from 'node:assert/strict';

import * as api from '../src/index.js';

const expected = [
  'SorobanEventStreamer',
  'decodeEvent',
  'unwrapScVal',
  'CheckpointManager',
  'MemoryCheckpointStore',
  'FileCheckpointStore',
  'EventPipeline',
  'BackfillEngine',
  'EventReplay',
  'EventHandoff',
  'MemoryEventStore',
  'SqliteEventStore',
  'LedgerConsistencyTracker',
  'Metrics',
  'createMetrics',
  'ReorgError',
  'ReorgRecovery',
  'ReorgMonitor',
  'ReorgMonitorError',
  'LiveBackfillEngine',
  'LiveBackfillError',
  'EventEngine',
  'EventEngineError',
  'LifecycleController',
  'LifecycleError',
  'compareEvents',
  'orderBackfillEvents'
];

test('public API exports documented symbols', () => {
  for (const name of expected) {
    assert.ok(
      name in api,
      `missing public export: ${name}`
    );
    assert.notEqual(
      api[name],
      undefined,
      `undefined public export: ${name}`
    );
  }
});

test('metrics factory returns Metrics', () => {
  const metrics = api.createMetrics();

  assert.ok(
    metrics instanceof api.Metrics
  );
});

test('memory checkpoint store has required methods', () => {
  const store =
    new api.MemoryCheckpointStore();

  for (const method of [
    'load',
    'save',
    'clear',
    'rewind'
  ]) {
    assert.equal(
      typeof store[method],
      'function',
      `missing checkpoint method: ${method}`
    );
  }
});

test('memory event store has required methods', () => {
  const store =
    new api.MemoryEventStore();

  for (const method of [
    'put',
    'get',
    'has',
    'delete',
    'list',
    'count',
    'clear'
  ]) {
    assert.equal(
      typeof store[method],
      'function',
      `missing event-store method: ${method}`
    );
  }
});
