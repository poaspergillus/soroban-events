import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

test('TypeScript declarations expose the current public API', async () => {
  const types = await readFile('src/index.d.ts', 'utf8');

  for (const symbol of [
    'SorobanEventStreamer',
    'CheckpointManager',
    'FileCheckpointStore',
    'EventPipeline',
    'BackfillEngine',
    'EventReplay',
    'EventHandoff',
    'MemoryEventStore',
    'SqliteEventStore',
    'LedgerConsistencyTracker',
    'Metrics',
    'ReorgRecovery',
    'LiveBackfillEngine',
    'ReorgMonitor',
    'EventEngine',
    'LifecycleController',
    'MemoryDeadLetterQueue',
    'createEventFilter',
    'queryEvents',
    'createEventQuery',
    'exportEvents',
    'exportEventStream'
  ]) {
    assert.match(types, new RegExp(`\\b${symbol}\\b`), `missing ${symbol}`);
  }

  assert.match(types, /class SorobanEventStreamer/);
  assert.doesNotMatch(types, /class Streamer\\s/);
  assert.match(types, /constructor\(directory: string\)/);
  assert.match(types, /observe\(ledger:/);
  assert.match(types, /accept\(event: Event\): boolean/);
});

test('package requires the Node runtime supported by the Stellar SDK', async () => {
  const pkg = JSON.parse(await readFile('package.json', 'utf8'));
  assert.equal(pkg.engines.node, '>=22.12.0');
});
