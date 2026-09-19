import test from 'node:test';
import assert from 'node:assert/strict';

import {
  MemoryCheckpointStore,
  CheckpointManager,
  SorobanEventStreamer
} from '../src/index.js';

function makeEvent(id, ledger) {
  return {
    id,
    type: 'contract',
    ledger,
    ledgerClosedAt: '2026-01-01T00:00:00Z',
    contractId: 'C_TEST',
    topic: [],
    value: 'x',
    inSuccessfulContractCall: true
  };
}

test('consume processes events and checkpoints after the whole ledger', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);

  streamer.getLatestLedger = async () => 100;

  streamer.getEventsWindowed = async () => [
    makeEvent('a', 100),
    makeEvent('b', 100),
    makeEvent('c', 100)
  ];

  const seen = [];

  const processed = await streamer.consume({
    startLedger: 100,
    checkpoint,
    checkpointKey: 'worker-1',
    maxEvents: 3,
    onEvent: async event => {
      seen.push(event.id);
    }
  });

  assert.equal(processed, 3);
  assert.deepEqual(seen, ['a', 'b', 'c']);
  assert.equal(await checkpoint.load('worker-1'), 101);
});

test('consume does not checkpoint a ledger when an event fails', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);

  streamer.getLatestLedger = async () => 100;

  streamer.getEventsWindowed = async () => [
    makeEvent('a', 100),
    makeEvent('b', 100),
    makeEvent('c', 100)
  ];

  let calls = 0;

  await assert.rejects(
    () =>
      streamer.consume({
        startLedger: 100,
        checkpoint,
        checkpointKey: 'worker-1',
        onEvent: async event => {
          calls++;

          if (event.id === 'b') {
            throw new Error('consumer failed');
          }
        }
      }),
    /consumer failed/
  );

  assert.equal(calls, 2);
  assert.equal(await checkpoint.load('worker-1'), null);
});

test('consume can checkpoint multiple ledgers independently', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);

  streamer.getLatestLedger = async () => 102;

  streamer.getEventsWindowed = async () => [
    makeEvent('a', 100),
    makeEvent('b', 100),
    makeEvent('c', 101),
    makeEvent('d', 102)
  ];

  const seen = [];

  const processed = await streamer.consume({
    startLedger: 100,
    checkpoint,
    checkpointKey: 'worker-1',
    maxEvents: 3,
    onEvent: async event => {
      seen.push(event.id);
    }
  });

  assert.equal(processed, 3);
  assert.deepEqual(seen, ['a', 'b', 'c']);
  assert.equal(await checkpoint.load('worker-1'), 102);
});

test('consume resumes from checkpoint and reprocesses the checkpoint ledger', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);

  await checkpoint.save(101, 'worker-1');

  streamer.getLatestLedger = async () => 101;

  streamer.getEventsWindowed = async ({ startLedger }) => {
    assert.equal(startLedger, 101);

    return [
      makeEvent('b', 101),
      makeEvent('c', 101)
    ];
  };

  const seen = [];

  const processed = await streamer.consume({
    checkpoint,
    checkpointKey: 'worker-1',
    maxEvents: 2,
    onEvent: async event => {
      seen.push(event.id);
    }
  });

  assert.equal(processed, 2);
  assert.deepEqual(seen, ['b', 'c']);
  assert.equal(await checkpoint.load('worker-1'), 102);
});

test('consume saves checkpoints with ledger then key', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.getLatestLedger = async () => 1;
  streamer.getEventsWindowed = async () => [
    { id: 'event-1', ledger: 1 }
  ];

  const calls = [];

  const checkpoint = {
    resumeFrom: async () => 1,
    save: async (key, ledger) => {
      calls.push([key, ledger]);
    }
  };

  const processed = await streamer.consume({
    startLedger: 1,
    checkpoint,
    checkpointKey: 'consumer-a',
    onEvent: async () => {},
    maxEvents: 1
  });

  assert.equal(processed, 1);
  assert.deepEqual(calls, [[2, 'consumer-a']]);
});

test('consume rejects missing handler', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  await assert.rejects(
    () =>
      streamer.consume({
        startLedger: 100
      }),
    /onEvent must be a function/
  );
});

test('consume rejects invalid maxEvents', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  await assert.rejects(
    () =>
      streamer.consume({
        startLedger: 100,
        maxEvents: 0,
        onEvent: async () => {}
      }),
    /maxEvents must be a positive safe integer/
  );
});


test('getLatestLedger metadata mode preserves sequence compatibility', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = {
    getLatestLedger: async () => ({
      sequence: 123,
      id: 'ledger-hash',
      closeTime: '2026-01-01T00:00:00Z',
      protocolVersion: 25
    })
  };

  assert.equal(
    await streamer.getLatestLedger(),
    123
  );

  assert.deepEqual(
    await streamer.getLatestLedger({ metadata: true }),
    {
      sequence: 123,
      hash: 'ledger-hash',
      closeTime: '2026-01-01T00:00:00Z',
      protocolVersion: 25
    }
  );
});
