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

function makeStreamer(events) {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid',
    { pollInterval: 1 }
  );

  streamer.getLatestLedger = async () => events.at(-1)?.ledger ?? 0;

  streamer.getEventsWindowed = async ({ startLedger, endLedger }) =>
    events.filter(
      event =>
        event.ledger >= startLedger &&
        event.ledger <= endLedger
    );

  return streamer;
}

test('stream does not checkpoint before consumer resumes iteration', async () => {
  const events = [makeEvent('1', 10)];
  const streamer = makeStreamer(events);

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);

  const controller = new AbortController();

  const iterator = streamer.stream({
    startLedger: 10,
    checkpoint,
    checkpointKey: 'worker-1',
    signal: controller.signal
  });

  const first = await iterator.next();

  assert.equal(first.done, false);
  assert.equal(first.value.id, '1');
  assert.equal(await checkpoint.load('worker-1'), null);

  controller.abort();
  await iterator.return();
});

test('stream checkpoints after consumer resumes iteration', async () => {
  const events = [makeEvent('1', 10)];

  const streamer = new SorobanEventStreamer(
    'https://example.invalid',
    { pollInterval: 1 }
  );

  streamer.getLatestLedger = async () => 10;

  let fetches = 0;

  streamer.getEventsWindowed = async ({ startLedger }) => {
    fetches++;

    if (fetches === 1 && startLedger === 10) {
      return events;
    }

    return [];
  };

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);
  const controller = new AbortController();

  const iterator = streamer.stream({
    startLedger: 10,
    checkpoint,
    checkpointKey: 'worker-1',
    signal: controller.signal
  });

  const first = await iterator.next();

  assert.equal(first.value.id, '1');
  assert.equal(await checkpoint.load('worker-1'), null);

  const nextPromise = iterator.next();

  for (let i = 0; i < 20; i++) {
    if (await checkpoint.load('worker-1') === 11) break;
    await new Promise(resolve => setTimeout(resolve, 1));
  }

  assert.equal(await checkpoint.load('worker-1'), 11);

  controller.abort();

  await Promise.race([
    nextPromise,
    new Promise(resolve => setTimeout(resolve, 100))
  ]);

  await iterator.return();
});

test('stream resumes from an existing checkpoint', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid',
    { pollInterval: 1 }
  );

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);

  await checkpoint.save(50, 'worker-1');

  const controller = new AbortController();

  streamer.getLatestLedger = async () => 60;

  let requestedStart = null;

  streamer.getEventsWindowed = async options => {
    requestedStart = options.startLedger;
    controller.abort();
    return [];
  };

  const iterator = streamer.stream({
    checkpoint,
    checkpointKey: 'worker-1',
    signal: controller.signal
  });

  await iterator.next();

  assert.equal(requestedStart, 50);

  await iterator.return();
});

test('explicit startLedger takes precedence over checkpoint', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid',
    { pollInterval: 1 }
  );

  const store = new MemoryCheckpointStore();
  const checkpoint = new CheckpointManager(store);

  await checkpoint.save(50, 'worker-1');

  const controller = new AbortController();

  streamer.getLatestLedger = async () => 60;

  let requestedStart = null;

  streamer.getEventsWindowed = async options => {
    requestedStart = options.startLedger;
    controller.abort();
    return [];
  };

  const iterator = streamer.stream({
    startLedger: 55,
    checkpoint,
    checkpointKey: 'worker-1',
    signal: controller.signal
  });

  await iterator.next();

  assert.equal(requestedStart, 55);

  await iterator.return();
});

test('stream rejects an invalid checkpoint object', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  const controller = new AbortController();

  const iterator = streamer.stream({
    startLedger: 10,
    checkpoint: {},
    signal: controller.signal
  });

  await assert.rejects(
    () => iterator.next(),
    /checkpoint must implement resumeFrom\(\) and save\(\)/
  );

  controller.abort();
  await iterator.return();
});
