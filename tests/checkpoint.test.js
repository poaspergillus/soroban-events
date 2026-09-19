import test from 'node:test';
import assert from 'node:assert/strict';

import {
  CheckpointError,
  CheckpointManager,
  MemoryCheckpointStore
} from '../src/index.js';

test('memory checkpoint starts empty', async () => {
  const store = new MemoryCheckpointStore();

  assert.equal(await store.load('worker-1'), null);
});

test('checkpoint can be saved and loaded', async () => {
  const store = new MemoryCheckpointStore();
  const manager = new CheckpointManager(store);

  await manager.save(100, 'worker-1');

  assert.equal(await manager.load('worker-1'), 100);
});

test('checkpoint cannot move backwards', async () => {
  const store = new MemoryCheckpointStore();
  const manager = new CheckpointManager(store);

  await manager.save(100, 'worker-1');

  await assert.rejects(
    () => manager.save(99, 'worker-1'),
    CheckpointError
  );
});

test('different consumers get independent checkpoints', async () => {
  const store = new MemoryCheckpointStore();
  const manager = new CheckpointManager(store);

  await manager.save(100, 'worker-a');
  await manager.save(500, 'worker-b');

  assert.equal(await manager.load('worker-a'), 100);
  assert.equal(await manager.load('worker-b'), 500);
});

test('resumeFrom returns fallback when no checkpoint exists', async () => {
  const store = new MemoryCheckpointStore();
  const manager = new CheckpointManager(store);

  assert.equal(await manager.resumeFrom('worker-1', 5000), 5000);
});

test('resumeFrom returns stored checkpoint', async () => {
  const store = new MemoryCheckpointStore();
  const manager = new CheckpointManager(store);

  await manager.save(7000, 'worker-1');

  assert.equal(await manager.resumeFrom('worker-1', 5000), 7000);
});

test('clear removes checkpoint', async () => {
  const store = new MemoryCheckpointStore();
  const manager = new CheckpointManager(store);

  await manager.save(100, 'worker-1');
  await manager.clear('worker-1');

  assert.equal(await manager.load('worker-1'), null);
});

test('invalid checkpoint ledger is rejected', async () => {
  const store = new MemoryCheckpointStore();
  const manager = new CheckpointManager(store);

  await assert.rejects(
    () => manager.save(0),
    /positive safe integer/
  );

  await assert.rejects(
    () => manager.save(-10),
    /positive safe integer/
  );

  await assert.rejects(
    () => manager.save(1.5),
    /positive safe integer/
  );
});


test('checkpoint can explicitly rewind', async () => {
  const store =
    new MemoryCheckpointStore();

  const manager =
    new CheckpointManager(store);

  await manager.save(
    100,
    'consumer'
  );

  await manager.rewind(
    99,
    'consumer'
  );

  assert.equal(
    await manager.resumeFrom(
      'consumer',
      999
    ),
    99
  );
});
