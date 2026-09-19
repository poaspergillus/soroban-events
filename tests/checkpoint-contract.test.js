import test from 'node:test';
import assert from 'node:assert/strict';

import {
  MemoryCheckpointStore,
  CheckpointManager,
  CheckpointError
} from '../src/index.js';

test('MemoryCheckpointStore starts empty', async () => {
  const store =
    new MemoryCheckpointStore();

  assert.equal(
    await store.load('default'),
    null
  );
});

test('MemoryCheckpointStore saves checkpoints', async () => {
  const store =
    new MemoryCheckpointStore();

  await store.save(
    'consumer',
    100
  );

  assert.equal(
    await store.load('consumer'),
    100
  );
});

test('MemoryCheckpointStore prevents normal backwards movement', async () => {
  const store =
    new MemoryCheckpointStore();

  await store.save(
    'consumer',
    100
  );

  await assert.rejects(
    () =>
      store.save(
        'consumer',
        99
      ),
    CheckpointError
  );
});

test('MemoryCheckpointStore supports explicit rewind', async () => {
  const store =
    new MemoryCheckpointStore();

  await store.save(
    'consumer',
    100
  );

  await store.rewind(
    'consumer',
    90
  );

  assert.equal(
    await store.load('consumer'),
    90
  );
});

test('CheckpointManager resumes from fallback', async () => {
  const manager =
    new CheckpointManager(
      new MemoryCheckpointStore()
    );

  assert.equal(
    await manager.resumeFrom(
      'consumer',
      50
    ),
    50
  );
});

test('CheckpointManager saves monotonically', async () => {
  const manager =
    new CheckpointManager(
      new MemoryCheckpointStore()
    );

  await manager.save(
    100,
    'consumer'
  );

  await manager.save(
    110,
    'consumer'
  );

  assert.equal(
    await manager.load('consumer'),
    110
  );
});

test('CheckpointManager rejects backwards save', async () => {
  const manager =
    new CheckpointManager(
      new MemoryCheckpointStore()
    );

  await manager.save(
    100,
    'consumer'
  );

  await assert.rejects(
    () =>
      manager.save(
        90,
        'consumer'
      ),
    CheckpointError
  );
});

test('CheckpointManager explicitly rewinds', async () => {
  const manager =
    new CheckpointManager(
      new MemoryCheckpointStore()
    );

  await manager.save(
    100,
    'consumer'
  );

  await manager.rewind(
    80,
    'consumer'
  );

  assert.equal(
    await manager.load('consumer'),
    80
  );
});

test('CheckpointManager clear removes checkpoint', async () => {
  const manager =
    new CheckpointManager(
      new MemoryCheckpointStore()
    );

  await manager.save(
    100,
    'consumer'
  );

  await manager.clear(
    'consumer'
  );

  assert.equal(
    await manager.load('consumer'),
    null
  );
});

test('CheckpointManager requires a checkpoint store', () => {
  assert.throws(
    () =>
      new CheckpointManager(null),
    TypeError
  );
});
