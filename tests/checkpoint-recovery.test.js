import test from 'node:test';
import assert from 'node:assert/strict';

import {
  mkdtemp,
  readdir,
  rm,
  writeFile
} from 'node:fs/promises';

import os from 'node:os';
import path from 'node:path';

import {
  CheckpointError,
  CheckpointManager,
  FileCheckpointStore,
  SorobanEventStreamer
} from '../src/index.js';

async function tempDirectory() {
  return mkdtemp(
    path.join(os.tmpdir(), 'soroban-events-recovery-')
  );
}

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

test('checkpoint survives complete store and manager recreation', async () => {
  const directory = await tempDirectory();

  try {
    const firstStore = new FileCheckpointStore(directory);
    const firstManager = new CheckpointManager(firstStore);

    await firstManager.save(500, 'worker-1');

    const secondStore = new FileCheckpointStore(directory);
    const secondManager = new CheckpointManager(secondStore);

    assert.equal(
      await secondManager.load('worker-1'),
      500
    );

    assert.equal(
      await secondManager.resumeFrom('worker-1', 1),
      500
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('failed consumer leaves durable checkpoint unchanged', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);
    const checkpoint = new CheckpointManager(store);

    await checkpoint.save(100, 'worker-1');

    const streamer = new SorobanEventStreamer(
      'https://example.invalid'
    );

    streamer.getLatestLedger = async () => 101;

    streamer.getEventsWindowed = async () => [
      makeEvent('a', 101),
      makeEvent('b', 101)
    ];

    await assert.rejects(
      () =>
        streamer.consume({
          checkpoint,
          checkpointKey: 'worker-1',
          onEvent: async event => {
            if (event.id === 'b') {
              throw new Error('simulated crash');
            }
          }
        }),
      /simulated crash/
    );

    const restartedStore = new FileCheckpointStore(directory);
    const restartedCheckpoint =
      new CheckpointManager(restartedStore);

    assert.equal(
      await restartedCheckpoint.load('worker-1'),
      100
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('restart reprocesses the uncommitted ledger', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);
    const checkpoint = new CheckpointManager(store);

    await checkpoint.save(100, 'worker-1');

    const firstStreamer = new SorobanEventStreamer(
      'https://example.invalid'
    );

    firstStreamer.getLatestLedger = async () => 101;

    firstStreamer.getEventsWindowed = async () => [
      makeEvent('a', 101),
      makeEvent('b', 101)
    ];

    await assert.rejects(
      () =>
        firstStreamer.consume({
          checkpoint,
          checkpointKey: 'worker-1',
          onEvent: async event => {
            if (event.id === 'b') {
              throw new Error('crash');
            }
          }
        }),
      /crash/
    );

    const restartedStore = new FileCheckpointStore(directory);
    const restartedCheckpoint =
      new CheckpointManager(restartedStore);

    const secondStreamer = new SorobanEventStreamer(
      'https://example.invalid'
    );

    secondStreamer.getLatestLedger = async () => 101;

    let requestedStart = null;

    secondStreamer.getEventsWindowed = async options => {
      requestedStart = options.startLedger;

      return [
        makeEvent('a', 101),
        makeEvent('b', 101)
      ];
    };

    const seen = [];

    const processed = await secondStreamer.consume({
      checkpoint: restartedCheckpoint,
      checkpointKey: 'worker-1',
      maxEvents: 2,
      onEvent: async event => {
        seen.push(event.id);
      }
    });

    assert.equal(processed, 2);
    assert.equal(requestedStart, 100);
    assert.deepEqual(seen, ['a', 'b']);

    assert.equal(
      await restartedCheckpoint.load('worker-1'),
      102
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('stale temporary checkpoint files do not affect recovery', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-1', 200);

    const files = await readdir(directory);

    const checkpointFile = files.find(
      file => file.endsWith('.json')
    );

    assert.ok(checkpointFile);

    await writeFile(
      path.join(
        directory,
        `${checkpointFile}.stale.tmp`
      ),
      '{"version":1,"ledger":999999}',
      'utf8'
    );

    const restarted = new FileCheckpointStore(directory);

    assert.equal(
      await restarted.load('worker-1'),
      200
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('saving a newer checkpoint replaces the old durable value', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-1', 100);
    await store.save('worker-1', 200);

    const restarted = new FileCheckpointStore(directory);

    assert.equal(
      await restarted.load('worker-1'),
      200
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('corrupted durable checkpoint fails loudly', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-1', 100);

    const file = path.join(
      directory,
      `${encodeURIComponent('worker-1')}.json`
    );

    await writeFile(
      file,
      '{this is not valid json',
      'utf8'
    );

    await assert.rejects(
      () => store.load('worker-1'),
      error => {
        assert.ok(error instanceof CheckpointError);
        assert.match(
          error.message,
          /invalid checkpoint file/
        );
        return true;
      }
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('durable checkpoint keys remain isolated after restart', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-a', 100);
    await store.save('worker-b', 900);

    const restarted = new FileCheckpointStore(directory);

    assert.equal(
      await restarted.load('worker-a'),
      100
    );

    assert.equal(
      await restarted.load('worker-b'),
      900
    );

    await restarted.clear('worker-a');

    assert.equal(
      await restarted.load('worker-a'),
      null
    );

    assert.equal(
      await restarted.load('worker-b'),
      900
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});
