import test from 'node:test';
import assert from 'node:assert/strict';

import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { promises as fs } from 'node:fs';

import {
  CheckpointError,
  FileCheckpointStore
} from '../src/index.js';

async function tempDirectory() {
  return mkdtemp(
    path.join(os.tmpdir(), 'soroban-events-checkpoint-')
  );
}

test('file checkpoint starts empty', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    assert.equal(
      await store.load('worker-1'),
      null
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('file checkpoint survives a new store instance', async () => {
  const directory = await tempDirectory();

  try {
    const first = new FileCheckpointStore(directory);

    await first.save('worker-1', 12345);

    const second = new FileCheckpointStore(directory);

    assert.equal(
      await second.load('worker-1'),
      12345
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('file checkpoint cannot move backwards', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-1', 100);

    await assert.rejects(
      () => store.save('worker-1', 99),
      CheckpointError
    );

    assert.equal(
      await store.load('worker-1'),
      100
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('different checkpoint keys are isolated', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-a', 100);
    await store.save('worker-b', 500);

    assert.equal(
      await store.load('worker-a'),
      100
    );

    assert.equal(
      await store.load('worker-b'),
      500
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('clear removes a file checkpoint', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-1', 100);
    await store.clear('worker-1');

    assert.equal(
      await store.load('worker-1'),
      null
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('checkpoint keys are safely encoded into filenames', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);
    const key = 'worker/a?b#c';

    await store.save(key, 42);

    assert.equal(
      await store.load(key),
      42
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('invalid checkpoint file is rejected', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);
    const key = 'worker-1';

    await store.save(key, 100);

    const file = path.join(
      directory,
      `${encodeURIComponent(key)}.json`
    );

    await writeFile(
      file,
      '{"broken":true}\n',
      'utf8'
    );

    await assert.rejects(
      () => store.load(key),
      /invalid checkpoint file format/
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('checkpoint file is valid JSON on disk', async () => {
  const directory = await tempDirectory();

  try {
    const store = new FileCheckpointStore(directory);

    await store.save('worker-1', 777);

    const file = path.join(
      directory,
      `${encodeURIComponent('worker-1')}.json`
    );

    const data = JSON.parse(
      await readFile(file, 'utf8')
    );

    assert.deepEqual(data, {
      version: 1,
      key: 'worker-1',
      ledger: 777
    });
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});

test('invalid file checkpoint inputs are rejected', async () => {
  const directory = await tempDirectory();

  try {
    assert.throws(
      () => new FileCheckpointStore(''),
      /checkpoint directory must be a non-empty string/
    );

    const store = new FileCheckpointStore(directory);

    await assert.rejects(
      () => store.save('', 1),
      /checkpoint key must be a non-empty string/
    );

    await assert.rejects(
      () => store.save('worker', 0),
      /positive safe integer/
    );

    await assert.rejects(
      () => store.save('worker', 1.5),
      /positive safe integer/
    );
  } finally {
    await rm(directory, {
      recursive: true,
      force: true
    });
  }
});


test('file checkpoint can explicitly rewind', async () => {
  const directory =
    path.join(
      process.cwd(),
      '.soroban-events-checkpoint-rewind-test'
    );

  await fs.rm(
    directory,
    {
      recursive: true,
      force: true
    }
  );

  const store =
    new FileCheckpointStore(
      directory
    );

  await store.save(
    'consumer',
    100
  );

  await store.rewind(
    'consumer',
    99
  );

  assert.equal(
    await store.load('consumer'),
    99
  );

  await fs.rm(
    directory,
    {
      recursive: true,
      force: true
    }
  );
});
