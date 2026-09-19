import test from 'node:test';
import assert from 'node:assert/strict';
import { rmSync } from 'node:fs';

import {
  EventStoreError,
  MemoryEventStore,
  SqliteEventStore,
  isEventStore,
  assertEventStore
} from '../src/store.js';

const events = [
  {
    id: 'b',
    ledger: 2,
    transactionIndex: 1,
    operationIndex: 1,
    value: 'second'
  },
  {
    id: 'a',
    ledger: 1,
    transactionIndex: 1,
    operationIndex: 1,
    value: 'first'
  },
  {
    id: 'c',
    ledger: 2,
    transactionIndex: 2,
    operationIndex: 1,
    value: 'third'
  }
];

async function exerciseStore(makeStore) {
  const store = await makeStore();

  assert.equal(await store.count(), 0);

  for (const event of events) {
    await store.put(event);
  }

  assert.equal(await store.count(), 3);

  assert.deepEqual(
    await store.get('a'),
    events[1]
  );

  assert.equal(
    await store.has('b'),
    true
  );

  assert.equal(
    await store.has('missing'),
    false
  );

  assert.deepEqual(
    await store.list(),
    [events[1], events[0], events[2]]
  );

  assert.deepEqual(
    await store.list({
      startLedger: 2,
      endLedger: 2
    }),
    [events[0], events[2]]
  );

  assert.deepEqual(
    await store.list({
      startLedger: 1,
      endLedger: 2,
      limit: 2
    }),
    [events[1], events[0]]
  );

  const replacement = {
    ...events[0],
    value: 'updated'
  };

  await store.put(replacement);

  assert.deepEqual(
    await store.get('b'),
    replacement
  );

  assert.equal(
    await store.delete('b'),
    true
  );

  assert.equal(
    await store.delete('b'),
    false
  );

  assert.equal(
    await store.count(),
    2
  );

  await store.clear();

  assert.equal(
    await store.count(),
    0
  );

  if (typeof store.close === 'function') {
    store.close();
  }
}

test('memory event store satisfies the contract', async () => {
  const store = new MemoryEventStore();

  assert.equal(
    isEventStore(store),
    true
  );

  assert.equal(
    assertEventStore(store),
    store
  );

  await exerciseStore(
    async () => store
  );
});

test('memory store rejects invalid events', async () => {
  const store = new MemoryEventStore();

  await assert.rejects(
    () => store.put({ ledger: 1 }),
    TypeError
  );

  await assert.rejects(
    () => store.put({
      id: 'x',
      ledger: 0
    }),
    TypeError
  );

  await assert.rejects(
    () => store.get(''),
    TypeError
  );
});

test('memory store returns clones', async () => {
  const store = new MemoryEventStore();

  const event = {
    id: 'x',
    ledger: 1,
    nested: {
      value: 1
    }
  };

  await store.put(event);

  const loaded = await store.get('x');
  loaded.nested.value = 99;

  assert.equal(
    (await store.get('x')).nested.value,
    1
  );
});

test('invalid event stores are rejected', () => {
  assert.equal(
    isEventStore(null),
    false
  );

  assert.throws(
    () => assertEventStore({}),
    TypeError
  );
});

test('sqlite event store persists and queries events', async () => {
  const store = new SqliteEventStore();

  await exerciseStore(
    async () => store
  );
});

test('sqlite store persists across store instances', async () => {
  const store = new SqliteEventStore();

  await store.put({
    id: 'persistent',
    ledger: 42,
    value: 'hello'
  });

  const filename = './.soroban-events-store-test.sqlite';

  store.close();

  try {
    const first = new SqliteEventStore(filename);

    await first.put({
      id: 'persistent',
      ledger: 42,
      value: 'hello'
    });

    first.close();

    const second =
      new SqliteEventStore(filename);

    assert.deepEqual(
      await second.get('persistent'),
      {
        id: 'persistent',
        ledger: 42,
        value: 'hello'
      }
    );

    second.close();
  } finally {
    rmSync(filename, { force: true });
  }
});

test('sqlite store rejects invalid ranges', async () => {
  const store = new SqliteEventStore();

  await assert.rejects(
    () => store.list({
      startLedger: 5,
      endLedger: 4
    }),
    TypeError
  );

  await assert.rejects(
    () => store.list({
      limit: 0
    }),
    TypeError
  );

  store.close();
});

test('sqlite store rejects missing IDs', async () => {
  const store = new SqliteEventStore();

  await assert.rejects(
    () => store.get(''),
    TypeError
  );

  await assert.rejects(
    () => store.delete(''),
    TypeError
  );

  store.close();
});

test('event store error is an Error', () => {
  const error = new EventStoreError('storage failed');

  assert.equal(
    error.name,
    'EventStoreError'
  );

  assert.ok(
    error instanceof Error
  );
});
