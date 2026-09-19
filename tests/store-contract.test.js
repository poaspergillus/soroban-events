import test from 'node:test';
import assert from 'node:assert/strict';

import {
  MemoryEventStore,
  isEventStore,
  assertEventStore
} from '../src/index.js';

function event(id, ledger) {
  return {
    id,
    ledger,
    contractId: 'CABC',
    type: 'contract',
    topics: [],
    value: null
  };
}

test('MemoryEventStore starts empty', async () => {
  const store =
    new MemoryEventStore();

  assert.equal(
    await store.count(),
    0
  );

  assert.deepEqual(
    await store.list(),
    []
  );
});

test('MemoryEventStore stores and retrieves events', async () => {
  const store =
    new MemoryEventStore();

  const value =
    event('event-1', 10);

  await store.put(value);

  assert.equal(
    await store.has('event-1'),
    true
  );

  assert.deepEqual(
    await store.get('event-1'),
    value
  );

  assert.equal(
    await store.count(),
    1
  );
});

test('MemoryEventStore replaces duplicate event IDs', async () => {
  const store =
    new MemoryEventStore();

  await store.put(
    event('event-1', 10)
  );

  await store.put({
    ...event('event-1', 10),
    value: 'updated'
  });

  assert.equal(
    await store.count(),
    1
  );

  assert.equal(
    (await store.get('event-1')).value,
    'updated'
  );
});

test('MemoryEventStore orders events deterministically', async () => {
  const store =
    new MemoryEventStore();

  await store.put(
    event('event-3', 12)
  );

  await store.put(
    event('event-1', 10)
  );

  await store.put(
    event('event-2', 11)
  );

  const events =
    await store.list();

  assert.deepEqual(
    events.map(item => item.id),
    [
      'event-1',
      'event-2',
      'event-3'
    ]
  );
});

test('MemoryEventStore deletes events', async () => {
  const store =
    new MemoryEventStore();

  await store.put(
    event('event-1', 10)
  );

  assert.equal(
    await store.delete('event-1'),
    true
  );

  assert.equal(
    await store.has('event-1'),
    false
  );

  assert.equal(
    await store.delete('event-1'),
    false
  );
});

test('MemoryEventStore clears all events', async () => {
  const store =
    new MemoryEventStore();

  await store.put(
    event('event-1', 10)
  );

  await store.put(
    event('event-2', 11)
  );

  await store.clear();

  assert.equal(
    await store.count(),
    0
  );
});

test('event store contract helpers recognize MemoryEventStore', () => {
  const store =
    new MemoryEventStore();

  assert.equal(
    isEventStore(store),
    true
  );

  assert.equal(
    assertEventStore(store),
    store
  );
});

test('event store contract helper rejects invalid stores', () => {
  assert.equal(
    isEventStore({}),
    false
  );

  assert.throws(
    () => assertEventStore({}),
    TypeError
  );
});

test('MemoryEventStore rejects invalid events', async () => {
  const store =
    new MemoryEventStore();

  await assert.rejects(
    () => store.put({}),
    TypeError
  );
});
