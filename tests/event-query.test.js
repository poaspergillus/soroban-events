import assert from 'node:assert/strict';
import test from 'node:test';

import {
  validateEventQuery,
  queryEvents,
  createEventQuery
} from '../src/index.js';

const events = [
  {
    id: 'event-3',
    ledger: 102,
    transactionIndex: 1,
    operationIndex: 0,
    contractId: 'C2',
    type: 'transfer',
    txHash: 'tx-c'
  },
  {
    id: 'event-1',
    ledger: 100,
    transactionIndex: 0,
    operationIndex: 0,
    contractId: 'C1',
    type: 'transfer',
    txHash: 'tx-a'
  },
  {
    id: 'event-2',
    ledger: 101,
    transactionIndex: 0,
    operationIndex: 1,
    contractId: 'C1',
    type: 'mint',
    txHash: 'tx-b'
  }
];

test('query orders events deterministically', () => {
  const result = queryEvents(events);

  assert.deepEqual(
    result.events.map((event) => event.id),
    ['event-1', 'event-2', 'event-3']
  );

  assert.equal(result.total, 3);
});

test('query filters by ledger range', () => {
  const result = queryEvents(events, {
    startLedger: 101,
    endLedger: 102
  });

  assert.deepEqual(
    result.events.map((event) => event.id),
    ['event-2', 'event-3']
  );
});

test('query filters by contract', () => {
  const result = queryEvents(events, {
    contractId: 'C1'
  });

  assert.deepEqual(
    result.events.map((event) => event.id),
    ['event-1', 'event-2']
  );
});

test('query filters by event type', () => {
  const result = queryEvents(events, {
    type: 'mint'
  });

  assert.deepEqual(
    result.events.map((event) => event.id),
    ['event-2']
  );
});

test('query filters by transaction hash', () => {
  const result = queryEvents(events, {
    txHash: 'tx-c'
  });

  assert.deepEqual(
    result.events.map((event) => event.id),
    ['event-3']
  );
});

test('query supports pagination', () => {
  const first = queryEvents(events, {
    limit: 2
  });

  assert.deepEqual(
    first.events.map((event) => event.id),
    ['event-1', 'event-2']
  );

  assert.equal(first.total, 3);
  assert.equal(first.hasMore, true);

  const second = queryEvents(events, {
    limit: 2,
    offset: 2
  });

  assert.deepEqual(
    second.events.map((event) => event.id),
    ['event-3']
  );

  assert.equal(second.hasMore, false);
});

test('query defaults to 100 events', () => {
  const result = queryEvents(
    Array.from(
      { length: 150 },
      (_, index) => ({
        id: String(index),
        ledger: index + 1
      })
    )
  );

  assert.equal(result.events.length, 100);
  assert.equal(result.total, 150);
  assert.equal(result.hasMore, true);
});

test('query validates ledger range', () => {
  assert.throws(
    () => validateEventQuery({
      startLedger: 20,
      endLedger: 10
    }),
    /startLedger cannot exceed endLedger/
  );
});

test('query validates pagination', () => {
  assert.throws(
    () => validateEventQuery({
      limit: 0
    }),
    /limit must be a positive safe integer/
  );

  assert.throws(
    () => validateEventQuery({
      offset: -1
    }),
    /offset must be a non-negative safe integer/
  );
});

test('query rejects non-array input', () => {
  assert.throws(
    () => queryEvents(null),
    /events must be an array/
  );
});

test('createEventQuery uses an event store', async () => {
  const store = {
    async list() {
      return events;
    }
  };

  const query = createEventQuery(store);

  const result = await query({
    contractId: 'C1'
  });

  assert.deepEqual(
    result.events.map((event) => event.id),
    ['event-1', 'event-2']
  );
});

test('createEventQuery rejects an invalid store', () => {
  assert.throws(
    () => createEventQuery({}),
    /event store must provide list/
  );
});
