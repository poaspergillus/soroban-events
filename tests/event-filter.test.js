import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createEventFilter,
  filterEvents,
  matchesEvent
} from '../src/index.js';

const events = [
  {
    id: '1',
    ledger: 100,
    contractId: 'C1',
    type: 'transfer',
    txHash: 'tx-a',
    topics: ['alice', 'bob']
  },
  {
    id: '2',
    ledger: 101,
    contractId: 'C1',
    type: 'mint',
    txHash: 'tx-b',
    topics: ['alice']
  },
  {
    id: '3',
    ledger: 102,
    contractId: 'C2',
    type: 'transfer',
    txHash: 'tx-c',
    topics: ['carol']
  }
];

test('filters by contract', () => {
  assert.deepEqual(
    filterEvents(events, {
      contractId: 'C1'
    }).map((event) => event.id),
    ['1', '2']
  );
});

test('filters by multiple contracts', () => {
  assert.deepEqual(
    filterEvents(events, {
      contractId: ['C1', 'C2']
    }).map((event) => event.id),
    ['1', '2', '3']
  );
});

test('filters by event type', () => {
  assert.deepEqual(
    filterEvents(events, {
      type: 'transfer'
    }).map((event) => event.id),
    ['1', '3']
  );
});

test('filters by transaction hash', () => {
  assert.deepEqual(
    filterEvents(events, {
      txHash: 'tx-b'
    }).map((event) => event.id),
    ['2']
  );
});

test('filters by exact ledger', () => {
  assert.deepEqual(
    filterEvents(events, {
      ledger: 101
    }).map((event) => event.id),
    ['2']
  );
});

test('filters by ledger range', () => {
  assert.deepEqual(
    filterEvents(events, {
      startLedger: 101,
      endLedger: 102
    }).map((event) => event.id),
    ['2', '3']
  );
});

test('filters by topic', () => {
  assert.deepEqual(
    filterEvents(events, {
      topic: 'alice'
    }).map((event) => event.id),
    ['1', '2']
  );
});

test('supports custom predicates', () => {
  assert.deepEqual(
    filterEvents(events, {
      predicate: (event) => event.ledger % 2 === 0
    }).map((event) => event.id),
    ['1', '3']
  );
});

test('combines multiple filter conditions', () => {
  assert.deepEqual(
    filterEvents(events, {
      contractId: 'C1',
      type: 'transfer',
      topic: 'bob'
    }).map((event) => event.id),
    ['1']
  );
});

test('matchesEvent checks one event', () => {
  assert.equal(
    matchesEvent(events[0], {
      contractId: 'C1',
      type: 'transfer'
    }),
    true
  );

  assert.equal(
    matchesEvent(events[0], {
      contractId: 'C2'
    }),
    false
  );
});

test('filter preserves event order', () => {
  const result = filterEvents(events, {
    predicate: () => true
  });

  assert.deepEqual(
    result.map((event) => event.id),
    ['1', '2', '3']
  );
});

test('invalid predicate is rejected', () => {
  assert.throws(
    () => createEventFilter({
      predicate: true
    }),
    /predicate must be a function/
  );
});

test('invalid ledger range is rejected', () => {
  assert.throws(
    () => createEventFilter({
      startLedger: 200,
      endLedger: 100
    }),
    /startLedger cannot exceed endLedger/
  );
});

test('invalid event collection is rejected', () => {
  assert.throws(
    () => filterEvents(null),
    /events must be an array/
  );
});

test('null events do not match', () => {
  assert.equal(
    matchesEvent(null, {}),
    false
  );
});
