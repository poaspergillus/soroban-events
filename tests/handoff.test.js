import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EventHandoff,
  HandoffError
} from '../src/handoff.js';

import {
  MemoryCheckpointStore
} from '../src/checkpoint.js';

test('handoff establishes a backfill/live boundary', async () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  assert.equal(
    await handoff.initialize(),
    100
  );

  assert.equal(handoff.boundaryLedger, 100);
});

test('events before the boundary are rejected', () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  assert.equal(
    handoff.accept({
      id: 'old',
      ledger: 99
    }),
    null
  );
});

test('boundary events are accepted once', () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  const event = {
    id: 'boundary-1',
    ledger: 100
  };

  assert.deepEqual(
    handoff.accept(event),
    event
  );

  assert.equal(
    handoff.accept(event),
    null
  );
});

test('events after the boundary are accepted', () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  assert.equal(
    handoff.accept({
      id: '101',
      ledger: 101
    }).ledger,
    101
  );
});

test('duplicate IDs are removed across the handoff', () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  const event = {
    id: 'same',
    ledger: 100
  };

  assert.ok(handoff.accept(event));
  assert.equal(
    handoff.accept({
      id: 'same',
      ledger: 101
    }),
    null
  );
});

test('events without IDs remain usable', () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  assert.deepEqual(
    handoff.accept({
      ledger: 101,
      value: 'x'
    }),
    {
      ledger: 101,
      value: 'x'
    }
  );
});

test('acceptMany preserves accepted order', () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  const result = handoff.acceptMany([
    { id: 'old', ledger: 99 },
    { id: 'a', ledger: 100 },
    { id: 'a', ledger: 101 },
    { id: 'b', ledger: 102 }
  ]);

  assert.deepEqual(
    result.map(event => event.id),
    ['a', 'b']
  );
});

test('checkpoint state can establish the boundary after restart', async () => {
  const store = new MemoryCheckpointStore();

  await store.save(
    'consumer',
    105
  );

  const handoff = new EventHandoff({
    checkpoint: store,
    checkpointKey: 'consumer',
    boundaryLedger: 100
  });

  assert.equal(
    await handoff.initialize(),
    105
  );

  assert.equal(
    handoff.accept({
      id: '104',
      ledger: 104
    }),
    null
  );

  assert.equal(
    handoff.accept({
      id: '105',
      ledger: 105
    }).ledger,
    105
  );
});

test('commit persists the handoff checkpoint', async () => {
  const store = new MemoryCheckpointStore();

  const handoff = new EventHandoff({
    checkpoint: store,
    checkpointKey: 'consumer',
    boundaryLedger: 100
  });

  await handoff.commit(101);

  assert.equal(
    await store.load('consumer'),
    101
  );

  assert.equal(
    handoff.boundaryLedger,
    101
  );
});

test('commit cannot move before the handoff boundary', async () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  await assert.rejects(
    () => handoff.commit(99),
    HandoffError
  );
});

test('checkpoint keys remain isolated', async () => {
  const store = new MemoryCheckpointStore();

  const a = new EventHandoff({
    checkpoint: store,
    checkpointKey: 'a',
    boundaryLedger: 10
  });

  const b = new EventHandoff({
    checkpoint: store,
    checkpointKey: 'b',
    boundaryLedger: 20
  });

  await a.commit(11);
  await b.commit(21);

  assert.equal(
    await store.load('a'),
    11
  );

  assert.equal(
    await store.load('b'),
    21
  );
});

test('resetSeen allows a fresh processing session', () => {
  const handoff = new EventHandoff({
    boundaryLedger: 100
  });

  const event = {
    id: 'same',
    ledger: 101
  };

  assert.ok(handoff.accept(event));
  assert.equal(handoff.accept(event), null);

  handoff.resetSeen();

  assert.deepEqual(
    handoff.accept(event),
    event
  );
});
