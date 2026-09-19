import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EventHandoff,
  MemoryCheckpointStore
} from '../src/index.js';

function event(id, ledger) {
  return {
    id,
    ledger,
    type: 'contract',
    topics: [],
    value: null
  };
}

test('EventHandoff establishes a boundary', async () => {
  const checkpoint =
    new MemoryCheckpointStore();

  const handoff =
    new EventHandoff({
      checkpoint,
      checkpointKey: 'live'
    });

  const result =
    await handoff.initialize(100);

  assert.equal(
    result,
    100
  );
});

test('EventHandoff rejects events before boundary', async () => {
  const handoff =
    new EventHandoff();

  await handoff.initialize(100);

  assert.equal(
    await handoff.accept(
      event('old', 99)
    ),
    null
  );
});

test('EventHandoff accepts boundary event once', async () => {
  const handoff =
    new EventHandoff();

  await handoff.initialize(100);

  const first =
    await handoff.accept(
      event('boundary', 100)
    );

  const second =
    await handoff.accept(
      event('boundary', 100)
    );

  assert.deepEqual(
    first,
    event('boundary', 100)
  );

  assert.equal(
    second,
    null
  );
});

test('EventHandoff accepts unique events after boundary', async () => {
  const handoff =
    new EventHandoff();

  await handoff.initialize(100);

  assert.deepEqual(
    await handoff.accept(
      event('event-101', 101)
    ),
    event('event-101', 101)
  );

  assert.deepEqual(
    await handoff.accept(
      event('event-102', 102)
    ),
    event('event-102', 102)
  );
});

test('EventHandoff deduplicates repeated IDs', async () => {
  const handoff =
    new EventHandoff();

  await handoff.initialize(100);

  const events = [
    event('a', 101),
    event('a', 101),
    event('b', 102)
  ];

  const accepted =
    await handoff.acceptMany(events);

  assert.equal(
    accepted.length,
    2
  );

  assert.deepEqual(
    accepted.map(item => item.id),
    [
      'a',
      'b'
    ]
  );
});

test('EventHandoff commits checkpoint', async () => {
  const checkpoint =
    new MemoryCheckpointStore();

  const handoff =
    new EventHandoff({
      checkpoint,
      checkpointKey: 'live'
    });

  await handoff.initialize(100);

  await handoff.commit(100);

  assert.equal(
    await checkpoint.load('live'),
    100
  );
});

test('EventHandoff rejects commit before boundary', async () => {
  const handoff =
    new EventHandoff();

  await handoff.initialize(100);

  await assert.rejects(
    () =>
      handoff.commit(99),
    error => {
      assert.equal(
        error.name,
        'HandoffError'
      );

      return true;
    }
  );
});

test('EventHandoff can reset deduplication state', async () => {
  const handoff =
    new EventHandoff();

  await handoff.initialize(100);

  assert.deepEqual(
    await handoff.accept(
      event('same', 101)
    ),
    event('same', 101)
  );

  handoff.resetSeen();

  assert.deepEqual(
    await handoff.accept(
      event('same', 101)
    ),
    event('same', 101)
  );
});
