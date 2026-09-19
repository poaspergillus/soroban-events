import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EventReplay,
  ReplayError,
  buildReplayWindows
} from '../src/replay.js';

import {
  MemoryCheckpointStore
} from '../src/checkpoint.js';

function makeStreamer(events, windowSize = 2) {
  return {
    windowSize,

    async getEventsWindowed({
      startLedger,
      endLedger
    }) {
      return events.filter(
        event =>
          event.ledger >= startLedger &&
          event.ledger <= endLedger
      );
    }
  };
}

test('buildReplayWindows creates inclusive windows', () => {
  assert.deepEqual(
    buildReplayWindows(1, 5, 2),
    [
      { startLedger: 1, endLedger: 2 },
      { startLedger: 3, endLedger: 4 },
      { startLedger: 5, endLedger: 5 }
    ]
  );
});

test('buildReplayWindows handles one ledger', () => {
  assert.deepEqual(
    buildReplayWindows(10, 10, 2),
    [
      { startLedger: 10, endLedger: 10 }
    ]
  );
});

test('buildReplayWindows rejects invalid ranges', () => {
  assert.throws(
    () => buildReplayWindows(5, 4, 2),
    ReplayError
  );
});

test('replay processes historical events in order', async () => {
  const streamer = makeStreamer([
    { id: '3', ledger: 3 },
    { id: '1', ledger: 1 },
    { id: '2', ledger: 2 }
  ]);

  const replay = new EventReplay(streamer);
  const seen = [];

  const result = await replay.run({
    startLedger: 1,
    endLedger: 3,
    onEvent: event => {
      seen.push(event.id);
    }
  });

  assert.deepEqual(
    seen,
    ['1', '2', '3']
  );

  assert.equal(result.eventsFetched, 3);
  assert.equal(result.eventsProcessed, 3);
  assert.equal(result.completed, true);
});

test('replay supports pipeline processing', async () => {
  const streamer = makeStreamer([
    { id: '1', ledger: 1 },
    { id: '2', ledger: 2 }
  ]);

  const replay = new EventReplay(streamer);
  const seen = [];

  const pipeline = {
    async process(event) {
      return {
        ...event,
        replayed: true
      };
    }
  };

  await replay.run({
    startLedger: 1,
    endLedger: 2,
    pipeline,
    onEvent: event => {
      seen.push(event);
    }
  });

  assert.deepEqual(
    seen,
    [
      { id: '1', ledger: 1, replayed: true },
      { id: '2', ledger: 2, replayed: true }
    ]
  );
});

test('pipeline can drop replay events', async () => {
  const streamer = makeStreamer([
    { id: '1', ledger: 1 },
    { id: '2', ledger: 2 }
  ]);

  const replay = new EventReplay(streamer);
  const seen = [];

  await replay.run({
    startLedger: 1,
    endLedger: 2,
    pipeline: {
      async process(event) {
        return event.id === '1'
          ? event
          : null;
      }
    },
    onEvent: event => {
      seen.push(event.id);
    }
  });

  assert.deepEqual(seen, ['1']);
});

test('replay checkpoint resumes from the saved ledger', async () => {
  const store = new MemoryCheckpointStore();

  await store.save(
    'replay',
    3
  );

  const streamer = makeStreamer([
    { id: '1', ledger: 1 },
    { id: '2', ledger: 2 },
    { id: '3', ledger: 3 },
    { id: '4', ledger: 4 }
  ]);

  const replay = new EventReplay(streamer, {
    checkpoint: store,
    checkpointKey: 'replay'
  });

  const seen = [];

  const result = await replay.run({
    startLedger: 1,
    endLedger: 4,
    onEvent: event => {
      seen.push(event.id);
    }
  });

  assert.deepEqual(
    seen,
    ['3', '4']
  );

  assert.equal(
    await store.load('replay'),
    5
  );

  assert.equal(
    result.resumedFrom,
    3
  );
});

test('replay does not advance checkpoint when event processing fails', async () => {
  const store = new MemoryCheckpointStore();

  const streamer = makeStreamer([
    { id: '1', ledger: 1 },
    { id: '2', ledger: 2 }
  ]);

  const replay = new EventReplay(streamer, {
    checkpoint: store,
    checkpointKey: 'replay'
  });

  await assert.rejects(
    () => replay.run({
      startLedger: 1,
      endLedger: 2,
      onEvent: event => {
        if (event.id === '2') {
          throw new Error('consumer failed');
        }
      }
    }),
    /consumer failed/
  );

  assert.equal(
    await store.load('replay'),
    null
  );
});

test('replay supports abort before work', async () => {
  const streamer = makeStreamer([
    { id: '1', ledger: 1 }
  ]);

  const replay = new EventReplay(streamer);
  const controller = new AbortController();

  controller.abort();

  const result = await replay.run({
    startLedger: 1,
    endLedger: 1,
    signal: controller.signal,
    onEvent: () => {
      throw new Error('must not run');
    }
  });

  assert.equal(result.completed, false);
  assert.equal(result.aborted, true);
});

test('replay returns completed when checkpoint is already past range', async () => {
  const store = new MemoryCheckpointStore();

  await store.save(
    'replay',
    10
  );

  const streamer = makeStreamer([]);

  const replay = new EventReplay(streamer, {
    checkpoint: store,
    checkpointKey: 'replay'
  });

  const result = await replay.run({
    startLedger: 1,
    endLedger: 5,
    onEvent: () => {
      throw new Error('must not run');
    }
  });

  assert.equal(result.completed, true);
  assert.equal(result.eventsFetched, 0);
});

test('replay validates required options', async () => {
  const streamer = makeStreamer([]);
  const replay = new EventReplay(streamer);

  await assert.rejects(
    () => replay.run({
      startLedger: 1,
      endLedger: 1
    }),
    TypeError
  );

  await assert.rejects(
    () => replay.run({
      startLedger: 0,
      endLedger: 1,
      onEvent: () => {}
    }),
    TypeError
  );

  await assert.rejects(
    () => replay.run({
      startLedger: 2,
      endLedger: 1,
      onEvent: () => {}
    }),
    ReplayError
  );
});

test('replay rejects an invalid streamer', () => {
  assert.throws(
    () => new EventReplay({}),
    TypeError
  );
});


test('replay persists events through an event store', async () => {
  const { MemoryEventStore } =
    await import('../src/store.js');

  const store = new MemoryEventStore();

  const streamer = {
    windowSize: 10,

    async getEventsWindowed() {
      return [
        {
          id: 'a',
          ledger: 1,
          transactionIndex: 0,
          operationIndex: 0
        },
        {
          id: 'b',
          ledger: 2,
          transactionIndex: 0,
          operationIndex: 0
        }
      ];
    }
  };

  const { EventReplay } =
    await import('../src/replay.js');

  const replay =
    new EventReplay(streamer);

  await replay.run({
    startLedger: 1,
    endLedger: 2,
    store,
    onEvent() {}
  });

  assert.equal(
    await store.count(),
    2
  );

  assert.deepEqual(
    await store.list(),
    [
      {
        id: 'a',
        ledger: 1,
        transactionIndex: 0,
        operationIndex: 0
      },
      {
        id: 'b',
        ledger: 2,
        transactionIndex: 0,
        operationIndex: 0
      }
    ]
  );
});

test('replay rejects an invalid event store', async () => {
  const streamer = {
    windowSize: 10,

    async getEventsWindowed() {
      return [];
    }
  };

  const { EventReplay } =
    await import('../src/replay.js');

  const replay =
    new EventReplay(streamer);

  await assert.rejects(
    () =>
      replay.run({
        startLedger: 1,
        endLedger: 1,
        store: {},
        onEvent() {}
      }),
    TypeError
  );
});
