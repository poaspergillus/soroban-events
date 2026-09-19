import test from 'node:test';
import assert from 'node:assert/strict';

import {
  compareEvents,
  orderBackfillEvents
} from '../src/backfill.js';

import {
  BackfillEngine,
  BackfillError,
  buildWindows,
  EventPipeline,
  MemoryCheckpointStore
} from '../src/index.js';

test('buildWindows creates inclusive ledger windows', () => {
  assert.deepEqual(
    buildWindows(1, 10, 4),
    [
      { startLedger: 1, endLedger: 4 },
      { startLedger: 5, endLedger: 8 },
      { startLedger: 9, endLedger: 10 }
    ]
  );
});

test('buildWindows handles a single ledger', () => {
  assert.deepEqual(
    buildWindows(50, 50, 10),
    [
      { startLedger: 50, endLedger: 50 }
    ]
  );
});

test('backfill processes every window', async () => {
  const calls = [];

  const streamer = {
    async getEventsWindowed(options) {
      calls.push({
        startLedger: options.startLedger,
        endLedger: options.endLedger
      });

      return [
        {
          id: `${options.startLedger}`,
          ledger: options.startLedger
        }
      ];
    }
  };

  const received = [];

  const engine = new BackfillEngine(
    streamer,
    {
      windowSize: 3
    }
  );

  const result = await engine.run({
    startLedger: 1,
    endLedger: 8,
    onEvent: event => {
      received.push(event);
    }
  });

  assert.equal(result.processed, 3);
  assert.equal(result.windows, 3);
  assert.equal(result.windowsCompleted, 3);

  assert.deepEqual(
    calls,
    [
      { startLedger: 1, endLedger: 3 },
      { startLedger: 4, endLedger: 6 },
      { startLedger: 7, endLedger: 8 }
    ]
  );

  assert.deepEqual(
    received.map(event => event.ledger),
    [1, 4, 7]
  );
});

test('backfill deduplicates event IDs', async () => {
  const streamer = {
    async getEventsWindowed({ startLedger }) {
      return [
        {
          id: 'same',
          ledger: startLedger
        },
        {
          id: `unique-${startLedger}`,
          ledger: startLedger
        }
      ];
    }
  };

  const received = [];

  const engine = new BackfillEngine(
    streamer,
    { windowSize: 1 }
  );

  const result = await engine.run({
    startLedger: 1,
    endLedger: 3,
    onEvent: event => {
      received.push(event);
    }
  });

  assert.equal(result.processed, 4);
  assert.equal(received.length, 4);

  assert.deepEqual(
    received.map(event => event.id),
    [
      'same',
      'unique-1',
      'unique-2',
      'unique-3'
    ]
  );
});

test('pipeline can filter and transform backfill events', async () => {
  const streamer = {
    async getEventsWindowed() {
      return [
        {
          id: 'a',
          ledger: 10,
          type: 'keep'
        },
        {
          id: 'b',
          ledger: 10,
          type: 'drop'
        }
      ];
    }
  };

  const pipeline = new EventPipeline()
    .filter(event => event.type === 'keep')
    .map(event => ({
      ...event,
      indexed: true
    }));

  const received = [];

  const engine = new BackfillEngine(streamer);

  const result = await engine.run({
    startLedger: 10,
    endLedger: 10,
    pipeline,
    onEvent: event => {
      received.push(event);
    }
  });

  assert.equal(result.processed, 1);

  assert.deepEqual(
    received,
    [
      {
        id: 'a',
        ledger: 10,
        type: 'keep',
        indexed: true
      }
    ]
  );
});

test('backfill reports progress', async () => {
  const progress = [];

  const streamer = {
    async getEventsWindowed({ startLedger }) {
      return [
        {
          id: String(startLedger),
          ledger: startLedger
        }
      ];
    }
  };

  const engine = new BackfillEngine(
    streamer,
    { windowSize: 2 }
  );

  await engine.run({
    startLedger: 1,
    endLedger: 5,
    onEvent: async () => {},
    onProgress: value => {
      progress.push({
        completed: value.windowsCompleted,
        total: value.windowsTotal,
        processed: value.processed
      });
    }
  });

  assert.deepEqual(
    progress,
    [
      { completed: 1, total: 3, processed: 1 },
      { completed: 2, total: 3, processed: 2 },
      { completed: 3, total: 3, processed: 3 }
    ]
  );
});

test('backfill checkpoint resumes from saved ledger', async () => {
  const store = new MemoryCheckpointStore();

  await store.save('default', 5);

  const calls = [];

  const streamer = {
    async getEventsWindowed({ startLedger }) {
      calls.push(startLedger);

      return [
        {
          id: String(startLedger),
          ledger: startLedger
        }
      ];
    }
  };

  const engine = new BackfillEngine(
    streamer,
    { windowSize: 2 }
  );

  const received = [];

  const result = await engine.run({
    startLedger: 1,
    endLedger: 8,
    checkpoint: {
      resumeFrom: async key =>
        store.load(key) ?? 1,
      save: async (ledger, key) =>
        store.save(key, ledger)
    },
    onEvent: event => {
      received.push(event);
    }
  });

  assert.equal(result.effectiveStart, 5);

  assert.deepEqual(
    calls,
    [5, 7]
  );

  assert.deepEqual(
    received.map(event => event.ledger),
    [5, 7]
  );

  assert.equal(
    await store.load('default'),
    9
  );
});

test('checkpoint is not advanced when window processing fails', async () => {
  const store = new MemoryCheckpointStore();

  const streamer = {
    async getEventsWindowed() {
      throw new Error('rpc failure');
    }
  };

  const engine = new BackfillEngine(streamer);

  await assert.rejects(
    () => engine.run({
      startLedger: 10,
      endLedger: 10,
      checkpoint: {
        resumeFrom: async () => 10,
        save: async (ledger, key) =>
          store.save(key, ledger)
      },
      onEvent: async () => {}
    }),
    /rpc failure/
  );

  assert.equal(
    await store.load('default'),
    null
  );
});

test('backfill aborts before starting work', async () => {
  const controller = new AbortController();
  controller.abort();

  const streamer = {
    async getEventsWindowed() {
      throw new Error('should not run');
    }
  };

  const engine = new BackfillEngine(streamer);

  await assert.rejects(
    () => engine.run({
      startLedger: 1,
      endLedger: 10,
      signal: controller.signal,
      onEvent: async () => {}
    }),
    error => {
      assert.equal(
        error instanceof BackfillError,
        true
      );

      assert.match(
        error.message,
        /aborted/
      );

      return true;
    }
  );
});

test('backfill validates options', async () => {
  const streamer = {
    async getEventsWindowed() {
      return [];
    }
  };

  assert.throws(
    () => new BackfillEngine(
      streamer,
      { concurrency: 0 }
    ),
    /concurrency/
  );

  assert.throws(
    () => new BackfillEngine(
      streamer,
      { windowSize: 0 }
    ),
    /windowSize/
  );

  const engine = new BackfillEngine(streamer);

  await assert.rejects(
    () => engine.run({
      startLedger: 10,
      endLedger: 1,
      onEvent: async () => {}
    }),
    /endLedger/
  );

  await assert.rejects(
    () => engine.run({
      startLedger: 1,
      endLedger: 1
    }),
    /onEvent/
  );

  await assert.rejects(
    () => engine.run({
      startLedger: 1,
      endLedger: 1,
      pipeline: {},
      onEvent: async () => {}
    }),
    /pipeline/
  );
});


test('concurrent backfill does not checkpoint past unfinished windows', async () => {
  const checkpoint = [];
  let releaseFirst;

  const firstWindow = new Promise(resolve => {
    releaseFirst = resolve;
  });

  const streamer = {
    async getEventsWindowed({ startLedger }) {
      if (startLedger === 1) {
        await firstWindow;
      }

      return [
        {
          id: String(startLedger),
          ledger: startLedger
        }
      ];
    }
  };

  const engine = new BackfillEngine(
    streamer,
    {
      windowSize: 1,
      concurrency: 2
    }
  );

  const run = engine.run({
    startLedger: 1,
    endLedger: 2,
    checkpoint: {
      resumeFrom: async () => 1,
      save: async ledger => {
        checkpoint.push(ledger);
      }
    },
    onEvent: async () => {}
  });

  // Let window 2 finish while window 1 is still blocked.
  await new Promise(resolve => setTimeout(resolve, 20));

  assert.deepEqual(
    checkpoint,
    []
  );

  releaseFirst();

  const result = await run;

  assert.equal(
    result.windowsCompleted,
    2
  );

  assert.deepEqual(
    checkpoint,
    [2, 3]
  );
});

test('concurrent backfill commits only the contiguous completed prefix', async () => {
  const checkpoint = [];

  let releaseSecond;

  const secondWindow = new Promise(resolve => {
    releaseSecond = resolve;
  });

  const streamer = {
    async getEventsWindowed({ startLedger }) {
      if (startLedger === 3) {
        await secondWindow;
      }

      return [
        {
          id: String(startLedger),
          ledger: startLedger
        }
      ];
    }
  };

  const engine = new BackfillEngine(
    streamer,
    {
      windowSize: 2,
      concurrency: 2
    }
  );

  const run = engine.run({
    startLedger: 1,
    endLedger: 6,
    checkpoint: {
      resumeFrom: async () => 1,
      save: async ledger => {
        checkpoint.push(ledger);
      }
    },
    onEvent: async () => {}
  });

  await new Promise(resolve => setTimeout(resolve, 20));

  // Window 1 (ledgers 1-2) can finish, but window 2
  // (ledgers 3-4) is still incomplete.
  assert.deepEqual(
    checkpoint,
    [3]
  );

  releaseSecond();

  const result = await run;

  assert.equal(
    result.windowsCompleted,
    3
  );

  assert.deepEqual(
    checkpoint,
    [3, 5, 7]
  );
});

test('concurrent backfill remains at-least-once after later-window failure', async () => {
  const checkpoint = [];

  const streamer = {
    async getEventsWindowed({ startLedger }) {
      if (startLedger === 3) {
        await new Promise(resolve =>
          setTimeout(resolve, 20)
        );

        throw new Error('window 2 failed');
      }

      await new Promise(resolve =>
        setTimeout(resolve, 5)
      );

      return [
        {
          id: String(startLedger),
          ledger: startLedger
        }
      ];
    }
  };

  const engine = new BackfillEngine(
    streamer,
    {
      windowSize: 2,
      concurrency: 2
    }
  );

  await assert.rejects(
    () => engine.run({
      startLedger: 1,
      endLedger: 6,
      checkpoint: {
        resumeFrom: async () => 1,
        save: async ledger => {
          checkpoint.push(ledger);
        }
      },
      onEvent: async () => {}
    }),
    /window 2 failed/
  );

  /*
   * Window 1 may have completed and can safely commit
   * ledger 3. The failed window must not be skipped.
   */
  assert.deepEqual(
    checkpoint,
    [3]
  );
});


test('compareEvents orders events by ledger', () => {
  const events = [
    { id: '3', ledger: 3 },
    { id: '1', ledger: 1 },
    { id: '2', ledger: 2 }
  ];

  events.sort(compareEvents);

  assert.deepEqual(
    events.map(event => event.ledger),
    [1, 2, 3]
  );
});

test('compareEvents uses transaction and operation order', () => {
  const events = [
    {
      id: 'b',
      ledger: 10,
      transactionIndex: 2,
      operationIndex: 1
    },
    {
      id: 'c',
      ledger: 10,
      transactionIndex: 2,
      operationIndex: 2
    },
    {
      id: 'a',
      ledger: 10,
      transactionIndex: 1,
      operationIndex: 5
    }
  ];

  events.sort(compareEvents);

  assert.deepEqual(
    events.map(event => event.id),
    ['a', 'b', 'c']
  );
});

test('compareEvents falls back to event ID', () => {
  const events = [
    { id: 'z', ledger: 10 },
    { id: 'a', ledger: 10 }
  ];

  events.sort(compareEvents);

  assert.deepEqual(
    events.map(event => event.id),
    ['a', 'z']
  );
});


test('orderBackfillEvents orders concurrent window results', () => {
  const result = orderBackfillEvents([
    [
      { id: '4', ledger: 4 },
      { id: '2', ledger: 2 }
    ],
    [
      { id: '3', ledger: 3 },
      { id: '1', ledger: 1 }
    ]
  ]);

  assert.deepEqual(
    result.map(event => event.ledger),
    [1, 2, 3, 4]
  );
});

test('orderBackfillEvents removes duplicate event IDs', () => {
  const result = orderBackfillEvents([
    [
      { id: 'same', ledger: 2 }
    ],
    [
      { id: 'same', ledger: 2 },
      { id: 'other', ledger: 3 }
    ]
  ]);

  assert.deepEqual(
    result.map(event => event.id),
    ['same', 'other']
  );
});

test('orderBackfillEvents preserves events without IDs', () => {
  const result = orderBackfillEvents([
    [
      { ledger: 2 },
      { ledger: 1 }
    ]
  ]);

  assert.deepEqual(
    result.map(event => event.ledger),
    [1, 2]
  );
});


test('backfill persists events through an event store', async () => {
  const { MemoryEventStore } =
    await import('../src/store.js');

  const store = new MemoryEventStore();

  const streamer = {
    windowSize: 1,

    async getEventsWindowed({ startLedger, endLedger }) {
      return [
        {
          id: `event-${startLedger}`,
          ledger: startLedger,
          transactionIndex: 0,
          operationIndex: 0
        }
      ];
    }
  };

  const { BackfillEngine } =
    await import('../src/backfill.js');

  const engine =
    new BackfillEngine(streamer);

  await engine.run({
    startLedger: 1,
    endLedger: 3,
    store,
    onEvent() {}
  });

  assert.equal(
    await store.count(),
    3
  );

  assert.deepEqual(
    await store.list(),
    [
      {
        id: 'event-1',
        ledger: 1,
        transactionIndex: 0,
        operationIndex: 0
      },
      {
        id: 'event-2',
        ledger: 2,
        transactionIndex: 0,
        operationIndex: 0
      },
      {
        id: 'event-3',
        ledger: 3,
        transactionIndex: 0,
        operationIndex: 0
      }
    ]
  );
});

test('backfill rejects an invalid event store', async () => {
  const streamer = {
    windowSize: 10,

    async getEventsWindowed() {
      return [];
    }
  };

  const { BackfillEngine } =
    await import('../src/backfill.js');

  const engine =
    new BackfillEngine(streamer);

  await assert.rejects(
    () =>
      engine.run({
        startLedger: 1,
        endLedger: 1,
        store: {}
      }),
    TypeError
  );
});
