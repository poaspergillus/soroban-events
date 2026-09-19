import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EventPipeline,
  PipelineError,
  SorobanEventStreamer,
  MemoryCheckpointStore
} from '../src/index.js';

test('empty pipeline returns original event', async () => {
  const pipeline = new EventPipeline();
  const event = { id: '1', ledger: 10 };

  assert.equal(pipeline.size, 0);
  assert.equal(
    await pipeline.process(event),
    event
  );
});

test('pipeline stages run in order', async () => {
  const calls = [];

  const pipeline = new EventPipeline()
    .use(event => {
      calls.push('first');
      return {
        ...event,
        value: event.value + 1
      };
    })
    .use(event => {
      calls.push('second');
      return {
        ...event,
        value: event.value * 2
      };
    });

  assert.deepEqual(
    await pipeline.process({ value: 3 }),
    { value: 8 }
  );

  assert.deepEqual(
    calls,
    ['first', 'second']
  );
});

test('undefined keeps current event', async () => {
  const pipeline = new EventPipeline([
    event => {
      event.seen = true;
    }
  ]);

  const event = { id: 'x' };

  assert.equal(
    await pipeline.process(event),
    event
  );

  assert.equal(event.seen, true);
});

test('null drops event', async () => {
  const pipeline = new EventPipeline()
    .filter(event => event.type === 'wanted');

  assert.equal(
    await pipeline.process({
      type: 'ignored'
    }),
    null
  );

  assert.deepEqual(
    await pipeline.process({
      type: 'wanted',
      id: '1'
    }),
    {
      type: 'wanted',
      id: '1'
    }
  );
});

test('map transforms events', async () => {
  const pipeline = new EventPipeline()
    .map(event => ({
      ...event,
      normalized: true
    }));

  assert.deepEqual(
    await pipeline.process({ id: 'a' }),
    {
      id: 'a',
      normalized: true
    }
  );
});

test('tap observes without changing event', async () => {
  const seen = [];
  const event = { id: 'abc' };

  const pipeline = new EventPipeline()
    .tap(event => seen.push(event.id));

  assert.equal(
    await pipeline.process(event),
    event
  );

  assert.deepEqual(seen, ['abc']);
});

test('context is shared with stages', async () => {
  const pipeline = new EventPipeline()
    .map((event, context) => ({
      ...event,
      worker: context.worker
    }));

  assert.deepEqual(
    await pipeline.process(
      { id: '1' },
      { worker: 'payments' }
    ),
    {
      id: '1',
      worker: 'payments'
    }
  );
});

test('processBatch preserves order and removes drops', async () => {
  const pipeline = new EventPipeline()
    .filter(event => event.keep)
    .map(event => ({
      ...event,
      processed: true
    }));

  assert.deepEqual(
    await pipeline.processBatch([
      { id: 'a', keep: true },
      { id: 'b', keep: false },
      { id: 'c', keep: true }
    ]),
    [
      {
        id: 'a',
        keep: true,
        processed: true
      },
      {
        id: 'c',
        keep: true,
        processed: true
      }
    ]
  );
});

test('stage errors become PipelineError', async () => {
  const cause = new Error('boom');

  const pipeline = new EventPipeline()
    .use(() => {
      throw cause;
    });

  await assert.rejects(
    () => pipeline.process({ id: '1' }),
    error => {
      assert.equal(
        error instanceof PipelineError,
        true
      );

      assert.match(
        error.message,
        /stage 0 failed/
      );

      assert.equal(error.cause, cause);

      return true;
    }
  );
});

test('PipelineError is not double wrapped', async () => {
  const error =
    new PipelineError('intentional');

  const pipeline = new EventPipeline()
    .use(() => {
      throw error;
    });

  await assert.rejects(
    () => pipeline.process({ id: '1' }),
    value => value === error
  );
});

test('invalid pipeline configuration is rejected', () => {
  assert.throws(
    () => new EventPipeline([null]),
    /pipeline stage must be a function/
  );

  assert.throws(
    () => new EventPipeline().use('bad'),
    /pipeline stage must be a function/
  );

  assert.throws(
    () => new EventPipeline().filter('bad'),
    /pipeline filter/
  );

  assert.throws(
    () => new EventPipeline().map('bad'),
    /pipeline mapper/
  );

  assert.throws(
    () => new EventPipeline().tap('bad'),
    /pipeline tap/
  );

  return assert.rejects(
    () => new EventPipeline().processBatch(null),
    /pipeline batch/
  );
});

test('clear removes all stages', async () => {
  const pipeline = new EventPipeline()
    .map(event => ({
      ...event,
      changed: true
    }));

  assert.equal(pipeline.size, 1);

  pipeline.clear();

  assert.equal(pipeline.size, 0);

  assert.deepEqual(
    await pipeline.process({ id: '1' }),
    { id: '1' }
  );
});

function fakeStreamer(events) {
  const streamer =
    new SorobanEventStreamer(
      'https://rpc.test',
      { pollInterval: 1 }
    );

  let calls = 0;

  streamer.getLatestLedger =
    async () => {
      calls++;
      return 20;
    };

  streamer.getEventsWindowed =
    async ({ startLedger }) => {
      // Return the fixture once. On later polling cycles, return
      // no events so consume() can advance into its wait path.
      if (startLedger > 20) return [];
      if (calls > 2) return [];
      return events;
    };

  return streamer;
}

test('consume sends events through pipeline', async () => {
  const received = [];

  const pipeline = new EventPipeline()
    .filter(event => event.type === 'wanted')
    .map(event => ({
      ...event,
      normalized: true
    }));

  const streamer = fakeStreamer([
    {
      id: '1',
      ledger: 10,
      type: 'wanted'
    },
    {
      id: '2',
      ledger: 10,
      type: 'ignored'
    },
    {
      id: '3',
      ledger: 11,
      type: 'wanted'
    }
  ]);

  const count = await streamer.consume({
    startLedger: 10,
    pipeline,
    onEvent: event => received.push(event),
    maxEvents: 2
  });

  assert.equal(count, 2);

  assert.deepEqual(
    received.map(event => event.id),
    ['1', '3']
  );

  assert.equal(
    received[0].normalized,
    true
  );
});

test('pipeline failure prevents current ledger checkpoint', async () => {
  const store =
    new MemoryCheckpointStore();

  const pipeline = new EventPipeline()
    .use(event => {
      if (event.id === '2') {
        throw new Error('pipeline failed');
      }

      return event;
    });

  const streamer = fakeStreamer([
    { id: '1', ledger: 10 },
    { id: '2', ledger: 10 }
  ]);

  await assert.rejects(
    () => streamer.consume({
      startLedger: 10,
      pipeline,
      checkpoint: {
        resumeFrom: async () => 10,
        save: async (ledger) =>
          store.save('default', ledger)
      },
      onEvent: async () => {}
    }),
    /pipeline stage 0 failed/
  );

  assert.equal(
    await store.load('default'),
    null
  );
});

test('pipeline receives consume context', async () => {
  let contextSeen;

  const pipeline = new EventPipeline()
    .tap((event, context) => {
      contextSeen = context;
    });

  const streamer = fakeStreamer([
    { id: '1', ledger: 10 }
  ]);

  await streamer.consume({
    startLedger: 10,
    checkpointKey: 'payments',
    pipeline,
    onEvent: async () => {},
    maxEvents: 1
  });

  assert.equal(
    contextSeen.checkpointKey,
    'payments'
  );

  assert.equal(
    contextSeen.ledger,
    10
  );
});
