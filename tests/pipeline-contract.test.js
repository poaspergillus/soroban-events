import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EventPipeline,
  PipelineError
} from '../src/index.js';

test('pipeline can filter events', async () => {
  const pipeline =
    new EventPipeline()
      .filter(
        event => event.keep === true
      );

  const accepted =
    await pipeline.process({
      id: 'a',
      keep: true
    });

  const rejected =
    await pipeline.process({
      id: 'b',
      keep: false
    });

  assert.deepEqual(
    accepted,
    {
      id: 'a',
      keep: true
    }
  );

  assert.equal(
    rejected,
    null
  );
});

test('pipeline maps events', async () => {
  const pipeline =
    new EventPipeline()
      .map(
        event => ({
          ...event,
          processed: true
        })
      );

  const result =
    await pipeline.process({
      id: 'a'
    });

  assert.deepEqual(
    result,
    {
      id: 'a',
      processed: true
    }
  );
});

test('pipeline tap observes without replacing event', async () => {
  const seen = [];

  const pipeline =
    new EventPipeline()
      .tap(
        event => {
          seen.push(event.id);
        }
      );

  const event = {
    id: 'a'
  };

  const result =
    await pipeline.process(event);

  assert.equal(
    result,
    event
  );

  assert.deepEqual(
    seen,
    ['a']
  );
});

test('pipeline supports chained stages', async () => {
  const pipeline =
    new EventPipeline()
      .filter(
        event => event.value > 1
      )
      .map(
        event => ({
          ...event,
          value: event.value * 2
        })
      )
      .tap(
        event => {
          event.seen = true;
        }
      );

  const result =
    await pipeline.process({
      id: 'a',
      value: 2
    });

  assert.deepEqual(
    result,
    {
      id: 'a',
      value: 4,
      seen: true
    }
  );
});

test('pipeline drops null results', async () => {
  const pipeline =
    new EventPipeline()
      .map(() => null);

  assert.equal(
    await pipeline.process({
      id: 'a'
    }),
    null
  );
});

test('pipeline preserves undefined stage results', async () => {
  const pipeline =
    new EventPipeline()
      .tap(() => undefined);

  const event = {
    id: 'a'
  };

  assert.equal(
    await pipeline.process(event),
    event
  );
});

test('pipeline batch processing preserves order', async () => {
  const pipeline =
    new EventPipeline()
      .map(
        event => event.value * 2
      );

  const result =
    await pipeline.processBatch([
      { value: 1 },
      { value: 2 },
      { value: 3 }
    ]);

  assert.deepEqual(
    result,
    [
      2,
      4,
      6
    ]
  );
});

test('pipeline wraps stage failures', async () => {
  const pipeline =
    new EventPipeline()
      .map(() => {
        throw new Error(
          'stage failed'
        );
      });

  await assert.rejects(
    () =>
      pipeline.process({
        id: 'a'
      }),
    error => {
      assert.ok(
        error instanceof PipelineError
      );

      assert.equal(
        error.message,
        'pipeline stage 0 failed'
      );

      assert.equal(
        error.cause?.message,
        'stage failed'
      );

      return true;
    }
  );
});
