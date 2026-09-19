import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DeadLetterError,
  MemoryDeadLetterQueue,
  isDeadLetterQueue
} from '../src/index.js';

test('dead-letter queue stores failed events', async () => {
  const queue = new MemoryDeadLetterQueue();

  const error = new Error('handler failed');
  error.code = 'HANDLER_FAILED';

  const entry = await queue.add({
    event: {
      id: 'event-1',
      ledger: 123
    },
    error,
    context: {
      consumer: 'payments'
    }
  });

  assert.equal(entry.id, 'event-1');
  assert.equal(entry.event.ledger, 123);
  assert.equal(entry.error.message, 'handler failed');
  assert.equal(entry.error.code, 'HANDLER_FAILED');
  assert.equal(entry.context.consumer, 'payments');
  assert.equal(entry.attempts, 1);
  assert.ok(entry.createdAt);
});

test('dead-letter queue can retrieve and remove an entry', async () => {
  const queue = new MemoryDeadLetterQueue();

  await queue.add({
    event: { id: 'event-2' },
    error: new Error('boom')
  });

  assert.equal(
    (await queue.get('event-2')).id,
    'event-2'
  );

  assert.equal(
    await queue.remove('event-2'),
    true
  );

  assert.equal(
    await queue.get('event-2'),
    undefined
  );
});

test('dead-letter queue lists entries in insertion order', async () => {
  const queue = new MemoryDeadLetterQueue();

  await queue.add({
    event: { id: 'a' },
    error: new Error('a')
  });

  await queue.add({
    event: { id: 'b' },
    error: new Error('b')
  });

  const entries = await queue.list();

  assert.deepEqual(
    entries.map((entry) => entry.id),
    ['a', 'b']
  );
});

test('dead-letter retry increments attempts', async () => {
  const queue = new MemoryDeadLetterQueue();

  await queue.add({
    event: { id: 'retry-me' },
    error: new Error('temporary failure'),
    attempts: 2
  });

  const entry = await queue.retry('retry-me');

  assert.equal(entry.attempts, 3);
});

test('dead-letter queue enforces maximum entries', async () => {
  const queue = new MemoryDeadLetterQueue({
    maxEntries: 2
  });

  await queue.add({
    event: { id: 'one' },
    error: new Error('one')
  });

  await queue.add({
    event: { id: 'two' },
    error: new Error('two')
  });

  await queue.add({
    event: { id: 'three' },
    error: new Error('three')
  });

  assert.equal(await queue.get('one'), undefined);
  assert.ok(await queue.get('two'));
  assert.ok(await queue.get('three'));
  assert.equal(queue.size, 2);
});

test('dead-letter entries are cloned', async () => {
  const queue = new MemoryDeadLetterQueue();

  const event = {
    id: 'clone-me',
    payload: {
      value: 1
    }
  };

  await queue.add({
    event,
    error: new Error('failure')
  });

  event.payload.value = 999;

  const stored = await queue.get('clone-me');

  assert.equal(
    stored.event.payload.value,
    1
  );
});

test('dead-letter queue recognizes the queue contract', () => {
  const queue = new MemoryDeadLetterQueue();

  assert.equal(
    isDeadLetterQueue(queue),
    true
  );

  assert.equal(
    isDeadLetterQueue({}),
    false
  );
});

test('DeadLetterError preserves cause and event', () => {
  const cause = new Error('root failure');
  const event = { id: 'event-9' };

  const error = new DeadLetterError(
    'event processing failed',
    {
      cause,
      event
    }
  );

  assert.equal(
    error.name,
    'DeadLetterError'
  );

  assert.equal(
    error.message,
    'event processing failed'
  );

  assert.equal(error.cause, cause);
  assert.equal(error.event, event);
});
