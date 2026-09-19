import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EventReplay,
  MemoryCheckpointStore,
  MemoryEventStore
} from '../src/index.js';

function makeStreamer() {
  const calls = [];

  return {
    calls,

    async getEventsWindowed(options) {
      calls.push(options);

      const output = [];

      for (
        let ledger = options.startLedger;
        ledger <= options.endLedger;
        ledger++
      ) {
        output.push({
          id: `event-${ledger}`,
          ledger,
          type: 'contract',
          topics: [],
          value: null
        });
      }

      return output;
    }
  };
}

test('EventReplay validates streamer', () => {
  assert.throws(
    () =>
      new EventReplay(
        {},
        {}
      ),
    TypeError
  );
});

test('EventReplay processes a historical range', async () => {
  const streamer =
    makeStreamer();

  const store =
    new MemoryEventStore();

  const replay =
    new EventReplay(
      streamer,
      {
        checkpoint:
          new MemoryCheckpointStore()
      }
    );

  const seen = [];

  const result =
    await replay.run({
      startLedger: 10,
      endLedger: 12,
      windowSize: 1,
      store,
      onEvent: event => {
        seen.push(event);
      }
    });

  assert.ok(result);
  assert.equal(
    seen.length,
    3
  );

  assert.equal(
    await store.count(),
    3
  );

  assert.ok(
    streamer.calls.length >= 1
  );
});

test('EventReplay rejects missing range', async () => {
  const replay =
    new EventReplay(
      makeStreamer()
    );

  await assert.rejects(
    () =>
      replay.run({}),
    TypeError
  );
});
