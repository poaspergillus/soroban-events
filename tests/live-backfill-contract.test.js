import test from 'node:test';
import assert from 'node:assert/strict';

import {
  LiveBackfillEngine
} from '../src/index.js';

function makeStreamer() {
  return {
    async getLatestLedger() {
      return {
        sequence: 100,
        hash: 'hash-100',
        closeTime: null,
        protocolVersion: null
      };
    },

    async getEventsWindowed() {
      return [];
    },

    async consume() {
      return {
        processed: 0
      };
    }
  };
}

test('LiveBackfillEngine validates streamer', () => {
  assert.throws(
    () =>
      new LiveBackfillEngine(
        {},
        {}
      ),
    TypeError
  );
});

test('LiveBackfillEngine can establish a boundary', async () => {
  const engine =
    new LiveBackfillEngine(
      makeStreamer()
    );

  const result =
    await engine.establishBoundary();

  assert.equal(
    result.sequence,
    100
  );
});

test('LiveBackfillEngine can backfill to a boundary', async () => {
  const calls = [];

  const backfill = {
    async run(options) {
      calls.push(options);

      return {
        processed: 0
      };
    }
  };

  const engine =
    new LiveBackfillEngine(
      makeStreamer(),
      {
        backfill
      }
    );

  await engine.establishBoundary();

  const result =
    await engine.backfillToBoundary({
      startLedger: 90,
      boundaryLedger: 100
    });

  assert.equal(
    result.processed,
    0
  );

  assert.equal(
    calls.length,
    1
  );

  assert.equal(
    calls[0].endLedger,
    100
  );
});

test('LiveBackfillEngine commits its boundary', async () => {
  const committed = [];

  const handoff = {
    async initialize() {},

    async accept(event) {
      return event;
    },

    async commit(ledger) {
      committed.push(ledger);
      return ledger;
    }
  };

  const engine =
    new LiveBackfillEngine(
      makeStreamer(),
      {
        handoff
      }
    );

  await engine.establishBoundary();

  const result =
    await engine.commitBoundary(100);

  assert.equal(
    result,
    100
  );

  assert.deepEqual(
    committed,
    [100]
  );
});

test('consumeLive delivers events through onEvent and handler', async () => {
  const event = {
    id: 'live-1',
    ledger: 100
  };

  const accepted = [];
  const onEvents = [];
  const handled = [];

  const streamer = {
    async getLatestLedger() {
      return {
        sequence: 100,
        hash: 'hash-100',
        closeTime: null,
        protocolVersion: null
      };
    },

    async getEventsWindowed() {
      return [];
    },

    async consume(options) {
      assert.equal(
        typeof options.onEvent,
        'function'
      );

      await options.onEvent(event, {
        ledger: 100
      });

      return 1;
    }
  };

  const handoff = {
    async initialize() {},
    async accept(value) {
      accepted.push(value);
      return true;
    },
    async commit() {}
  };

  const engine =
    new LiveBackfillEngine(
      streamer,
      { handoff }
    );

  const result =
    await engine.consumeLive({
      startLedger: 100,
      onEvent: async value => {
        onEvents.push(value);
      },
      handler: async value => {
        handled.push(value);
      }
    });

  assert.equal(result, 1);
  assert.deepEqual(
    accepted,
    [event]
  );
  assert.deepEqual(
    onEvents,
    [event]
  );
  assert.deepEqual(
    handled,
    [event]
  );
});

test('LiveBackfillEngine can run its setup sequence', async () => {
  const calls = [];

  const backfill = {
    async run(options) {
      calls.push([
        'backfill',
        options.endLedger
      ]);

      return {
        processed: 0
      };
    }
  };

  const handoff = {
    async initialize(ledger) {
      calls.push([
        'initialize',
        ledger
      ]);
    },

    async accept(event) {
      return event;
    },

    async commit(ledger) {
      calls.push([
        'commit',
        ledger
      ]);
    }
  };

  const engine =
    new LiveBackfillEngine(
      makeStreamer(),
      {
        backfill,
        handoff
      }
    );

  await engine.establishBoundary();
  await engine.backfillToBoundary({
    startLedger: 90,
    boundaryLedger: 100
  });
  await engine.commitBoundary(100);

  assert.deepEqual(
    calls,
    [
      ['initialize', 100],
      ['backfill', 100],
      ['commit', 100]
    ]
  );
});
