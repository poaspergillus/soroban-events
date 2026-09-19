import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EventEngine,
  EventEngineError,
  Metrics
} from '../src/index.js';

function makeStreamer() {
  return {
    async getLatestLedger(options = {}) {
      if (options.metadata) {
        return {
          sequence: 100,
          hash: 'hash-100',
          closeTime: null,
          protocolVersion: null
        };
      }

      return 100;
    },

    async getEventsWindowed() {
      return [];
    },

    async getLedgerHistory({
      startLedger,
      endLedger
    }) {
      const ledgers = [];

      for (
        let sequence = startLedger;
        sequence <= endLedger;
        sequence++
      ) {
        ledgers.push({
          sequence,
          hash: `hash-${sequence}`
        });
      }

      return {
        startLedger,
        endLedger,
        count: ledgers.length,
        ledgers,
        last: ledgers.at(-1)
      };
    },

    async consume() {
      return {
        processed: 0
      };
    }
  };
}

test('EventEngine exposes orchestration components', () => {
  const engine =
    new EventEngine(
      makeStreamer()
    );

  assert.ok(
    engine.backfillEngine
  );

  assert.ok(
    engine.replayEngine
  );

  assert.ok(
    engine.handoff
  );

  assert.ok(
    engine.recovery
  );

  assert.ok(
    engine.monitor
  );

  assert.ok(
    engine.metrics instanceof Metrics
  );
});

test('EventEngine starts idle', () => {
  const engine =
    new EventEngine(
      makeStreamer()
    );

  assert.equal(
    engine.state,
    'idle'
  );

  assert.equal(
    engine.signal,
    null
  );
});

test('EventEngine lifecycle can start and stop', () => {
  const engine =
    new EventEngine(
      makeStreamer()
    );

  const signal =
    engine.startLifecycle();

  assert.equal(
    engine.state,
    'running'
  );

  assert.equal(
    engine.signal,
    signal
  );

  assert.equal(
    signal.aborted,
    false
  );

  assert.equal(
    engine.stop(),
    true
  );

  assert.equal(
    engine.state,
    'stopped'
  );

  assert.equal(
    signal.aborted,
    true
  );
});

test('EventEngine reset returns lifecycle to idle', () => {
  const engine =
    new EventEngine(
      makeStreamer()
    );

  engine.startLifecycle();
  engine.stop();

  assert.equal(
    engine.reset(),
    engine
  );

  assert.equal(
    engine.state,
    'idle'
  );
});

test('EventEngine rejects invalid streamer', () => {
  assert.throws(
    () => new EventEngine({}),
    TypeError
  );
});

test('EventEngine backfill delegates to BackfillEngine', async () => {
  const calls = [];

  const backfill = {
    async run(options) {
      calls.push(options);

      return {
        ok: true
      };
    }
  };

  const engine =
    new EventEngine(
      makeStreamer(),
      {
        backfill
      }
    );

  const result =
    await engine.backfill({
      startLedger: 1,
      endLedger: 10
    });

  assert.deepEqual(
    result,
    {
      ok: true
    }
  );

  assert.equal(
    calls.length,
    1
  );

  assert.equal(
    calls[0].startLedger,
    1
  );

  assert.equal(
    calls[0].endLedger,
    10
  );
});

test('EventEngine recover delegates to recovery', async () => {
  const calls = [];

  const recovery = {
    detect({ expected, actual }) {
      return {
        reorg:
          expected.hash !== actual.hash,
        reason:
          expected.hash !== actual.hash
            ? 'hash-mismatch'
            : 'same-hash',
        sequence: actual.sequence
      };
    },

    async recover(options) {
      calls.push(options);

      return {
        recovered: true
      };
    }
  };

  const engine =
    new EventEngine(
      makeStreamer(),
      {
        recovery
      }
    );

  const result =
    await engine.recover({
      affectedLedger: 50
    });

  assert.deepEqual(
    result,
    {
      recovered: true
    }
  );

  assert.deepEqual(
    calls,
    [
      {
        affectedLedger: 50
      }
    ]
  );
});

test('EventEngine watchReorg delegates to monitor', async () => {
  const calls = [];

  const monitor = {
    async monitor(options) {
      calls.push(options);

      return {
        watched: true
      };
    }
  };

  const engine =
    new EventEngine(
      makeStreamer(),
      {
        monitor
      }
    );

  const result =
    await engine.watchReorg({
      getExpected: async () => ({
        sequence: 100,
        hash: 'hash-100'
      }),
      stopOnRecovery: true
    });

  assert.deepEqual(
    result,
    {
      watched: true
    }
  );

  assert.equal(
    calls.length,
    1
  );

  assert.equal(
    calls[0].stopOnRecovery,
    true
  );
});

test('EventEngine propagates backfill failures', async () => {
  const failure =
    new Error('backfill failed');

  const backfill = {
    async run() {
      throw failure;
    }
  };

  const engine =
    new EventEngine(
      makeStreamer(),
      {
        backfill
      }
    );

  await assert.rejects(
    () =>
      engine.backfill({
        startLedger: 1,
        endLedger: 10
      }),
    error => error === failure
  );
});
