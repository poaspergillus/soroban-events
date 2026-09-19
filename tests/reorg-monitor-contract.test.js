import test from 'node:test';
import assert from 'node:assert/strict';

import {
  ReorgMonitor,
  ReorgMonitorError
} from '../src/index.js';

function makeStreamer() {
  return {
    async getLatestLedger() {
      return 105;
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
          hash: `hash-${sequence}`,
          previousHash:
            sequence > 1
              ? `hash-${sequence - 1}`
              : null
        });
      }

      return {
        startLedger,
        endLedger,
        count: ledgers.length,
        ledgers,
        last: ledgers.at(-1)
      };
    }
  };
}

function makeRecovery() {
  return {
    detect({ expected, actual }) {
      if (expected.hash === actual.hash) {
        return {
          reorg: false,
          reason: 'same-hash',
          sequence: actual.sequence
        };
      }

      return {
        reorg: true,
        reason: 'hash-mismatch',
        sequence: actual.sequence,
        expectedHash: expected.hash,
        actualHash: actual.hash
      };
    },

    async recover(options) {
      return {
        ...options,
        recovered: true
      };
    }
  };
}

test('ReorgMonitor validates streamer contract', () => {
  assert.throws(
    () =>
      new ReorgMonitor(
        {},
        {}
      ),
    TypeError
  );
});

test('ReorgMonitor checks expected ledger history', async () => {
  const monitor =
    new ReorgMonitor(
      makeStreamer(),
      {
        lookback: 10,
        recovery: makeRecovery()
      }
    );

  const result =
    await monitor.check({
      expected: {
        sequence: 100,
        hash: 'hash-100'
      }
    });

  assert.equal(
    result.reorg,
    false
  );

  assert.equal(
    result.sequence,
    100
  );
});

test('ReorgMonitor detects a mismatching expected hash', async () => {
  const monitor =
    new ReorgMonitor(
      makeStreamer(),
      {
        lookback: 10,
        recovery: makeRecovery()
      }
    );

  const result =
    await monitor.check({
      expected: {
        sequence: 100,
        hash: 'wrong-hash'
      }
    });

  assert.equal(
    result.reorg,
    true
  );

  assert.equal(
    result.sequence,
    100
  );
});

test('ReorgMonitor reports expected ledger outside lookback', async () => {
  const monitor =
    new ReorgMonitor(
      makeStreamer(),
      {
        lookback: 3,
        recovery: makeRecovery()
      }
    );

  const result =
    await monitor.check({
      expected: {
        sequence: 90,
        hash: 'hash-90'
      }
    });

  assert.equal(
    result.reorg,
    false
  );

  assert.equal(
    result.reason,
    'expected-ledger-outside-window'
  );
});

test('ReorgMonitor verifyExpected aliases check', async () => {
  const monitor =
    new ReorgMonitor(
      makeStreamer(),
      {
        recovery: makeRecovery()
      }
    );

  const result =
    await monitor.check({
      expected: {
        sequence: 104,
        hash: 'hash-104'
      }
    });

  assert.equal(
    result.reorg,
    false
  );
});

test('ReorgMonitor checkAndRecover invokes recovery on mismatch', async () => {
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

  const monitor =
    new ReorgMonitor(
      makeStreamer(),
      {
        recovery
      }
    );

  const result =
    await monitor.checkAndRecover({
      expected: {
        sequence: 100,
        hash: 'wrong-hash'
      }
    });

  assert.equal(
    result.reorg,
    true
  );

  assert.equal(
    result.recovery.recovered,
    true
  );

  assert.equal(
    calls.length,
    1
  );

  assert.equal(
    calls[0].affectedLedger,
    100
  );
});

test('ReorgMonitor does not recover when history matches', async () => {
  let recovered = false;

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

    async recover() {
      recovered = true;
      return {};
    }
  };

  const monitor =
    new ReorgMonitor(
      makeStreamer(),
      {
        recovery
      }
    );

  const result =
    await monitor.checkAndRecover({
      expected: {
        sequence: 103,
        hash: 'hash-103'
      }
    });

  assert.equal(
    result.reorg,
    false
  );

  assert.equal(
    recovered,
    false
  );
});
