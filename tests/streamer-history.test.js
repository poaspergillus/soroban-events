import test from 'node:test';
import assert from 'node:assert/strict';

import {
  SorobanEventStreamer
} from '../src/streamer.js';

import {
  ConsistencyError,
  LedgerConsistencyTracker
} from '../src/consistency.js';

function makeStreamer(getLedgers) {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = { getLedgers };

  return streamer;
}

test('getLedgerHistory retrieves and verifies consecutive ledgers', async () => {
  const calls = [];

  const streamer = makeStreamer(async request => {
    calls.push(request);

    return {
      ledgers: [
        {
          sequence: 100,
          hash: 'h100',
          previousHash: 'h99'
        },
        {
          sequence: 101,
          hash: 'h101',
          previousHash: 'h100'
        },
        {
          sequence: 102,
          hash: 'h102',
          previousHash: 'h101'
        }
      ],
      cursor: null
    };
  });

  const tracker =
    new LedgerConsistencyTracker();

  const result =
    await streamer.getLedgerHistory({
      startLedger: 100,
      endLedger: 102,
      consistency: tracker
    });

  assert.deepEqual(
    result.map(ledger => ledger.sequence),
    [100, 101, 102]
  );

  assert.equal(tracker.sequence, 102);
  assert.equal(tracker.hash, 'h102');

  assert.deepEqual(calls, [
    {
      startLedger: 100,
      limit: 1000
    }
  ]);
});

test('getLedgerHistory follows RPC pagination', async () => {
  let calls = 0;

  const streamer = makeStreamer(async request => {
    calls++;

    if (calls === 1) {
      assert.deepEqual(request, {
        startLedger: 100,
        limit: 2
      });

      return {
        ledgers: [
          {
            sequence: 100,
            hash: 'h100',
            previousHash: 'h99'
          },
          {
            sequence: 101,
            hash: 'h101',
            previousHash: 'h100'
          }
        ],
        cursor: 'next'
      };
    }

    assert.deepEqual(request, {
      limit: 2,
      pagination: {
        cursor: 'next'
      }
    });

    return {
      ledgers: [
        {
          sequence: 102,
          hash: 'h102',
          previousHash: 'h101'
        }
      ],
      cursor: null
    };
  });

  const result =
    await streamer.getLedgerHistory({
      startLedger: 100,
      endLedger: 102,
      limit: 2
    });

  assert.deepEqual(
    result.map(ledger => ledger.sequence),
    [100, 101, 102]
  );

  assert.equal(calls, 2);
});

test('getLedgerHistory stops at endLedger', async () => {
  let calls = 0;

  const streamer = makeStreamer(async request => {
    calls++;

    return {
      ledgers: [
        {
          sequence: 100,
          hash: 'h100',
          previousHash: 'h99'
        },
        {
          sequence: 101,
          hash: 'h101',
          previousHash: 'h100'
        },
        {
          sequence: 102,
          hash: 'h102',
          previousHash: 'h101'
        }
      ],
      cursor: 'should-not-be-used'
    };
  });

  const result =
    await streamer.getLedgerHistory({
      startLedger: 100,
      endLedger: 101
    });

  assert.deepEqual(
    result.map(ledger => ledger.sequence),
    [100, 101]
  );

  assert.equal(calls, 1);
});

test('getLedgerHistory detects a ledger sequence gap', async () => {
  const streamer = makeStreamer(async () => ({
    ledgers: [
      {
        sequence: 100,
        hash: 'h100',
        previousHash: 'h99'
      },
      {
        sequence: 102,
        hash: 'h102',
        previousHash: 'h100'
      }
    ],
    cursor: null
  }));

  await assert.rejects(
    () =>
      streamer.getLedgerHistory({
        startLedger: 100
      }),
    ConsistencyError
  );
});

test('getLedgerHistory detects a broken previous-hash link', async () => {
  const streamer = makeStreamer(async () => ({
    ledgers: [
      {
        sequence: 100,
        hash: 'h100',
        previousHash: 'h99'
      },
      {
        sequence: 101,
        hash: 'h101',
        previousHash: 'WRONG'
      }
    ],
    cursor: null
  }));

  await assert.rejects(
    () =>
      streamer.getLedgerHistory({
        startLedger: 100
      }),
    /does not reference previous ledger hash/
  );
});

test('getLedgerHistory rejects non-advancing cursor', async () => {
  const streamer = makeStreamer(async () => ({
    ledgers: [
      {
        sequence: 100,
        hash: 'h100',
        previousHash: 'h99'
      }
    ],
    cursor: 'same'
  }));

  await assert.rejects(
    () =>
      streamer.getLedgerHistory({
        startLedger: 100
      }),
    /pagination cursor did not advance/
  );
});

test('getLedgerHistory validates options', async () => {
  const streamer = makeStreamer(async () => ({
    ledgers: [],
    cursor: null
  }));

  await assert.rejects(
    () =>
      streamer.getLedgerHistory({
        startLedger: 0
      }),
    /startLedger must be a positive safe integer/
  );

  await assert.rejects(
    () =>
      streamer.getLedgerHistory({
        startLedger: 100,
        endLedger: 99
      }),
    /endLedger must be a safe integer >= startLedger/
  );

  await assert.rejects(
    () =>
      streamer.getLedgerHistory({
        startLedger: 100,
        limit: 1001
      }),
    /limit must be a safe integer between 1 and 1000/
  );
});

test('verifyLedgerHistory returns verification summary', async () => {
  const streamer = makeStreamer(async () => ({
    ledgers: [
      {
        sequence: 200,
        hash: 'h200',
        previousHash: 'h199'
      },
      {
        sequence: 201,
        hash: 'h201',
        previousHash: 'h200'
      }
    ],
    cursor: null
  }));

  const result =
    await streamer.verifyLedgerHistory({
      startLedger: 200,
      endLedger: 201
    });

  assert.equal(result.startLedger, 200);
  assert.equal(result.endLedger, 201);
  assert.equal(result.count, 2);
  assert.equal(result.last.sequence, 201);
  assert.equal(result.last.hash, 'h201');
  assert.equal(result.ledgers.length, 2);
});

test('getLedgerHistory can use an allow-gaps tracker', async () => {
  const streamer = makeStreamer(async () => ({
    ledgers: [
      {
        sequence: 100,
        hash: 'h100',
        previousHash: 'h99'
      },
      {
        sequence: 102,
        hash: 'h102',
        previousHash: 'h100'
      }
    ],
    cursor: null
  }));

  const tracker =
    new LedgerConsistencyTracker({
      allowGaps: true
    });

  const result =
    await streamer.getLedgerHistory({
      startLedger: 100,
      consistency: tracker
    });

  assert.deepEqual(
    result.map(ledger => ledger.sequence),
    [100, 102]
  );

  assert.equal(tracker.sequence, 102);
});


test('ledger verification increments metrics', async () => {
  const streamer = makeStreamer(async () => ({
    ledgers: [
      {
        sequence: 300,
        hash: 'h300',
        previousHash: 'h299'
      },
      {
        sequence: 301,
        hash: 'h301',
        previousHash: 'h300'
      }
    ],
    cursor: null
  }));

  await streamer.verifyLedgerHistory({
    startLedger: 300,
    endLedger: 301
  });

  assert.equal(
    streamer.metrics.getCounter(
      'ledgers_verified'
    ),
    2
  );
});


test('streamer records RPC retry metrics', async () => {
  let calls = 0;

  const streamer = makeStreamer(
    async () => {
      calls++;

      if (calls === 1) {
        const error = new Error('rate limit');
        error.status = 429;
        throw error;
      }

      return {
        ledgers: [
          {
            sequence: 400,
            hash: 'h400',
            previousHash: 'h399'
          }
        ],
        cursor: null
      };
    }
  );

  await streamer.getLedgerHistory({
    startLedger: 400,
    endLedger: 400
  });

  assert.equal(
    streamer.metrics.getCounter(
      'rpc_attempts'
    ),
    2
  );

  assert.equal(
    streamer.metrics.getCounter(
      'rpc_requests'
    ),
    1
  );

  assert.equal(
    streamer.metrics.getCounter(
      'rpc_retries'
    ),
    1
  );

  assert.equal(
    streamer.metrics.getCounter(
      'rpc_rate_limits'
    ),
    1
  );

  assert.equal(
    streamer.metrics.getHistogram(
      'rpc_ledger_request_duration_ms'
    ).count,
    2
  );
});
