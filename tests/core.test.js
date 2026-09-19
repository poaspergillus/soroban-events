import test from 'node:test';
import assert from 'node:assert/strict';

import {
  SorobanEventStreamer,
  MAX_SAFE_LEDGER_SPAN
} from '../src/index.js';

function makeEvent(id, ledger, value = 'x') {
  return {
    id,
    type: 'contract',
    ledger,
    ledgerClosedAt: '2026-01-01T00:00:00Z',
    contractId: 'C_TEST',
    topic: [],
    value,
    inSuccessfulContractCall: true
  };
}

function makeStreamer(pages, options = {}) {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid',
    options
  );

  let calls = 0;

  streamer.server.getEvents = async params => {
    calls++;

    if (typeof pages === 'function') {
      return pages(params, calls);
    }

    return pages.shift() ?? {
      events: []
    };
  };

  streamer._testCalls = () => calls;

  return streamer;
}

test('unlimited window retrieval follows pagination past 10000 events', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  let calls = 0;

  streamer.server = {
    getEvents: async request => {
      calls++;

      const cursor = request.pagination?.cursor ?? null;

      if (cursor === null) {
        assert.equal(request.startLedger, 100);
        assert.equal(request.endLedger, 101);
      }

      if (cursor === null) {
        return {
          events: Array.from(
            { length: 10000 },
            (_, i) => ({
              id: `event-${i}`,
              ledger: 100
            })
          ),
          cursor: 'page-2'
        };
      }

      if (cursor === 'page-2') {
        return {
          events: Array.from(
            { length: 250 },
            (_, i) => ({
              id: `event-${10000 + i}`,
              ledger: 100
            })
          ),
          cursor: null
        };
      }

      throw new Error(`unexpected cursor: ${cursor}`);
    }
  };

  const events = await streamer.getEventsWindowed({
    startLedger: 100,
    endLedger: 100,
    limit: null
  });

  assert.equal(events.length, 10250);
  assert.equal(events[0].id, 'event-0');
  assert.equal(events[9999].id, 'event-9999');
  assert.equal(events[10000].id, 'event-10000');
  assert.equal(events[10249].id, 'event-10249');
  assert.equal(calls, 2);
});

test('null limit does not truncate a large single page', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = {
    getEvents: async () => ({
      events: Array.from(
        { length: 1200 },
        (_, i) => ({
          id: `unlimited-${i}`,
          ledger: 300
        })
      ),
      cursor: null
    })
  };

  const events = await streamer.getEventsWindowed({
    startLedger: 300,
    endLedger: 300,
    limit: null
  });

  assert.equal(events.length, 1200);
});

test('window size never exceeds safe maximum', () => {
  const s = new SorobanEventStreamer(
    'https://example.invalid',
    { windowSize: 999999 }
  );

  assert.equal(s.windowSize, MAX_SAFE_LEDGER_SPAN);
});

test('single-page event retrieval works', async () => {
  const s = makeStreamer([
    {
      events: [
        makeEvent('1', 10),
        makeEvent('2', 11)
      ]
    }
  ]);

  const result = await s.getEventsWindowed({
    startLedger: 10,
    endLedger: 11,
    filters: [],
    limit: 10
  });

  assert.equal(result.length, 2);
  assert.equal(result[0].id, '1');
  assert.equal(result[1].id, '2');
});

test('pagination follows cursor', async () => {
  const calls = [];

  const s = makeStreamer((params) => {
    calls.push(params);

    if (calls.length === 1) {
      return {
        events: [makeEvent('1', 10)],
        cursor: 'CURSOR-1'
      };
    }

    return {
      events: [makeEvent('2', 11)]
    };
  });

  const result = await s.getEventsWindowed({
    startLedger: 10,
    endLedger: 20,
    filters: [],
    limit: 2
  });

  assert.equal(result.length, 2);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].startLedger, 10);
  assert.equal(calls[0].endLedger, 21);
  assert.equal(calls[0].pagination.cursor, undefined);
  assert.equal(calls[1].pagination.cursor, 'CURSOR-1');
  assert.equal(calls[1].startLedger, undefined);
  assert.equal(calls[1].endLedger, undefined);
});

test('duplicate event IDs are removed', async () => {
  const s = makeStreamer([
    {
      events: [
        makeEvent('same', 10),
        makeEvent('same', 10),
        makeEvent('other', 11)
      ]
    }
  ]);

  const result = await s.getEventsWindowed({
    startLedger: 10,
    endLedger: 11,
    limit: 10
  });

  assert.deepEqual(
    result.map(x => x.id),
    ['same', 'other']
  );
});

test('ledger windows use exclusive end boundary', async () => {
  const calls = [];

  const s = makeStreamer(params => {
    calls.push(params);
    return { events: [] };
  }, { windowSize: 3 });

  await s.getEventsWindowed({
    startLedger: 10,
    endLedger: 16,
    limit: 10
  });

  assert.deepEqual(
    calls.map(x => [x.startLedger, x.endLedger]),
    [
      [10, 13],
      [13, 16],
      [16, 17]
    ]
  );
});

test('limit is respected', async () => {
  const s = makeStreamer([
    {
      events: [
        makeEvent('1', 10),
        makeEvent('2', 11),
        makeEvent('3', 12)
      ]
    }
  ]);

  const result = await s.getEventsWindowed({
    startLedger: 10,
    endLedger: 12,
    limit: 2
  });

  assert.equal(result.length, 2);
});

test('rate limits are retried', async () => {
  let calls = 0;

  const s = makeStreamer(() => {
    calls++;

    if (calls < 3) {
      const error = new Error('429 rate limit');
      error.status = 429;
      throw error;
    }

    return {
      events: [makeEvent('1', 10)]
    };
  }, {
    retryBaseMs: 1,
    maxRetries: 3
  });

  const result = await s.getEventsWindowed({
    startLedger: 10,
    endLedger: 10,
    limit: 1
  });

  assert.equal(result.length, 1);
  assert.equal(calls, 3);
});

test('transient failures retry', async () => {
  let calls = 0;

  const s = makeStreamer(() => {
    calls++;

    if (calls === 1) {
      const error = new Error('timeout');
      error.status = 500;
      throw error;
    }

    return {
      events: [makeEvent('1', 10)]
    };
  }, {
    retryBaseMs: 1,
    maxRetries: 2
  });

  const result = await s.getEventsWindowed({
    startLedger: 10,
    endLedger: 10,
    limit: 1
  });

  assert.equal(result.length, 1);
  assert.equal(calls, 2);
});

test('fatal errors are not endlessly retried', async () => {
  let calls = 0;

  const s = makeStreamer(() => {
    calls++;
    const error = new Error('invalid request');
    error.status = 400;
    throw error;
  });

  await assert.rejects(
    () => s.getEventsWindowed({
      startLedger: 10,
      endLedger: 10,
      limit: 1
    }),
    /invalid request/
  );

  assert.equal(calls, 1);
});

test('non-advancing cursor throws', async () => {
  const s = makeStreamer(() => ({
    events: [makeEvent('1', 10)],
    cursor: 'STUCK'
  }));

  await assert.rejects(
    () => s.getEventsWindowed({
      startLedger: 10,
      endLedger: 10,
      limit: 2
    }),
    /cursor did not advance/
  );
});

test('tail returns newest events in chronological order', async () => {
  const s = makeStreamer([
    {
      events: [
        makeEvent('1', 10),
        makeEvent('2', 11),
        makeEvent('3', 12)
      ]
    }
  ], {
    windowSize: 10
  });

  s.getLatestLedger = async () => 12;

  const result = await s.tail({
    limit: 2,
    maxLookbackLedgers: 10
  });

  assert.deepEqual(
    result.map(x => x.id),
    ['2', '3']
  );
});

test('tail uses contract filter when contractId is provided', async () => {
  let captured;

  const s = makeStreamer(params => {
    captured = params;
    return {
      events: []
    };
  });

  s.getLatestLedger = async () => 100;

  await s.tail({
    contractId: 'CONTRACT123',
    limit: 1,
    maxLookbackLedgers: 10
  });

  assert.deepEqual(captured.filters, [
    {
      type: 'contract',
      contractIds: ['CONTRACT123']
    }
  ]);
});

test('abort signal is respected', async () => {
  const controller = new AbortController();
  controller.abort();

  const s = makeStreamer({
    events: []
  });

  await assert.rejects(
    () => s.getEventsWindowed({
      startLedger: 10,
      endLedger: 10,
      limit: 1,
      signal: controller.signal
    }),
    error => error.name === 'AbortError'
  );
});

test('invalid start ledger is rejected', async () => {
  const s = makeStreamer([]);

  await assert.rejects(
    () => s.getEventsWindowed({
      startLedger: 0,
      endLedger: 10
    }),
    /startLedger/
  );
});

test('invalid pagination options are rejected', async () => {
  assert.throws(
    () => new SorobanEventStreamer(
      'https://example.invalid',
      { pageSize: 0 }
    ),
    /pageSize/
  );

  assert.throws(
    () => new SorobanEventStreamer(
      'https://example.invalid',
      { pageSize: -1 }
    ),
    /pageSize/
  );

  assert.throws(
    () => new SorobanEventStreamer(
      'https://example.invalid',
      { windowSize: 0 }
    ),
    /windowSize/
  );

  assert.throws(
    () => new SorobanEventStreamer(
      'https://example.invalid',
      { windowSize: -1 }
    ),
    /windowSize/
  );

  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  await assert.rejects(
    () => streamer.getEventsWindowed({
      startLedger: 1,
      endLedger: 2,
      limit: 0
    }),
    /limit/
  );

  await assert.rejects(
    () => streamer.getEventsWindowed({
      startLedger: 1,
      endLedger: 2,
      limit: -1
    }),
    /limit/
  );

  await assert.rejects(
    () => streamer.getEventsWindowed({
      startLedger: 1,
      endLedger: 2,
      limit: 1.5
    }),
    /limit/
  );
});


test('getHealth exposes RPC retention metadata', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = {
    getHealth: async () => ({
      status: 'healthy',
      latestLedger: 200,
      oldestLedger: 100,
      ledgerRetentionWindow: 101
    })
  };

  assert.deepEqual(
    await streamer.getHealth(),
    {
      status: 'healthy',
      latestLedger: 200,
      oldestLedger: 100,
      ledgerRetentionWindow: 101
    }
  );
});

test('checkRetention detects retained and expired ledgers', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = {
    getHealth: async () => ({
      status: 'healthy',
      latestLedger: 200,
      oldestLedger: 100,
      ledgerRetentionWindow: 101
    })
  };

  assert.equal(
    (await streamer.checkRetention(99)).retained,
    false
  );

  assert.equal(
    (await streamer.checkRetention(100)).retained,
    true
  );
});

test('checkRpcHealth reports healthy RPC state', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = {
    getHealth: async () => ({
      status: 'healthy',
      latestLedger: 200,
      oldestLedger: 100,
      ledgerRetentionWindow: 101
    })
  };

  assert.deepEqual(
    await streamer.checkRpcHealth(),
    {
      healthy: true,
      status: 'healthy',
      latestLedger: 200,
      oldestLedger: 100,
      ledgerRetentionWindow: 101
    }
  );
});

test('checkRpcHealth reports unhealthy RPC state', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = {
    getHealth: async () => ({
      status: 'syncing',
      latestLedger: 200,
      oldestLedger: 100,
      ledgerRetentionWindow: 101
    })
  };

  const health = await streamer.checkRpcHealth();

  assert.equal(health.healthy, false);
  assert.equal(health.status, 'syncing');
});

test('isLedgerAvailable detects retained ledger', async () => {
  const streamer = new SorobanEventStreamer(
    'https://example.invalid'
  );

  streamer.server = {
    getHealth: async () => ({
      status: 'healthy',
      latestLedger: 200,
      oldestLedger: 100,
      ledgerRetentionWindow: 101
    })
  };

  assert.equal(
    await streamer.isLedgerAvailable(100),
    true
  );

  assert.equal(
    await streamer.isLedgerAvailable(99),
    false
  );
});

test('switchRpc rotates to the configured failover RPC', () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      failoverRpcUrls: [
        'https://secondary.example'
      ]
    }
  );

  assert.equal(streamer.rpcIndex, 0);
  assert.equal(streamer.switchRpc(), true);
  assert.equal(streamer.rpcIndex, 1);
  assert.equal(streamer.switchRpc(), true);
  assert.equal(streamer.rpcIndex, 0);
});

test('switchRpc reports false without failover RPCs', () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example'
  );

  assert.equal(streamer.switchRpc(), false);
});

test('failover retries each configured RPC endpoint once', async () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      failoverRpcUrls: [
        'https://secondary.example'
      ],
      maxRetries: 0
    }
  );

  let calls = 0;

  streamer.server = {
    getEvents: async () => {
      calls++;
      throw Object.assign(
        new Error('socket reset'),
        { status: 500 }
      );
    }
  };

  const originalSwitch = streamer.switchRpc.bind(streamer);

  streamer.switchRpc = () => {
    const switched = originalSwitch();

    if (switched) {
      streamer.server = {
        getEvents: async () => ({
          events: ['recovered']
        })
      };
    }

    return switched;
  };

  const result = await streamer.requestWithRetry({});

  assert.deepEqual(result, {
    events: ['recovered']
  });
  assert.equal(calls, 1);
});

test('failover stops after all RPC endpoints fail', async () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      failoverRpcUrls: [
        'https://secondary.example'
      ],
      maxRetries: 0
    }
  );

  let calls = 0;

  streamer.server = {
    getEvents: async () => {
      calls++;
      throw Object.assign(
        new Error('socket reset'),
        { status: 500 }
      );
    }
  };

  streamer.switchRpc = () => {
    streamer.rpcIndex++;
    streamer.server = {
      getEvents: async () => {
        calls++;
        throw Object.assign(
          new Error('socket reset'),
          { status: 500 }
        );
      }
    };
    return true;
  };

  await assert.rejects(
    streamer.requestWithRetry({}),
    /socket reset/
  );

  assert.equal(calls, 2);
});

test('circuit breaker opens after repeated RPC failures', async () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      maxRetries: 0,
      circuitBreakerThreshold: 2,
      circuitBreakerCooldownMs: 60000
    }
  );

  streamer.server = {
    getEvents: async () => {
      throw Object.assign(
        new Error('socket reset'),
        { status: 500 }
      );
    }
  };

  await assert.rejects(
    streamer.requestWithRetry({}),
    /socket reset/
  );

  await assert.rejects(
    streamer.requestWithRetry({}),
    /socket reset/
  );

  await assert.rejects(
    streamer.requestWithRetry({}),
    error => error.code === 'RPC_CIRCUIT_OPEN'
  );

  assert.equal(streamer.rpcFailureCount, 2);
  assert.notEqual(streamer.circuitOpenedAt, 0);
});

test('successful RPC closes the circuit and resets failures', async () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      maxRetries: 0,
      circuitBreakerThreshold: 2
    }
  );

  streamer.rpcFailureCount = 1;
  streamer.circuitOpenedAt = 0;

  streamer.server = {
    getEvents: async () => ({
      events: []
    })
  };

  const result = await streamer.requestWithRetry({});

  assert.deepEqual(result, {
    events: []
  });
  assert.equal(streamer.rpcFailureCount, 0);
  assert.equal(streamer.circuitOpenedAt, 0);
});



test('adaptive rate limiting increases backoff after 429 and decays after success', async () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      maxRetries: 0,
      retryBaseMs: 10,
      retryMaxMs: 100,
      adaptiveRateLimit: true
    }
  );

  streamer.server = {
    getEvents: async () => {
      throw Object.assign(
        new Error('rate limited'),
        { status: 429 }
      );
    }
  };

  await assert.rejects(
    streamer.requestWithRetry({}),
    /rate limited/
  );

  const firstDelay =
    streamer.rateLimitDelayMs;

  assert.ok(firstDelay >= 10);
  assert.ok(firstDelay <= 100);
  assert.ok(
    streamer.lastRateLimitAt > 0
  );

  streamer.server = {
    getEvents: async () => ({
      events: []
    })
  };

  await streamer.requestWithRetry({});

  assert.ok(
    streamer.rateLimitDelayMs < firstDelay ||
    streamer.rateLimitDelayMs === 0
  );
});

test('adaptive rate limiting can be disabled', async () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      maxRetries: 0,
      retryBaseMs: 10,
      adaptiveRateLimit: false
    }
  );

  streamer.server = {
    getEvents: async () => {
      throw Object.assign(
        new Error('rate limited'),
        { status: 429 }
      );
    }
  };

  await assert.rejects(
    streamer.requestWithRetry({}),
    /rate limited/
  );

  assert.equal(
    streamer.rateLimitDelayMs,
    0
  );
});

test('adaptive retry delay is capped at retryMaxMs', async () => {
  const streamer = new SorobanEventStreamer(
    'https://primary.example',
    {
      maxRetries: 1,
      retryBaseMs: 100,
      retryMaxMs: 150,
      retryJitter: 0,
      adaptiveRateLimit: true
    }
  );

  let calls = 0;

  streamer.server = {
    getEvents: async () => {
      calls++;

      throw Object.assign(
        new Error('rate limited'),
        { status: 429 }
      );
    }
  };

  const started = Date.now();

  await assert.rejects(
    streamer.requestWithRetry({}),
    /rate limited/
  );

  const elapsed = Date.now() - started;

  assert.equal(calls, 2);
  assert.ok(elapsed >= 150);
  assert.ok(
    streamer.rateLimitDelayMs <= 150
  );
});
