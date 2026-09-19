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
