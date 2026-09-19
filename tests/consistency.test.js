import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ConsistencyError,
  LedgerConsistencyTracker,
  isLedgerMetadata
} from '../src/consistency.js';

test('consistency tracker initializes from first ledger', () => {
  const tracker =
    new LedgerConsistencyTracker();

  const result = tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  assert.equal(result.status, 'initialized');
  assert.equal(tracker.sequence, 100);
  assert.equal(tracker.hash, 'h100');
});

test('consistency tracker accepts consecutive ledgers', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  const result = tracker.observe({
    sequence: 101,
    hash: 'h101',
    previousHash: 'h100'
  });

  assert.equal(result.status, 'advanced');
  assert.equal(tracker.sequence, 101);
});

test('consistency tracker rejects ledger gaps', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  assert.throws(
    () =>
      tracker.observe({
        sequence: 102,
        hash: 'h102'
      }),
    ConsistencyError
  );
});

test('consistency tracker can explicitly allow gaps', () => {
  const tracker =
    new LedgerConsistencyTracker({
      allowGaps: true
    });

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  const result = tracker.observe({
    sequence: 102,
    hash: 'h102'
  });

  assert.equal(result.status, 'gap');
  assert.equal(tracker.sequence, 102);
});

test('consistency tracker rejects sequence regression', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  assert.throws(
    () =>
      tracker.observe({
        sequence: 99,
        hash: 'h99'
      }),
    ConsistencyError
  );
});

test('same ledger with same hash is a duplicate', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  const result = tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  assert.equal(result.status, 'duplicate');
  assert.equal(tracker.sequence, 100);
});

test('same ledger with different hash is rejected', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  assert.throws(
    () =>
      tracker.observe({
        sequence: 100,
        hash: 'different'
      }),
    ConsistencyError
  );
});

test('previous hash mismatch is rejected', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  assert.throws(
    () =>
      tracker.observe({
        sequence: 101,
        hash: 'h101',
        previousHash: 'wrong'
      }),
    ConsistencyError
  );
});

test('previous hash can be supplied as prevHash', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  const result = tracker.observe({
    sequence: 101,
    hash: 'h101',
    prevHash: 'h100'
  });

  assert.equal(result.status, 'advanced');
});

test('assertCurrent validates the tracked ledger', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  assert.equal(
    tracker.assertCurrent(100, 'h100'),
    true
  );

  assert.throws(
    () => tracker.assertCurrent(99),
    ConsistencyError
  );

  assert.throws(
    () => tracker.assertCurrent(100, 'wrong'),
    ConsistencyError
  );
});

test('reset clears consistency state', () => {
  const tracker =
    new LedgerConsistencyTracker();

  tracker.observe({
    sequence: 100,
    hash: 'h100'
  });

  tracker.reset();

  assert.equal(tracker.last, null);
  assert.equal(tracker.sequence, null);
  assert.equal(tracker.hash, null);
});

test('observeMany processes ledgers in order', () => {
  const tracker =
    new LedgerConsistencyTracker();

  const results = tracker.observeMany([
    { sequence: 1, hash: 'h1' },
    { sequence: 2, hash: 'h2', previousHash: 'h1' },
    { sequence: 3, hash: 'h3', previousHash: 'h2' }
  ]);

  assert.equal(results.length, 3);
  assert.equal(results[0].status, 'initialized');
  assert.equal(results[1].status, 'advanced');
  assert.equal(results[2].status, 'advanced');
  assert.equal(tracker.sequence, 3);
});

test('invalid ledger metadata is rejected', () => {
  const tracker =
    new LedgerConsistencyTracker();

  assert.throws(
    () => tracker.observe({ sequence: 0 }),
    TypeError
  );

  assert.throws(
    () => tracker.observe({ sequence: 1, hash: '' }),
    TypeError
  );

  assert.throws(
    () => tracker.observe(null),
    TypeError
  );
});

test('isLedgerMetadata validates ledger sequence', () => {
  assert.equal(
    isLedgerMetadata({ sequence: 1 }),
    true
  );

  assert.equal(
    isLedgerMetadata({ sequence: 0 }),
    false
  );

  assert.equal(
    isLedgerMetadata({}),
    false
  );

  assert.equal(
    isLedgerMetadata(null),
    false
  );
});

test('consistency error is an Error', () => {
  const error =
    new ConsistencyError('test');

  assert.equal(error instanceof Error, true);
  assert.equal(error.name, 'ConsistencyError');
});
