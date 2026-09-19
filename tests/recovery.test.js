import test from 'node:test';
import assert from 'node:assert/strict';

import {
  ReorgError,
  ReorgRecovery
} from '../src/recovery.js';

import {
  MemoryCheckpointStore,
  CheckpointManager
} from '../src/checkpoint.js';

import {
  MemoryEventStore
} from '../src/store.js';

test('reorg recovery detects a ledger hash mismatch', () => {
  const recovery =
    new ReorgRecovery();

  const result =
    recovery.detect({
      expected: {
        sequence: 100,
        hash: 'old-hash'
      },
      actual: {
        sequence: 100,
        hash: 'new-hash'
      }
    });

  assert.deepEqual(
    result,
    {
      reorg: true,
      reason: 'hash-mismatch',
      sequence: 100,
      expectedHash: 'old-hash',
      actualHash: 'new-hash'
    }
  );
});

test('reorg recovery accepts matching hashes', () => {
  const recovery =
    new ReorgRecovery();

  const result =
    recovery.detect({
      expected: {
        sequence: 100,
        hash: 'same'
      },
      actual: {
        sequence: 100,
        hash: 'same'
      }
    });

  assert.equal(
    result.reorg,
    false
  );

  assert.equal(
    result.reason,
    'same-hash'
  );
});

test('reorg recovery does not compare different ledger sequences', () => {
  const recovery =
    new ReorgRecovery();

  const result =
    recovery.detect({
      expected: {
        sequence: 100,
        hash: 'old'
      },
      actual: {
        sequence: 101,
        hash: 'new'
      }
    });

  assert.equal(
    result.reorg,
    false
  );

  assert.equal(
    result.reason,
    'different-ledger'
  );
});

test('reorg recovery handles unavailable hashes explicitly', () => {
  const recovery =
    new ReorgRecovery();

  const result =
    recovery.detect({
      expected: {
        sequence: 100,
        hash: null
      },
      actual: {
        sequence: 100,
        hash: 'new'
      }
    });

  assert.equal(
    result.reorg,
    false
  );

  assert.equal(
    result.reason,
    'hash-unavailable'
  );
});

test('reorg recovery plans a rewind', () => {
  const recovery =
    new ReorgRecovery({
      rewindDepth: 3
    });

  const plan =
    recovery.plan({
      affectedLedger: 100,
      latestVerifiedLedger: 105
    });

  assert.deepEqual(
    plan,
    {
      affectedLedger: 100,
      latestVerifiedLedger: 105,
      rewindTo: 97,
      resumeFrom: 97,
      invalidateFrom: 100,
      invalidateThrough: 105,
      replayFrom: 97
    }
  );
});

test('reorg recovery never rewinds below ledger one', () => {
  const recovery =
    new ReorgRecovery({
      rewindDepth: 100
    });

  const plan =
    recovery.plan({
      affectedLedger: 5,
      latestVerifiedLedger: 8
    });

  assert.equal(
    plan.rewindTo,
    1
  );
});

test('reorg recovery rewinds checkpoint and invalidates stored events', async () => {
  const checkpointStore =
    new MemoryCheckpointStore();

  const checkpoint =
    new CheckpointManager(
      checkpointStore
    );

  const store =
    new MemoryEventStore();

  await checkpoint.save(
    100,
    'consumer'
  );

  await store.put({
    id: 'event-99',
    ledger: 99
  });

  await store.put({
    id: 'event-100',
    ledger: 100
  });

  await store.put({
    id: 'event-101',
    ledger: 101
  });

  const recovery =
    new ReorgRecovery({
      checkpoint,
      store,
      checkpointKey: 'consumer',
      rewindDepth: 1
    });

  const result =
    await recovery.recover({
      affectedLedger: 100,
      latestVerifiedLedger: 101
    });

  assert.equal(
    result.rewindTo,
    99
  );

  assert.equal(
    result.deletedEvents,
    2
  );

  assert.equal(
    await checkpoint.resumeFrom(
      'consumer',
      999
    ),
    99
  );

  assert.equal(
    await store.has('event-99'),
    true
  );

  assert.equal(
    await store.has('event-100'),
    false
  );

  assert.equal(
    await store.has('event-101'),
    false
  );
});

test('reorg recovery can rewind checkpoint without storage', async () => {
  const checkpointStore =
    new MemoryCheckpointStore();

  const checkpoint =
    new CheckpointManager(
      checkpointStore
    );

  const recovery =
    new ReorgRecovery({
      checkpoint,
      checkpointKey: 'consumer',
      rewindDepth: 2
    });

  const result =
    await recovery.recover({
      affectedLedger: 50,
      latestVerifiedLedger: 52,
      invalidateStoredEvents: false
    });

  assert.equal(
    result.rewindTo,
    48
  );

  assert.equal(
    result.deletedEvents,
    0
  );

  assert.equal(
    result.storageInvalidated,
    false
  );

  assert.equal(
    await checkpoint.resumeFrom(
      'consumer',
      999
    ),
    48
  );
});

test('reorg recovery validates constructor options', () => {
  assert.throws(
    () =>
      new ReorgRecovery({
        rewindDepth: -1
      }),
    /rewindDepth must be a non-negative safe integer/
  );

  assert.throws(
    () =>
      new ReorgRecovery({
        checkpointKey: ''
      }),
    /checkpointKey must be a non-empty string/
  );

  assert.throws(
    () =>
      new ReorgRecovery({
        checkpoint: {}
      }),
    /checkpoint must implement resumeFrom\(\) and rewind\(\)/
  );
});

test('reorg recovery validates ledger metadata', () => {
  const recovery =
    new ReorgRecovery();

  assert.throws(
    () =>
      recovery.detect({
        expected: {
          sequence: 0,
          hash: 'x'
        },
        actual: {
          sequence: 1,
          hash: 'y'
        }
      }),
    /ledger sequence must be a positive safe integer/
  );

  assert.throws(
    () =>
      recovery.plan({
        affectedLedger: 20,
        latestVerifiedLedger: 10
      }),
    /latestVerifiedLedger must be >= affectedLedger/
  );
});

test('ReorgError extends Error', () => {
  assert.equal(
    new ReorgError('reorg')
      instanceof Error,
    true
  );
});


test(
  'recovery does not delete storage when checkpoint rewind fails',
  async () => {
    const events = [];

    const checkpoint = {
      async resumeFrom() {
        return 100;
      },

      async rewind() {
        throw new Error('checkpoint unavailable');
      }
    };

    const store = {
      async list() {
        return [
          {
            id: 'event-100',
            ledger: 100
          }
        ];
      },

      async delete(id) {
        events.push(id);
      }
    };

    const recovery =
      new ReorgRecovery({
        checkpoint,
        store,
        checkpointKey: 'consumer'
      });

    await assert.rejects(
      recovery.recover({
        affectedLedger: 100,
        latestVerifiedLedger: 101
      }),
      error => {
        assert.equal(
          error.name,
          'ReorgError'
        );

        assert.equal(
          error.message,
          'failed to rewind checkpoint to ledger 99'
        );

        assert.equal(
          error.cause?.message,
          'checkpoint unavailable'
        );

        return true;
      }
    );

    assert.deepEqual(
      events,
      []
    );
  }
);

test(
  'recovery reports storage deletion failure after checkpoint rewind',
  async () => {
    let rewoundTo = null;

    const checkpoint = {
      async resumeFrom() {
        return 100;
      },

      async rewind(ledger) {
        rewoundTo = ledger;
      }
    };

    const store = {
      async list() {
        return [
          {
            id: 'event-100',
            ledger: 100
          }
        ];
      },

      async delete() {
        throw new Error('storage unavailable');
      }
    };

    const recovery =
      new ReorgRecovery({
        checkpoint,
        store,
        checkpointKey: 'consumer'
      });

    await assert.rejects(
      recovery.recover({
        affectedLedger: 100,
        latestVerifiedLedger: 101
      }),
      error => {
        assert.equal(
          error.name,
          'ReorgError'
        );

        assert.equal(
          error.message,
          'failed to invalidate stored event event-100'
        );

        assert.equal(
          error.cause?.message,
          'storage unavailable'
        );

        return true;
      }
    );

    assert.equal(
      rewoundTo,
      99
    );
  }
);
