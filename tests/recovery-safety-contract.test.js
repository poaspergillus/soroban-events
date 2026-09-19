import test from 'node:test';
import assert from 'node:assert/strict';

import {
  ReorgRecovery,
  ReorgError
} from '../src/index.js';

function makeCheckpoint() {
  return {
    rewinds: [],

    async resumeFrom() {
      return 100;
    },

    async rewind(ledger, key) {
      this.rewinds.push({
        key,
        ledger
      });
    }
  };
}

function makeStore() {
  const deleted = [];

  return {
    deleted,

    async list(options = {}) {
      const startLedger =
        options.startLedger ?? 1;

      const endLedger =
        options.endLedger ??
        Number.MAX_SAFE_INTEGER;

      return [
        {
          id: 'event-1',
          ledger: 90
        },
        {
          id: 'event-2',
          ledger: 100
        },
        {
          id: 'event-3',
          ledger: 105
        },
        {
          id: 'event-4',
          ledger: 120
        }
      ].filter(
        event =>
          event.ledger >= startLedger &&
          event.ledger <= endLedger
      );
    },

    async delete(id) {
      deleted.push(id);
      return true;
    }
  };
}


test('recovery detects a hash mismatch', () => {
  const recovery =
    new ReorgRecovery({
      rewindDepth: 10
    });

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

  assert.equal(
    result.reorg,
    true
  );

  assert.equal(
    result.sequence,
    100
  );
});

test('recovery does not flag different ledger sequence', () => {
  const recovery =
    new ReorgRecovery();

  const result =
    recovery.detect({
      expected: {
        sequence: 100,
        hash: 'hash-100'
      },
      actual: {
        sequence: 101,
        hash: 'hash-101'
      }
    });

  assert.equal(
    result.reorg,
    false
  );
});

test('recovery does not flag matching ledger', () => {
  const recovery =
    new ReorgRecovery();

  const result =
    recovery.detect({
      expected: {
        sequence: 100,
        hash: 'hash-100'
      },
      actual: {
        sequence: 100,
        hash: 'hash-100'
      }
    });

  assert.equal(
    result.reorg,
    false
  );
});

test('recovery plan rewinds by configured depth', () => {
  const recovery =
    new ReorgRecovery({
      rewindDepth: 10
    });

  const plan =
    recovery.plan({
      affectedLedger: 100,
      latestVerifiedLedger: 120
    });

  assert.equal(
    plan.rewindTo,
    90
  );

  assert.equal(
    plan.invalidateFrom,
    100
  );

  assert.equal(
    plan.invalidateThrough,
    120
  );
});

test('recovery never rewinds below ledger one', () => {
  const recovery =
    new ReorgRecovery({
      rewindDepth: 100
    });

  const plan =
    recovery.plan({
      affectedLedger: 5,
      latestVerifiedLedger: 20
    });

  assert.equal(
    plan.rewindTo,
    1
  );
});

test('recovery rewinds checkpoint before deleting storage', async () => {
  const checkpoint =
    makeCheckpoint();

  const store =
    makeStore();

  const order = [];

  const originalRewind =
    checkpoint.rewind.bind(
      checkpoint
    );

  checkpoint.rewind =
    async (...args) => {
      order.push('rewind');
      return originalRewind(...args);
    };

  const originalDelete =
    store.delete.bind(store);

  store.delete =
    async (...args) => {
      order.push('delete');
      return originalDelete(...args);
    };

  const recovery =
    new ReorgRecovery({
      checkpoint,
      checkpointKey: 'consumer',
      store,
      rewindDepth: 10
    });

  const result =
    await recovery.recover({
      affectedLedger: 100,
      latestVerifiedLedger: 110
    });

  assert.equal(
    result.checkpointRewound,
    true
  );

  assert.equal(
    order[0],
    'rewind'
  );

  assert.ok(
    order.includes('delete')
  );
});

test('recovery does not delete storage if checkpoint rewind fails', async () => {
  let deletes = 0;

  const checkpoint = {
    async resumeFrom() {
      return 100;
    },

    async rewind() {
      throw new Error(
        'checkpoint unavailable'
      );
    }
  };

  const store = {
    async list() {
      return [
        {
          id: 'event-1',
          ledger: 100
        }
      ];
    },

    async delete() {
      deletes++;
    }
  };

  const recovery =
    new ReorgRecovery({
      checkpoint,
      store,
      rewindDepth: 10
    });

  await assert.rejects(
    () =>
      recovery.recover({
        affectedLedger: 100,
        latestVerifiedLedger: 110
      }),
    error => {
      assert.ok(
        error instanceof ReorgError
      );

      return true;
    }
  );

  assert.equal(
    deletes,
    0
  );
});

test('recovery reports storage deletion failures', async () => {
  const checkpoint =
    makeCheckpoint();

  const store = {
    async list() {
      return [
        {
          id: 'event-1',
          ledger: 100
        }
      ];
    },

    async delete() {
      throw new Error(
        'storage unavailable'
      );
    }
  };

  const recovery =
    new ReorgRecovery({
      checkpoint,
      store,
      rewindDepth: 10
    });

  await assert.rejects(
    () =>
      recovery.recover({
        affectedLedger: 100,
        latestVerifiedLedger: 110
      }),
    error => {
      assert.ok(
        error instanceof ReorgError
      );

      return true;
    }
  );

  assert.equal(
    checkpoint.rewinds.length,
    1
  );
});

test('recovery invalidates only affected stored events', async () => {
  const checkpoint =
    makeCheckpoint();

  const store =
    makeStore();

  const recovery =
    new ReorgRecovery({
      checkpoint,
      store,
      rewindDepth: 10
    });

  await recovery.recover({
    affectedLedger: 100,
    latestVerifiedLedger: 110
  });

  assert.deepEqual(
    store.deleted.sort(),
    ['event-2', 'event-3']
  );
});
