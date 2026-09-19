export class ReorgError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'ReorgError';
  }
}

export class ReorgRecovery {
  #checkpoint;
  #store;
  #checkpointKey;
  #rewindDepth;

  constructor({
    checkpoint = null,
    store = null,
    checkpointKey = 'default',
    rewindDepth = 1
  } = {}) {
    if (
      checkpoint != null &&
      (
        typeof checkpoint.resumeFrom !== 'function' ||
        typeof checkpoint.rewind !== 'function'
      )
    ) {
      throw new TypeError(
        'checkpoint must implement resumeFrom() and rewind()'
      );
    }

    if (
      store != null &&
      (
        typeof store.list !== 'function' ||
        typeof store.delete !== 'function'
      )
    ) {
      throw new TypeError(
        'store must implement list() and delete()'
      );
    }

    if (
      typeof checkpointKey !== 'string' ||
      checkpointKey.length === 0
    ) {
      throw new TypeError(
        'checkpointKey must be a non-empty string'
      );
    }

    if (
      !Number.isSafeInteger(rewindDepth) ||
      rewindDepth < 0
    ) {
      throw new TypeError(
        'rewindDepth must be a non-negative safe integer'
      );
    }

    this.#checkpoint = checkpoint;
    this.#store = store;
    this.#checkpointKey = checkpointKey;
    this.#rewindDepth = rewindDepth;
  }

  get checkpoint() {
    return this.#checkpoint;
  }

  get store() {
    return this.#store;
  }

  get checkpointKey() {
    return this.#checkpointKey;
  }

  get rewindDepth() {
    return this.#rewindDepth;
  }

  detect({
    expected,
    actual
  }) {
    validateLedger(expected);
    validateLedger(actual);

    if (
      expected.sequence !== actual.sequence
    ) {
      return {
        reorg: false,
        reason: 'different-ledger'
      };
    }

    if (
      expected.hash == null ||
      actual.hash == null
    ) {
      return {
        reorg: false,
        reason: 'hash-unavailable'
      };
    }

    if (expected.hash === actual.hash) {
      return {
        reorg: false,
        reason: 'same-hash'
      };
    }

    return {
      reorg: true,
      reason: 'hash-mismatch',
      sequence: actual.sequence,
      expectedHash: expected.hash,
      actualHash: actual.hash
    };
  }

  plan({
    affectedLedger,
    latestVerifiedLedger = affectedLedger
  }) {
    validateLedgerNumber(affectedLedger);
    validateLedgerNumber(latestVerifiedLedger);

    if (
      latestVerifiedLedger <
      affectedLedger
    ) {
      throw new TypeError(
        'latestVerifiedLedger must be >= affectedLedger'
      );
    }

    const rewindTo = Math.max(
      1,
      affectedLedger - this.#rewindDepth
    );

    return {
      affectedLedger,
      latestVerifiedLedger,
      rewindTo,
      resumeFrom: rewindTo,
      invalidateFrom: affectedLedger,
      invalidateThrough:
        latestVerifiedLedger,
      replayFrom: rewindTo
    };
  }

  async recover({
    affectedLedger,
    latestVerifiedLedger = affectedLedger,
    invalidateStoredEvents = true
  }) {
    const plan = this.plan({
      affectedLedger,
      latestVerifiedLedger
    });

    let deleted = 0;

    // Rewind the durable processing position first.
    // This prevents a failed checkpoint operation from
    // leaving the consumer permanently past invalid data.
    if (this.#checkpoint != null) {
      try {
        await this.#checkpoint.rewind(
          plan.resumeFrom,
          this.#checkpointKey
        );
      } catch (error) {
        throw new ReorgError(
          `failed to rewind checkpoint to ledger ${plan.resumeFrom}`,
          { cause: error }
        );
      }
    }

    // Only invalidate stored events after the checkpoint
    // has successfully moved back. If deletion later fails,
    // replay can still reconstruct the affected range.
    if (
      invalidateStoredEvents &&
      this.#store != null
    ) {
      let events;

      try {
        events = await this.#store.list({
          startLedger:
            plan.invalidateFrom,
          endLedger:
            plan.invalidateThrough
        });
      } catch (error) {
        throw new ReorgError(
          `failed to list stored events for ledgers ${plan.invalidateFrom}-${plan.invalidateThrough}`,
          { cause: error }
        );
      }

      for (const event of events) {
        try {
          await this.#store.delete(
            event.id
          );

          deleted++;
        } catch (error) {
          throw new ReorgError(
            `failed to invalidate stored event ${event.id}`,
            { cause: error }
          );
        }
      }
    }

    return {
      ...plan,
      deletedEvents: deleted,
      checkpointRewound:
        this.#checkpoint != null,
      storageInvalidated:
        this.#store != null &&
        invalidateStoredEvents
    };
  }
}

function validateLedger(ledger) {
  if (!ledger || typeof ledger !== 'object') {
    throw new TypeError(
      'ledger metadata must be an object'
    );
  }

  validateLedgerNumber(
    ledger.sequence
  );

  if (
    ledger.hash != null &&
    (
      typeof ledger.hash !== 'string' ||
      ledger.hash.length === 0
    )
  ) {
    throw new TypeError(
      'ledger hash must be a non-empty string'
    );
  }
}

function validateLedgerNumber(sequence) {
  if (
    !Number.isSafeInteger(sequence) ||
    sequence < 1
  ) {
    throw new TypeError(
      'ledger sequence must be a positive safe integer'
    );
  }
}
