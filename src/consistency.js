export class ConsistencyError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'ConsistencyError';
  }
}

export class LedgerConsistencyTracker {
  #last = null;
  #allowGaps;

  constructor(options = {}) {
    this.#allowGaps = options.allowGaps ?? false;

    if (typeof this.#allowGaps !== 'boolean') {
      throw new TypeError('allowGaps must be a boolean');
    }
  }

  get last() {
    return this.#last == null
      ? null
      : { ...this.#last };
  }

  get sequence() {
    return this.#last?.sequence ?? null;
  }

  get hash() {
    return this.#last?.hash ?? null;
  }

  reset() {
    this.#last = null;
    return this;
  }

  observe(ledger) {
    validateLedger(ledger);

    const current = {
      sequence: ledger.sequence,
      hash: normalizeHash(ledger.hash),
      previousHash: normalizeHash(
        ledger.previousHash ?? ledger.prevHash
      )
    };

    if (this.#last == null) {
      this.#last = current;
      return {
        status: 'initialized',
        ...current
      };
    }

    const previous = this.#last;

    if (current.sequence < previous.sequence) {
      throw new ConsistencyError(
        `ledger sequence moved backwards: ${current.sequence} < ${previous.sequence}`
      );
    }

    if (current.sequence === previous.sequence) {
      if (
        current.hash != null &&
        previous.hash != null &&
        current.hash !== previous.hash
      ) {
        throw new ConsistencyError(
          `ledger ${current.sequence} changed hash`
        );
      }

      return {
        status: 'duplicate',
        ...current
      };
    }

    const expected = previous.sequence + 1;

    if (
      current.sequence !== expected &&
      !this.#allowGaps
    ) {
      throw new ConsistencyError(
        `ledger gap detected: expected ${expected}, got ${current.sequence}`
      );
    }

    if (
      current.previousHash != null &&
      previous.hash != null &&
      current.previousHash !== previous.hash
    ) {
      throw new ConsistencyError(
        `ledger ${current.sequence} does not reference previous ledger hash`
      );
    }

    this.#last = current;

    return {
      status:
        current.sequence === expected
          ? 'advanced'
          : 'gap',
      ...current,
      previousSequence: previous.sequence
    };
  }

  observeMany(ledgers) {
    if (!Array.isArray(ledgers)) {
      throw new TypeError('ledgers must be an array');
    }

    return ledgers.map(ledger =>
      this.observe(ledger)
    );
  }

  assertCurrent(sequence, hash = null) {
    validateLedgerNumber(sequence);

    if (this.#last == null) {
      throw new ConsistencyError(
        'no ledger has been observed'
      );
    }

    if (sequence !== this.#last.sequence) {
      throw new ConsistencyError(
        `current ledger mismatch: expected ${this.#last.sequence}, got ${sequence}`
      );
    }

    if (
      hash != null &&
      this.#last.hash != null &&
      String(hash) !== this.#last.hash
    ) {
      throw new ConsistencyError(
        `current ledger ${sequence} hash mismatch`
      );
    }

    return true;
  }
}

export function isLedgerMetadata(value) {
  return Boolean(
    value &&
    typeof value === 'object' &&
    Number.isSafeInteger(value.sequence) &&
    value.sequence >= 1
  );
}

function validateLedger(ledger) {
  if (!ledger || typeof ledger !== 'object') {
    throw new TypeError('ledger metadata must be an object');
  }

  validateLedgerNumber(ledger.sequence);

  if (
    ledger.hash != null &&
    (typeof ledger.hash !== 'string' ||
      ledger.hash.length === 0)
  ) {
    throw new TypeError(
      'ledger hash must be a non-empty string'
    );
  }

  const previousHash =
    ledger.previousHash ??
    ledger.prevHash;

  if (
    previousHash != null &&
    (typeof previousHash !== 'string' ||
      previousHash.length === 0)
  ) {
    throw new TypeError(
      'ledger previousHash must be a non-empty string'
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

function normalizeHash(value) {
  if (value == null) return null;

  if (
    typeof value !== 'string' ||
    value.length === 0
  ) {
    throw new TypeError(
      'ledger hash must be a non-empty string'
    );
  }

  return value;
}
