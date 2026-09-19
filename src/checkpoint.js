const INTEGER_MIN = 1;

export class CheckpointError extends Error {
  constructor(message) {
    super(message);
    this.name = 'CheckpointError';
  }
}

export class MemoryCheckpointStore {
  #checkpoints = new Map();

  async load(key) {
    validateKey(key);
    return this.#checkpoints.get(key) ?? null;
  }

  async save(key, ledger) {
    validateKey(key);
    validateLedger(ledger);

    const previous = this.#checkpoints.get(key);

    if (previous != null && ledger < previous) {
      throw new CheckpointError(
        `checkpoint cannot move backwards: ${ledger} < ${previous}`
      );
    }

    this.#checkpoints.set(key, ledger);
  }

  async rewind(key, ledger) {
    validateKey(key);
    validateLedger(ledger);

    this.#checkpoints.set(key, ledger);
  }

  async clear(key) {
    validateKey(key);
    this.#checkpoints.delete(key);
  }
}

export class CheckpointManager {
  constructor(store, options = {}) {
    if (!store || typeof store.load !== 'function') {
      throw new TypeError(
        'checkpoint store must implement load()'
      );
    }

    if (typeof store.save !== 'function') {
      throw new TypeError(
        'checkpoint store must implement save()'
      );
    }

    if (typeof store.clear !== 'function') {
      throw new TypeError(
        'checkpoint store must implement clear()'
      );
    }

    this.store = store;
    this.defaultKey = options.defaultKey ?? 'default';
  }

  async load(key = this.defaultKey) {
    validateKey(key);

    const ledger = await this.store.load(key);

    if (ledger == null) {
      return null;
    }

    validateLedger(ledger);
    return ledger;
  }

  async save(ledger, key = this.defaultKey) {
    validateKey(key);
    validateLedger(ledger);

    const current = await this.load(key);

    if (current != null && ledger < current) {
      throw new CheckpointError(
        `checkpoint cannot move backwards: ${ledger} < ${current}`
      );
    }

    await this.store.save(key, ledger);
    return ledger;
  }

  async rewind(ledger, key = this.defaultKey) {
    validateKey(key);
    validateLedger(ledger);

    if (typeof this.store.rewind !== 'function') {
      throw new CheckpointError(
        'checkpoint store does not support rewind()'
      );
    }

    await this.store.rewind(
      key,
      ledger
    );

    return ledger;
  }

  async clear(key = this.defaultKey) {
    validateKey(key);
    await this.store.clear(key);
  }

  async resumeFrom(key = this.defaultKey, fallbackLedger = 1) {
    validateKey(key);
    validateLedger(fallbackLedger);

    const checkpoint = await this.load(key);

    return checkpoint == null
      ? fallbackLedger
      : checkpoint;
  }
}

export function isCheckpointStore(value) {
  return Boolean(
    value &&
    typeof value.load === 'function' &&
    typeof value.save === 'function' &&
    typeof value.clear === 'function'
  );
}

export function assertCheckpointStore(value) {
  if (!isCheckpointStore(value)) {
    throw new TypeError(
      'checkpoint store must implement load(), save(), and clear()'
    );
  }

  return value;
}

function validateKey(key) {
  if (typeof key !== 'string' || key.length === 0) {
    throw new TypeError(
      'checkpoint key must be a non-empty string'
    );
  }
}

function validateLedger(ledger) {
  if (!Number.isSafeInteger(ledger) || ledger < INTEGER_MIN) {
    throw new TypeError(
      'checkpoint ledger must be a positive safe integer'
    );
  }
}
