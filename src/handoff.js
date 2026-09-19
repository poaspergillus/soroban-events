import {
  assertCheckpointStore
} from './checkpoint.js';

function validateKey(key) {
  if (typeof key !== 'string' || key.length === 0) {
    throw new TypeError(
      'checkpoint key must be a non-empty string'
    );
  }
}

function validateLedger(ledger) {
  if (!Number.isSafeInteger(ledger) || ledger < 1) {
    throw new TypeError(
      'checkpoint ledger must be a positive safe integer'
    );
  }
}

export class HandoffError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'HandoffError';
  }
}

export class EventHandoff {
  #checkpoint;
  #checkpointKey;
  #seen = new Set();
  #boundary = null;

  constructor(options = {}) {
    if (options.checkpoint != null) {
      assertCheckpointStore(options.checkpoint);
      this.#checkpoint = options.checkpoint;
    }

    this.#checkpointKey =
      options.checkpointKey ?? 'default';

    validateKey(this.#checkpointKey);

    if (
      options.boundaryLedger != null
    ) {
      validateLedger(options.boundaryLedger);
      this.#boundary = options.boundaryLedger;
    }
  }

  get boundaryLedger() {
    return this.#boundary;
  }

  get checkpointKey() {
    return this.#checkpointKey;
  }

  async initialize(boundaryLedger = this.#boundary) {
    if (boundaryLedger != null) {
      validateLedger(boundaryLedger);
      this.#boundary = boundaryLedger;
    }

    if (!this.#checkpoint) {
      return this.#boundary;
    }

    const stored =
      await this.#checkpoint.load(
        this.#checkpointKey
      );

    if (stored != null) {
      validateLedger(stored);

      if (
        this.#boundary == null ||
        stored > this.#boundary
      ) {
        this.#boundary = stored;
      }
    }

    return this.#boundary;
  }

  accept(event) {
    if (!event || typeof event !== 'object') {
      throw new TypeError(
        'handoff event must be an object'
      );
    }

    if (
      !Number.isSafeInteger(event.ledger) ||
      event.ledger < 1
    ) {
      throw new TypeError(
        'handoff event ledger must be a positive safe integer'
      );
    }

    if (this.#boundary != null &&
        event.ledger < this.#boundary) {
      return null;
    }

    if (
      this.#boundary != null &&
      event.ledger === this.#boundary
    ) {
      return this.#acceptBoundary(event);
    }

    return this.#acceptUnique(event);
  }

  acceptMany(events) {
    if (!Array.isArray(events)) {
      throw new TypeError(
        'handoff events must be an array'
      );
    }

    const output = [];

    for (const event of events) {
      const accepted = this.accept(event);

      if (accepted !== null) {
        output.push(accepted);
      }
    }

    return output;
  }

  async commit(ledger) {
    validateLedger(ledger);

    if (
      this.#boundary != null &&
      ledger < this.#boundary
    ) {
      throw new HandoffError(
        `handoff checkpoint cannot move before boundary: ${ledger} < ${this.#boundary}`
      );
    }

    if (this.#checkpoint) {
      await this.#checkpoint.save(
        this.#checkpointKey,
        ledger
      );
    }

    if (
      this.#boundary == null ||
      ledger > this.#boundary
    ) {
      this.#boundary = ledger;
    }

    return ledger;
  }

  resetSeen() {
    this.#seen.clear();
  }

  /**
   * Rewind the handoff boundary after a confirmed reorg.
   *
   * Unlike commit(), rewind() is intentionally allowed to move
   * the boundary backwards. The in-memory deduplication set is
   * also cleared because events from the affected range may be
   * replaced by different events after the reorg.
   */
  rewind(ledger) {
    validateLedger(ledger);

    this.#boundary = ledger;
    this.#seen.clear();

    return ledger;
  }

  #acceptBoundary(event) {
    const id = event.id;

    if (id == null) {
      return event;
    }

    if (this.#seen.has(id)) {
      return null;
    }

    this.#seen.add(id);
    return event;
  }

  #acceptUnique(event) {
    const id = event.id;

    if (id == null) {
      return event;
    }

    if (this.#seen.has(id)) {
      return null;
    }

    this.#seen.add(id);
    return event;
  }
}
