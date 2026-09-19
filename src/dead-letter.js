export class DeadLetterError extends Error {
  constructor(message, options = {}) {
    super(message);
    this.name = 'DeadLetterError';

    if (options.cause !== undefined) {
      this.cause = options.cause;
    }

    if (options.event !== undefined) {
      this.event = options.event;
    }

    if (options.context !== undefined) {
      this.context = options.context;
    }
  }
}

export class MemoryDeadLetterQueue {
  #entries = new Map();
  #nextId = 1;

  constructor(options = {}) {
    this.maxEntries = options.maxEntries ?? 10000;

    if (
      !Number.isSafeInteger(this.maxEntries) ||
      this.maxEntries <= 0
    ) {
      throw new TypeError(
        'maxEntries must be a positive safe integer'
      );
    }
  }

  #clone(value) {
    if (value === undefined) return undefined;

    try {
      return structuredClone(value);
    } catch {
      return value;
    }
  }

  #createId(event) {
    if (event?.id != null) {
      return String(event.id);
    }

    return `dead-${this.#nextId++}`;
  }

  async add({
    event,
    error,
    context = undefined,
    attempts = 1
  } = {}) {
    if (event === undefined) {
      throw new TypeError('dead-letter event is required');
    }

    if (!(error instanceof Error)) {
      error = new Error(String(error ?? 'Unknown error'));
    }

    if (
      !Number.isSafeInteger(attempts) ||
      attempts < 1
    ) {
      throw new TypeError(
        'dead-letter attempts must be a positive safe integer'
      );
    }

    const id = this.#createId(event);

    const entry = {
      id,
      event: this.#clone(event),
      error: {
        name: error.name,
        message: error.message,
        code: error.code,
        stack: error.stack,
        cause: error.cause
      },
      context: this.#clone(context),
      attempts,
      createdAt: new Date().toISOString()
    };

    this.#entries.set(id, entry);

    while (this.#entries.size > this.maxEntries) {
      const oldest = this.#entries.keys().next().value;
      this.#entries.delete(oldest);
    }

    return this.#clone(entry);
  }

  async get(id) {
    return this.#clone(this.#entries.get(String(id)));
  }

  async list(options = {}) {
    const limit = options.limit ?? this.#entries.size;

    if (
      !Number.isSafeInteger(limit) ||
      limit < 0
    ) {
      throw new TypeError(
        'dead-letter limit must be a non-negative safe integer'
      );
    }

    const values = [...this.#entries.values()];

    return this.#clone(
      values.slice(
        Math.max(0, values.length - limit)
      )
    );
  }

  async remove(id) {
    return this.#entries.delete(String(id));
  }

  async retry(id) {
    const entry = this.#entries.get(String(id));

    if (!entry) {
      return undefined;
    }

    entry.attempts++;

    return this.#clone(entry);
  }

  async clear() {
    this.#entries.clear();
  }

  get size() {
    return this.#entries.size;
  }
}

export function isDeadLetterQueue(value) {
  return (
    value != null &&
    typeof value.add === 'function' &&
    typeof value.get === 'function' &&
    typeof value.list === 'function' &&
    typeof value.remove === 'function'
  );
}
