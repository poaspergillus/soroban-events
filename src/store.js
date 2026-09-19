export class EventStoreError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'EventStoreError';
  }
}

export class MemoryEventStore {
  #events = new Map();

  async put(event) {
    validateEvent(event);
    this.#events.set(String(event.id), clone(event));
    return clone(event);
  }

  async get(id) {
    validateId(id);
    const event = this.#events.get(String(id));
    return event == null ? null : clone(event);
  }

  async has(id) {
    validateId(id);
    return this.#events.has(String(id));
  }

  async delete(id) {
    validateId(id);
    return this.#events.delete(String(id));
  }

  async list(options = {}) {
    const events = [...this.#events.values()]
      .map(clone)
      .sort(compareEvents);

    const startLedger =
      options.startLedger ?? 1;

    const endLedger =
      options.endLedger ?? Number.MAX_SAFE_INTEGER;

    validateLedger(startLedger, 'startLedger');
    validateLedger(endLedger, 'endLedger');

    if (endLedger < startLedger) {
      throw new TypeError(
        'endLedger must be greater than or equal to startLedger'
      );
    }

    const limit =
      options.limit ??
      Math.max(events.length, 1);

    if (
      !Number.isSafeInteger(limit) ||
      limit < 1
    ) {
      throw new TypeError(
        'limit must be a positive safe integer'
      );
    }

    return events
      .filter(
        event =>
          event.ledger >= startLedger &&
          event.ledger <= endLedger
      )
      .slice(0, limit);
  }

  async count() {
    return this.#events.size;
  }

  async clear() {
    this.#events.clear();
  }
}

export class SqliteEventStore {
  #db;

  constructor(filename = ':memory:') {
    if (
      typeof filename !== 'string' ||
      filename.length === 0
    ) {
      throw new TypeError(
        'SQLite filename must be a non-empty string'
      );
    }

    let DatabaseSync;

    try {
      ({ DatabaseSync } = requireNodeSqlite());
    } catch (error) {
      throw new EventStoreError(
        'SQLiteEventStore requires Node.js with node:sqlite support',
        { cause: error }
      );
    }

    this.#db = new DatabaseSync(filename);

    this.#db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ledger INTEGER NOT NULL,
        transaction_index INTEGER,
        operation_index INTEGER,
        payload TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_events_ledger
      ON events(ledger);
    `);
  }

  async put(event) {
    validateEvent(event);

    const id = String(event.id);

    this.#db
      .prepare(`
        INSERT INTO events (
          id,
          ledger,
          transaction_index,
          operation_index,
          payload
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          ledger = excluded.ledger,
          transaction_index = excluded.transaction_index,
          operation_index = excluded.operation_index,
          payload = excluded.payload
      `)
      .run(
        id,
        event.ledger,
        nullableIndex(
          event.transactionIndex ?? event.txIndex
        ),
        nullableIndex(
          event.operationIndex ?? event.opIndex
        ),
        JSON.stringify(event)
      );

    return clone(event);
  }

  async get(id) {
    validateId(id);

    const row = this.#db
      .prepare(`
        SELECT payload
        FROM events
        WHERE id = ?
      `)
      .get(String(id));

    return row == null
      ? null
      : JSON.parse(row.payload);
  }

  async has(id) {
    validateId(id);

    const row = this.#db
      .prepare(`
        SELECT 1 AS found
        FROM events
        WHERE id = ?
        LIMIT 1
      `)
      .get(String(id));

    return row != null;
  }

  async delete(id) {
    validateId(id);

    const result = this.#db
      .prepare(`
        DELETE FROM events
        WHERE id = ?
      `)
      .run(String(id));

    return Number(result.changes) > 0;
  }

  async list(options = {}) {
    const startLedger =
      options.startLedger ?? 1;

    const endLedger =
      options.endLedger ?? Number.MAX_SAFE_INTEGER;

    validateLedger(startLedger, 'startLedger');
    validateLedger(endLedger, 'endLedger');

    if (endLedger < startLedger) {
      throw new TypeError(
        'endLedger must be greater than or equal to startLedger'
      );
    }

    const limit = options.limit ?? 1000;

    if (
      !Number.isSafeInteger(limit) ||
      limit < 1
    ) {
      throw new TypeError(
        'limit must be a positive safe integer'
      );
    }

    const rows = this.#db
      .prepare(`
        SELECT payload
        FROM events
        WHERE ledger >= ?
          AND ledger <= ?
        ORDER BY
          ledger ASC,
          transaction_index ASC,
          operation_index ASC,
          id ASC
        LIMIT ?
      `)
      .all(
        startLedger,
        endLedger,
        limit
      );

    return rows.map(
      row => JSON.parse(row.payload)
    );
  }

  async count() {
    const row = this.#db
      .prepare(`
        SELECT COUNT(*) AS count
        FROM events
      `)
      .get();

    return Number(row.count);
  }

  async clear() {
    this.#db.exec('DELETE FROM events');
  }

  close() {
    this.#db.close();
  }
}

export function isEventStore(value) {
  return Boolean(
    value &&
    typeof value.put === 'function' &&
    typeof value.get === 'function' &&
    typeof value.has === 'function' &&
    typeof value.delete === 'function' &&
    typeof value.list === 'function' &&
    typeof value.count === 'function' &&
    typeof value.clear === 'function'
  );
}

export function assertEventStore(value) {
  if (!isEventStore(value)) {
    throw new TypeError(
      'event store must implement put(), get(), has(), delete(), list(), count(), and clear()'
    );
  }

  return value;
}

function validateEvent(event) {
  if (!event || typeof event !== 'object') {
    throw new TypeError(
      'event must be an object'
    );
  }

  if (
    event.id == null ||
    String(event.id).length === 0
  ) {
    throw new TypeError(
      'event id must be non-empty'
    );
  }

  validateLedger(
    event.ledger,
    'event ledger'
  );
}

function validateId(id) {
  if (
    id == null ||
    String(id).length === 0
  ) {
    throw new TypeError(
      'event id must be non-empty'
    );
  }
}

function validateLedger(value, name) {
  if (
    !Number.isSafeInteger(value) ||
    value < 1
  ) {
    throw new TypeError(
      `${name} must be a positive safe integer`
    );
  }
}

function nullableIndex(value) {
  return Number.isSafeInteger(value)
    ? value
    : null;
}

function compareEvents(a, b) {
  if (a.ledger !== b.ledger) {
    return a.ledger - b.ledger;
  }

  const txA =
    a.transactionIndex ??
    a.txIndex ??
    Number.MAX_SAFE_INTEGER;

  const txB =
    b.transactionIndex ??
    b.txIndex ??
    Number.MAX_SAFE_INTEGER;

  if (txA !== txB) {
    return txA - txB;
  }

  const opA =
    a.operationIndex ??
    a.opIndex ??
    Number.MAX_SAFE_INTEGER;

  const opB =
    b.operationIndex ??
    b.opIndex ??
    Number.MAX_SAFE_INTEGER;

  if (opA !== opB) {
    return opA - opB;
  }

  return String(a.id).localeCompare(
    String(b.id)
  );
}

function clone(value) {
  return JSON.parse(
    JSON.stringify(value)
  );
}

function requireNodeSqlite() {
  try {
    return {
      DatabaseSync:
        process.getBuiltinModule('node:sqlite')
          .DatabaseSync
    };
  } catch (error) {
    throw error;
  }
}
