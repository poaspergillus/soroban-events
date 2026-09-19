export function createEventFilter(options = {}) {
  const {
    contractId,
    type,
    topic,
    txHash,
    ledger,
    startLedger,
    endLedger,
    predicate
  } = options;

  if (predicate !== undefined && typeof predicate !== 'function') {
    throw new TypeError('predicate must be a function');
  }

  const contracts = contractId == null
    ? null
    : new Set(
        Array.isArray(contractId)
          ? contractId.map(String)
          : [String(contractId)]
      );

  const types = type == null
    ? null
    : new Set(
        Array.isArray(type)
          ? type.map(String)
          : [String(type)]
      );

  const hashes = txHash == null
    ? null
    : new Set(
        Array.isArray(txHash)
          ? txHash.map(String)
          : [String(txHash)]
      );

  const topics = topic == null
    ? null
    : new Set(
        Array.isArray(topic)
          ? topic.map(normalizeTopic)
          : [normalizeTopic(topic)]
      );

  if (
    ledger !== undefined &&
    (!Number.isSafeInteger(ledger) || ledger <= 0)
  ) {
    throw new TypeError('ledger must be a positive safe integer');
  }

  if (
    startLedger !== undefined &&
    (!Number.isSafeInteger(startLedger) || startLedger <= 0)
  ) {
    throw new TypeError(
      'startLedger must be a positive safe integer'
    );
  }

  if (
    endLedger !== undefined &&
    (!Number.isSafeInteger(endLedger) || endLedger <= 0)
  ) {
    throw new TypeError(
      'endLedger must be a positive safe integer'
    );
  }

  if (
    startLedger !== undefined &&
    endLedger !== undefined &&
    startLedger > endLedger
  ) {
    throw new RangeError(
      'startLedger cannot exceed endLedger'
    );
  }

  return (event) => {
    if (event == null) return false;

    if (
      contracts &&
      !contracts.has(String(event.contractId))
    ) {
      return false;
    }

    if (
      types &&
      !types.has(String(event.type))
    ) {
      return false;
    }

    if (
      hashes &&
      !hashes.has(String(event.txHash))
    ) {
      return false;
    }

    if (ledger !== undefined &&
        event.ledger !== ledger) {
      return false;
    }

    if (
      startLedger !== undefined &&
      (!Number.isSafeInteger(event.ledger) ||
       event.ledger < startLedger)
    ) {
      return false;
    }

    if (
      endLedger !== undefined &&
      (!Number.isSafeInteger(event.ledger) ||
       event.ledger > endLedger)
    ) {
      return false;
    }

    if (topics) {
      const eventTopics = Array.isArray(event.topics)
        ? event.topics.map(normalizeTopic)
        : [];

      const matches = eventTopics.some(
        (value) => topics.has(value)
      );

      if (!matches) return false;
    }

    return predicate ? predicate(event) : true;
  };
}

function normalizeTopic(value) {
  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return String(value);
  }

  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString('hex');
  }

  return JSON.stringify(value);
}

export function filterEvents(events, options = {}) {
  if (!Array.isArray(events)) {
    throw new TypeError('events must be an array');
  }

  const filter = createEventFilter(options);
  return events.filter(filter);
}

export function matchesEvent(event, options = {}) {
  return createEventFilter(options)(event);
}
