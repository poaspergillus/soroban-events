export function validateEventQuery(options = {}) {
  const {
    startLedger,
    endLedger,
    contractId,
    type,
    txHash,
    limit = 100,
    offset = 0
  } = options;

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

  if (
    !Number.isSafeInteger(limit) ||
    limit <= 0
  ) {
    throw new TypeError(
      'limit must be a positive safe integer'
    );
  }

  if (
    !Number.isSafeInteger(offset) ||
    offset < 0
  ) {
    throw new TypeError(
      'offset must be a non-negative safe integer'
    );
  }

  return {
    startLedger,
    endLedger,
    contractId:
      contractId === undefined
        ? undefined
        : String(contractId),
    type:
      type === undefined
        ? undefined
        : String(type),
    txHash:
      txHash === undefined
        ? undefined
        : String(txHash),
    limit,
    offset
  };
}

export function queryEvents(events, options = {}) {
  if (!Array.isArray(events)) {
    throw new TypeError('events must be an array');
  }

  const query = validateEventQuery(options);

  const filtered = events
    .filter((event) => {
      if (event == null) return false;

      if (
        query.startLedger !== undefined &&
        (!Number.isSafeInteger(event.ledger) ||
         event.ledger < query.startLedger)
      ) {
        return false;
      }

      if (
        query.endLedger !== undefined &&
        (!Number.isSafeInteger(event.ledger) ||
         event.ledger > query.endLedger)
      ) {
        return false;
      }

      if (
        query.contractId !== undefined &&
        String(event.contractId) !== query.contractId
      ) {
        return false;
      }

      if (
        query.type !== undefined &&
        String(event.type) !== query.type
      ) {
        return false;
      }

      if (
        query.txHash !== undefined &&
        String(event.txHash) !== query.txHash
      ) {
        return false;
      }

      return true;
    })
    .sort(compareStoredEvents);

  const total = filtered.length;

  const page = filtered.slice(
    query.offset,
    query.offset + query.limit
  );

  return {
    events: page,
    total,
    offset: query.offset,
    limit: query.limit,
    hasMore:
      query.offset + page.length < total
  };
}

function compareStoredEvents(a, b) {
  const ledgerA =
    Number.isSafeInteger(a?.ledger)
      ? a.ledger
      : Number.MAX_SAFE_INTEGER;

  const ledgerB =
    Number.isSafeInteger(b?.ledger)
      ? b.ledger
      : Number.MAX_SAFE_INTEGER;

  if (ledgerA !== ledgerB) {
    return ledgerA - ledgerB;
  }

  const txA = a?.transactionIndex ?? 0;
  const txB = b?.transactionIndex ?? 0;

  if (txA !== txB) {
    return txA - txB;
  }

  const opA = a?.operationIndex ?? 0;
  const opB = b?.operationIndex ?? 0;

  if (opA !== opB) {
    return opA - opB;
  }

  return String(a?.id ?? '')
    .localeCompare(String(b?.id ?? ''));
}

export function createEventQuery(store) {
  if (
    store == null ||
    typeof store.list !== 'function'
  ) {
    throw new TypeError(
      'event store must provide list()'
    );
  }

  return async (options = {}) => {
    const events = await store.list();
    return queryEvents(events, options);
  };
}
