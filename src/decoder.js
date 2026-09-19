import { scValToNative } from '@stellar/stellar-sdk';

const DEFAULT_MAX_DEPTH = 50;

export function unwrapScVal(scVal, options = {}) {
  if (scVal == null) return null;

  const maxDepth = options.maxDepth ?? DEFAULT_MAX_DEPTH;

  try {
    return normalizeNative(scValToNative(scVal), 0, maxDepth);
  } catch {
    return parseScValDirect(scVal);
  }
}

function normalizeNative(value, depth, maxDepth) {
  if (depth > maxDepth) {
    throw new Error(`ScVal nesting exceeds maximum depth of ${maxDepth}`);
  }

  if (value === null || value === undefined) return null;

  if (typeof value === 'bigint') return value.toString();

  if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
    return Buffer.from(value).toString('hex');
  }

  if (Array.isArray(value)) {
    return value.map(v => normalizeNative(v, depth + 1, maxDepth));
  }

  if (value instanceof Map) {
    return new Map(
      [...value.entries()].map(([k, v]) => [
        normalizeNative(k, depth + 1, maxDepth),
        normalizeNative(v, depth + 1, maxDepth)
      ])
    );
  }

  if (typeof value === 'object') {
    const result = Object.create(null);

    for (const [key, val] of Object.entries(value)) {
      result[key] = normalizeNative(val, depth + 1, maxDepth);
    }

    return result;
  }

  return value;
}

function parseScValDirect(scVal) {
  try {
    if (!scVal?.switch) return null;

    const name = scVal.switch().name;

    switch (name) {
      case 'scvVoid': return null;
      case 'scvBool': return scVal.b();
      case 'scvU32': return scVal.u32();
      case 'scvI32': return scVal.i32();
      case 'scvU64': return scVal.u64().toString();
      case 'scvI64': return scVal.i64().toString();
      case 'scvU128': return scVal.u128().toString();
      case 'scvI128': return scVal.i128().toString();
      case 'scvU256': return scVal.u256().toString();
      case 'scvI256': return scVal.i256().toString();
      case 'scvSymbol': return scVal.sym().toString();
      case 'scvString': return scVal.str().toString();
      case 'scvBytes':
        return Buffer.from(
          scVal.bytes?.() ?? scVal.bin?.() ?? []
        ).toString('hex');
      case 'scvAddress':
        return scVal.address().toString();
      default:
        return `[Unresolved ScVal: ${name}]`;
    }
  } catch (error) {
    return `[ScVal decode error: ${error.message}]`;
  }
}

function normalizeContractId(value) {
  if (value == null || typeof value === 'string') return value;

  if (typeof value.toString === 'function') {
    const text = value.toString();

    if (
      text &&
      text !== '[object Object]' &&
      !/^\[object Object\]$/.test(text)
    ) {
      return text;
    }
  }

  return value;
}

export function decodeEvent(rawEvent, options = {}) {
  const topics = Array.isArray(rawEvent.topic)
    ? rawEvent.topic.map(topic => {
        try {
          return unwrapScVal(topic, options);
        } catch {
          return topic;
        }
      })
    : [];

  let value = null;

  if (rawEvent.value != null) {
    try {
      value = unwrapScVal(rawEvent.value, options);
    } catch {
      value = rawEvent.value;
    }
  }

  return {
    id: rawEvent.id,
    type: rawEvent.type,
    ledger: rawEvent.ledger,
    ledgerClosedAt: rawEvent.ledgerClosedAt,
    contractId: normalizeContractId(rawEvent.contractId),
    transactionIndex: rawEvent.transactionIndex,
    operationIndex: rawEvent.operationIndex,
    txHash: rawEvent.txHash,
    topics,
    value,
    inSuccessfulContractCall:
      rawEvent.inSuccessfulContractCall ?? true
  };
}
