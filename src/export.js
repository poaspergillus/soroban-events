function normalizeFormat(format) {
  const value = String(format ?? 'jsonl').toLowerCase();

  if (value !== 'jsonl' && value !== 'csv') {
    throw new TypeError(
      'format must be jsonl or csv'
    );
  }

  return value;
}

function normalizeEvent(event) {
  if (event === null || typeof event !== 'object') {
    throw new TypeError(
      'export event must be an object'
    );
  }

  return event;
}

function jsonReplacer(_key, value) {
  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString('hex');
  }

  return value;
}

function toJsonLine(event) {
  return JSON.stringify(
    normalizeEvent(event),
    jsonReplacer
  );
}

function csvEscape(value) {
  if (value === undefined || value === null) {
    return '';
  }

  const text =
    typeof value === 'string'
      ? value
      : JSON.stringify(value, jsonReplacer);

  if (
    text.includes('"') ||
    text.includes(',') ||
    text.includes('\n') ||
    text.includes('\r')
  ) {
    return `"${text.replaceAll('"', '""')}"`;
  }

  return text;
}

const DEFAULT_COLUMNS = [
  'id',
  'ledger',
  'ledgerClosedAt',
  'contractId',
  'type',
  'transactionIndex',
  'operationIndex',
  'txHash',
  'topics',
  'value',
  'inSuccessfulContractCall'
];

function toCsvRow(event, columns) {
  return columns
    .map((column) =>
      csvEscape(normalizeEvent(event)[column])
    )
    .join(',');
}

export function createExporter(options = {}) {
  const format = normalizeFormat(options.format);
  const columns =
    options.columns ?? DEFAULT_COLUMNS;

  if (
    !Array.isArray(columns) ||
    columns.length === 0 ||
    columns.some(
      (column) => typeof column !== 'string' || !column
    )
  ) {
    throw new TypeError(
      'columns must be a non-empty array of strings'
    );
  }

  return {
    format,

    header() {
      return format === 'csv'
        ? columns.map(csvEscape).join(',')
        : null;
    },

    encode(event) {
      return format === 'jsonl'
        ? toJsonLine(event)
        : toCsvRow(event, columns);
    },

    columns: [...columns]
  };
}

export async function exportEvents(
  events,
  options = {}
) {
  const exporter = createExporter(options);
  const chunks = [];

  if (exporter.header() !== null) {
    chunks.push(exporter.header());
  }

  for await (const event of events) {
    chunks.push(exporter.encode(event));
  }

  return chunks.join('\n') +
    (chunks.length ? '\n' : '');
}

export async function* exportEventStream(
  events,
  options = {}
) {
  const exporter = createExporter(options);

  if (exporter.header() !== null) {
    yield exporter.header() + '\n';
  }

  for await (const event of events) {
    yield exporter.encode(event) + '\n';
  }
}

export const DEFAULT_EXPORT_COLUMNS = [
  ...DEFAULT_COLUMNS
];
