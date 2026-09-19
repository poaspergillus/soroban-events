import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createExporter,
  exportEvents,
  exportEventStream,
  DEFAULT_EXPORT_COLUMNS
} from '../src/index.js';

const events = [
  {
    id: 'e1',
    ledger: 10,
    contractId: 'C1',
    type: 'transfer',
    txHash: 'abc',
    topics: ['alice', 'bob'],
    value: {
      amount: 100
    }
  },
  {
    id: 'e2',
    ledger: 11,
    contractId: 'C2',
    type: 'mint',
    txHash: 'def',
    topics: ['carol'],
    value: {
      amount: 50
    }
  }
];

test('JSONL exports one JSON object per line', async () => {
  const output = await exportEvents(
    events,
    { format: 'jsonl' }
  );

  const lines = output.trim().split('\n');

  assert.equal(lines.length, 2);
  assert.deepEqual(
    JSON.parse(lines[0]),
    events[0]
  );
  assert.deepEqual(
    JSON.parse(lines[1]),
    events[1]
  );
});

test('JSONL exports bigint safely', async () => {
  const output = await exportEvents(
    [{
      id: 'big',
      value: 12345678901234567890n
    }],
    { format: 'jsonl' }
  );

  assert.match(
    output,
    /12345678901234567890/
  );
});

test('CSV exports a header and rows', async () => {
  const output = await exportEvents(
    events,
    {
      format: 'csv',
      columns: [
        'id',
        'ledger',
        'contractId',
        'type'
      ]
    }
  );

  const lines = output.trim().split('\n');

  assert.equal(
    lines[0],
    'id,ledger,contractId,type'
  );

  assert.equal(
    lines[1],
    'e1,10,C1,transfer'
  );

  assert.equal(
    lines[2],
    'e2,11,C2,mint'
  );
});

test('CSV escapes commas quotes and newlines', async () => {
  const output = await exportEvents(
    [{
      id: 'e,1',
      value: 'hello "world"\nnext'
    }],
    {
      format: 'csv',
      columns: ['id', 'value']
    }
  );

  assert.equal(
    output.trim(),
    'id,value\n"e,1","hello ""world""\nnext"'
  );
});

test('stream exporter emits chunks incrementally', async () => {
  async function* source() {
    yield events[0];
    yield events[1];
  }

  const chunks = [];

  for await (
    const chunk of exportEventStream(
      source(),
      {
        format: 'jsonl'
      }
    )
  ) {
    chunks.push(chunk);
  }

  assert.equal(chunks.length, 2);
  assert.equal(
    JSON.parse(chunks[0]).id,
    'e1'
  );
  assert.equal(
    JSON.parse(chunks[1]).id,
    'e2'
  );
});

test('CSV stream starts with header', async () => {
  async function* source() {
    yield events[0];
  }

  const chunks = [];

  for await (
    const chunk of exportEventStream(
      source(),
      {
        format: 'csv',
        columns: ['id', 'ledger']
      }
    )
  ) {
    chunks.push(chunk);
  }

  assert.equal(
    chunks[0],
    'id,ledger\n'
  );

  assert.equal(
    chunks[1],
    'e1,10\n'
  );
});

test('custom CSV columns are preserved', () => {
  const exporter = createExporter({
    format: 'csv',
    columns: ['txHash', 'id']
  });

  assert.deepEqual(
    exporter.columns,
    ['txHash', 'id']
  );

  assert.equal(
    exporter.header(),
    'txHash,id'
  );
});

test('default export columns are available', () => {
  assert.ok(
    DEFAULT_EXPORT_COLUMNS.includes('id')
  );

  assert.ok(
    DEFAULT_EXPORT_COLUMNS.includes('ledger')
  );

  assert.ok(
    DEFAULT_EXPORT_COLUMNS.includes('txHash')
  );
});

test('invalid export format is rejected', () => {
  assert.throws(
    () => createExporter({
      format: 'xml'
    }),
    /format must be jsonl or csv/
  );
});

test('invalid columns are rejected', () => {
  assert.throws(
    () => createExporter({
      format: 'csv',
      columns: []
    }),
    /columns must be a non-empty array/
  );
});

test('invalid events are rejected', async () => {
  await assert.rejects(
    exportEvents(
      [null],
      { format: 'jsonl' }
    ),
    /export event must be an object/
  );
});

test('empty export returns an empty string', async () => {
  assert.equal(
    await exportEvents([], {
      format: 'jsonl'
    }),
    ''
  );

  assert.equal(
    await exportEvents([], {
      format: 'csv'
    }),
    'id,ledger,ledgerClosedAt,contractId,type,transactionIndex,operationIndex,txHash,topics,value,inSuccessfulContractCall\n'
  );
});
