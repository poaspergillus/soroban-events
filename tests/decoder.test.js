import test from 'node:test';
import assert from 'node:assert/strict';

import { xdr } from '@stellar/stellar-sdk';
import {
  decodeEvent,
  unwrapScVal
} from '../src/index.js';

test('decodeEvent preserves basic fields', () => {
  const event = decodeEvent({
    id: 'abc',
    type: 'contract',
    ledger: 123,
    ledgerClosedAt: '2026-01-01T00:00:00Z',
    contractId: 'C123',
    topic: [],
    value: null,
    inSuccessfulContractCall: false
  });

  assert.equal(event.id, 'abc');
  assert.equal(event.ledger, 123);
  assert.equal(event.contractId, 'C123');
  assert.equal(event.inSuccessfulContractCall, false);
});

test('missing topic becomes empty array', () => {
  const event = decodeEvent({
    id: 'abc',
    ledger: 1,
    contractId: 'C123'
  });

  assert.deepEqual(event.topics, []);
  assert.equal(event.value, null);
});

test('SDK ScVal u64 is decoded without losing precision', () => {
  const scVal = xdr.ScVal.scvU64(
    xdr.Uint64.fromString('18446744073709551615')
  );

  assert.equal(
    unwrapScVal(scVal),
    '18446744073709551615'
  );
});

test('SDK ScVal symbol is decoded', () => {
  const scVal = xdr.ScVal.scvSymbol('hello');

  assert.equal(
    unwrapScVal(scVal),
    'hello'
  );
});

test('SDK ScVal bool is decoded', () => {
  const scVal = xdr.ScVal.scvBool(true);

  assert.equal(
    unwrapScVal(scVal),
    true
  );
});

test('SDK ScVal bytes become hex', () => {
  const scVal = xdr.ScVal.scvBytes(
    Buffer.from('hello')
  );

  assert.equal(
    unwrapScVal(scVal),
    Buffer.from('hello').toString('hex')
  );
});

test('decodeEvent decodes ScVal topic and value', () => {
  const event = decodeEvent({
    id: 'event-1',
    type: 'contract',
    ledger: 100,
    contractId: 'C123',
    topic: [
      xdr.ScVal.scvSymbol('transfer'),
      xdr.ScVal.scvBool(true)
    ],
    value: xdr.ScVal.scvU64(
      xdr.Uint64.fromString('9007199254740993')
    )
  });

  assert.deepEqual(
    event.topics,
    ['transfer', true]
  );

  assert.equal(
    event.value,
    '9007199254740993'
  );
});

test('contractId string is preserved', () => {
  const event = decodeEvent({
    id: 'event-2',
    ledger: 1,
    contractId: 'CABC',
    topic: []
  });

  assert.equal(event.contractId, 'CABC');
});
