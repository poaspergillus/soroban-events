import test from 'node:test';
import assert from 'node:assert/strict';

import {
  decodeEvent,
  unwrapScVal,
  ScValDepthError
} from '../src/index.js';

test('unwrapScVal handles primitive values', () => {
  assert.equal(
    unwrapScVal(42),
    42
  );

  assert.equal(
    unwrapScVal('hello'),
    'hello'
  );

  assert.equal(
    unwrapScVal(true),
    true
  );
});

test('unwrapScVal normalizes bigint', () => {
  assert.equal(
    unwrapScVal(12345678901234567890n),
    '12345678901234567890'
  );
});

test('unwrapScVal normalizes bytes', () => {
  const value =
    unwrapScVal(
      new Uint8Array([
        0,
        1,
        255
      ])
    );

  assert.equal(
    value,
    '0001ff'
  );
});

test('unwrapScVal recursively normalizes arrays', () => {
  const value =
    unwrapScVal([
      1n,
      [2n, 3n]
    ]);

  assert.deepEqual(
    value,
    [
      '1',
      [
        '2',
        '3'
      ]
    ]
  );
});

test('unwrapScVal enforces maximum depth', () => {
  let value = 'leaf';

  for (let i = 0; i < 10; i++) {
    value = [value];
  }

  assert.throws(
    () =>
      unwrapScVal(
        value,
        {
          maxDepth: 3
        }
      ),
    ScValDepthError
  );
});

test('decodeEvent normalizes event metadata', () => {
  const raw = {
    id: 'event-1',
    type: 'contract',
    ledger: 123,
    ledgerClosedAt:
      '2026-01-01T00:00:00Z',
    transactionHash:
      'tx-hash',
    contractId:
      'contract-id',
    topics: [],
    value: null
  };

  const decoded =
    decodeEvent(raw);

  assert.equal(
    decoded.id,
    'event-1'
  );

  assert.equal(
    decoded.ledger,
    123
  );

  assert.equal(
    decoded.txHash,
    'tx-hash'
  );

  assert.equal(
    decoded.contractId,
    'contract-id'
  );
});

test('decodeEvent preserves successful contract-call information', () => {
  const decoded =
    decodeEvent({
      id: 'event-2',
      type: 'contract',
      ledger: 200,
      transactionSuccessful: true,
      topics: [],
      value: null
    });

  assert.equal(
    decoded.inSuccessfulContractCall,
    true
  );
});

test('decodeEvent accepts decoder depth options', () => {
  const decoded =
    decodeEvent(
      {
        id: 'event-3',
        ledger: 1,
        type: 'contract',
        topics: [],
        value: null
      },
      {
        maxDepth: 10
      }
    );

  assert.equal(
    decoded.id,
    'event-3'
  );
});
