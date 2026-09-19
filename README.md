# soroban-events

Lightweight Soroban RPC event ingestion, pagination, retries, deduplication, and ScVal decoding for Node.js.

Build Soroban dashboards, activity feeds, lightweight indexers, analytics tools, monitoring services, bots, and event-driven backends without implementing RPC pagination and event decoding yourself.

## Features

- Ledger-range windowing
- RPC cursor pagination
- Event deduplication
- Rate-limit and transient retries
- Soroban `ScVal` decoding
- Large integer precision preservation
- Contract event filtering
- Recent-event lookup with `tail()`
- Async event streaming with `stream()`
- AbortSignal support

## Install

```bash
npm install soroban-events
```

Requires Node.js 20+.

## Quick start

```js
import { SorobanEventStreamer } from 'soroban-events';

const streamer = new SorobanEventStreamer(
  'https://soroban-testnet.stellar.org'
);

const latest = await streamer.getLatestLedger();

const events = await streamer.getEventsWindowed({
  startLedger: latest - 100,
  endLedger: latest,
  filters: [{ type: 'contract' }],
  limit: 10
});

for (const event of events) {
  console.log(event);
}
```

## Filter by contract

```js
const events = await streamer.getEventsWindowed({
  startLedger: latest - 1000,
  endLedger: latest,
  filters: [
    {
      type: 'contract',
      contractIds: ['YOUR_CONTRACT_ID']
    }
  ],
  limit: 100
});
```

## Get recent events

```js
const events = await streamer.tail({
  contractId: 'YOUR_CONTRACT_ID',
  limit: 20
});

console.log(events);
```

## Stream events

```js
for await (const event of streamer.stream({
  startLedger: 4750000,
  filters: [
    {
      type: 'contract',
      contractIds: ['YOUR_CONTRACT_ID']
    }
  ]
})) {
  console.log('New event:', event);
}
```

Stop a stream with `AbortController`:

```js
const controller = new AbortController();

setTimeout(() => controller.abort(), 30_000);

for await (const event of streamer.stream({
  startLedger: 4750000,
  signal: controller.signal
})) {
  console.log(event);
}
```

## Decode ScVal

The decoder can also be used independently:

```js
import { unwrapScVal } from 'soroban-events';

const value = unwrapScVal(scVal);
console.log(value);
```

Common Soroban values supported include integers, symbols, strings, booleans, bytes, addresses, vectors, and maps.

Large integer values are normalized without relying on JavaScript `Number` precision.

## Event format

Events are normalized into a consistent object containing fields such as:

```js
{
  id,
  type,
  ledger,
  ledgerClosedAt,
  contractId,
  transactionIndex,
  operationIndex,
  txHash,
  topics,
  value,
  inSuccessfulContractCall
}
```

## Configuration

```js
const streamer = new SorobanEventStreamer(RPC_URL, {
  pollInterval: 3000,
  windowSize: 9500,
  pageSize: 1000,
  maxRetries: 3,
  retryBaseMs: 500
});
```

| Option | Default | Description |
|---|---:|---|
| `pollInterval` | `3000` | Polling interval for `stream()` |
| `windowSize` | `9500` | Maximum ledger window |
| `pageSize` | `1000` | RPC page size |
| `maxRetries` | `3` | Maximum retry attempts |
| `retryBaseMs` | `500` | Base retry delay |

## How it works

```text
Soroban RPC
    |
    v
ledger windows
    |
    v
cursor pagination
    |
    v
retry handling
    |
    v
deduplication
    |
    v
ScVal decoding
    |
    v
your application
```

No database is required.

No full indexer stack is required.

Use the normalized events in whatever application or storage layer you need.

## Testing

Run the unit tests:

```bash
npm test
```

Live Stellar Testnet verification:

```bash
npm run test:live
```

The project currently includes 22 automated tests covering windowing, pagination, deduplication, limits, retries, abort handling, filtering, ScVal decoding, large integers, and event normalization.

The live test has also been verified against Stellar Testnet RPC.

## Status

**v0.1.0**

The current release focuses on the core RPC ingestion and ScVal decoding layer.

This is intentionally a lightweight library, not a database-backed blockchain indexer.

Potential future work includes durable cursor persistence, stronger recovery strategies, webhook delivery, richer filtering helpers, and production indexing integrations.

## Contributing

Bug reports, edge cases, documentation improvements, test cases, and pull requests are welcome.

When reporting an RPC issue, include the endpoint/network, ledger range, relevant RPC error or response, expected behavior, and actual behavior.

## License

MIT
