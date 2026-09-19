# soroban-events

A lightweight Node.js event ingestion and ScVal decoding layer for Soroban RPC.

## Features

- RPC ledger windowing
- cursor pagination
- duplicate event dedupliccation
- transient and rate-limit retrier
- AbortSignal support
- ScVal XDRdecoding
- large integer preservation

## Install

```
npm install soroban-events
```

## Basic usage

```js
async function main() {
  const streamer = new SorobanEventStreamer(
    'https://soroban-testnet.stellar.org'
  );

  const latest = await streamer.getLatestLedger();

  const events = await streamer.getEventsWindowed({
    startLedger: Math.max(1, latest - 100),
    endLedger: latest,
    filters: [{ type: 'contract' }],
    limit: 100
  });

  console.log(events);
}

main();
```

## Event decoding

```js
const events = await streamer.tail({
  contractId: 'YOUR_CONTRACT_ID',
  limit: 10
});
```

## Development

```bash
n test
```

Tests use Node's built-in test runner and require no extra testing dependencies.

## Status

Version 0.1.0 focuses on the core Soroban RPC ingestion and ScVal decoding layer.

## License

MIT
