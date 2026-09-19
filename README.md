# soroban-events

[![npm](https://img.shields.io/npm/v/soroban-events)](https://www.npmjs.com/package/soroban-events)
[![license](https://img.shields.io/npm/l/soroban-events)](LICENSE)

**Soroban RPC event infrastructure for Node.js.**

`soroban-events` provides reusable building blocks for retrieving, decoding, processing, storing, replaying, and streaming Soroban RPC events.

## What it solves

Soroban event consumers commonly need to handle:

- bounded ledger windows
- RPC pagination
- retry and rate-limit handling
- ScVal/XDR decoding
- event normalization and deduplication
- historical backfill
- replay
- durable checkpoints
- event storage
- ledger consistency
- recovery and reorg monitoring
- historical-to-live handoff
- pipelines
- metrics

The library keeps those concerns separate so an application can choose the pieces it needs.

```text
Soroban RPC
    |
    v
SorobanEventStreamer
    |
    +-- windowing / pagination / retries
    +-- decoding / normalization / deduplication
    |
    +-- BackfillEngine
    +-- EventReplay
    +-- EventPipeline
    +-- checkpoints
    +-- event stores
    +-- consistency / recovery
    +-- live consumption
```

## Install

```bash
npm install soroban-events
```

## Quick start

```js
import { SorobanEventStreamer } from "soroban-events";

const streamer = new SorobanEventStreamer(
  "https://soroban-testnet.stellar.org"
);

const latest = await streamer.getLatestLedger();

const events = await streamer.getEventsWindowed({
  startLedger: latest.sequence - 100,
  endLedger: latest.sequence,
  limit: 100
});

for (const event of events) {
  console.log(event);
}
```

## Event retrieval

### Latest ledger

```js
const latest = await streamer.getLatestLedger();

console.log(latest.sequence);
console.log(latest.hash);
console.log(latest.closeTime);
console.log(latest.protocolVersion);
```

### Windowed retrieval

```js
const events = await streamer.getEventsWindowed {
  startLedger: 1000000,
  endLedger: 1010000,
  limit: 100
});
```

Ranges are processed through bounded windows. The implementation defaults to a maximum window size of `9500` ledgers and a page size of `1000`.

### RPC filters

```js
const events = await streamer.getEventsWindowed({
  startLedger: 1000000,
  endLedge: