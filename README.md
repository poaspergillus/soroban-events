# soroban-events

Resilient, windowed event streaming and XDR decoding for Soroban RPC.

`soroban-events` provides production-oriented primitives for retrieving, decoding, processing, storing, replaying, and exporting Soroban contract events.

## Why `soroban-events` exists

Soroban applications increasingly depend on events as a source of application data, but consuming those events reliably from Soroban RPC requires more than making a single `getEvents` request.

Applications need to deal with bounded ledger ranges, pagination, duplicate events, temporary RPC failures, historical backfills, checkpointing, reorgs, event decoding, and RPC retention limits. Building these reliability mechanisms independently in every application can lead to duplicated infrastructure and difficult-to-detect data-loss bugs.

`soroban-events` provides that ingestion layer as a reusable Node.js library.

Instead of each application building its own event polling and recovery system:

**Soroban RPC → `soroban-events` → application / database / indexer**

The goal is not to replace existing Soroban indexers or data platforms. It is to provide a reusable event-ingestion foundation that applications and larger indexing systems can build on.

### The problem it solves

A production event consumer cannot assume that an RPC request will always be complete, continuous, or successful.

`soroban-events` is designed around those failure modes:

- **Large historical ranges** → bounded ledger windows
- **RPC page limits** → automatic cursor pagination
- **Repeated requests / overlapping windows** → event-ID deduplication
- **Process crashes** → persistent checkpoints
- **Historical synchronization** → backfill and replay
- **Live applications** → streaming and consumption APIs
- **RPC failures** → retries, adaptive backoff, failover, and circuit breaking
- **Ledger reorganizations** → consistency tracking and recovery
- **Raw ScVal/XDR payloads** → event decoding
- **RPC retention limits** → health and retention checks
- **Application persistence** → SQLite and checkpoint/storage interfaces

### Who is this for?

`soroban-events` is intended for developers building:

- Soroban applications that need durable event data
- custom indexers
- analytics pipelines
- event-driven services
- webhook and automation systems
- data ingestion pipelines
- applications that need historical backfills and live event consumption

It can be used directly by an application or as the ingestion layer underneath a larger indexing system.

## Features

- Safe windowed retrieval for ledger ranges larger than the Soroban RPC range limit.
- Cursor-based pagination.
- Event-ID deduplication.
- Unlimited internal pagination with `limit: null`.
- Reliable ledger checkpointing.
- Durable file checkpoints.
- Memory and SQLite event stores.
- Backfill and historical replay.
- Backfill/live handoff protection.
- Reorg detection and recovery.
- RPC health and ledger-retention inspection.
- Configurable RPC failover.
- Circuit-breaker protection.
- Adaptive rate-limit handling.
- Exponential retry with jitter.
- Event filtering and custom predicates.
- Deterministic event querying.
- Event processing pipelines.
- JSONL and CSV export.
- Streaming export.
- Optional dead-letter queue primitives.
- CLI commands for status, backfill, replay, and health inspection.
- TypeScript declarations.
- Packed npm-consumer verification.

## Requirements

Node.js 22.12.0 or newer.

The runtime requirement follows the current Stellar SDK dependency requirements.

## Installation

```bash
npm install soroban-events
```

## Basic usage

```js
import { SorobanEventStreamer } from "soroban-events";

const streamer = new SorobanEventStreamer(
  "https://soroban-testnet.stellar.org"
);

const events = await streamer.getEventsWindowed({
  startLedger: 1000000,
  endLedger: 1010000,
  limit: null
});

for (const event of events) {
  console.log(event);
}
```

## Latest ledger

```js
const latest = await streamer.getLatestLedger();

console.log(latest);
```

Metadata can be requested when ledger information beyond the sequence is needed:

```js
const latest = await streamer.getLatestLedger({
  metadata: true
});

console.log(latest.sequence);
console.log(latest.hash);
```

## Large event ranges

Soroban RPC limits individual ledger-range queries. `soroban-events` automatically divides large ranges into safe windows.

Internal consumers such as backfill and consume use unlimited pagination so events are not silently truncated by a default event limit.

Public window retrieval accepts either a finite limit or `null` for unlimited retrieval:

```js
const events = await streamer.getEventsWindowed({
  startLedger: 1000000,
  endLedger: 1200000,
  limit: null
});
```

## Backfill

```js
import { backfill } from "soroban-events";

await backfill(streamer, {
  startLedger: 1000000,
  endLedger: 1100000,
  onEvent: async (event) => {
    console.log(event);
  }
});
```

Backfill supports checkpointing, event stores, pipelines, progress reporting, and concurrent window processing.

## Replay

```js
import { EventReplay } from "soroban-events";

const replay = new EventReplay(streamer);

await replay.run({
  startLedger: 1000000,
  endLedger: 1010000,
  onEvent: async (event) => {
    console.log(event);
  }
});
```

Replay supports checkpoint recovery, pipelines, persistence, and abort signals.

## Streaming

```js
const controller = new AbortController();

for await (const event of streamer.stream({
  startLedger: 1000000,
  signal: controller.signal
})) {
  console.log(event);
}

controller.abort();
```

Stream checkpointing does not advance past a ledger until all events from that ledger have been successfully consumed.

## Checkpointing

Checkpoint stores use the raw store contract:

```js
await store.save(key, ledger);
const ledger = await store.load(key);
await store.clear(key);
```

For monotonic checkpoint management and explicit rewinds, use `CheckpointManager`:

```js
const manager = new CheckpointManager(store);

await manager.save(ledger, "my-consumer");

const resumeLedger = await manager.resumeFrom(
  "my-consumer",
  startLedger
);

await manager.rewind(
  rewindLedger,
  "my-consumer"
);
```

Normal checkpoint saves cannot move backwards. Explicit rewinds are used for recovery workflows.

## File checkpoints

```js
import {
  FileCheckpointStore,
  CheckpointManager
} from "soroban-events";

const store = new FileCheckpointStore("./checkpoints");
const checkpoints = new CheckpointManager(store);

await checkpoints.save(
  1000000,
  "my-consumer"
);

const resumeLedger = await checkpoints.resumeFrom(
  "my-consumer",
  1
);
```

File checkpoints use atomic replacement so a partially written checkpoint does not replace the last valid checkpoint.

## Event filtering

```js
import { createEventFilter } from "soroban-events";

const filter = createEventFilter({
  contractId: "C...",
  type: "transfer"
});

const matching = events.filter(filter);
```

Filters can match:

- contract ID
- event type
- topics
- transaction hash
- exact ledger
- ledger ranges
- multiple contracts
- custom predicates

## Event querying

```js
import { createEventQuery } from "soroban-events";

const query = createEventQuery(store);

const results = await query({
  contractId: "C...",
  startLedger: 1000000,
  endLedger: 1010000,
  limit: 100
});
```

Queries provide deterministic ordering and pagination.

## Event storage

The package provides memory and SQLite event stores.

Memory storage:

```js
import { MemoryEventStore } from "soroban-events";

const store = new MemoryEventStore();

await store.put(event);

const events = await store.query({
  startLedger: 1000000,
  endLedger: 1010000
});
```

SQLite storage:

```js
import { SqliteEventStore } from "soroban-events";

const store = new SqliteEventStore("./events.db");

await store.put(event);
```

SQLite uses Node's built-in SQLite support and therefore requires a supported Node.js runtime.

## Event pipelines

Pipelines can filter, transform, observe, batch, and handle event-processing failures.

```js
import { EventPipeline } from "soroban-events";

const pipeline = new EventPipeline()
  .filter((event) => event.type === "transfer")
  .map((event) => ({
    ...event,
    processed: true
  }))
  .tap((event) => {
    console.log("processed", event.id);
  });
```

## RPC health

```js
const health = await streamer.getHealth();

console.log(health.status);
console.log(health.latestLedger);
console.log(health.oldestLedger);
console.log(health.ledgerRetentionWindow);
```

Retention can be checked directly:

```js
const result = await streamer.checkRetention(1000000);

console.log(result.retained);
```

## RPC failover

```js
const streamer = new SorobanEventStreamer(
  "https://primary-rpc.example",
  {
    failoverRpcUrls: [
      "https://secondary-rpc.example",
      "https://third-rpc.example"
    ]
  }
);
```

The streamer can rotate through configured RPC endpoints when transient failures exhaust retry attempts.

## Circuit breaker

Circuit-breaker behavior can be configured:

```js
const streamer = new SorobanEventStreamer(
  rpcUrl,
  {
    circuitBreakerThreshold: 3,
    circuitBreakerCooldownMs: 30000
  }
);
```

The circuit protects the consumer from repeatedly hammering an unavailable RPC endpoint.

## Adaptive retry

Retry behavior supports exponential backoff, rate-limit adaptation, maximum delay, and jitter:

```js
const streamer = new SorobanEventStreamer(
  rpcUrl,
  {
    maxRetries: 3,
    retryBaseMs: 500,
    retryMaxMs: 30000,
    retryJitter: 0.2,
    adaptiveRateLimit: true
  }
);
```

## Reorg recovery

The package provides ledger consistency tracking and reorg recovery primitives.

Recovery can:

- detect ledger hash mismatches;
- determine an affected rewind point;
- rewind checkpoints;
- invalidate affected stored events;
- restore the backfill/live handoff boundary.

## Event export

JSONL:

```js
import { exportEvents } from "soroban-events";

const jsonl = exportEvents(events, {
  format: "jsonl"
});
```

CSV:

```js
const csv = exportEvents(events, {
  format: "csv"
});
```

Streaming export is also supported for large event collections.

## CLI

Check RPC status:

```bash
npx soroban-events status --rpc <RPC_URL>
```

Backfill a ledger range:

```bash
npx soroban-events backfill --rpc <RPC_URL> --start <LEDGER> --end <LEDGER>
```

Replay a ledger range:

```bash
npx soroban-events replay --rpc <RPC_URL> --start <LEDGER> --end <LEDGER>
```

Health information:

```bash
npx soroban-events health --rpc <RPC_URL>
```

The CLI supports JSONL and CSV export options where applicable.

## TypeScript

TypeScript declarations are included with the package.

```ts
import {
  SorobanEventStreamer,
  CheckpointManager,
  MemoryCheckpointStore
} from "soroban-events";

const store = new MemoryCheckpointStore();
const checkpoints = new CheckpointManager(store);
```

## Reliability guarantees

The library is designed around several important ingestion properties.

### Pagination safety

Large RPC ranges are split into safe ledger windows and paginated using RPC cursors.

### Event deduplication

Event IDs are deduplicated across pagination and processing boundaries.

### Checkpoint safety

Consumer checkpoints are advanced only after the relevant ledger has been completely processed.

### Restart recovery

A failed consumer can resume from its last durable checkpoint and reprocess the uncommitted ledger.

### Backfill/live handoff

The handoff layer protects the transition between historical backfill and live consumption from duplicate delivery.

### Reorg handling

Ledger consistency checks can detect hash mismatches and trigger controlled checkpoint rewind and storage invalidation.

### RPC resilience

Transient failures, rate limits, endpoint failures, and unavailable RPC services can be handled through retry, adaptive backoff, failover, and circuit-breaking mechanisms.

## Testing

Run the complete test suite:

```bash
npm test
```

Run the packed-consumer test:

```bash
npm run test:pack
```

Run the release verification suite:

```bash
npm run test:release
```

The release verification includes:

- complete test suite;
- packed npm consumer verification;
- npm pack dry-run;
- Git whitespace validation.

## v3.0.0

Version 3 is a major compatibility boundary for the expanded public API.

Before upgrading, review:

- checkpoint store method signatures;
- `CheckpointManager` usage;
- streamer `limit` behavior;
- event filtering and querying;
- event storage APIs;
- export APIs;
- pipeline APIs;
- RPC failover and retry configuration;
- TypeScript declarations;
- Node.js 22.12.0 or newer requirement.

The v3.0.0 release is available on npm.

## Project status

`soroban-events` is an open-source project for reliable Soroban event ingestion and XDR decoding.

Development is focused on improving reliability, decoding, storage, backfill and replay, performance, testing, and developer experience.

Contributions are welcome. Bug reports, small improvements, tests, documentation updates, and new features are all useful.

For larger changes, please open an issue first so the scope can be discussed before implementation.
