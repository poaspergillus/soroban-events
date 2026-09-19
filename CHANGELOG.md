# Changelog

All notable changes to `soroban-events` are documented here.

## [3.0.0] - Unreleased

### Added

- Safe windowed Soroban RPC event retrieval.
- Unlimited internal cursor pagination.
- Event-ID deduplication across pagination and processing boundaries.
- Durable file checkpointing with atomic replacement.
- Unified checkpoint management and explicit rewind support.
- Backfill and historical replay.
- Backfill/live handoff protection.
- Ledger consistency and reorg recovery primitives.
- RPC health and retention inspection.
- Configurable RPC failover.
- Circuit-breaker protection.
- Adaptive rate-limit handling.
- Exponential retry with jitter and maximum delay.
- Event filtering and custom predicates.
- Deterministic event querying.
- Memory and SQLite event stores.
- Event processing pipelines.
- JSONL and CSV export.
- Streaming export.
- Optional dead-letter queue primitives.
- CLI commands for status, backfill, replay, and health inspection.
- TypeScript declarations.
- Packed npm-consumer verification.

### Compatibility

- Node.js `22.12.0` or newer is required.
- Checkpoint APIs were unified around the `CheckpointManager` and raw checkpoint-store contract.
- `SqliteEventStore` is the public SQLite store name.
- `limit: null` is supported for unlimited internal/public event retrieval.
- v3 represents a major API/reliability boundary.

## [2.0.2]

Previous release.
