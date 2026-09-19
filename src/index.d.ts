export type LifecycleState =
  | 'idle'
  | 'running'
  | 'stopping'
  | 'stopped';

export declare const LIFECYCLE_STATES:
  readonly [
    'idle',
    'running',
    'stopping',
    'stopped'
  ];

export interface StreamerOptions {
  rpcUrl: string;
  pollInterval?: number;
  windowSize?: number;
  pageSize?: number;
  maxRetries?: number;
  retryBaseMs?: number;
  metrics?: Metrics;
}

export interface EventFilters {
  contractIds?: string[];
  topics?: unknown[];
  eventTypes?: string[];
}

export interface CheckpointStore {
  load(key: string): Promise<number | null>;
  save(key: string, ledger: number): Promise<void>;
  clear(key: string): Promise<void>;
  rewind?(
    key: string,
    ledger: number
  ): Promise<void>;
}

export interface RewindableCheckpointStore
  extends CheckpointStore {
  rewind(
    key: string,
    ledger: number
  ): Promise<void>;
}

export declare class CheckpointError extends Error {}

export declare class MemoryCheckpointStore
  implements CheckpointStore {
  load(key: string): Promise<number | null>;
  save(key: string, ledger: number): Promise<void>;
  rewind(key: string, ledger: number): Promise<void>;
  clear(key: string): Promise<void>;
}

export declare class CheckpointManager {
  constructor(
    store: CheckpointStore,
    options?: {
      defaultKey?: string;
    }
  );

  load(
    key?: string
  ): Promise<number | null>;

  save(
    ledger: number,
    key?: string
  ): Promise<number>;

  rewind(
    ledger: number,
    key?: string
  ): Promise<number>;

  clear(
    key?: string
  ): Promise<void>;

  resumeFrom(
    key?: string,
    fallbackLedger?: number
  ): Promise<number>;
}

export declare class FileCheckpointStore
  implements CheckpointStore {
  constructor(filename: string);

  load(key: string): Promise<number | null>;
  save(key: string, ledger: number): Promise<void>;
  rewind(key: string, ledger: number): Promise<void>;
  clear(key: string): Promise<void>;
}

export declare function isCheckpointStore(
  value: unknown
): value is CheckpointStore;

export declare function assertCheckpointStore(
  value: unknown
): CheckpointStore;

export declare class ScValDepthError extends Error {}

export declare function unwrapScVal(
  value: unknown,
  options?: {
    maxDepth?: number;
  }
): unknown;

export declare function decodeEvent(
  rawEvent: unknown,
  options?: {
    maxDepth?: number;
  }
): Record<string, unknown>;

export declare class Streamer {
  constructor(
    options: StreamerOptions
  );

  getLatestLedger(
    options?: {
      metadata?: boolean;
    }
  ): Promise<number | {
    sequence: number;
    hash: string | null;
    closeTime: string | null;
    protocolVersion: number | null;
  }>;

  getEventsWindowed(
    options: {
      startLedger: number;
      endLedger?: number;
      filters?: EventFilters[];
      limit?: number;
      signal?: AbortSignal;
    }
  ): Promise<unknown[]>;

  tail(
    options?: {
      contractId?: string;
      limit?: number;
      maxLookbackLedgers?: number;
      filters?: EventFilters[];
      signal?: AbortSignal;
    }
  ): Promise<unknown[]>;

  consume(
    options?: Record<string, unknown>
  ): AsyncIterable<unknown>;

  stream(
    options?: Record<string, unknown>
  ): AsyncIterable<unknown>;

  getLedgerHistory(
    options: {
      startLedger: number;
      endLedger?: number;
      limit?: number;
      signal?: AbortSignal;
    }
  ): Promise<unknown>;

  verifyLedgerHistory(
    options: {
      startLedger: number;
      endLedger?: number;
      signal?: AbortSignal;
    }
  ): Promise<unknown>;
}

export declare class PipelineError extends Error {}

export declare class EventPipeline {
  constructor(
    stages?: unknown[]
  );

  filter(
    predicate: Function
  ): EventPipeline;

  map(
    mapper: Function
  ): EventPipeline;

  tap(
    handler: Function
  ): EventPipeline;

  process(
    event: unknown,
    context?: unknown
  ): Promise<unknown>;

  processBatch(
    events: unknown[],
    context?: unknown
  ): Promise<unknown[]>;
}

export declare class BackfillError extends Error {}

export declare class BackfillEngine {
  constructor(
    streamer: Streamer,
    options?: {
      concurrency?: number;
      windowSize?: number;
    }
  );

  run(
    options: Record<string, unknown>
  ): Promise<Record<string, unknown>>;
}

export declare function compareEvents(
  a: unknown,
  b: unknown
): number;

export declare function orderBackfillEvents(
  events: unknown[]
): unknown[];

export declare class ReplayError extends Error {}

export declare class EventReplay {
  constructor(
    streamer: Streamer,
    options?: Record<string, unknown>
  );

  run(
    options: Record<string, unknown>
  ): Promise<Record<string, unknown>>;
}

export declare class EventHandoff {
  constructor(
    options?: Record<string, unknown>
  );

  initialize(
    boundaryLedger: number
  ): Promise<unknown>;

  accept(
    event: unknown
  ): Promise<boolean>;

  acceptMany(
    events: unknown[]
  ): Promise<unknown[]>;

  commit(
    ledger: number
  ): Promise<number>;

  resetSeen(): void;
}

export declare class EventStoreError extends Error {}

export interface EventStore {
  put(event: unknown): Promise<unknown>;
  get(id: string): Promise<unknown | null>;
  has(id: string): Promise<boolean>;
  delete(id: string): Promise<boolean>;
  list(options?: Record<string, unknown>): Promise<unknown[]>;
  count(): Promise<number>;
  clear(): Promise<void>;
}

export declare class MemoryEventStore
  implements EventStore {
  put(event: unknown): Promise<unknown>;
  get(id: string): Promise<unknown | null>;
  has(id: string): Promise<boolean>;
  delete(id: string): Promise<boolean>;
  list(options?: Record<string, unknown>): Promise<unknown[]>;
  count(): Promise<number>;
  clear(): Promise<void>;
}

export declare class SqliteEventStore
  implements EventStore {
  constructor(
    options?: {
      filename?: string;
    } | string
  );

  put(event: unknown): Promise<unknown>;
  get(id: string): Promise<unknown | null>;
  has(id: string): Promise<boolean>;
  delete(id: string): Promise<boolean>;
  list(options?: Record<string, unknown>): Promise<unknown[]>;
  count(): Promise<number>;
  clear(): Promise<void>;
  close(): void;
}

export declare function isEventStore(
  value: unknown
): value is EventStore;

export declare function assertEventStore(
  value: unknown
): EventStore;

export declare class ConsistencyError extends Error {}

export declare class LedgerConsistencyTracker {
  constructor(
    options?: Record<string, unknown>
  );

  verify(
    ledger: unknown
  ): unknown;

  verifyMany(
    ledgers: unknown[]
  ): unknown;
}

export declare class Metrics {
  constructor();

  increment(
    name: string,
    value?: number
  ): void;

  decrement(
    name: string,
    value?: number
  ): void;

  gauge(
    name: string,
    value: number
  ): void;

  observe(
    name: string,
    value: number
  ): void;

  timer(
    name: string
  ): unknown;

  snapshot(): Record<string, unknown>;

  reset(): void;

  merge(
    other: Metrics
  ): void;

  toPrometheus(): string;
}

export declare function createMetrics(
  value?: Metrics | null
): Metrics;

export declare class ReorgError extends Error {}

export declare class ReorgRecovery {
  constructor(
    options?: Record<string, unknown>
  );

  detect(
    options: Record<string, unknown>
  ): Record<string, unknown>;

  plan(
    options: Record<string, unknown>
  ): Record<string, unknown>;

  recover(
    options: Record<string, unknown>
  ): Promise<Record<string, unknown>>;
}

export declare class ReorgMonitorError extends Error {}

export declare class ReorgMonitor {
  constructor(
    streamer: Streamer,
    options?: Record<string, unknown>
  );

  check(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  verifyExpected(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  checkAndRecover(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  monitor(
    options?: Record<string, unknown>
  ): Promise<unknown>;
}

export declare class LiveBackfillError extends Error {}

export declare class LiveBackfillEngine {
  constructor(
    streamer: Streamer,
    options?: Record<string, unknown>
  );

  establishBoundary(
    boundaryLedger?: number
  ): Promise<unknown>;

  backfillToBoundary(
    options?: Record<string, unknown>
  ): Promise<unknown>;

  commitBoundary(
    ledger: number
  ): Promise<unknown>;

  consumeLive(
    options?: Record<string, unknown>
  ): Promise<unknown>;

  run(
    options?: Record<string, unknown>
  ): Promise<unknown>;
}

export declare class EventEngineError extends Error {}

export declare class EventEngine {
  constructor(
    streamer: Streamer,
    options?: {
      checkpoint?: CheckpointStore | null;
      checkpointKey?: string;
      store?: EventStore | null;
      backfill?: BackfillEngine | null;
      replay?: EventReplay | null;
      handoff?: EventHandoff | null;
      recovery?: ReorgRecovery | null;
      monitor?: ReorgMonitor | null;
      rewindDepth?: number;
      reorgLookback?: number;
      reorgInterval?: number;
      metrics?: Metrics | null;
    }
  );

  readonly streamer: Streamer;
  readonly checkpoint: CheckpointStore | null;
  readonly checkpointKey: string;
  readonly store: EventStore | null;
  readonly backfillEngine: BackfillEngine;
  readonly replayEngine: EventReplay;
  readonly handoff: EventHandoff;
  readonly recovery: ReorgRecovery;
  readonly monitor: ReorgMonitor;
  readonly state: LifecycleState;
  readonly signal: AbortSignal | null;
  readonly metrics: Metrics;

  startLifecycle(): AbortSignal;

  stop(): boolean;

  reset(): this;

  backfill(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  replayEvents(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  establishHandoff(
    boundaryLedger?: number
  ): Promise<unknown>;

  commitHandoff(
    ledger: number
  ): Promise<number>;

  consume(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  checkReorg(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  watchReorg(
    options?: Record<string, unknown>
  ): Promise<unknown>;

  start(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;

  recover(
    options?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;
}

export declare class LifecycleError extends Error {}

export declare class LifecycleController {
  readonly state: LifecycleState;
  readonly signal: AbortSignal | null;

  start(): AbortSignal;

  stop(): boolean;

  reset(): this;
}
