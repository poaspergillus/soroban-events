export type LifecycleState = 'idle' | 'running' | 'stopping' | 'stopped';

export declare const LIFECYCLE_STATES: readonly [
  'idle',
  'running',
  'stopping',
  'stopped'
];

export interface Event {
  id?: string;
  type?: string;
  ledger?: number;
  ledgerClosedAt?: string | null;
  contractId?: string | null;
  transactionIndex?: number;
  operationIndex?: number;
  txHash?: string | null;
  topics?: unknown[];
  value?: unknown;
  inSuccessfulContractCall?: boolean;
  [key: string]: unknown;
}

export interface EventFilters {
  contractId?: string;
  contractIds?: string[];
  type?: string;
  eventTypes?: string[];
  topic?: unknown;
  topics?: unknown[];
  txHash?: string;
  ledger?: number;
  startLedger?: number;
  endLedger?: number;
  [key: string]: unknown;
}

export interface RpcHealth {
  status: string;
  latestLedger: number | null;
  oldestLedger: number | null;
  ledgerRetentionWindow: number | null;
}

export interface RpcHealthCheck extends RpcHealth {
  healthy: boolean;
}

export interface LedgerMetadata {
  sequence: number;
  hash: string | null;
  closeTime: string | null;
  protocolVersion: number | null;
}

export interface StreamerOptions {
  serverOptions?: Record<string, unknown>;
  failoverRpcUrls?: string[];
  pollInterval?: number;
  windowSize?: number;
  pageSize?: number;
  maxRetries?: number;
  retryBaseMs?: number;
  retryMaxMs?: number;
  retryJitter?: number;
  rateLimitBackoffMultiplier?: number;
  adaptiveRateLimit?: boolean;
  circuitBreakerThreshold?: number;
  circuitBreakerCooldownMs?: number;
  metrics?: Metrics;
}

export interface GetEventsWindowedOptions {
  startLedger: number;
  endLedger?: number;
  filters?: EventFilters[];
  limit?: number | null;
  signal?: AbortSignal;
}

export interface ConsumeOptions {
  startLedger?: number;
  filters?: EventFilters[];
  maxEvents?: number;
  signal?: AbortSignal;
  pipeline?: EventPipeline;
  checkpoint?: CheckpointManager | CheckpointStore;
  checkpointKey?: string;
  onEvent?: (event: Event, context?: unknown) => unknown | Promise<unknown>;
  handler?: (event: Event, context?: unknown) => unknown | Promise<unknown>;
  [key: string]: unknown;
}

export interface StreamOptions {
  startLedger?: number;
  filters?: EventFilters[];
  pollInterval?: number;
  signal?: AbortSignal;
  checkpoint?: CheckpointManager | CheckpointStore;
  checkpointKey?: string;
  [key: string]: unknown;
}

export declare const MAX_SAFE_LEDGER_SPAN: number;
export declare const DEFAULT_PAGE_SIZE: number;

export declare class SorobanEventStreamer {
  constructor(rpcUrl: string, options?: StreamerOptions);

  getLatestLedger(options?: {
    metadata?: false;
  }): Promise<number>;

  getLatestLedger(options: {
    metadata: true;
  }): Promise<LedgerMetadata>;

  getEventsWindowed(
    options: GetEventsWindowedOptions
  ): Promise<Event[]>;

  tail(options?: {
    contractId?: string;
    limit?: number;
    maxLookbackLedgers?: number;
    filters?: EventFilters[];
    signal?: AbortSignal;
  }): Promise<Event[]>;

  consume(options: ConsumeOptions): Promise<unknown>;

  stream(options?: StreamOptions): AsyncGenerator<Event, void, unknown>;

  getHealth(): Promise<RpcHealth>;

  checkRetention(startLedger: number): Promise<{
    retained: boolean;
    startLedger: number;
    oldestLedger: number | null;
    latestLedger: number | null;
    ledgerRetentionWindow: number | null;
  }>;

  checkRpcHealth(): Promise<RpcHealthCheck>;

  isLedgerAvailable(ledger: number): Promise<boolean>;

  switchRpc(): boolean;

  getLedgerHistory(options: {
    startLedger: number;
    endLedger?: number;
    limit?: number;
    signal?: AbortSignal;
  }): Promise<unknown>;

  verifyLedgerHistory(options: {
    startLedger: number;
    endLedger?: number;
    signal?: AbortSignal;
  }): Promise<unknown>;
}

export declare class ScValDepthError extends Error {}

export declare function unwrapScVal(
  value: unknown,
  options?: { maxDepth?: number }
): unknown;

export declare function decodeEvent(
  rawEvent: unknown,
  options?: { maxDepth?: number }
): Event;

export declare class CheckpointError extends Error {}

export interface CheckpointStore {
  load(key: string): Promise<number | null>;
  save(key: string, ledger: number): Promise<void>;
  clear(key: string): Promise<void>;
  rewind?(key: string, ledger: number): Promise<void>;
}

export interface RewindableCheckpointStore extends CheckpointStore {
  rewind(key: string, ledger: number): Promise<void>;
}

export declare class MemoryCheckpointStore implements CheckpointStore {
  load(key: string): Promise<number | null>;
  save(key: string, ledger: number): Promise<void>;
  rewind(key: string, ledger: number): Promise<void>;
  clear(key: string): Promise<void>;
}

export declare class CheckpointManager {
  constructor(
    store: CheckpointStore,
    options?: { defaultKey?: string }
  );

  load(key?: string): Promise<number | null>;
  save(ledger: number, key?: string): Promise<number>;
  rewind(ledger: number, key?: string): Promise<number>;
  clear(key?: string): Promise<void>;
  resumeFrom(key?: string, fallbackLedger?: number): Promise<number>;
}

export declare class FileCheckpointStore implements CheckpointStore {
  constructor(directory: string);

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

export declare class PipelineError extends Error {}

export declare class EventPipeline {
  constructor(stages?: unknown[]);

  filter(
    predicate: (event: Event, context?: unknown) => boolean | Promise<boolean>
  ): EventPipeline;

  map(
    mapper: (event: Event, context?: unknown) => unknown | Promise<unknown>
  ): EventPipeline;

  tap(
    handler: (event: Event, context?: unknown) => unknown | Promise<unknown>
  ): EventPipeline;

  process(event: Event, context?: unknown): Promise<unknown>;
  processBatch(events: Event[], context?: unknown): Promise<Event[]>;
  clear(): void;
}

export declare class BackfillError extends Error {}

export interface BackfillOptions {
  startLedger: number;
  endLedger?: number;
  filters?: EventFilters[];
  concurrency?: number;
  windowSize?: number;
  pipeline?: EventPipeline;
  checkpoint?: CheckpointManager | CheckpointStore;
  checkpointKey?: string;
  signal?: AbortSignal;
  onEvent?: (event: Event, context?: unknown) => unknown | Promise<unknown>;
  handler?: (event: Event, context?: unknown) => unknown | Promise<unknown>;
  eventStore?: EventStore;
  [key: string]: unknown;
}

export declare class BackfillEngine {
  constructor(
    streamer: SorobanEventStreamer,
    options?: {
      concurrency?: number;
      windowSize?: number;
    }
  );

  run(options: BackfillOptions): Promise<Record<string, unknown>>;
}

export declare function buildWindows(
  startLedger: number,
  endLedger: number,
  windowSize?: number
): Array<{ startLedger: number; endLedger: number }>;

export declare function compareEvents(a: Event, b: Event): number;

export declare function orderBackfillEvents(events: Event[]): Event[];

export declare class ReplayError extends Error {}

export interface ReplayOptions {
  startLedger: number;
  endLedger: number;
  filters?: EventFilters[];
  pipeline?: EventPipeline;
  checkpoint?: CheckpointManager | CheckpointStore;
  checkpointKey?: string;
  signal?: AbortSignal;
  onEvent?: (event: Event, context?: unknown) => unknown | Promise<unknown>;
  handler?: (event: Event, context?: unknown) => unknown | Promise<unknown>;
  eventStore?: EventStore;
  [key: string]: unknown;
}

export declare class EventReplay {
  constructor(
    streamer: SorobanEventStreamer,
    options?: Record<string, unknown>
  );

  run(options: ReplayOptions): Promise<Record<string, unknown>>;
}

export declare function buildReplayWindows(
  startLedger: number,
  endLedger: number,
  windowSize?: number
): Array<{ startLedger: number; endLedger: number }>;

export declare class EventHandoff {
  constructor(options?: Record<string, unknown>);

  initialize(boundaryLedger: number): number;
  accept(event: Event): boolean;
  acceptMany(events: Event[]): Event[];
  commit(ledger: number): number;
  rewind(ledger: number): number;
  resetSeen(): void;
}

export declare class HandoffError extends Error {}

export declare class EventStoreError extends Error {}

export interface EventStore {
  put(event: Event): Promise<Event>;
  get(id: string): Promise<Event | null>;
  has(id: string): Promise<boolean>;
  delete(id: string): Promise<boolean>;
  list(options?: Record<string, unknown>): Promise<Event[]>;
  count(): Promise<number>;
  clear(): Promise<void>;
}

export declare class MemoryEventStore implements EventStore {
  put(event: Event): Promise<Event>;
  get(id: string): Promise<Event | null>;
  has(id: string): Promise<boolean>;
  delete(id: string): Promise<boolean>;
  list(options?: Record<string, unknown>): Promise<Event[]>;
  count(): Promise<number>;
  clear(): Promise<void>;
}

export declare class SqliteEventStore implements EventStore {
  constructor(options?: { filename?: string } | string);

  put(event: Event): Promise<Event>;
  get(id: string): Promise<Event | null>;
  has(id: string): Promise<boolean>;
  delete(id: string): Promise<boolean>;
  list(options?: Record<string, unknown>): Promise<Event[]>;
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

export interface LedgerMetadataInput {
  sequence: number;
  hash?: string | null;
  prevHash?: string | null;
  closeTime?: string | null;
  protocolVersion?: number | null;
}

export declare function isLedgerMetadata(
  value: unknown
): value is LedgerMetadataInput;

export declare class LedgerConsistencyTracker {
  constructor(options?: Record<string, unknown>);

  observe(ledger: LedgerMetadataInput): LedgerMetadataInput;
  observeMany(ledgers: LedgerMetadataInput[]): LedgerMetadataInput[];
  assertCurrent(ledger: LedgerMetadataInput): boolean;
  reset(): void;
}

export declare class MetricsError extends Error {}

export declare class Metrics {
  constructor(options?: Record<string, unknown>);

  increment(name: string, value?: number): void;
  decrement(name: string, value?: number): void;
  setGauge(name: string, value: number): void;
  addGauge(name: string, value: number): void;
  gauge(name: string, value: number): void;
  observe(name: string, value: number): void;
  histogram(name: string, value: number): void;
  timer(name: string, value: number): void;
  time<T>(name: string, fn: () => T | Promise<T>): Promise<T>;
  snapshot(): Record<string, unknown>;
  toPrometheus(options?: Record<string, unknown>): string;
  reset(): void;
  merge(other: Metrics): void;
}

export declare function createMetrics(
  value?: Metrics | Record<string, unknown>
): Metrics;

export declare class ReorgError extends Error {}

export declare class ReorgRecovery {
  constructor(
    streamer: SorobanEventStreamer,
    options?: Record<string, unknown>
  );

  recover(options?: Record<string, unknown>): Promise<Record<string, unknown>>;
}

export declare class LiveBackfillError extends Error {}

export declare class LiveBackfillEngine {
  constructor(
    streamer: SorobanEventStreamer,
    options?: Record<string, unknown>
  );

  consumeLive(options: Record<string, unknown>): Promise<unknown>;
  establishBoundary(options?: Record<string, unknown>): Promise<number>;
  backfillToBoundary(options?: Record<string, unknown>): Promise<unknown>;
  commitBoundary(options?: Record<string, unknown>): Promise<unknown>;
  run(options?: Record<string, unknown>): Promise<unknown>;
}

export declare class ReorgMonitorError extends Error {}

export declare class ReorgMonitor {
  constructor(
    streamer: SorobanEventStreamer,
    recovery: ReorgRecovery,
    options?: Record<string, unknown>
  );

  check(options?: Record<string, unknown>): Promise<Record<string, unknown>>;
  verifyExpected(options?: Record<string, unknown>): Promise<Record<string, unknown>>;
  checkAndRecover(options?: Record<string, unknown>): Promise<Record<string, unknown>>;
}

export declare class EventEngineError extends Error {}

export declare class EventEngine {
  constructor(
    streamer: SorobanEventStreamer,
    options?: Record<string, unknown>
  );

  readonly streamer: SorobanEventStreamer;
  readonly backfill: BackfillEngine;
  readonly replay: EventReplay;
  readonly recovery: ReorgRecovery;
  readonly monitor: ReorgMonitor;
  readonly handoff: EventHandoff;

  start(): Promise<unknown>;
  stop(): Promise<unknown>;
  reset(): Promise<unknown>;
  backfillRun(options: BackfillOptions): Promise<Record<string, unknown>>;
  recover(options?: Record<string, unknown>): Promise<Record<string, unknown>>;
  watchReorg(options?: Record<string, unknown>): Promise<unknown>;
  consume(options: ConsumeOptions): Promise<unknown>;
}

export declare class LifecycleError extends Error {}

export declare class LifecycleController {
  constructor(options?: Record<string, unknown>);

  readonly state: LifecycleState;
  readonly signal: AbortSignal | null;

  start(): AbortSignal;
  stop(): void;
  reset(): this;
}

export declare class DeadLetterError extends Error {
  event: Event;
  cause?: unknown;
}

export interface DeadLetterEntry {
  id: string;
  event: Event;
  error: {
    name: string;
    message: string;
    code?: string;
    stack?: string;
    cause?: unknown;
  };
  context?: unknown;
  attempts: number;
  createdAt: string;
}

export interface DeadLetterAddOptions {
  event: Event;
  error: unknown;
  context?: unknown;
  attempts?: number;
}

export declare class MemoryDeadLetterQueue {
  constructor(options?: { maxEntries?: number });

  readonly size: number;

  add(options: DeadLetterAddOptions): DeadLetterEntry;
  get(id: string): DeadLetterEntry | null;
  list(options?: { limit?: number }): DeadLetterEntry[];
  remove(id: string): boolean;
  retry(id: string): DeadLetterEntry | null;
  clear(): void;
}

export declare function isDeadLetterQueue(
  value: unknown
): value is MemoryDeadLetterQueue;

export interface EventFilterOptions {
  contractId?: string | string[];
  type?: string | string[];
  topic?: unknown | unknown[];
  txHash?: string | string[];
  ledger?: number;
  startLedger?: number;
  endLedger?: number;
  predicate?: (event: Event) => boolean;
}

export declare function createEventFilter(
  options?: EventFilterOptions
): (event: Event) => boolean;

export declare function matchesEvent(
  event: Event,
  options?: EventFilterOptions
): boolean;

export declare function filterEvents(
  events: Event[],
  options?: EventFilterOptions
): Event[];

export interface EventQueryOptions {
  startLedger?: number;
  endLedger?: number;
  limit?: number;
  offset?: number;
  contractId?: string;
  type?: string;
  txHash?: string;
}

export interface EventQueryResult {
  events: Event[];
  total: number;
  offset: number;
  limit: number;
  hasMore: boolean;
}

export declare function validateEventQuery(
  options?: EventQueryOptions
): EventQueryOptions;

export declare function queryEvents(
  events: Event[],
  options?: EventQueryOptions
): EventQueryResult;

export declare function createEventQuery(
  store: EventStore
): (options?: EventQueryOptions) => Promise<EventQueryResult>;

export type ExportFormat = 'jsonl' | 'csv';

export const DEFAULT_EXPORT_COLUMNS: readonly string[];

export interface ExportOptions {
  format?: ExportFormat;
  columns?: string[];
  includeHeader?: boolean;
}

export declare function createExporter(
  options?: ExportOptions
): (events: Event[] | AsyncIterable<Event> | Iterable<Event>) => string | AsyncGenerator<string>;

export declare function exportEvents(
  events: Event[],
  options?: ExportOptions
): string;

export declare function exportEventStream(
  events: AsyncIterable<Event> | Iterable<Event>,
  options?: ExportOptions
): AsyncGenerator<string>;
