import {
  BackfillEngine
} from './backfill.js';

import {
  EventReplay
} from './replay.js';

import {
  EventHandoff
} from './handoff.js';

import {
  ReorgRecovery
} from './recovery.js';

import {
  ReorgMonitor
} from './reorg-monitor.js';

import {
  LifecycleController
} from './lifecycle.js';

import {
  createMetrics
} from './metrics.js';

export class EventEngineError extends Error {
  constructor(message, options = {}) {
    super(message);

    this.name = 'EventEngineError';

    if (options.cause !== undefined) {
      this.cause = options.cause;
    }
  }
}

function assertStreamer(streamer) {
  if (!streamer) {
    throw new TypeError(
      'streamer is required'
    );
  }

  for (const method of [
    'getLatestLedger',
    'getEventsWindowed',
    'consume'
  ]) {
    if (typeof streamer[method] !== 'function') {
      throw new TypeError(
        `streamer must implement ${method}()`
      );
    }
  }
}

function validateLedger(value) {
  if (
    !Number.isSafeInteger(value) ||
    value < 1
  ) {
    throw new TypeError(
      'ledger must be a positive safe integer'
    );
  }
}

export class EventEngine {
  #streamer;
  #checkpoint;
  #checkpointKey;
  #store;
  #backfill;
  #replay;
  #handoff;
  #recovery;
  #monitor;
  #lifecycle;
  #metrics;

  constructor(
    streamer,
    options = {}
  ) {
    assertStreamer(streamer);

    const {
      checkpoint = null,
      checkpointKey = 'default',
      store = null,
      backfill = null,
      replay = null,
      handoff = null,
      recovery = null,
      monitor = null,
      rewindDepth = 10,
      reorgLookback = 10,
      reorgInterval = 3000,
      metrics = null
    } = options;

    this.#streamer = streamer;
    this.#checkpoint = checkpoint;
    this.#checkpointKey =
      checkpointKey;
    this.#store = store;

    this.#metrics =
      createMetrics(metrics);

    this.#backfill =
      backfill ??
      new BackfillEngine(
        streamer
      );

    this.#replay =
      replay ??
      new EventReplay(
        streamer,
        {
          checkpoint,
          checkpointKey
        }
      );

    this.#handoff =
      handoff ??
      new EventHandoff({
        checkpoint,
        checkpointKey
      });

    this.#recovery =
      recovery ??
      new ReorgRecovery({
        checkpoint,
        checkpointKey,
        store,
        rewindDepth
      });

    this.#monitor =
      monitor ??
      new ReorgMonitor(
        streamer,
        {
          recovery: this.#recovery,
          lookback: reorgLookback,
          interval: reorgInterval
        }
      );

    this.#lifecycle =
      new LifecycleController();
  }

  get streamer() {
    return this.#streamer;
  }

  get checkpoint() {
    return this.#checkpoint;
  }

  get checkpointKey() {
    return this.#checkpointKey;
  }

  get store() {
    return this.#store;
  }

  get backfillEngine() {
    return this.#backfill;
  }

  get replayEngine() {
    return this.#replay;
  }

  get handoff() {
    return this.#handoff;
  }

  get recovery() {
    return this.#recovery;
  }

  get monitor() {
    return this.#monitor;
  }

  get state() {
    return this.#lifecycle.state;
  }

  get signal() {
    return this.#lifecycle.signal;
  }

  get metrics() {
    return this.#metrics;
  }

  startLifecycle() {
    return this.#lifecycle.start();
  }

  stop() {
    return this.#lifecycle.stop();
  }

  reset() {
    this.#lifecycle.reset();
    return this;
  }

  async backfill(options = {}) {
    this.#metrics.increment(
      'engine_backfill_runs'
    );

    const started =
      Date.now();

    try {
      const result =
        await this.#backfill.run({
          ...options,
          store:
            options.store ??
            this.#store
        });

      this.#metrics.increment(
        'engine_backfill_completed'
      );

      this.#metrics.observe(
        'engine_backfill_duration_ms',
        Date.now() - started
      );

      return result;
    } catch (error) {
      this.#metrics.increment(
        'engine_backfill_errors'
      );

      throw error;
    }
  }

  async replayEvents(options = {}) {
    this.#metrics.increment(
      'engine_replay_runs'
    );

    const started =
      Date.now();

    try {
      const result =
        await this.#replay.run({
          ...options,
          store:
            options.store ??
            this.#store
        });

      this.#metrics.increment(
        'engine_replay_completed'
      );

      this.#metrics.observe(
        'engine_replay_duration_ms',
        Date.now() - started
      );

      return result;
    } catch (error) {
      this.#metrics.increment(
        'engine_replay_errors'
      );

      throw error;
    }
  }

  async establishHandoff(
    boundaryLedger = null
  ) {
    let boundary =
      boundaryLedger;

    if (boundary == null) {
      const latest =
        await this.#streamer.getLatestLedger({
          metadata: true
        });

      boundary =
        typeof latest === 'number'
          ? latest
          : latest.sequence;
    }

    validateLedger(boundary);

    await this.#handoff.initialize(
      boundary
    );

    this.#metrics.increment(
      'engine_handoffs_initialized'
    );

    this.#metrics.setGauge(
      'engine_handoff_boundary_ledger',
      boundary
    );

    return boundary;
  }

  async commitHandoff(
    boundaryLedger
  ) {
    validateLedger(boundaryLedger);

    const result =
      await this.#handoff.commit(
        boundaryLedger
      );

    this.#metrics.increment(
      'engine_handoffs_committed'
    );

    this.#metrics.setGauge(
      'engine_handoff_boundary_ledger',
      boundaryLedger
    );

    return result;
  }

  async consume(options = {}) {
    this.#metrics.increment(
      'engine_live_consumers'
    );

    const {
      checkpoint = this.#checkpoint,
      checkpointKey =
        options.checkpointKey ??
        this.#checkpointKey,
      signal =
        options.signal ??
        this.#lifecycle.signal
    } = options;

    return this.#streamer.consume({
      ...options,
      checkpoint,
      checkpointKey,
      signal,
      handler: async (
        event,
        context
      ) => {
        const accepted =
          await this.#handoff.accept(
            event
          );

        if (!accepted) {
          return;
        }

        if (
          typeof options.handler ===
          'function'
        ) {
          await options.handler(
            event,
            context
          );
        }
      }
    });
  }

  async checkReorg(
    options = {}
  ) {
    this.#metrics.increment(
      'engine_reorg_checks'
    );

    try {
      const result =
        await this.#monitor.checkAndRecover(
          options
        );

      if (result?.recovered) {
        this.#metrics.increment(
          'engine_reorg_recoveries'
        );

        if (
          Number.isSafeInteger(
            result.recovery?.rewindTo
          )
        ) {
          this.#metrics.setGauge(
            'engine_recovery_rewind_ledger',
            result.recovery.rewindTo
          );
        }
      }

      return result;
    } catch (error) {
      this.#metrics.increment(
        'engine_reorg_errors'
      );

      throw error;
    }
  }

  async watchReorg({
    getExpected,
    onCheck = null,
    onRecovery = null,
    signal = null,
    stopOnRecovery = false
  } = {}) {
    return this.#monitor.monitor({
      getExpected,
      onCheck,
      onRecovery,
      signal,
      stopOnRecovery
    });
  }

  async start({
    startLedger,
    filters = [],
    pipeline = null,
    onEvent = null,
    onProgress = null,
    signal = null,
    store = this.#store
  } = {}) {
    validateLedger(startLedger);

    const lifecycleSignal =
      this.#lifecycle.start();

    const effectiveSignal =
      signal ?? lifecycleSignal;

    try {
      const latest =
        await this.#streamer.getLatestLedger({
          metadata: true
        });

      const boundary =
        typeof latest === 'number'
          ? latest
          : latest.sequence;

      validateLedger(boundary);

      await this.#handoff.initialize(
        boundary
      );

      const backfillResult =
        await this.backfill({
          startLedger,
          endLedger: boundary,
          filters,
          pipeline,
          onEvent,
          onProgress,
          signal: effectiveSignal,
          store
        });

      await this.#handoff.commit(
        boundary
      );

      const liveResult =
        await this.consume({
          filters,
          signal: effectiveSignal,
          handler: onEvent
        });

      return {
        boundaryLedger: boundary,
        backfill: backfillResult,
        live: liveResult,
        state: this.#lifecycle.state
      };
    } catch (error) {
      if (this.#lifecycle.state === 'running') {
        this.#lifecycle.stop();
      }

      throw error;
    }
  }

  async recover(options = {}) {
    this.#metrics.increment(
      'engine_recoveries'
    );

    return this.#recovery.recover(
      options
    );
  }
}
