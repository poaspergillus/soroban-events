import { BackfillEngine } from './backfill.js';
import { EventHandoff } from './handoff.js';

export class LiveBackfillError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'LiveBackfillError';
  }
}

/**
 * Coordinates historical backfill with live consumption.
 *
 * The important invariant is the handoff ledger:
 *
 *   backfill -> boundary ledger -> live
 *
 * The boundary ledger is deliberately processed by both sides.
 * EventHandoff deduplicates the overlap so an event cannot be
 * delivered twice during the transition.
 */
export class LiveBackfillEngine {
  #streamer;
  #backfill;
  #handoff;
  #checkpoint;
  #checkpointKey;

  constructor(
    streamer,
    {
      backfill = null,
      handoff = null,
      checkpoint = null,
      checkpointKey = 'default'
    } = {}
  ) {
    if (
      !streamer ||
      typeof streamer.getLatestLedger !== 'function' ||
      typeof streamer.consume !== 'function'
    ) {
      throw new TypeError(
        'streamer must implement getLatestLedger() and consume()'
      );
    }

    if (
      backfill != null &&
      typeof backfill.run !== 'function'
    ) {
      throw new TypeError(
        'backfill must implement run()'
      );
    }

    if (
      handoff != null &&
      (
        typeof handoff.initialize !== 'function' ||
        typeof handoff.accept !== 'function' ||
        typeof handoff.commit !== 'function'
      )
    ) {
      throw new TypeError(
        'handoff must implement initialize(), accept(), and commit()'
      );
    }

    if (
      typeof checkpointKey !== 'string' ||
      checkpointKey.length === 0
    ) {
      throw new TypeError(
        'checkpointKey must be a non-empty string'
      );
    }

    this.#streamer = streamer;
    this.#backfill =
      backfill ?? new BackfillEngine(streamer);
    this.#handoff =
      handoff ??
      new EventHandoff({
        checkpoint,
        checkpointKey
      });
    this.#checkpoint = checkpoint;
    this.#checkpointKey = checkpointKey;
  }

  get streamer() {
    return this.#streamer;
  }

  get backfill() {
    return this.#backfill;
  }

  get handoff() {
    return this.#handoff;
  }

  get checkpoint() {
    return this.#checkpoint;
  }

  get checkpointKey() {
    return this.#checkpointKey;
  }

  /**
   * Establish the immutable handoff boundary.
   */
  async establishBoundary() {
    const metadata =
      await this.#streamer.getLatestLedger({
        metadata: true
      });

    if (
      !metadata ||
      !Number.isSafeInteger(metadata.sequence) ||
      metadata.sequence < 1
    ) {
      throw new LiveBackfillError(
        'streamer returned invalid latest-ledger metadata'
      );
    }

    await this.#handoff.initialize(
      metadata.sequence
    );

    return metadata;
  }

  /**
   * Backfill through the boundary.
   *
   * The boundary is intentionally included. Live consumption
   * starts from that same ledger, and EventHandoff removes the
   * overlap safely.
   */
  async backfillToBoundary({
    startLedger,
    boundaryLedger,
    filters = [],
    pipeline = null,
    onEvent = null,
    onProgress = null,
    signal = null,
    dedupe = true,
    store = null
  } = {}) {
    validateLedger(startLedger);
    validateLedger(boundaryLedger);

    if (startLedger > boundaryLedger) {
      throw new TypeError(
        'startLedger must be <= boundaryLedger'
      );
    }

    return this.#backfill.run({
      startLedger,
      endLedger: boundaryLedger,
      filters,
      pipeline,
      signal,
      dedupe,
      store,
      onProgress,
      onEvent: async event => {
        const accepted =
          await this.#handoff.accept(event);

        if (!accepted) {
          return;
        }

        if (onEvent != null) {
          await onEvent(event);
        }
      }
    });
  }

  /**
   * Commit the completed historical boundary.
   */
  async commitBoundary(boundaryLedger) {
    validateLedger(boundaryLedger);

    return this.#handoff.commit(
      boundaryLedger
    );
  }

  /**
   * Start live processing from the handoff boundary.
   *
   * The boundary event may be encountered again by the live
   * consumer; EventHandoff prevents duplicate delivery.
   */
  async consumeLive({
    startLedger,
    filters = [],
    maxEvents = 100,
    signal = null,
    pipeline = null,
    handler = null,
    onEvent = null,
    checkpoint = this.#checkpoint,
    checkpointKey = this.#checkpointKey
  } = {}) {
    if (handler != null && typeof handler !== 'function') {
      throw new TypeError(
        'handler must be a function'
      );
    }

    if (onEvent != null && typeof onEvent !== 'function') {
      throw new TypeError(
        'onEvent must be a function'
      );
    }

    return this.#streamer.consume({
      startLedger,
      filters,
      maxEvents,
      signal,
      pipeline,
      checkpoint,
      checkpointKey,
      onEvent: async (event, context) => {
        const accepted =
          await this.#handoff.accept(event);

        if (!accepted) {
          return;
        }

        if (onEvent != null) {
          await onEvent(event, context);
        }

        if (handler != null) {
          await handler(event, context);
        }
      }
    });
  }

  /**
   * Complete one backfill -> live transition.
   */
  async run({
    startLedger,
    filters = [],
    pipeline = null,
    signal = null,
    onEvent = null,
    onProgress = null,
    handler = null,
    maxEvents = 100,
    dedupe = true,
    store = null,
    checkpoint = this.#checkpoint,
    checkpointKey = this.#checkpointKey
  } = {}) {
    validateLedger(startLedger);

    const boundary =
      await this.establishBoundary();

    if (boundary.sequence < startLedger) {
      throw new LiveBackfillError(
        `latest ledger ${boundary.sequence} is before start ledger ${startLedger}`
      );
    }

    const backfillResult =
      await this.backfillToBoundary({
        startLedger,
        boundaryLedger: boundary.sequence,
        filters,
        pipeline,
        signal,
        dedupe,
        store,
        onProgress,
        onEvent
      });

    await this.commitBoundary(
      boundary.sequence
    );

    const liveResult =
      await this.consumeLive({
        startLedger: boundary.sequence,
        filters,
        maxEvents,
        signal,
        pipeline,
        handler,
        onEvent,
        checkpoint,
        checkpointKey
      });

    return {
      boundary,
      backfill: backfillResult,
      live: liveResult
    };
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
