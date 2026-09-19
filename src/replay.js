import {
  orderBackfillEvents
} from './backfill.js';

import {
  CheckpointManager,
  assertCheckpointStore
} from './checkpoint.js';
import { assertEventStore } from './store.js';

export class ReplayError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'ReplayError';
  }
}

export class EventReplay {
  #streamer;
  #checkpoint;
  #checkpointKey;

  constructor(streamer, options = {}) {
    if (
      !streamer ||
      typeof streamer.getEventsWindowed !== 'function'
    ) {
      throw new TypeError(
        'replay requires a streamer with getEventsWindowed()'
      );
    }

    this.#streamer = streamer;

    if (options.checkpoint != null) {
      assertCheckpointStore(options.checkpoint);

      /*
       * Normalize the persistence primitive to the same
       * manager-style checkpoint API used by Streamer,
       * Backfill, and ReorgRecovery.
       *
       * Raw stores expose:
       *   save(key, ledger)
       *
       * CheckpointManager exposes:
       *   save(ledger, key)
       *
       * Keeping this boundary here prevents argument-order
       * mistakes from leaking into replay logic.
       */
      this.#checkpoint = new CheckpointManager(
        options.checkpoint,
        {
          defaultKey:
            options.checkpointKey ?? 'replay'
        }
      );
    }

    this.#checkpointKey =
      options.checkpointKey ?? 'replay';

    if (
      typeof this.#checkpointKey !== 'string' ||
      this.#checkpointKey.length === 0
    ) {
      throw new TypeError(
        'replay checkpoint key must be a non-empty string'
      );
    }
  }

  async run(options = {}) {
    if (options.store != null) {
      assertEventStore(options.store);
    }
    const startLedger = options.startLedger;
    const endLedger = options.endLedger;

    validateLedger(startLedger, 'startLedger');
    validateLedger(endLedger, 'endLedger');

    if (endLedger < startLedger) {
      throw new ReplayError(
        'endLedger must be greater than or equal to startLedger'
      );
    }

    const filters = options.filters ?? [];
    const signal = options.signal;
    const pipeline = options.pipeline;
    const onEvent = options.onEvent;

    if (
      pipeline != null &&
      typeof pipeline.process !== 'function'
    ) {
      throw new TypeError(
        'pipeline must implement process()'
      );
    }

    if (typeof onEvent !== 'function') {
      throw new TypeError(
        'onEvent must be a function'
      );
    }

    let resumeFrom = startLedger;

    if (this.#checkpoint) {
      const stored =
        await this.#checkpoint.load(
          this.#checkpointKey
        );

      if (stored != null) {
        validateLedger(
          stored,
          'checkpoint ledger'
        );

        resumeFrom = Math.max(
          startLedger,
          stored
        );
      }
    }

    if (resumeFrom > endLedger) {
      return {
        startLedger,
        endLedger,
        resumedFrom: resumeFrom,
        eventsFetched: 0,
        eventsProcessed: 0,
        completed: true
      };
    }

    const windows = buildReplayWindows(
      resumeFrom,
      endLedger,
      this.#streamer.windowSize ?? 9000
    );

    let eventsFetched = 0;
    let eventsProcessed = 0;

    for (const window of windows) {
      if (signal?.aborted) {
        return {
          startLedger,
          endLedger,
          resumedFrom: resumeFrom,
          eventsFetched,
          eventsProcessed,
          completed: false,
          aborted: true
        };
      }

      const events =
        await this.#streamer.getEventsWindowed({
          startLedger: window.startLedger,
          endLedger: window.endLedger,
          filters,
          limit: null,
          signal
        });

      eventsFetched += events.length;

      const ordered =
        orderBackfillEvents([events]);

      if (options.store != null) {
        for (const event of ordered) {
          await options.store.put(event);
        }
      }

      for (const event of ordered) {
        if (signal?.aborted) {
          return {
            startLedger,
            endLedger,
            resumedFrom: resumeFrom,
            eventsFetched,
            eventsProcessed,
            completed: false,
            aborted: true
          };
        }

        const processed = pipeline
          ? await pipeline.process(event, {
              signal,
              replay: true,
              checkpointKey: this.#checkpointKey,
              streamer: this.#streamer
            })
          : event;

        if (processed !== null) {
          await onEvent(processed);
          eventsProcessed++;
        }
      }

      if (this.#checkpoint) {
        await this.#checkpoint.save(
          window.endLedger + 1,
          this.#checkpointKey
        );
      }
    }

    return {
      startLedger,
      endLedger,
      resumedFrom: resumeFrom,
      eventsFetched,
      eventsProcessed,
      completed: true,
      aborted: false
    };
  }
}

export function buildReplayWindows(
  startLedger,
  endLedger,
  windowSize = 9000
) {
  validateLedger(startLedger, 'startLedger');
  validateLedger(endLedger, 'endLedger');

  if (
    !Number.isSafeInteger(windowSize) ||
    windowSize < 1
  ) {
    throw new TypeError(
      'windowSize must be a positive safe integer'
    );
  }

  if (endLedger < startLedger) {
    throw new ReplayError(
      'endLedger must be greater than or equal to startLedger'
    );
  }

  const windows = [];

  let cursor = startLedger;

  while (cursor <= endLedger) {
    const windowEnd = Math.min(
      endLedger,
      cursor + windowSize - 1
    );

    windows.push({
      startLedger: cursor,
      endLedger: windowEnd
    });

    cursor = windowEnd + 1;
  }

  return windows;
}

function validateLedger(value, name) {
  if (
    !Number.isSafeInteger(value) ||
    value < 1
  ) {
    throw new TypeError(
      `${name} must be a positive safe integer`
    );
  }
}
