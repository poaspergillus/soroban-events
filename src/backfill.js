import { assertEventStore } from './store.js';
export function orderBackfillEvents(windowResults) {
  if (!Array.isArray(windowResults)) {
    throw new TypeError(
      'window results must be an array'
    );
  }

  const events = [];

  for (const result of windowResults) {
    if (!Array.isArray(result)) {
      throw new TypeError(
        'each window result must be an array'
      );
    }

    events.push(...result);
  }

  events.sort(compareEvents);

  const seen = new Set();
  const output = [];

  for (const event of events) {
    const id = event?.id;

    if (id != null) {
      if (seen.has(id)) continue;
      seen.add(id);
    }

    output.push(event);
  }

  return output;
}

export function compareEvents(a, b) {
  const ledgerA = Number(a?.ledger ?? 0);
  const ledgerB = Number(b?.ledger ?? 0);

  if (ledgerA !== ledgerB) {
    return ledgerA - ledgerB;
  }

  const txA = Number(
    a?.transactionIndex ??
    a?.txIndex ??
    0
  );

  const txB = Number(
    b?.transactionIndex ??
    b?.txIndex ??
    0
  );

  if (txA !== txB) {
    return txA - txB;
  }

  const opA = Number(
    a?.operationIndex ??
    a?.opIndex ??
    0
  );

  const opB = Number(
    b?.operationIndex ??
    b?.opIndex ??
    0
  );

  if (opA !== opB) {
    return opA - opB;
  }

  return String(a?.id ?? "").localeCompare(
    String(b?.id ?? "")
  );
}

export class BackfillError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'BackfillError';
  }
}

export class BackfillEngine {
  constructor(streamer, options = {}) {
    if (
      !streamer ||
      typeof streamer.getEventsWindowed !== 'function'
    ) {
      throw new TypeError(
        'backfill streamer must implement getEventsWindowed()'
      );
    }

    this.streamer = streamer;
    this.concurrency = options.concurrency ?? 1;
    this.windowSize =
      options.windowSize ??
      streamer.windowSize ??
      9000;

    if (
      !Number.isSafeInteger(this.concurrency) ||
      this.concurrency < 1
    ) {
      throw new TypeError(
        'backfill concurrency must be a positive safe integer'
      );
    }

    if (
      !Number.isSafeInteger(this.windowSize) ||
      this.windowSize < 1
    ) {
      throw new TypeError(
        'backfill windowSize must be a positive safe integer'
      );
    }
  }

  async run({
    startLedger,
    endLedger,
    filters = [],
    pipeline = null,
    onEvent,
    onProgress,
    signal,
    checkpoint = null,
    checkpointKey = 'default',
    dedupe = true,
    store = null
  } = {}) {
    validateLedger(startLedger, 'startLedger');
    validateLedger(endLedger, 'endLedger');

    if (endLedger < startLedger) {
      throw new TypeError(
        'endLedger must be greater than or equal to startLedger'
      );
    }

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

    if (
      onProgress != null &&
      typeof onProgress !== 'function'
    ) {
      throw new TypeError(
        'onProgress must be a function'
      );
    }

    if (checkpoint != null) {
      if (
        typeof checkpoint.resumeFrom !== 'function' ||
        typeof checkpoint.save !== 'function'
      ) {
        throw new TypeError(
          'checkpoint must implement resumeFrom() and save()'
        );
      }
    }

    if (store != null) {
      assertEventStore(store);
    }

    throwIfAborted(signal);

    let effectiveStart = startLedger;

    if (checkpoint) {
      effectiveStart = await checkpoint.resumeFrom(
        checkpointKey,
        startLedger
      );

      if (
        !Number.isSafeInteger(effectiveStart) ||
        effectiveStart < 1
      ) {
        throw new BackfillError(
          'checkpoint returned an invalid ledger'
        );
      }

      if (effectiveStart > endLedger) {
        return {
          startLedger,
          endLedger,
          effectiveStart,
          processed: 0,
          windows: 0,
          windowsCompleted: 0,
          skipped: true
        };
      }
    }

    const windows = buildWindows(
      effectiveStart,
      endLedger,
      this.windowSize
    );

    let processed = 0;
    let windowsCompleted = 0;

    const seen = dedupe ? new Set() : null;
    const queue = windows.map((window, index) => ({
      ...window,
      index
    }));

    /*
     * A window may finish before an earlier window.
     *
     * We therefore track completed window indexes separately
     * and only advance the durable checkpoint across the
     * contiguous completed prefix.
     *
     * Example:
     *
     *   window 0 = running
     *   window 1 = complete
     *
     * checkpoint stays at window 0.
     *
     * Once window 0 completes, both can be committed:
     *
     *   checkpoint -> window 2 start
     */

    const completed = new Set();

    let nextCheckpointIndex = 0;
    let checkpointChain = Promise.resolve();

    const advanceCheckpoint = () => {
      if (!checkpoint) return checkpointChain;

      checkpointChain = checkpointChain.then(async () => {
        while (
          completed.has(nextCheckpointIndex)
        ) {
          throwIfAborted(signal);

          const window =
            windows[nextCheckpointIndex];

          await checkpoint.save(
            window.endLedger + 1,
            checkpointKey
          );

          completed.delete(nextCheckpointIndex);
          nextCheckpointIndex++;
        }
      });

      return checkpointChain;
    };

    const processWindow = async window => {
      throwIfAborted(signal);

      const events =
        await this.streamer.getEventsWindowed({
          startLedger: window.startLedger,
          endLedger: window.endLedger,
          filters,
          limit: null,
          signal
        });

      for (const event of events) {
        throwIfAborted(signal);

        if (
          seen &&
          event?.id != null
        ) {
          if (seen.has(event.id)) {
            continue;
          }

          seen.add(event.id);
        }

        if (store != null) {
          await store.put(event);
        }

        const processedEvent = pipeline
          ? await pipeline.process(event, {
              signal,
              checkpointKey,
              ledger: event.ledger,
              window,
              streamer: this.streamer
            })
          : event;

        if (processedEvent === null) {
          continue;
        }

        await onEvent(processedEvent);
        processed++;
      }

      windowsCompleted++;

      completed.add(window.index);

      await advanceCheckpoint();

      if (onProgress) {
        await onProgress({
          startLedger,
          endLedger,
          effectiveStart,
          windowsTotal: windows.length,
          windowsCompleted,
          processed,
          checkpointLedger:
            checkpoint
              ? nextCheckpointIndex < windows.length
                ? windows[nextCheckpointIndex].startLedger
                : endLedger + 1
              : null,
          currentWindow: {
            startLedger: window.startLedger,
            endLedger: window.endLedger
          }
        });
      }
    };

    const worker = async () => {
      while (true) {
        throwIfAborted(signal);

        const window = queue.shift();

        if (!window) return;

        await processWindow(window);
      }
    };

    const workerCount = Math.min(
      this.concurrency,
      windows.length || 1
    );

    const workers = [];

    for (let i = 0; i < workerCount; i++) {
      workers.push(worker());
    }

    await Promise.all(workers);

    /*
     * All workers have finished. There may still be a queued
     * checkpoint advancement waiting on the serialization chain.
     */
    await checkpointChain;

    return {
      startLedger,
      endLedger,
      effectiveStart,
      processed,
      windows: windows.length,
      windowsCompleted,
      skipped: false
    };
  }
}

function buildWindows(startLedger, endLedger, windowSize) {
  const windows = [];

  let cursor = startLedger;

  while (cursor <= endLedger) {
    const end = Math.min(
      endLedger,
      cursor + windowSize - 1
    );

    windows.push({
      startLedger: cursor,
      endLedger: end
    });

    cursor = end + 1;
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

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw new BackfillError(
      'backfill aborted',
      {
        cause: signal.reason
      }
    );
  }
}

export { buildWindows };


