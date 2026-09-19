import { rpc } from '@stellar/stellar-sdk';
import { decodeEvent } from './decoder.js';
import { LedgerConsistencyTracker } from './consistency.js';
import { Metrics } from './metrics.js';

export const MAX_SAFE_LEDGER_SPAN = 9500;
export const DEFAULT_PAGE_SIZE = 1000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function classifyError(error) {
  const message = String(error?.message ?? error).toLowerCase();

  const status =
    error?.response?.status ??
    error?.status ??
    error?.statusCode;

  if (status === 429 || message.includes('429') || message.includes('rate limit')) {
    return 'rate-limit';
  }

  if (
    status >= 500 ||
    message.includes('timeout') ||
    message.includes('timed out') ||
    message.includes('econnreset') ||
    message.includes('socket')
  ) {
    return 'transient';
  }

  return 'fatal';
}

export class SorobanEventStreamer {
  constructor(rpcUrl, options = {}) {
    if (!rpcUrl) {
      throw new TypeError('rpcUrl is required');
    }

    this.server = new rpc.Server(
      rpcUrl,
      options.serverOptions || {}
    );

    this.metrics =
      options.metrics instanceof Metrics
        ? options.metrics
        : new Metrics();

    this.pollInterval = options.pollInterval ?? 3000;

    const windowSize = options.windowSize ?? MAX_SAFE_LEDGER_SPAN;
    if (!Number.isInteger(windowSize) || windowSize < 1) {
      throw new TypeError("windowSize must be a positive integer");
    }

    this.windowSize = Math.min(windowSize, MAX_SAFE_LEDGER_SPAN);

    const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 10000) {
      throw new TypeError("pageSize must be an integer between 1 and 10000");
    }

    this.pageSize = pageSize;

    this.maxRetries = options.maxRetries ?? 3;
    this.retryBaseMs = options.retryBaseMs ?? 500;
  }

  async getLatestLedger(options = {}) {
    const response = await this.server.getLatestLedger();

    if (options?.metadata === true) {
      return {
        sequence: response.sequence,
        hash: response.id ?? null,
        closeTime: response.closeTime ?? null,
        protocolVersion: response.protocolVersion ?? null
      };
    }

    return response.sequence;
  }

  async requestLedgerWithRetry(request) {
    let attempt = 0;

    while (true) {
      this.metrics.counter(
        'rpc_attempts'
      );

      const stop =
        this.metrics.timer(
          'rpc_ledger_request_duration_ms'
        );

      try {
        const response =
          await this.server.getLedgers(request);

        stop();

        this.metrics.counter(
          'rpc_requests'
        );

        return response;
      } catch (error) {
        stop();

        const kind = classifyError(error);

        this.metrics.counter(
          'rpc_errors'
        );

        if (kind === 'rate-limit') {
          this.metrics.counter(
            'rpc_rate_limits'
          );
        }

        if (
          (kind !== 'transient' &&
            kind !== 'rate-limit') ||
          attempt >= this.maxRetries
        ) {
          throw error;
        }

        const multiplier =
          kind === 'rate-limit' ? 2 : 1;

        const delay =
          this.retryBaseMs *
          multiplier *
          (2 ** attempt);

        this.metrics.counter(
          'rpc_retries'
        );

        await sleep(delay);
        attempt++;
      }
    }
  }

  async getLedgerHistory({
    startLedger,
    endLedger,
    limit = 1000,
    signal,
    consistency = null
  } = {}) {
    if (
      !Number.isSafeInteger(startLedger) ||
      startLedger < 1
    ) {
      throw new TypeError(
        'startLedger must be a positive safe integer'
      );
    }

    if (
      endLedger != null &&
      (
        !Number.isSafeInteger(endLedger) ||
        endLedger < startLedger
      )
    ) {
      throw new TypeError(
        'endLedger must be a safe integer >= startLedger'
      );
    }

    if (
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > 1000
    ) {
      throw new TypeError(
        'limit must be a safe integer between 1 and 1000'
      );
    }

    const consistencyTracker =
      consistency == null
        ? new LedgerConsistencyTracker()
        : consistency;

    if (
      typeof consistencyTracker.observe !== 'function'
    ) {
      throw new TypeError(
        'consistency must implement observe()'
      );
    }

    const ledgers = [];
    let cursor = null;
    let expectedStart = startLedger;
    let pageCount = 0;

    while (!signal?.aborted) {
      const request = {
        startLedger: expectedStart,
        limit
      };

      if (cursor != null) {
        request.pagination = { cursor };
        delete request.startLedger;
      }

      const response =
        await this.requestLedgerWithRetry(request);

      const page = Array.isArray(response?.ledgers)
        ? response.ledgers
        : [];

      pageCount++;

      if (page.length === 0) {
        break;
      }

      for (const ledger of page) {
        if (
          !ledger ||
          !Number.isSafeInteger(ledger.sequence) ||
          ledger.sequence < 1
        ) {
          throw new TypeError(
            'RPC returned invalid ledger metadata'
          );
        }

        if (
          ledger.sequence < startLedger ||
          (
            endLedger != null &&
            ledger.sequence > endLedger
          )
        ) {
          continue;
        }

        consistencyTracker.observe({
          sequence: ledger.sequence,
          hash: ledger.hash ?? null,
          previousHash:
            ledger.previousHash ??
            ledger.prevHash ??
            null
        });

        ledgers.push(ledger);
      }

      const last = page[page.length - 1];

      if (
        endLedger != null &&
        Number.isSafeInteger(last?.sequence) &&
        last.sequence >= endLedger
      ) {
        break;
      }

      const nextCursor =
        response?.cursor ?? null;

      if (nextCursor == null || nextCursor === '') {
        break;
      }

      if (nextCursor === cursor) {
        throw new Error(
          'ledger pagination cursor did not advance'
        );
      }

      cursor = nextCursor;

      if (
        Number.isSafeInteger(last?.sequence)
      ) {
        expectedStart = last.sequence + 1;
      }

      if (pageCount > 100000) {
        throw new Error(
          'ledger pagination exceeded safety limit'
        );
      }
    }

    this.metrics.counter(
      'ledgers_verified',
      ledgers.length
    );

    return ledgers;
  }

  async verifyLedgerHistory({
    startLedger,
    endLedger,
    limit = 1000,
    signal,
    consistency
  } = {}) {
    const tracker =
      consistency ??
      new LedgerConsistencyTracker();

    const ledgers =
      await this.getLedgerHistory({
        startLedger,
        endLedger,
        limit,
        signal,
        consistency: tracker
      });

    return {
      startLedger,
      endLedger:
        endLedger ??
        (ledgers.length
          ? ledgers[ledgers.length - 1].sequence
          : null),
      count: ledgers.length,
      last: tracker.last,
      ledgers
    };
  }

  async getEventsWindowed({
    startLedger,
    endLedger,
    filters = [],
    limit = 100,
    signal
  }) {
    if (!Number.isInteger(startLedger) || startLedger < 1) {
      throw new TypeError('startLedger must be a positive integer');
    }

    if (!Number.isInteger(limit) || limit < 1) {
      throw new TypeError('limit must be a positive integer');
    }

    if (endLedger != null && (!Number.isInteger(endLedger) || endLedger < startLedger)) {
      throw new TypeError('endLedger must be >= startLedger');
    }

    const targetEnd = endLedger ?? await this.getLatestLedger();

    if (signal?.aborted) {
      throw new DOMException('Operation aborted', 'AbortError');
    }

    const results = [];
    const seen = new Set();

    let currentStart = startLedger;

    while (currentStart <= targetEnd && results.length < limit) {
      if (signal?.aborted) {
        throw new DOMException('Operation aborted', 'AbortError');
      }

      const currentEndExclusive = Math.min(
        currentStart + this.windowSize,
        targetEnd + 1
      );

      const remaining = limit - results.length;

      const events = await this.fetchWindow(
        currentStart,
        currentEndExclusive,
        filters,
        remaining,
        signal
      );

      for (const raw of events) {
        if (!raw?.id || seen.has(raw.id)) continue;

        seen.add(raw.id);
        results.push(decodeEvent(raw));

        if (results.length >= limit) break;
      }

      currentStart = currentEndExclusive;
    }

    this.metrics.counter(
      'events_fetched',
      results.length
    );

    return results;
  }

  async fetchWindow(
    startLedger,
    endLedgerExclusive,
    filters,
    limit,
    signal
  ) {
    const rawEvents = [];
    let cursor;

    while (rawEvents.length < limit) {
      if (signal?.aborted) {
        throw new DOMException('Operation aborted', 'AbortError');
      }

      const pagination = {
        limit: Math.min(this.pageSize, limit - rawEvents.length)
      };

      if (cursor) {
        pagination.cursor = cursor;
      }

      const params = {
        filters,
        pagination
      };

      if (!cursor) {
        params.startLedger = startLedger;
        params.endLedger = endLedgerExclusive;
      }

      const response = await this.requestWithRetry(params, signal);

      const events = response?.events ?? [];

      rawEvents.push(...events);

      const nextCursor = response?.cursor;

      if (!nextCursor || events.length === 0) {
        break;
      }

      if (nextCursor === cursor) {
        throw new Error(
          `RPC pagination cursor did not advance for ledger range ${startLedger}-${endLedgerExclusive}`
        );
      }

      cursor = nextCursor;
    }

    return rawEvents.slice(0, limit);
  }

  async fetchWindowNewest(
    startLedger,
    endLedgerExclusive,
    filters,
    limit,
    signal
  ) {
    const newest = [];
    let cursor;

    while (true) {
      if (signal?.aborted) {
        throw new DOMException("Operation aborted", "AbortError");
      }

      const pagination = {
        limit: this.pageSize
      };

      if (cursor) {
        pagination.cursor = cursor;
      }

      const params = {
        filters,
        pagination
      };

      if (!cursor) {
        params.startLedger = startLedger;
        params.endLedger = endLedgerExclusive;
      }

      const response = await this.requestWithRetry(params, signal);
      const events = response?.events ?? [];

      for (const event of events) {
        if (!event?.id) continue;

        newest.push(event);

        if (newest.length > limit) {
          newest.shift();
        }
      }

      const nextCursor = response?.cursor;

      if (!nextCursor || events.length === 0) {
        break;
      }

      if (nextCursor === cursor) {
        throw new Error(
          `RPC pagination cursor did not advance for ledger range ${startLedger}-${endLedgerExclusive}`
        );
      }

      cursor = nextCursor;
    }

    return newest;
  }

  async requestWithRetry(params, signal) {
    let attempt = 0;

    while (true) {
      if (signal?.aborted) {
        throw new DOMException('Operation aborted', 'AbortError');
      }

      this.metrics.counter(
        'rpc_attempts'
      );

      const stop =
        this.metrics.timer(
          'rpc_request_duration_ms'
        );

      try {
        const response =
          await this.server.getEvents(params);

        stop();

        this.metrics.counter(
          'rpc_requests'
        );

        return response;
      } catch (error) {
        stop();

        const kind = classifyError(error);

        this.metrics.counter(
          'rpc_errors'
        );

        if (kind === 'rate-limit') {
          this.metrics.counter(
            'rpc_rate_limits'
          );
        }

        if (
          (kind !== 'rate-limit' && kind !== 'transient') ||
          attempt >= this.maxRetries
        ) {
          throw error;
        }

        const delay =
          this.retryBaseMs *
          2 ** attempt *
          (kind === 'rate-limit' ? 2 : 1);

        this.metrics.counter(
          'rpc_retries'
        );

        await sleep(delay);
        attempt++;
      }
    }
  }

  async tail({
    contractId,
    limit = 10,
    maxLookbackLedgers = 50000,
    filters,
    signal
  } = {}) {
    if (limit <= 0) return [];

    const latest = await this.getLatestLedger();

    const effectiveFilters =
      filters ??
      (contractId
        ? [{ type: 'contract', contractIds: [contractId] }]
        : [{ type: 'contract' }]);

    const collected = [];
    const seen = new Set();

    let endExclusive = latest + 1;
    const minimumLedger = Math.max(
      1,
      latest - maxLookbackLedgers
    );

    while (
      endExclusive > minimumLedger &&
      collected.length < limit
    ) {
      const start = Math.max(
        minimumLedger,
        endExclusive - this.windowSize
      );

      const events = await this.fetchWindowNewest(
        start,
        endExclusive,
        effectiveFilters,
        limit,
        signal
      );

      for (let i = events.length - 1; i >= 0; i--) {
        const event = events[i];

        if (!event?.id || seen.has(event.id)) continue;

        seen.add(event.id);
        collected.unshift(decodeEvent(event));

        if (collected.length >= limit) break;
      }

      endExclusive = start;
    }

    return collected.slice(-limit);
  }

  async consume({
    startLedger,
    filters = [],
    pollInterval = this.pollInterval,
    signal,
    checkpoint,
    checkpointKey = 'default',
    onEvent,
    maxEvents,
    pipeline = null
  } = {}) {
    if (typeof onEvent !== 'function') {
      throw new TypeError('onEvent must be a function');
    }

    if (
      pipeline != null &&
      typeof pipeline.process !== 'function'
    ) {
      throw new TypeError(
        'pipeline must implement process()'
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

    if (
      maxEvents != null &&
      (!Number.isSafeInteger(maxEvents) || maxEvents < 1)
    ) {
      throw new TypeError(
        'maxEvents must be a positive safe integer'
      );
    }

    let cursorLedger;

    if (startLedger != null) {
      cursorLedger = startLedger;
    } else if (checkpoint) {
      cursorLedger = await checkpoint.resumeFrom(
        checkpointKey,
        await this.getLatestLedger()
      );
    } else {
      cursorLedger = await this.getLatestLedger();
    }

    let processed = 0;

    while (!signal?.aborted) {
      const latest = await this.getLatestLedger();

      if (cursorLedger <= latest) {
        const events = await this.getEventsWindowed({
          startLedger: cursorLedger,
          endLedger: latest,
          filters,
          limit: 10000,
          signal
        });

        let index = 0;

        while (index < events.length) {
          if (signal?.aborted) return processed;

          const ledger = events[index].ledger;
          const ledgerEvents = [];

          while (
            index < events.length &&
            events[index].ledger === ledger
          ) {
            ledgerEvents.push(events[index]);
            index++;
          }

          let ledgerCompleted = true;

          for (const event of ledgerEvents) {
            if (signal?.aborted) return processed;

            const processedEvent = pipeline
              ? await pipeline.process(event, {
                  signal,
                  checkpointKey,
                  ledger,
                  streamer: this
                })
              : event;

            if (processedEvent !== null) {
              await onEvent(processedEvent);
              processed++;

              this.metrics.counter(
                'events_processed'
              );
            } else {
              this.metrics.counter(
                'events_dropped'
              );
            }

            if (
              maxEvents != null &&
              processed >= maxEvents
            ) {
              if (
                event !==
                ledgerEvents[ledgerEvents.length - 1]
              ) {
                ledgerCompleted = false;
              }
              break;
            }
          }

          if (ledgerCompleted) {
            if (checkpoint) {
              await checkpoint.save(
                ledger + 1,
                checkpointKey
              );

              this.metrics.counter(
                'checkpoint_saves'
              );
            }

            cursorLedger = Math.max(
              cursorLedger,
              ledger + 1
            );
          }

          if (
            maxEvents != null &&
            processed >= maxEvents
          ) {
            return processed;
          }
        }
      }

      if (cursorLedger > latest) {
        if (signal?.aborted) return processed;

        await sleep(pollInterval);
      }
    }

    return processed;
  }

  async *stream({
    startLedger,
    filters = [],
    pollInterval = this.pollInterval,
    signal,
    checkpoint,
    checkpointKey = 'default'
  } = {}) {
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

    let cursorLedger;

    if (startLedger != null) {
      cursorLedger = startLedger;
    } else if (checkpoint) {
      cursorLedger = await checkpoint.resumeFrom(
        checkpointKey,
        await this.getLatestLedger()
      );
    } else {
      cursorLedger = await this.getLatestLedger();
    }

    while (!signal?.aborted) {
      const latest = await this.getLatestLedger();

      if (cursorLedger <= latest) {
        const events = await this.getEventsWindowed({
          startLedger: cursorLedger,
          endLedger: latest,
          filters,
          limit: 10000,
          signal
        });

        for (const event of events) {
          if (signal?.aborted) return;

          yield event;

          const nextLedger = Math.max(
            cursorLedger,
            event.ledger + 1
          );

          if (checkpoint) {
            await checkpoint.save(
              nextLedger,
              checkpointKey
            );
          }

          cursorLedger = nextLedger;
        }
      }

      if (cursorLedger > latest) {
        await sleep(pollInterval);
      }
    }
  }
}
