import { rpc } from '@stellar/stellar-sdk';
import { decodeEvent } from './decoder.js';

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

    this.pollInterval = options.pollInterval ?? 3000;
    this.windowSize = Math.min(
      options.windowSize ?? MAX_SAFE_LEDGER_SPAN,
      MAX_SAFE_LEDGER_SPAN
    );

    this.pageSize = Math.min(
      options.pageSize ?? DEFAULT_PAGE_SIZE,
      10000
    );

    this.maxRetries = options.maxRetries ?? 3;
    this.retryBaseMs = options.retryBaseMs ?? 500;
  }

  async getLatestLedger() {
    const response = await this.server.getLatestLedger();
    return response.sequence;
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

      try {
        return await this.server.getEvents(params);
      } catch (error) {
        const kind = classifyError(error);

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

  async *stream({
    startLedger,
    filters = [],
    pollInterval = this.pollInterval,
    signal
  } = {}) {
    let cursorLedger =
      startLedger ?? await this.getLatestLedger();

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

          cursorLedger = Math.max(
            cursorLedger,
            event.ledger + 1
          );
        }
      }

      if (cursorLedger > latest) {
        await sleep(pollInterval);
      }
    }
  }
}
