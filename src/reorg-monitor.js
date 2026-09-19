import { ReorgRecovery } from './recovery.js';

export class ReorgMonitorError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'ReorgMonitorError';
  }
}

/**
 * Periodically verifies a previously processed ledger range.
 *
 * This class deliberately does not guess that a reorg happened.
 * It compares the locally expected ledger metadata with verified
 * RPC history and only invokes recovery after an actual hash
 * mismatch is observed.
 */
export class ReorgMonitor {
  #streamer;
  #recovery;
  #lookback;
  #interval;

  constructor(
    streamer,
    {
      recovery,
      lookback = 10,
      interval = 3000
    } = {}
  ) {
    if (
      !streamer ||
      typeof streamer.getLatestLedger !== 'function' ||
      typeof streamer.getLedgerHistory !== 'function'
    ) {
      throw new TypeError(
        'streamer must implement getLatestLedger() and getLedgerHistory()'
      );
    }

    if (
      !recovery ||
      typeof recovery.detect !== 'function' ||
      typeof recovery.recover !== 'function'
    ) {
      throw new TypeError(
        'recovery must implement detect() and recover()'
      );
    }

    if (
      !Number.isSafeInteger(lookback) ||
      lookback < 1
    ) {
      throw new TypeError(
        'lookback must be a positive safe integer'
      );
    }

    if (
      !Number.isSafeInteger(interval) ||
      interval < 1
    ) {
      throw new TypeError(
        'interval must be a positive safe integer'
      );
    }

    this.#streamer = streamer;
    this.#recovery = recovery;
    this.#lookback = lookback;
    this.#interval = interval;
  }

  get lookback() {
    return this.#lookback;
  }

  get interval() {
    return this.#interval;
  }

  /**
   * Verify a range ending at latestLedger.
   *
   * `expected` is supplied by the caller because the library
   * cannot safely invent a canonical local chain hash.
   */
  async check({
    expected,
    latestLedger = null,
    signal = null
  } = {}) {
    if (!expected || typeof expected !== 'object') {
      throw new TypeError(
        'expected ledger metadata is required'
      );
    }

    validateLedger(expected);

    if (signal?.aborted) {
      throw createAbortError();
    }

    const latest =
      latestLedger ??
      await this.#streamer.getLatestLedger();

    validateLedgerNumber(latest);

    const start =
      Math.max(
        1,
        latest - this.#lookback + 1
      );

    const history =
      await this.#streamer.getLedgerHistory({
        startLedger: start,
        endLedger: latest,
        signal
      });

    if (!history.ledgers.length) {
      return {
        reorg: false,
        reason: 'no-history',
        latestLedger: latest
      };
    }

    const actual =
      history.ledgers.find(
        ledger =>
          ledger.sequence === expected.sequence
      );

    if (!actual) {
      return {
        reorg: false,
        reason: 'expected-ledger-outside-window',
        latestLedger: latest
      };
    }

    return {
      ...this.#recovery.detect({
        expected,
        actual
      }),
      latestLedger: latest,
      verifiedFrom: start,
      verifiedThrough: latest
    };
  }

  /**
   * Check once and recover immediately if a hash mismatch
   * is detected.
   */
  async checkAndRecover({
    expected,
    latestLedger = null,
    latestVerifiedLedger = null,
    invalidateStoredEvents = true,
    signal = null
  } = {}) {
    const result =
      await this.check({
        expected,
        latestLedger,
        signal
      });

    if (!result.reorg) {
      return {
        ...result,
        recovered: false
      };
    }

    if (signal?.aborted) {
      throw createAbortError();
    }

    const recovery =
      await this.#recovery.recover({
        affectedLedger: result.sequence,
        latestVerifiedLedger:
          latestVerifiedLedger ??
          result.latestLedger,
        invalidateStoredEvents
      });

    return {
      ...result,
      recovered: true,
      recovery
    };
  }

  /**
   * Poll until stopped.
   *
   * `getExpected` must return the locally trusted ledger
   * metadata to compare against the network.
   *
   * The callback receives every check result and may decide
   * how the local expected ledger changes after recovery.
   */
  async monitor({
    getExpected,
    onCheck = null,
    onRecovery = null,
    signal = null,
    stopOnRecovery = false
  } = {}) {
    if (typeof getExpected !== 'function') {
      throw new TypeError(
        'getExpected must be a function'
      );
    }

    if (onCheck != null && typeof onCheck !== 'function') {
      throw new TypeError(
        'onCheck must be a function'
      );
    }

    if (
      onRecovery != null &&
      typeof onRecovery !== 'function'
    ) {
      throw new TypeError(
        'onRecovery must be a function'
      );
    }

    let checks = 0;
    let recoveries = 0;

    while (!signal?.aborted) {
      const expected =
        await getExpected();

      const result =
        await this.checkAndRecover({
          expected,
          signal
        });

      checks++;

      if (onCheck != null) {
        await onCheck(
          result
        );
      }

      if (result.recovered) {
        recoveries++;

        if (onRecovery != null) {
          await onRecovery(
            result
          );
        }

        if (stopOnRecovery) {
          break;
        }
      }

      await delay(
        this.#interval,
        signal
      );
    }

    return {
      checks,
      recoveries,
      aborted: Boolean(signal?.aborted)
    };
  }
}

function validateLedger(ledger) {
  if (
    !ledger ||
    typeof ledger !== 'object'
  ) {
    throw new TypeError(
      'ledger metadata must be an object'
    );
  }

  validateLedgerNumber(
    ledger.sequence
  );

  if (
    ledger.hash != null &&
    (
      typeof ledger.hash !== 'string' ||
      ledger.hash.length === 0
    )
  ) {
    throw new TypeError(
      'ledger hash must be a non-empty string'
    );
  }
}

function validateLedgerNumber(value) {
  if (
    !Number.isSafeInteger(value) ||
    value < 1
  ) {
    throw new TypeError(
      'ledger must be a positive safe integer'
    );
  }
}

function createAbortError() {
  const error =
    new Error('operation aborted');

  error.name =
    'AbortError';

  return error;
}

function delay(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(createAbortError());
      return;
    }

    const timer =
      setTimeout(
        resolve,
        ms
      );

    if (signal) {
      signal.addEventListener(
        'abort',
        () => {
          clearTimeout(timer);
          reject(createAbortError());
        },
        { once: true }
      );
    }
  });
}
