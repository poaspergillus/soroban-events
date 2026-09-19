export const LIFECYCLE_STATES = Object.freeze([
  'idle',
  'running',
  'stopping',
  'stopped'
]);

export class LifecycleError extends Error {
  constructor(message) {
    super(message);
    this.name = 'LifecycleError';
  }
}

export class LifecycleController {
  #state = 'idle';
  #abortController = null;

  get state() {
    return this.#state;
  }

  get signal() {
    return this.#abortController?.signal ?? null;
  }

  start() {
    if (this.#state === 'running') {
      throw new LifecycleError(
        'lifecycle is already running'
      );
    }

    if (this.#state === 'stopping') {
      throw new LifecycleError(
        'lifecycle is stopping'
      );
    }

    this.#abortController =
      new AbortController();

    this.#state = 'running';

    return this.#abortController.signal;
  }

  stop() {
    if (
      this.#state === 'idle' ||
      this.#state === 'stopped'
    ) {
      return false;
    }

    this.#state = 'stopping';

    this.#abortController?.abort();

    this.#state = 'stopped';

    return true;
  }

  reset() {
    if (
      this.#state === 'running' ||
      this.#state === 'stopping'
    ) {
      throw new LifecycleError(
        'cannot reset a running lifecycle'
      );
    }

    this.#abortController = null;
    this.#state = 'idle';

    return this;
  }
}
