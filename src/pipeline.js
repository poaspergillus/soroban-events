export class PipelineError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'PipelineError';
  }
}

export class EventPipeline {
  #stages = [];

  constructor(stages = []) {
    if (!Array.isArray(stages)) {
      throw new TypeError('pipeline stages must be an array');
    }

    for (const stage of stages) {
      this.use(stage);
    }
  }

  use(stage) {
    if (typeof stage !== 'function') {
      throw new TypeError(
        'pipeline stage must be a function'
      );
    }

    this.#stages.push(stage);
    return this;
  }

  get size() {
    return this.#stages.length;
  }

  async process(event, context = {}) {
    let current = event;

    for (let index = 0; index < this.#stages.length; index++) {
      const stage = this.#stages[index];

      let result;

      try {
        result = await stage(current, context);
      } catch (error) {
        if (error instanceof PipelineError) {
          throw error;
        }

        throw new PipelineError(
          `pipeline stage ${index} failed`,
          { cause: error }
        );
      }

      if (result === null) {
        return null;
      }

      if (result !== undefined) {
        current = result;
      }
    }

    return current;
  }

  async processBatch(events, context = {}) {
    if (!Array.isArray(events)) {
      throw new TypeError(
        'pipeline batch must be an array'
      );
    }

    const output = [];

    for (const event of events) {
      const result =
        await this.process(event, context);

      if (result !== null) {
        output.push(result);
      }
    }

    return output;
  }

  filter(predicate) {
    if (typeof predicate !== 'function') {
      throw new TypeError(
        'pipeline filter must be a function'
      );
    }

    return this.use(
      async (event, context) =>
        (await predicate(event, context))
          ? event
          : null
    );
  }

  map(transform) {
    if (typeof transform !== 'function') {
      throw new TypeError(
        'pipeline mapper must be a function'
      );
    }

    return this.use(transform);
  }

  tap(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError(
        'pipeline tap must be a function'
      );
    }

    return this.use(
      async (event, context) => {
        await handler(event, context);
        return event;
      }
    );
  }

  clear() {
    this.#stages.length = 0;
    return this;
  }
}
