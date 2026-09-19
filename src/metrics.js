export class MetricsError extends Error {
  constructor(message) {
    super(message);
    this.name = 'MetricsError';
  }
}

export class Metrics {
  #counters = new Map();
  #gauges = new Map();
  #histograms = new Map();

  counter(name, value = 1) {
    validateName(name);
    validateNumber(value);

    const next =
      (this.#counters.get(name) ?? 0) + value;

    this.#counters.set(name, next);

    return next;
  }

  increment(name, value = 1) {
    return this.counter(name, value);
  }

  getCounter(name) {
    validateName(name);
    return this.#counters.get(name) ?? 0;
  }

  gauge(name, value) {
    validateName(name);
    validateNumber(value);

    this.#gauges.set(name, value);

    return value;
  }

  setGauge(name, value) {
    return this.gauge(name, value);
  }

  addGauge(name, value) {
    validateName(name);
    validateNumber(value);

    const next =
      (this.#gauges.get(name) ?? 0) + value;

    this.#gauges.set(name, next);

    return next;
  }

  getGauge(name) {
    validateName(name);
    return this.#gauges.get(name) ?? null;
  }

  observe(name, value) {
    validateName(name);
    validateNumber(value);

    let histogram =
      this.#histograms.get(name);

    if (!histogram) {
      histogram = {
        count: 0,
        sum: 0,
        min: null,
        max: null
      };

      this.#histograms.set(name, histogram);
    }

    histogram.count++;
    histogram.sum += value;
    histogram.min =
      histogram.min == null
        ? value
        : Math.min(histogram.min, value);
    histogram.max =
      histogram.max == null
        ? value
        : Math.max(histogram.max, value);

    return { ...histogram };
  }

  getHistogram(name) {
    validateName(name);

    const value =
      this.#histograms.get(name);

    return value
      ? { ...value }
      : {
          count: 0,
          sum: 0,
          min: null,
          max: null
        };
  }

  timer(name) {
    validateName(name);

    const started = performance.now();
    let stopped = false;

    return () => {
      if (stopped) return null;

      stopped = true;

      const elapsed =
        performance.now() - started;

      this.observe(name, elapsed);

      return elapsed;
    };
  }

  async time(name, fn) {
    validateName(name);

    if (typeof fn !== 'function') {
      throw new TypeError(
        'fn must be a function'
      );
    }

    const stop = this.timer(name);

    try {
      return await fn();
    } finally {
      stop();
    }
  }

  snapshot() {
    return {
      counters: Object.fromEntries(
        this.#counters
      ),
      gauges: Object.fromEntries(
        this.#gauges
      ),
      histograms: Object.fromEntries(
        [...this.#histograms.entries()].map(
          ([name, value]) => [
            name,
            { ...value }
          ]
        )
      )
    };
  }

  reset() {
    this.#counters.clear();
    this.#gauges.clear();
    this.#histograms.clear();

    return this;
  }

  toPrometheus(options = {}) {
    const prefix =
      options.prefix ?? 'soroban_events';

    if (
      typeof prefix !== 'string' ||
      !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(prefix)
    ) {
      throw new TypeError(
        'prefix must be a valid Prometheus metric prefix'
      );
    }

    const lines = [];
    const snapshot = this.snapshot();

    for (
      const [name, value]
      of Object.entries(snapshot.counters)
    ) {
      const metricName =
        `${prefix}_${normalizeMetricName(name)}`;

      lines.push(
        `# TYPE ${metricName} counter`
      );
      lines.push(
        `${metricName} ${formatNumber(value)}`
      );
    }

    for (
      const [name, value]
      of Object.entries(snapshot.gauges)
    ) {
      const metricName =
        `${prefix}_${normalizeMetricName(name)}`;

      lines.push(
        `# TYPE ${metricName} gauge`
      );
      lines.push(
        `${metricName} ${formatNumber(value)}`
      );
    }

    for (
      const [name, value]
      of Object.entries(snapshot.histograms)
    ) {
      const metricName =
        `${prefix}_${normalizeMetricName(name)}`;

      lines.push(
        `# TYPE ${metricName} summary`
      );

      lines.push(
        `${metricName}_count ${value.count}`
      );

      lines.push(
        `${metricName}_sum ${formatNumber(value.sum)}`
      );

      if (value.min != null) {
        lines.push(
          `${metricName}_min ${formatNumber(value.min)}`
        );
      }

      if (value.max != null) {
        lines.push(
          `${metricName}_max ${formatNumber(value.max)}`
        );
      }
    }

    return lines.length
      ? `${lines.join('\n')}\n`
      : '';
  }

  merge(other) {
    if (
      !other ||
      typeof other.snapshot !== 'function'
    ) {
      throw new TypeError(
        'metrics must implement snapshot()'
      );
    }

    const snapshot = other.snapshot();

    for (
      const [name, value]
      of Object.entries(snapshot.counters ?? {})
    ) {
      this.counter(name, value);
    }

    for (
      const [name, value]
      of Object.entries(snapshot.gauges ?? {})
    ) {
      this.gauge(name, value);
    }

    for (
      const [name, value]
      of Object.entries(snapshot.histograms ?? {})
    ) {
      validateName(name);

      if (
        !value ||
        !Number.isSafeInteger(value.count) ||
        value.count < 0 ||
        !Number.isFinite(value.sum)
      ) {
        throw new MetricsError(
          `invalid histogram: ${name}`
        );
      }

      if (value.count === 0) continue;

      const current =
        this.#histograms.get(name);

      if (!current) {
        this.#histograms.set(name, {
          count: value.count,
          sum: value.sum,
          min: value.min,
          max: value.max
        });

        continue;
      }

      current.count += value.count;
      current.sum += value.sum;

      current.min =
        current.min == null
          ? value.min
          : Math.min(
              current.min,
              value.min
            );

      current.max =
        current.max == null
          ? value.max
          : Math.max(
              current.max,
              value.max
            );
    }

    return this;
  }
}

export function createMetrics(value) {
  if (value instanceof Metrics) {
    return value;
  }

  return new Metrics();
}

function normalizeMetricName(name) {
  return name
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+/, '');
}

function formatNumber(value) {
  if (Object.is(value, -0)) {
    return '0';
  }

  return String(value);
}

function validateName(name) {
  if (
    typeof name !== 'string' ||
    name.length === 0
  ) {
    throw new TypeError(
      'metric name must be a non-empty string'
    );
  }
}

function validateNumber(value) {
  if (
    typeof value !== 'number' ||
    !Number.isFinite(value)
  ) {
    throw new TypeError(
      'metric value must be a finite number'
    );
  }
}
