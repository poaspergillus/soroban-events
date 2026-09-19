import { promises as fs } from 'node:fs';
import path from 'node:path';

import { CheckpointError, assertCheckpointStore } from './checkpoint.js';

export class FileCheckpointStore {
  constructor(directory) {
    if (typeof directory !== 'string' || directory.length === 0) {
      throw new TypeError(
        'checkpoint directory must be a non-empty string'
      );
    }

    this.directory = path.resolve(directory);

    assertCheckpointStore(this);
  }

  async load(key) {
    validateKey(key);

    const file = this.#fileFor(key);

    try {
      const text = await fs.readFile(file, 'utf8');
      const data = JSON.parse(text);

      validateStoredCheckpoint(data);

      return data.ledger;
    } catch (error) {
      if (error?.code === 'ENOENT') {
        return null;
      }

      if (error instanceof CheckpointError) {
        throw error;
      }

      if (error instanceof SyntaxError) {
        throw new CheckpointError(
          `invalid checkpoint file for key: ${key}`
        );
      }

      throw error;
    }
  }

  async save(key, ledger) {
    validateKey(key);
    validateLedger(ledger);

    await fs.mkdir(this.directory, {
      recursive: true
    });

    const file = this.#fileFor(key);
    const temporary = `${file}.${process.pid}.${Date.now()}.tmp`;

    const previous = await this.load(key);

    if (previous != null && ledger < previous) {
      throw new CheckpointError(
        `checkpoint cannot move backwards: ${ledger} < ${previous}`
      );
    }

    const data = {
      version: 1,
      key,
      ledger
    };

    await fs.writeFile(
      temporary,
      `${JSON.stringify(data)}\n`,
      'utf8'
    );

    await fs.rename(temporary, file);
  }

  async rewind(key, ledger) {
    validateKey(key);
    validateLedger(ledger);

    await fs.mkdir(this.directory, {
      recursive: true
    });

    const file = this.#fileFor(key);
    const temporary =
      `${file}.${process.pid}.${Date.now()}.tmp`;

    const data = {
      version: 1,
      key,
      ledger
    };

    await fs.writeFile(
      temporary,
      `${JSON.stringify(data)}\n`,
      'utf8'
    );

    await fs.rename(
      temporary,
      file
    );
  }

  async clear(key) {
    validateKey(key);

    try {
      await fs.unlink(this.#fileFor(key));
    } catch (error) {
      if (error?.code !== 'ENOENT') {
        throw error;
      }
    }
  }

  #fileFor(key) {
    return path.join(
      this.directory,
      `${encodeURIComponent(key)}.json`
    );
  }
}

function validateKey(key) {
  if (typeof key !== 'string' || key.length === 0) {
    throw new TypeError(
      'checkpoint key must be a non-empty string'
    );
  }
}

function validateLedger(ledger) {
  if (!Number.isSafeInteger(ledger) || ledger < 1) {
    throw new TypeError(
      'checkpoint ledger must be a positive safe integer'
    );
  }
}

function validateStoredCheckpoint(data) {
  if (
    !data ||
    data.version !== 1 ||
    typeof data.key !== 'string'
  ) {
    throw new CheckpointError(
      'invalid checkpoint file format'
    );
  }

  validateKey(data.key);
  validateLedger(data.ledger);
}
