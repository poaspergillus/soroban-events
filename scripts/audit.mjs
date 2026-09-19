#!/usr/bin/env node

import { readFile } from 'node:fs/promises';
import { access } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = fileURLToPath(
  new URL('..', import.meta.url)
);

const requiredFiles = [
  'package.json',
  'README.md',
  'LICENSE',
  'src/index.js',
  'src/index.d.ts',
  'src/cli.js',
  'src/streamer.js',
  'src/decoder.js',
  'src/checkpoint.js',
  'src/checkpoint-file.js',
  'src/pipeline.js',
  'src/backfill.js',
  'src/replay.js',
  'src/handoff.js',
  'src/store.js',
  'src/consistency.js',
  'src/metrics.js',
  'src/recovery.js',
  'src/reorg-monitor.js',
  'src/live-backfill.js',
  'src/engine.js',
  'src/lifecycle.js'
];

for (const relative of requiredFiles) {
  await access(
    new URL(
      relative,
      `file://${root}/`
    )
  );
}

const packageJson =
  JSON.parse(
    await readFile(
      new URL(
        'package.json',
        `file://${root}/`
      ),
      'utf8'
    )
  );

if (!packageJson.types) {
  throw new Error(
    'package.json is missing types'
  );
}

if (
  packageJson.types !==
  './src/index.d.ts'
) {
  throw new Error(
    'package.json types points to the wrong file'
  );
}

if (
  packageJson.bin?.['soroban-events'] !==
  './src/cli.js'
) {
  throw new Error(
    'CLI bin entry is missing or incorrect'
  );
}

if (
  packageJson.exports?.['.']?.import !==
  './src/index.js'
) {
  throw new Error(
    'ESM export is missing or incorrect'
  );
}

if (
  packageJson.exports?.['.']?.types !==
  './src/index.d.ts'
) {
  throw new Error(
    'types export is missing or incorrect'
  );
}

const sourceFiles =
  requiredFiles.filter(
    file =>
      file.endsWith('.js')
  );

for (const relative of sourceFiles) {
  const result = spawnSync(
    process.execPath,
    [
      '--check',
      `${root}/${relative}`
    ],
    {
      encoding: 'utf8'
    }
  );

  if (result.status !== 0) {
    process.stderr.write(
      result.stderr ||
      result.stdout ||
      ''
    );

    throw new Error(
      `syntax check failed: ${relative}`
    );
  }
}

process.stdout.write(
  'static audit passed\n'
);
