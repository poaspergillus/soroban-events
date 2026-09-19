#!/usr/bin/env node

import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const exec =
  promisify(execFile);

const result =
  await exec(
    'npm',
    [
      'pack',
      '--dry-run',
      '--json'
    ],
    {
      encoding: 'utf8'
    }
  );

const output =
  JSON.parse(
    result.stdout
  );

const pack =
  Array.isArray(output)
    ? output[0]
    : output;

const files =
  pack?.files ?? [];

const names =
  new Set(
    files.map(
      file => file.path
    )
  );

const required = [
  'package.json',
  'README.md',
  'LICENSE',
  'src/index.js',
  'src/index.d.ts',
  'src/cli.js'
];

for (const file of required) {
  if (!names.has(file)) {
    throw new Error(
      `npm package is missing ${file}`
    );
  }
}

process.stdout.write(
  `npm package audit passed (${files.length} files)\n`
);
