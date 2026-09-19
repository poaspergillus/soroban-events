import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const packageJson =
  JSON.parse(
    await readFile(
      new URL(
        '../package.json',
        import.meta.url
      ),
      'utf8'
    )
  );

test('package exposes TypeScript declarations', () => {
  assert.equal(
    packageJson.types,
    './src/index.d.ts'
  );

  assert.equal(
    packageJson.exports?.['.']?.types,
    './src/index.d.ts'
  );
});

test('package exposes ESM entrypoint', () => {
  assert.equal(
    packageJson.exports?.['.']?.import,
    './src/index.js'
  );
});

test('package exposes CLI', () => {
  assert.equal(
    packageJson.bin?.['soroban-events'],
    './src/cli.js'
  );
});

test('package declares required published files', () => {
  assert.ok(
    packageJson.files?.includes('src/')
  );

  assert.ok(
    packageJson.files?.includes('README.md')
  );

  assert.ok(
    packageJson.files?.includes('LICENSE')
  );
});

test('package has a license', () => {
  assert.equal(
    packageJson.license,
    'MIT'
  );
});

test('package has development audit scripts', () => {
  assert.equal(
    packageJson.scripts?.audit,
    'node scripts/audit.mjs'
  );

  assert.equal(
    packageJson.scripts?.['pack:audit'],
    'node scripts/pack-audit.mjs'
  );
});
