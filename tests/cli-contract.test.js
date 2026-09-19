import test from 'node:test';
import assert from 'node:assert/strict';
import { access } from 'node:fs/promises';

test('CLI entrypoint exists', async () => {
  await assert.doesNotReject(
    access(
      new URL(
        '../src/cli.js',
        import.meta.url
      )
    )
  );
});

test('CLI command parser exists', async () => {
  const module =
    await import(
      '../src/cli-command.js'
    );

  assert.equal(
    typeof module.Command,
    'function'
  );
});

test('package CLI points to executable source', async () => {
  const { readFile } =
    await import(
      'node:fs/promises'
    );

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

  assert.equal(
    packageJson.bin['soroban-events'],
    './src/cli.js'
  );
});
