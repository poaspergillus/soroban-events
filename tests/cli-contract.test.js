import test from 'node:test';
import { readFile } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { access } from 'node:fs/promises';


test('CLI source exposes status, backfill, and replay commands', async () => {
  const source = await readFile(
    new URL(
      '../src/cli.js',
      import.meta.url
    ),
    'utf8'
  );

  assert.match(
    source,
    /\.command\('status'\)/
  );

  assert.match(
    source,
    /\.command\('backfill'\)/
  );

  assert.match(
    source,
    /\.command\('replay'\)/
  );
});

test('CLI source exposes ledger range and export options', async () => {
  const source = await readFile(
    new URL(
      '../src/cli.js',
      import.meta.url
    ),
    'utf8'
  );

  assert.match(
    source,
    /--start <ledger>/
  );

  assert.match(
    source,
    /--end <ledger>/
  );

  assert.match(
    source,
    /--format <format>/
  );

  assert.match(
    source,
    /--output <file>/
  );
});

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

test('CLI root help lists commands', async () => {
  const { Command } =
    await import(
      '../src/cli-command.js'
    );

  const originalWrite =
    process.stdout.write;

  let output = '';

  process.stdout.write =
    chunk => {
      output += String(chunk);
      return true;
    };

  try {
    const program =
      new Command()
        .name('soroban-events')
        .description('test CLI');

    program.command('health');
    program.command('latest');

    await program.parse([
      'node',
      'cli',
      '--help'
    ]);
  } finally {
    process.stdout.write =
      originalWrite;
  }

  assert.match(
    output,
    /Commands:/
  );

  assert.match(
    output,
    /health/
  );

  assert.match(
    output,
    /latest/
  );
});

test('CLI command help is command-specific', async () => {
  const { Command } =
    await import(
      '../src/cli-command.js'
    );

  const originalWrite =
    process.stdout.write;

  let output = '';

  process.stdout.write =
    chunk => {
      output += String(chunk);
      return true;
    };

  try {
    const program =
      new Command()
        .name('soroban-events')
        .description('test CLI');

    program
      .command('health')
      .description('Check RPC health')
      .option(
        '--rpc <url>',
        'Soroban RPC URL'
      );

    await program.parse([
      'node',
      'cli',
      'health',
      '--help'
    ]);
  } finally {
    process.stdout.write =
      originalWrite;
  }

  assert.match(
    output,
    /^health\n/
  );

  assert.match(
    output,
    /Check RPC health/
  );

  assert.match(
    output,
    /--rpc  Soroban RPC URL/
  );

  assert.doesNotMatch(
    output,
    /Commands:/
  );
});

test('CLI latest help is command-specific', async () => {
  const { Command } =
    await import(
      '../src/cli-command.js'
    );

  const originalWrite =
    process.stdout.write;

  let output = '';

  process.stdout.write =
    chunk => {
      output += String(chunk);
      return true;
    };

  try {
    const program =
      new Command()
        .name('soroban-events')
        .description('test CLI');

    program
      .command('latest')
      .description('Fetch latest ledger')
      .option(
        '--rpc <url>',
        'Soroban RPC URL'
      );

    await program.parse([
      'node',
      'cli',
      'latest',
      '--help'
    ]);
  } finally {
    process.stdout.write =
      originalWrite;
  }

  assert.match(
    output,
    /^latest\n/
  );

  assert.match(
    output,
    /Fetch latest ledger/
  );

  assert.match(
    output,
    /--rpc  Soroban RPC URL/
  );
});


test('CLI command with no arguments runs its action', async () => {
  const { Command } =
    await import(
      '../src/cli-command.js'
    );

  let called = false;

  const program =
    new Command()
      .name('soroban-events');

  program
    .command('health')
    .action(
      async () => {
        called = true;
      }
    );

  await program.parse([
    'node',
    'cli',
    'health'
  ]);

  assert.equal(called, true);
});

test('CLI command help still works after empty-command fix', async () => {
  const { Command } =
    await import(
      '../src/cli-command.js'
    );

  const originalWrite =
    process.stdout.write;

  let output = '';

  process.stdout.write =
    chunk => {
      output += String(chunk);
      return true;
    };

  try {
    const program =
      new Command()
        .name('soroban-events');

    program
      .command('health')
      .description('Check RPC health');

    await program.parse([
      'node',
      'cli',
      'health',
      '--help'
    ]);
  } finally {
    process.stdout.write =
      originalWrite;
  }

  assert.match(output, /^health\n/);
  assert.match(output, /Check RPC health/);
});
