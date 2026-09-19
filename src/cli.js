#!/usr/bin/env node

import {
  readFile
} from 'node:fs/promises';

import {
  Command
} from './cli-command.js';

import {
  SorobanEventStreamer,
  EventReplay,
  exportEventStream
} from './index.js';

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

const program =
  new Command();

program
  .name('soroban-events')
  .description(
    'Soroban RPC event ingestion and streaming tools'
  )
  .version(
    packageJson.version
  );

program
  .command('health')
  .description(
    'Check whether an RPC HTTP endpoint is reachable'
  )
  .option(
    '--rpc <url>',
    'Soroban RPC URL'
  )
  .action(
    async options => {
      const rpc =
        options.rpc ??
        process.env.SOROBAN_RPC_URL;

      if (!rpc) {
        throw new Error(
          'RPC URL required: use --rpc <url> or SOROBAN_RPC_URL'
        );
      }

      const response =
        await fetch(
          rpc,
          {
            method: 'POST',
            headers: {
              'content-type':
                'application/json'
            },
            body: JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'getHealth',
              params: {}
            })
          }
        );

      process.stdout.write(
        JSON.stringify(
          {
            ok: response.ok,
            status: response.status,
            statusText: response.statusText
          },
          null,
          2
        ) + '\n'
      );

      if (!response.ok) {
        process.exitCode = 1;
      }
    }
  );


program
  .command('status')
  .description(
    'Show Soroban RPC health and ledger retention status'
  )
  .option(
    '--rpc <url>',
    'Soroban RPC URL'
  )
  .action(
    async options => {
      const rpc =
        options.rpc ??
        process.env.SOROBAN_RPC_URL;

      if (!rpc) {
        throw new Error(
          'RPC URL required: use --rpc <url> or SOROBAN_RPC_URL'
        );
      }

      const streamer =
        new SorobanEventStreamer(rpc);

      const status =
        await streamer.checkRpcHealth();

      process.stdout.write(
        JSON.stringify(
          status,
          null,
          2
        ) + '\n'
      );

      if (!status.healthy) {
        process.exitCode = 1;
      }
    }
  );

program
  .command('backfill')
  .description(
    'Backfill Soroban events for a ledger range'
  )
  .option(
    '--rpc <url>',
    'Soroban RPC URL'
  )
  .option(
    '--start <ledger>',
    'First ledger'
  )
  .option(
    '--end <ledger>',
    'Last ledger'
  )
  .option(
    '--format <format>',
    'Output format: jsonl or csv',
    'jsonl'
  )
  .option(
    '--output <file>',
    'Write output to a file instead of stdout'
  )
  .action(
    async options => {
      const rpc =
        options.rpc ??
        process.env.SOROBAN_RPC_URL;

      if (!rpc) {
        throw new Error(
          'RPC URL required: use --rpc <url> or SOROBAN_RPC_URL'
        );
      }

      const startLedger =
        parseCliLedger(
          options.start,
          'start'
        );

      const endLedger =
        parseCliLedger(
          options.end,
          'end'
        );

      if (endLedger < startLedger) {
        throw new Error(
          'end must be greater than or equal to start'
        );
      }

      const streamer =
        new SorobanEventStreamer(rpc);

      const events =
        await streamer.getEventsWindowed({
          startLedger,
          endLedger,
          limit: null
        });

      const output =
        await exportEventStream(
          events,
          {
            format: options.format
          }
        );

      await writeCliOutput(
        output,
        options.output
      );

      process.stderr.write(
        `backfill: ${events.length} events\n`
      );
    }
  );

program
  .command('replay')
  .description(
    'Replay Soroban events for a ledger range'
  )
  .option(
    '--rpc <url>',
    'Soroban RPC URL'
  )
  .option(
    '--start <ledger>',
    'First ledger'
  )
  .option(
    '--end <ledger>',
    'Last ledger'
  )
  .option(
    '--format <format>',
    'Output format: jsonl or csv',
    'jsonl'
  )
  .option(
    '--output <file>',
    'Write output to a file instead of stdout'
  )
  .action(
    async options => {
      const rpc =
        options.rpc ??
        process.env.SOROBAN_RPC_URL;

      if (!rpc) {
        throw new Error(
          'RPC URL required: use --rpc <url> or SOROBAN_RPC_URL'
        );
      }

      const startLedger =
        parseCliLedger(
          options.start,
          'start'
        );

      const endLedger =
        parseCliLedger(
          options.end,
          'end'
        );

      if (endLedger < startLedger) {
        throw new Error(
          'end must be greater than or equal to start'
        );
      }

      const streamer =
        new SorobanEventStreamer(rpc);

      const replay =
        new EventReplay(streamer);

      const events = [];

      await replay.run({
        startLedger,
        endLedger,
        onEvent: event => {
          events.push(event);
        }
      });

      const output =
        await exportEventStream(
          events,
          {
            format: options.format
          }
        );

      await writeCliOutput(
        output,
        options.output
      );

      process.stderr.write(
        `replay: ${events.length} events\n`
      );
    }
  );

program
  .command('latest')
  .description(
    'Fetch the latest Soroban ledger'
  )
  .option(
    '--rpc <url>',
    'Soroban RPC URL'
  )
  .action(
    async options => {
      const rpc =
        options.rpc ??
        process.env.SOROBAN_RPC_URL;

      if (!rpc) {
        throw new Error(
          'RPC URL required: use --rpc <url> or SOROBAN_RPC_URL'
        );
      }

      const response =
        await fetch(
          rpc,
          {
            method: 'POST',
            headers: {
              'content-type':
                'application/json'
            },
            body: JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'getLatestLedger',
              params: {}
            })
          }
        );

      const body =
        await response.text();

      let parsed;

      try {
        parsed =
          JSON.parse(body);
      } catch {
        throw new Error(
          `RPC returned non-JSON response (HTTP ${response.status})`
        );
      }

      const rpcError =
        parsed &&
        parsed.error;

      if (
        response.ok === false ||
        rpcError
      ) {
        process.stdout.write(
          JSON.stringify(
            rpcError ?? {
              status: response.status,
              statusText: response.statusText
            },
            null,
            2
          ) + '\n'
        );

        process.exitCode = 1;
      } else {
        const sequence =
          parsed &&
          parsed.result &&
          parsed.result.sequence;

        if (
          sequence === undefined
        ) {
          throw new Error(
            'RPC response did not contain result.sequence'
          );
        }

        process.stdout.write(
          `${sequence}\n`
        );
      }
    }
  );


function parseCliLedger(value, name) {
  if (
    value == null ||
    value === ''
  ) {
    throw new Error(
      `--${name} <ledger> is required`
    );
  }

  const ledger =
    Number(value);

  if (
    !Number.isSafeInteger(ledger) ||
    ledger < 1
  ) {
    throw new Error(
      `--${name} must be a positive safe integer`
    );
  }

  return ledger;
}

async function writeCliOutput(
  output,
  filename
) {
  if (!filename) {
    for await (const chunk of output) {
      process.stdout.write(chunk);
    }
    return;
  }

  const chunks = [];

  for await (const chunk of output) {
    chunks.push(chunk);
  }

  const { writeFile } =
    await import('node:fs/promises');

  await writeFile(
    filename,
    chunks.join(''),
    'utf8'
  );
}

try {
  await program.parse(
    process.argv
  );
} catch (error) {
  process.stderr.write(
    `${error?.message ?? error}\n`
  );

  process.exitCode = 1;
}
