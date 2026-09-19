#!/usr/bin/env node

import {
  readFile
} from 'node:fs/promises';

import {
  Command
} from './cli-command.js';

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

      process.stdout.write(
        JSON.stringify(
          parsed,
          null,
          2
        ) + '\n'
      );

      if (!response.ok) {
        process.exitCode = 1;
      }

      if (
        parsed &&
        parsed.error
      ) {
        process.exitCode = 1;
      }
    }
  );

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
