# Contributing to soroban-events

Thanks for considering a contribution. The project aims to provide reliable event ingestion and decoding primitives for Stellar Soroban applications.

## Before starting

- For substantial changes, open an issue first so the problem and proposed approach can be discussed.
- Keep pull requests focused. One behaviour change per PR is easier to review and release.
- Avoid including RPC credentials, private keys, tokens, or production event data in issues, commits, fixtures, or logs.
- Do not make a live Soroban RPC call a requirement for the default unit-test suite.

## Runtime and checks

The supported runtime is Node.js 22.12.0 or newer. GitHub Actions runs the test suite against Node.js 22 and 24. The pull-request CI workflow is the required shared check; it installs the lockfile-defined dependencies and runs the repository test suite on GitHub-hosted runners.

The repository also defines additional package, release, and optional live-test scripts in `package.json`. Live tests contact Stellar Testnet and should be run deliberately, not as a substitute for deterministic unit tests.

## What to include in a pull request

- Explain the user-facing problem and the chosen solution.
- Add or update deterministic tests for changed behaviour.
- Update `src/index.d.ts` when changing the public API.
- Update README examples and `CHANGELOG.md` when behaviour or public interfaces change.
- Include relevant error cases, cancellation behaviour, and retry behaviour when applicable.
- Confirm that GitHub Actions passes, or explain any failure that is unrelated to the change.

## Review and release boundaries

A maintainer may request changes before merging. Do not change the package version, create a release tag, or publish a release as part of an ordinary feature or bug-fix PR unless the maintainer explicitly asks for it.
