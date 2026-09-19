import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

const ROOT = process.cwd();

function run(command, args, cwd, extraEnv = {}) {
  const result = spawnSync(command, args, {
    cwd,
    env: { ...process.env, ...extraEnv },
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  if (result.status !== 0) {
    throw new Error(
      [
        `${command} ${args.join(" ")} failed with exit code ${result.status}`,
        result.stdout,
        result.stderr,
      ].filter(Boolean).join("\n")
    );
  }

  return result.stdout;
}

test("packed package installs and works from a clean consumer", async () => {
  const tempRoot = await mkdtemp(join(tmpdir(), "soroban-events-consumer-"));

  try {
    const packJson = run(
      "npm",
      ["pack", "--json", "--ignore-scripts"],
      ROOT
    );

    const packed = JSON.parse(packJson);
    assert.equal(packed.length, 1);
    assert.equal(packed[0].name, "soroban-events");
    assert.match(packed[0].filename, /^soroban-events-\d+\.\d+\.\d+\.tgz$/);

    const tarball = join(ROOT, packed[0].filename);

    run(
      "npm",
      [
        "install",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        tarball,
      ],
      tempRoot
    );

    const packageJson = JSON.parse(
      await readFile(join(tempRoot, "node_modules", "soroban-events", "package.json"), "utf8")
    );

    assert.equal(packageJson.name, "soroban-events");
    assert.ok(packageJson.types);
    assert.ok(packageJson.bin);

    const consumerCheck = run(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `
          import * as pkg from "soroban-events";

          const required = [
            "SorobanEventStreamer",
            "EventEngine",
            "EventReplay",
            "BackfillEngine",
            "MemoryCheckpointStore",
            "FileCheckpointStore",
            "MemoryEventStore",
            "SqliteEventStore",
            "EventPipeline",
            "EventHandoff",
            "Metrics",
            "createMetrics",
            "DeadLetterError",
            "MemoryDeadLetterQueue",
            "createEventFilter",
            "filterEvents",
            "createEventQuery",
            "exportEvents",
            "exportEventStream"
          ];

          for (const name of required) {
            if (!(name in pkg)) {
              throw new Error("Missing public export: " + name);
            }
          }

          if (typeof pkg.SorobanEventStreamer !== "function") {
            throw new Error("SorobanEventStreamer is not constructible");
          }

          if (typeof pkg.exportEvents !== "function") {
            throw new Error("exportEvents is not callable");
          }

          console.log("consumer import: ok");
          console.log("public exports: " + required.length);
        `,
      ],
      tempRoot
    );

    assert.match(consumerCheck, /consumer import: ok/);
    assert.match(consumerCheck, /public exports: 19/);

    const cli = join(
      tempRoot,
      "node_modules",
      ".bin",
      "soroban-events"
    );

    const cliResult = spawnSync(cli, ["--help"], {
      cwd: tempRoot,
      env: process.env,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });

    assert.equal(cliResult.status, 0, cliResult.stderr);
    assert.match(cliResult.stdout, /soroban-events/);
    assert.match(cliResult.stdout, /backfill/);
    assert.match(cliResult.stdout, /replay/);
    assert.match(cliResult.stdout, /status/);

    console.log("packed consumer: ok");
    console.log(`package: ${packageJson.version}`);
    console.log(`tarball: ${packed[0].filename}`);
  } finally {
    await rm(tempRoot, { recursive: true, force: true });
  }
});
