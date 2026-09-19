import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

test("package declares v3.0.0 release metadata", async () => {
  const pkg = JSON.parse(await readFile("package.json", "utf8"));
  assert.equal(pkg.version, "3.0.0");
  assert.equal(pkg.engines?.node, ">=22.12.0");
});

test("package lock matches v3.0.0", async () => {
  const lock = JSON.parse(await readFile("package-lock.json", "utf8"));
  assert.equal(lock.version, "3.0.0");
  assert.equal(lock.packages?.[""]?.version, "3.0.0");
});

test("changelog documents v3.0.0", async () => {
  const changelog = await readFile("CHANGELOG.md", "utf8");
  assert.match(changelog, /^## \[3\.0\.0\] - Unreleased/m);
});

test("README documents the unreleased v3.0.0 boundary", async () => {
  const readme = await readFile("README.md", "utf8");
  assert.match(readme, /## v3\.0\.0/);
  assert.match(readme, /currently unreleased/i);
  assert.match(readme, /SqliteEventStore/);
});
