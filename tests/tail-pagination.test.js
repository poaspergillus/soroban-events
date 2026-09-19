import test from "node:test";
import assert from "node:assert/strict";
import { SorobanEventStreamer } from "../src/index.js";

test("tail paginates backward until it has enough newest events", async () => {
  const streamer = new SorobanEventStreamer("https://example.invalid", {
    pageSize: 2,
    windowSize: 100
  });

  streamer.getLatestLedger = async () => 100;

  const calls = [];

  streamer.server.getEvents = async params => {
    calls.push(params);

    if (!params.pagination.cursor) {
      return {
        events: [
          { id: "e1", ledger: 50 },
          { id: "e2", ledger: 51 }
        ],
        cursor: "next"
      };
    }

    return {
      events: [
        { id: "e3", ledger: 52 },
        { id: "e4", ledger: 53 }
      ]
    };
  };

  const events = await streamer.tail({ limit: 3 });

  assert.deepEqual(
    events.map(event => event.id),
    ["e2", "e3", "e4"]
  );

  assert.equal(calls.length, 2);
  assert.equal(calls[0].pagination.limit, 2);
  assert.equal(calls[1].pagination.cursor, "next");
});
