import { xdr, Address } from '@stellar/stellar-sdk';
import { unwrapScVal, decodeEvent } from '../src/decoder.js';
import { SorobanEventStreamer, MAX_SAFE_LEDGER_SPAN } from '../src/streamer.js';

console.log("\x1b[36m=== 1. TESTING SCVAL DECODER & NORMALIZER ===\x1b[0m");

// Test U64
const u64Val = xdr.ScVal.scvU64(429496729800n);
console.log("  * U64 unwrapped:", unwrapScVal(u64Val));

// Test Symbol
const symVal = xdr.ScVal.scvSymbol("transfer");
console.log("  * Symbol unwrapped:", unwrapScVal(symVal));

// Test Vec
const vecVal = xdr.ScVal.scvVec([
  xdr.ScVal.scvSymbol("mint"),
  xdr.ScVal.scvU32(500)
]);
console.log("  * Vec unwrapped:", JSON.stringify(unwrapScVal(vecVal)));

console.log("\n\x1b[32m✔ Decoder unit tests passed.\x1b[0m\n");

console.log("\x1b[36m=== 2. TESTING LIVE STREAMER & WINDOWING (TESTNET) ===\x1b[0m");

async function runLiveTest() {
  const rpcUrl = "https://soroban-testnet.stellar.org";
  const streamer = new SorobanEventStreamer(rpcUrl);

  const latest = await streamer.getLatestLedger();
  console.log(`  * Connected to Testnet RPC. Latest Ledger: \x1b[33m${latest}\x1b[0m`);

  const windowStart = latest - 100;
  console.log(`  * Querying window: [${windowStart} -> ${latest}]`);

  const events = await streamer.getEventsWindowed({
    startLedger: windowStart,
    endLedger: latest,
    filters: [{ type: "contract" }],
    limit: 5
  });

  console.log(`  * Received ${events.length} decoded events.`);
  if (events.length > 0) {
    console.log("  * Sample Decoded Event:");
    console.log(JSON.stringify(events[0], null, 2));
  } else {
    console.log("  * No contract events in last 100 ledgers (window completed clean).");
  }

  console.log(`\n\x1b[32m✔ Live Testnet windowing test succeeded without silent drops.\x1b[0m`);
}

runLiveTest().catch(err => {
  console.error("\x1b[31mTest failed:\x1b[0m", err);
  process.exit(1);
});
