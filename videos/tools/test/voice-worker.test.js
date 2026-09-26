import assert from "node:assert/strict";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { startVoiceWorker } from "../lib/voice-worker.js";

const fake = path.join(path.dirname(fileURLToPath(import.meta.url)), "fake-voice-worker.js");
const start = (log) => startVoiceWorker(process.execPath, [fake], { log });

test("the worker is ready once it says so, and each reply finds its request, whatever else it prints", async () => {
  const worker = start();
  const ready = await worker.ready;
  assert.equal(ready.sampleRate, 24000);
  assert.deepEqual(ready.versions, { fake: "1.0" });
  const [first, second] = await Promise.all([
    worker.request({ text: "Neither waits its turn.", seed: 1 }),
    worker.request({ text: "Pick your way in.", seed: 2 }),
  ]);
  assert.deepEqual(first.transcripts, { fake: "Neither waits its turn." });
  assert.equal(first.seed, 1);
  assert.deepEqual(second.transcripts, { fake: "Pick your way in." });
  assert.equal(await worker.close(), 0);
});

test("a failure the worker reports rejects that request only", async () => {
  const worker = start();
  await worker.ready;
  await assert.rejects(worker.request({ text: "fail" }), /RuntimeError: no voice/);
  const after = await worker.request({ text: "Still here." });
  assert.equal(after.ok, true);
  await worker.close();
});

test("a worker that dies rejects what is waiting and everything after, with the last of what it said", async () => {
  const lines = [];
  const worker = start({ write: (chunk) => lines.push(chunk) });
  await worker.ready;
  await assert.rejects(worker.request({ text: "crash" }), /exited \(code 3\):[\s\S]*ran out of memory/);
  await assert.rejects(worker.request({ text: "Too late." }), /exited/);
  assert.match(lines.join(""), /loading models/, "stderr goes to the log");
});

test("a worker that cannot start rejects its readiness", async () => {
  const worker = startVoiceWorker(path.join(path.dirname(fake), "no-such-python.exe"), []);
  await assert.rejects(worker.ready, /could not start|exited/);
});
