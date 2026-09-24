import assert from "node:assert/strict";
import { test } from "node:test";
import { PROBE_ATTEMPTS, speak } from "../lib/tts.js";

const options = { voice: "bf_emma", lang: "en-gb", speed: 1, output: "out.wav" };
const ok = { code: 0, stdout: '{"ok":true,"durationSeconds":2.5}\n' };
const probeFailure = {
  code: 1,
  stdout: '{"ok":false,"error":"The kokoro-onnx package is not installed. Run: pip install kokoro-onnx soundfile"}\n',
};

/** A fake CLI runner that replays the given results and records each call. */
function scripted(...results) {
  const calls = [];
  const run = async (args, runOptions) => {
    calls.push({ args, runOptions });
    return results[Math.min(calls.length - 1, results.length - 1)];
  };
  return { run, calls };
}

test("a clip passes the voice settings to the CLI and returns its result", async () => {
  const { run, calls } = scripted(ok);
  const result = await speak("Hello.", options, run);
  assert.equal(result.durationSeconds, 2.5);
  assert.deepEqual(calls[0].args, [
    "tts", "Hello.", "--voice", "bf_emma", "--lang", "en-gb", "--speed", "1", "--output", "out.wav", "--json",
  ]);
  assert.equal(calls[0].runOptions.capture, true);
});

test("a timed-out Kokoro probe is retried and then succeeds", async () => {
  const { run, calls } = scripted(probeFailure, ok);
  const result = await speak("Hello.", options, run);
  assert.equal(result.ok, true);
  assert.equal(calls.length, 2);
});

test("a probe failure that persists is reported after the last attempt", async () => {
  const { run, calls } = scripted(probeFailure);
  await assert.rejects(speak("Hello.", options, run), /not installed/);
  assert.equal(calls.length, PROBE_ATTEMPTS);
});

test("any other failure is reported at once, without a retry", async () => {
  const { run, calls } = scripted({ code: 1, stdout: '{"ok":false,"error":"unknown voice zz_nobody"}\n' });
  await assert.rejects(speak("Hello.", options, run), /unknown voice/);
  assert.equal(calls.length, 1);
});

test("a failure with no JSON result still reports something", async () => {
  const { run } = scripted({ code: 3, stdout: "" });
  await assert.rejects(speak("Hello.", options, run), /exited with code 3/);
});
