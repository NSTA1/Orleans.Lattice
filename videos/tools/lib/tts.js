import { lastJsonObject, runHyperframes } from "./hyperframes.js";

// The CLI reports Kokoro as "not installed" whenever `python -c "import
// kokoro_onnx"` outlasts its fixed 10-second probe timeout, and that import
// (onnxruntime, numpy, the phonemizer) can take longer on a loaded machine.
// Those probe failures are retried after a growing pause, to let the load
// pass; any other failure is reported at once.
const PROBE_FAILURE = /is not installed|Python 3 is required/;

/** Attempts per clip before a probe failure is treated as real. */
export const PROBE_ATTEMPTS = 4;

/** The pause after the nth failed probe is n times this. */
export const PROBE_BACKOFF_MS = 3000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Synthesises one clip with the pinned CLI's local Kokoro voice and resolves
 * with its JSON result ({ ok, durationSeconds, outputPath, ... }). `run` is the
 * CLI runner and `wait` the pause between probe retries, both injectable for
 * tests.
 */
export async function speak(text, { voice, lang, speed, output }, run = runHyperframes, wait = sleep) {
  const args = ["tts", text, "--voice", voice, "--lang", lang, "--speed", String(speed), "--output", output, "--json"];
  let failure = "";
  for (let attempt = 1; attempt <= PROBE_ATTEMPTS; attempt++) {
    const { code, stdout } = await run(args, { capture: true });
    let result;
    try {
      result = lastJsonObject(stdout);
    } catch {
      result = { ok: false, error: stdout.trim() || `the CLI exited with code ${code}` };
    }
    if (code === 0 && result.ok !== false) {
      return result;
    }
    failure = result.error ?? `the CLI exited with code ${code}`;
    if (!PROBE_FAILURE.test(failure)) {
      break;
    }
    if (attempt < PROBE_ATTEMPTS) {
      await wait(attempt * PROBE_BACKOFF_MS);
    }
  }
  throw new Error(failure);
}
