// A stand-in voice worker that speaks the protocol of tools/voice_worker.py,
// for tools/test/voice-worker.test.js. It prints library-style noise around
// its protocol lines, answers each request by echoing its text as the
// transcript, reports a failure for the text "fail", and exits abruptly on
// "crash", after writing a last line to stderr.
import { createInterface } from "node:readline";

const MARK = "@@voice ";
const send = (message) => process.stdout.write(`${MARK}${JSON.stringify(message)}\n`);

process.stdout.write("loaded PerthNet (Implicit) at step 250,000\n");
process.stderr.write("loading models...\n");
send({ ready: true, sampleRate: 24000, loadSeconds: 0.1, versions: { fake: "1.0" } });

createInterface({ input: process.stdin }).on("line", (line) => {
  const request = JSON.parse(line);
  if (request.text === "crash") {
    process.stderr.write("Traceback: the model ran out of memory\n");
    process.exit(3);
  }
  if (request.text === "fail") {
    send({ id: request.id, ok: false, error: "RuntimeError: no voice" });
    return;
  }
  process.stdout.write("Sampling: 100%|##########| 10/10\n");
  send({ id: request.id, ok: true, seconds: request.text.length / 10, transcripts: { fake: request.text }, seed: request.seed });
});
