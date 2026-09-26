import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

/** The prefix that marks the worker's protocol lines on its stdout (see tools/voice_worker.py). */
export const PROTOCOL_MARK = "@@voice ";

/**
 * Starts a voice worker, a long-lived process that loads its models once and
 * then answers one request per line (tools/voice_worker.py). Returns
 * { ready, request, close }: `ready` resolves with the worker's first message
 * once its models are loaded; `request(payload)` sends one request and
 * resolves with its reply, or rejects with the worker's error; `close()` ends
 * the worker and resolves when it has exited. Lines on stdout without the
 * protocol mark are ignored, so a library's print() cannot be taken for a
 * reply. stderr goes to `log`, and its last lines explain an unexpected exit.
 */
export function startVoiceWorker(command, args, { env = {}, log } = {}) {
  const child = spawn(command, args, {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, PYTHONUNBUFFERED: "1", PYTHONIOENCODING: "utf-8", ...env },
  });
  let tail = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    log?.write(chunk);
    tail = (tail + chunk).slice(-4000);
  });

  const pending = new Map();
  let nextId = 1;
  let failure = null;
  let settleReady;
  const ready = new Promise((resolve, reject) => {
    settleReady = { resolve, reject };
  });
  // A rejection nobody is waiting for yet must not crash the process.
  ready.catch(() => {});

  const fail = (error) => {
    failure ??= error;
    settleReady.reject(failure);
    for (const { reject } of pending.values()) reject(failure);
    pending.clear();
  };

  createInterface({ input: child.stdout }).on("line", (line) => {
    if (!line.startsWith(PROTOCOL_MARK)) return;
    let message;
    try {
      message = JSON.parse(line.slice(PROTOCOL_MARK.length));
    } catch {
      return;
    }
    if (message.ready) {
      settleReady.resolve(message);
      return;
    }
    const waiting = pending.get(message.id);
    if (!waiting) return;
    pending.delete(message.id);
    if (message.ok === false) waiting.reject(new Error(message.error ?? "the voice worker reported a failure"));
    else waiting.resolve(message);
  });

  const exited = new Promise((resolve) => {
    child.on("close", (code, signal) => {
      const lastLines = tail.trim().split(/\r?\n/).slice(-8).join("\n");
      fail(new Error(`the voice worker exited (${signal ?? `code ${code}`})${lastLines ? `:\n${lastLines}` : ""}`));
      resolve(code);
    });
  });
  child.on("error", (error) => fail(new Error(`the voice worker could not start: ${error.message}`)));
  child.stdin.on("error", () => {});

  return {
    ready,
    request(payload) {
      if (failure) return Promise.reject(failure);
      const id = nextId++;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        child.stdin.write(`${JSON.stringify({ ...payload, id })}\n`);
      });
    },
    close() {
      child.stdin.end();
      return exited;
    },
  };
}
