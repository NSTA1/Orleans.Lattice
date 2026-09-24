import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);

/** The workspace root (videos/), whatever the caller's working directory. */
export const workspaceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");

/**
 * The environment every workspace invocation of the HyperFrames CLI runs with:
 * no anonymous telemetry, no update check, and no skill installation into the
 * repository. Skills are installed per user instead (see README.md).
 */
export const quietEnvironment = Object.freeze({
  HYPERFRAMES_NO_TELEMETRY: "1",
  HYPERFRAMES_NO_UPDATE_CHECK: "1",
  HYPERFRAMES_SKIP_SKILLS: "1",
  DO_NOT_TRACK: "1",
});

/** Absolute path of the pinned CLI entry point, resolved from node_modules. */
export function cliEntryPoint() {
  const manifestPath = require.resolve("hyperframes/package.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  return path.join(path.dirname(manifestPath), manifest.bin.hyperframes);
}

/**
 * The arguments as they are actually passed to the CLI. `snapshot` sends
 * captured frames to a hosted vision model whenever GEMINI_API_KEY is set; the
 * workspace never wants that implicitly, so it is switched off unless the
 * caller passes --describe explicitly.
 */
export function guardArguments(args) {
  const describes = args.some((arg) => arg === "--describe" || arg.startsWith("--describe="));
  return args[0] === "snapshot" && !describes ? [...args, "--describe", "false"] : [...args];
}

/**
 * Runs the pinned CLI from the workspace root and resolves with its exit code
 * and, when `capture` is set, its standard output.
 */
export function runHyperframes(args, { capture = false, cwd = workspaceRoot } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [cliEntryPoint(), ...guardArguments(args)], {
      cwd,
      env: { ...process.env, ...quietEnvironment },
      stdio: capture ? ["ignore", "pipe", "inherit"] : "inherit",
    });
    let stdout = "";
    if (capture) child.stdout.on("data", (chunk) => (stdout += chunk));
    child.on("error", reject);
    child.on("close", (code) => resolve({ code: code ?? 1, stdout }));
  });
}

/** The last line of CLI output that parses as a JSON object (for --json commands). */
export function lastJsonObject(stdout) {
  const lines = stdout.split(/\r?\n/).map((line) => line.trim()).filter((line) => line.startsWith("{"));
  for (let i = lines.length - 1; i >= 0; i--) {
    try {
      return JSON.parse(lines[i]);
    } catch {
      // Not a complete JSON line; keep looking further up.
    }
  }
  throw new Error("expected a JSON result from the HyperFrames CLI, found none");
}
