// Keeping FFmpeg and Chrome warm while a render starts. Before rendering, the
// HyperFrames CLI probes `ffmpeg -version` and `chrome --version` with a
// 5-second timeout. With every core busy and memory short, a binary's pages
// are evicted during the render's minute-long start-up, re-reading them misses
// the 5 seconds, and the render stops with "Failed to run ... --version" (the
// video-production skill, "Gotchas"). One idle FFmpeg held open for the
// length of the render, and a probe of each binary every two seconds, keep
// them resident; with that, a render on a loaded machine passes its probes
// first time. `npm run render -- ... --warm` turns it on.
import { spawn } from "node:child_process";
import { existsSync, readdirSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";

const SHELL = process.platform === "win32" ? "chrome-headless-shell.exe" : "chrome-headless-shell";

/** The headless Chrome the CLI downloaded (hyperframes browser ensure), or null. */
export function cachedChrome(root = path.join(homedir(), ".cache", "hyperframes", "chrome"), depth = 5) {
  if (depth < 0 || !existsSync(root)) return null;
  const entries = readdirSync(root, { withFileTypes: true });
  const here = entries.find((entry) => entry.isFile() && entry.name === SHELL);
  if (here) return path.join(root, here.name);
  for (const entry of entries.filter((candidate) => candidate.isDirectory()).sort((a, b) => b.name.localeCompare(a.name))) {
    const found = cachedChrome(path.join(root, entry.name), depth - 1);
    if (found) return found;
  }
  return null;
}

/**
 * Starts the idle FFmpeg and the probes; returns the function that stops
 * them. A binary that cannot be started is skipped: warming is an aid, and
 * never the reason a render fails.
 */
export function keepWarm({ ffmpeg = "ffmpeg", ffprobe = "ffprobe", chrome = cachedChrome(), everyMs = 2000, spawnProcess = spawn } = {}) {
  const quiet = { stdio: "ignore", windowsHide: true };
  const idle = spawnProcess(
    ffmpeg,
    ["-hide_banner", "-loglevel", "error", "-re", "-f", "lavfi", "-i", "anullsrc=r=8000:cl=mono", "-t", "2400", "-f", "null", "-"],
    quiet,
  );
  idle.on("error", () => {});
  const probe = () => {
    spawnProcess(ffprobe, ["-version"], quiet).on("error", () => {});
    if (chrome) spawnProcess(chrome, ["--version"], quiet).on("error", () => {});
  };
  probe();
  const timer = setInterval(probe, everyMs);
  return () => {
    clearInterval(timer);
    if (idle.exitCode === null && !idle.killed) idle.kill();
  };
}
