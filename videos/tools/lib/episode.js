import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./hyperframes.js";
import { episodePaths, rendersDir, workspaceIndex } from "./layout.js";

// The CLI's project commands (preview, lint, check, snapshot, ...) take only a
// project directory and always open its index.html, and lint discovers other
// compositions only under a folder named compositions/. An episode's
// composition lives in episodes/<slug>/composition.html instead, so to run one
// of those commands on it, the episode stands in as index.html for the length
// of the command and the workspace's own index.html is put back afterwards.
// Paths in an episode are root-relative, exactly as in index.html, so nothing
// else changes.

/**
 * Where the workspace's index.html waits while an episode stands in for it.
 * It is outside the project root on purpose: a second root-level composition
 * fails lint (multiple_root_compositions).
 */
export const defaultIndexBackup = path.join(rendersDir, ".workspace-index.html");

/**
 * Separates `--episode <slug>` or `--episode=<slug>` from the arguments passed
 * on to the CLI. The slug is null when the flag is absent.
 */
export function takeEpisodeArgument(args) {
  const rest = [];
  let slug = null;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    let value;
    if (arg === "--episode") {
      value = args[++i];
      if (value === undefined || value.startsWith("-")) {
        throw new Error("--episode needs an episode slug, for example --episode introduction");
      }
    } else if (arg.startsWith("--episode=")) {
      value = arg.slice("--episode=".length);
    } else {
      rest.push(arg);
      continue;
    }
    if (slug !== null) {
      throw new Error("--episode may be given once");
    }
    slug = value;
  }
  return { slug, args: rest };
}

/**
 * Puts back a workspace index.html that an interrupted run left swapped out.
 * Returns true when there was one to put back.
 */
export function recoverWorkspaceIndex({ index = workspaceIndex, backup = defaultIndexBackup } = {}) {
  if (!existsSync(backup)) {
    return false;
  }
  // Written, not copied: a copy keeps the backup's timestamp, and a file
  // watcher or an incremental tool would then miss that the file changed.
  writeFileSync(index, readFileSync(backup));
  rmSync(backup);
  return true;
}

/**
 * Runs `task` with the episode's composition standing in as the workspace's
 * index.html, and restores the workspace's own index.html however the task
 * ends: on return, on a throw, on Ctrl+C, and at process exit.
 */
export async function withEpisode(slug, task, { index = workspaceIndex, backup = defaultIndexBackup, composition } = {}) {
  const source = composition ?? episodePaths(slug).composition;
  if (!existsSync(source)) {
    const shown = path.relative(workspaceRoot, source).split(path.sep).join("/");
    throw new Error(`episode '${slug}' has no composition: ${shown} does not exist`);
  }
  recoverWorkspaceIndex({ index, backup });
  mkdirSync(path.dirname(backup), { recursive: true });
  writeFileSync(backup, readFileSync(index));

  const restore = () => recoverWorkspaceIndex({ index, backup });
  // The CLI shares the console, so Ctrl+C reaches it too; wait for it to exit
  // and restore in `finally`. A second Ctrl+C restores at once and leaves.
  let interrupts = 0;
  const onSignal = (signal) => {
    interrupts += 1;
    if (interrupts > 1) {
      restore();
      process.exit(signal === "SIGINT" ? 130 : 143);
    }
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
  process.on("exit", restore);
  try {
    writeFileSync(index, readFileSync(source));
    return await task();
  } finally {
    restore();
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
    process.off("exit", restore);
  }
}
