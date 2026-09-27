// Write-through copies of what narration and auditions make, kept in the
// per-user state directory (layout.js, stateDir) so that they outlive the
// checkout that made them. A scheduled run works in a new worktree each time;
// without these copies it would speak an episode again from nothing (the
// better part of an hour), and lose the takes a reviewer picked.
//
// The workspace copy under renders/ stays the one every tool and review page
// reads, so nothing else changes: a clip or take is copied in from the cache
// when the workspace lacks it, and copied out whenever it is made or picked.
// Clip names are digests of what a clip says and how, so a cached clip can
// only ever stand for the cue it was made for.
import { copyFileSync, existsSync, mkdirSync, readdirSync } from "node:fs";
import path from "node:path";

const PARTS = [".wav", ".json"];

function copyParts(fromDir, toDir, name) {
  mkdirSync(toDir, { recursive: true });
  for (const ext of PARTS) {
    const from = path.join(fromDir, `${name}${ext}`);
    if (existsSync(from)) copyFileSync(from, path.join(toDir, `${name}${ext}`));
  }
}

/** The workspace folder a clip lives in. */
const clipsIn = (paths) => path.join(paths.narration, "clips");

/**
 * Copies a cached clip, and its check record, into the workspace when the
 * workspace lacks it. True when it did.
 */
export function restoreClip(paths, name) {
  if (existsSync(path.join(clipsIn(paths), `${name}.wav`)) || !existsSync(path.join(paths.clipCache, `${name}.wav`))) return false;
  copyParts(paths.clipCache, clipsIn(paths), name);
  return true;
}

/** Copies a workspace clip, and its check record, into the cache. */
export function saveClip(paths, name) {
  copyParts(clipsIn(paths), paths.clipCache, name);
}

/** Copies the cached takes of a clip into the workspace, keeping any it already has. The number copied. */
export function restoreTakes(paths, name) {
  const from = path.join(paths.takeCache, name);
  if (!existsSync(from)) return 0;
  const to = path.join(paths.takes, name);
  mkdirSync(to, { recursive: true });
  let copied = 0;
  for (const file of readdirSync(from)) {
    if (!/^take\d+\.(wav|json)$/.test(file) || existsSync(path.join(to, file))) continue;
    copyFileSync(path.join(from, file), path.join(to, file));
    if (file.endsWith(".wav")) copied++;
  }
  return copied;
}

/** Copies one take of a clip, and its check record, into the cache. */
export function saveTake(paths, name, take) {
  copyParts(path.join(paths.takes, name), path.join(paths.takeCache, name), `take${take}`);
}
