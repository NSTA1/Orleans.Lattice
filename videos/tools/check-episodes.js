#!/usr/bin/env node
// Runs the CLI's full check (lint, runtime, layout, motion, WCAG contrast) on
// every episode in turn, as `npm run check` does on the smoke test.
//
// Narration audio is never committed - a published episode's MP4 carries its
// sound - so on a machine that
// has not narrated an episode - CI among them - its composition names a track
// that does not exist, and lint rejects that. Such an episode is checked
// against a silent stand-in of the track's stamped length, written where the
// composition looks for it and removed afterwards: its pictures, timing and
// structure are checked, and its sound is not. Narrate it to hear it.
//
//   npm run check:episodes
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths, listEpisodes } from "./lib/layout.js";
import { posterTime, readEpisode } from "./lib/publication.js";
import { openingTags } from "./lib/timeline.js";
import { silentWav } from "./lib/wav.js";

const failed = [];
let checked = 0;
for (const slug of listEpisodes()) {
  const { composition, metadata } = episodePaths(slug);
  if (!existsSync(composition)) {
    console.log(`episode ${slug}: no composition yet, nothing to check`);
    continue;
  }
  const html = readFileSync(composition, "utf8");
  const tags = openingTags(html);
  const root = tags.find((tag) => tag.get("data-composition-id") !== undefined);
  if (!root || !(Number(root.get("data-duration")) > 0)) {
    console.error(`episode ${slug}: its composition is not stamped with a duration; run 'npm run timeline -- ${slug}'`);
    failed.push(slug);
    continue;
  }
  // Its metadata, once it has any, must be valid and its poster must fall
  // inside the stamped timeline; a companion page cannot be written without it.
  if (existsSync(metadata)) {
    try {
      posterTime(html, readEpisode(slug).poster);
    } catch (error) {
      console.error(`episode ${slug}: ${error.message}`);
      failed.push(slug);
      continue;
    }
  }

  const standIns = [];
  for (const audio of tags.filter((tag) => tag.name === "audio")) {
    const file = path.join(workspaceRoot, audio.get("src") ?? "");
    if (existsSync(file)) continue;
    mkdirSync(path.dirname(file), { recursive: true });
    writeFileSync(file, silentWav(Number(audio.get("data-duration")) || Number(root.get("data-duration"))));
    standIns.push(file);
  }
  if (standIns.length > 0) {
    console.log(`episode ${slug}: not narrated here, so checked against silence of its stamped length`);
  }
  try {
    const result = spawnSync(process.execPath, [path.join(workspaceRoot, "tools", "hf.js"), "check", ".", "--episode", slug], {
      cwd: workspaceRoot,
      stdio: "inherit",
    });
    checked++;
    if (result.status !== 0) failed.push(slug);
  } finally {
    for (const file of standIns) rmSync(file, { force: true });
  }
}

if (checked === 0 && failed.length === 0) {
  console.log("check:episodes: no episode has a composition yet");
} else if (failed.length > 0) {
  console.error(`check:episodes: ${failed.length} episode(s) failed: ${failed.join(", ")}`);
  process.exitCode = 1;
} else {
  console.log(`check:episodes: ${checked} episode(s) passed`);
}
