#!/usr/bin/env node
// Stamps an episode's narration timeline into its composition (see
// tools/lib/timeline.js for what is stamped where).
//
//   npm run timeline -- <slug>           rewrite episodes/<slug>/composition.html
//   npm run timeline -- <slug> --check   fail if it is not stamped with the current narration
//
// Run it after every `npm run narrate -- <slug>`.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths } from "./lib/layout.js";
import { parseScript } from "./lib/narration.js";
import { stampComposition } from "./lib/timeline.js";

const args = process.argv.slice(2);
const check = args.includes("--check");
const relative = (file) => path.relative(workspaceRoot, file).split(path.sep).join("/");

let paths;
try {
  paths = episodePaths(args.find((arg) => !arg.startsWith("--")));
} catch (error) {
  console.error(`timeline: ${error.message}`);
  console.error("usage: npm run timeline -- <episode-slug> [--check]");
  process.exit(2);
}

const manifestFile = path.join(paths.narration, "cues.json");
for (const [file, remedy] of [
  [paths.composition, "write the episode's composition first"],
  [manifestFile, `run 'npm run narrate -- ${paths.slug}' first`],
]) {
  if (!existsSync(file)) {
    console.error(`timeline: ${relative(file)} does not exist; ${remedy}`);
    process.exit(1);
  }
}

// Stamping from a narration of an older script would time the pictures to
// words that are no longer said.
const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
const { cues } = parseScript(readFileSync(paths.script, "utf8"), relative(paths.script));
const narrated = manifest.cues.map((cue) => cue.text);
if (narrated.length !== cues.length || cues.some((cue, i) => cue.text !== narrated[i])) {
  console.error(`timeline: ${relative(paths.script)} has changed since it was narrated; run 'npm run narrate -- ${paths.slug}' first`);
  process.exit(1);
}

const original = readFileSync(paths.composition, "utf8");
let stamped;
try {
  stamped = stampComposition(original, manifest, { clipRoot: relative(paths.narration) });
} catch (error) {
  console.error(`${relative(paths.composition)}: ${error.message}`);
  process.exit(1);
}

const summary = `${manifest.scenes.length} scene(s), ${manifest.cues.length} cue(s), ${manifest.duration}s`;
if (check) {
  if (stamped !== original) {
    console.error(`timeline: ${relative(paths.composition)} is not stamped with the current narration; run 'npm run timeline -- ${paths.slug}'`);
    process.exit(1);
  }
  console.log(`timeline: ${relative(paths.composition)} matches its narration (${summary})`);
} else if (stamped === original) {
  console.log(`timeline: ${relative(paths.composition)} was already stamped (${summary})`);
} else {
  writeFileSync(paths.composition, stamped);
  console.log(`timeline: stamped ${relative(paths.composition)} (${summary})`);
}
