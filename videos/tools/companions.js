#!/usr/bin/env node
// Keeps every companion page (docs/videos/<slug>.md) in step with its
// episode: the video block the documentation site plays, and the transcript
// (see tools/lib/companion.js). Every page needs an episode folder with an
// episode.json and a stamped composition. It also checks docs-site/media/:
// every cut a page pins must have its three files there, and nothing that no
// page pins may be left behind.
//
//   npm run companions          rewrite every companion page from its episode
//   npm run companions:check    fail if any page has drifted from its episode (CI)
import { existsSync, readdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { companionUpdate } from "./lib/companion.js";
import { episodePaths, isSlug } from "./lib/layout.js";
import { companionsDir, findVideoBlocks, isMediaName, mediaNames, repoRoot, siteMediaDir } from "./lib/publication.js";

const check = process.argv.includes("--check");
const relative = (file) => path.relative(repoRoot, file).split(path.sep).join("/");

const problems = [];
const pinned = new Map();
let pages = 0;
let rewritten = 0;
const files = existsSync(companionsDir) ? readdirSync(companionsDir).filter((name) => name.endsWith(".md")).sort() : [];
for (const name of files) {
  const slug = name.slice(0, -".md".length);
  const page = path.join(companionsDir, name);
  pages++;
  if (!isSlug(slug) || !existsSync(episodePaths(slug).dir)) {
    problems.push(`${relative(page)}: there is no episode folder videos/episodes/${slug}/ for it to follow`);
    continue;
  }
  let result;
  try {
    result = companionUpdate(slug);
  } catch (error) {
    problems.push(`${relative(page)}: ${error.message}`);
    continue;
  }
  for (const { attributes } of findVideoBlocks(result.updated)) {
    if (attributes.cut === undefined) continue;
    for (const media of Object.values(mediaNames(slug, attributes.cut))) pinned.set(media, slug);
  }
  if (result.updated === result.original) continue;
  if (check) {
    problems.push(`${relative(page)}: differs from videos/episodes/${slug}/; run 'npm run companions'`);
  } else {
    writeFileSync(page, result.updated);
    rewritten++;
  }
}

for (const [media, slug] of pinned) {
  if (!existsSync(path.join(siteMediaDir, media))) {
    problems.push(`${relative(path.join(siteMediaDir, media))}, which docs/videos/${slug}.md plays, is missing; run 'npm run publish -- ${slug}'`);
  }
}
const present = existsSync(siteMediaDir) ? readdirSync(siteMediaDir).filter(isMediaName) : [];
for (const media of present) {
  if (!pinned.has(media)) {
    problems.push(`${relative(path.join(siteMediaDir, media))} is not played by any companion page; delete it`);
  }
}

console.log(`${pages} companion page(s), ${pinned.size} published file(s)` + (check ? "" : `; ${rewritten} page(s) rewritten`));
for (const problem of problems) {
  console.error(`error: ${problem}`);
}
process.exitCode = problems.length === 0 ? 0 : 1;
