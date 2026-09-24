#!/usr/bin/env node
// Publishes an episode to the documentation site. It puts the three files
// the site plays into docs-site/media/, named by their cut:
//   - the video
//   - its captions
//   - its poster
// It then records the cut in episodes/<slug>/episode.json and in the
// companion page, and removes the episode's earlier cut
// (series.md, "Hosting - decided"). Commit all three; the site plays what
// is committed.
//
//   npm run publish -- <slug> [--render <file>]
//
// It publishes renders/<slug>-high.mp4 (or --render <file>), rendered with
//   npm run render -- --episode <slug> --quality high -o renders/<slug>-high.mp4
// and only while that render's receipt matches the sources as they are now.
// The captions are written from the narration's timeline, and the poster is
// the frame at the moment episode.json names (this needs ffmpeg).
import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { syncBrand } from "./lib/brand.js";
import { companionUpdate } from "./lib/companion.js";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths, rendersDir } from "./lib/layout.js";
import { captionCues, toWebVtt } from "./lib/narration.js";
import {
  compositionDuration,
  cutOf,
  fileDigest,
  MEDIA,
  mediaNames,
  posterTime,
  readEpisode,
  repoRoot,
  siteMediaDir,
  writeEpisode,
} from "./lib/publication.js";
import { digestFiles, readReceipt, renderInputs } from "./lib/receipt.js";

const fail = (message, code = 1) => {
  console.error(`publish: ${message}`);
  process.exit(code);
};

const argv = process.argv.slice(2);
const renderAt = argv.indexOf("--render");
const slug = argv.find((arg, i) => !arg.startsWith("--") && argv[i - 1] !== "--render");
let paths;
try {
  paths = episodePaths(slug);
} catch (error) {
  fail(`${error.message}\nusage: npm run publish -- <episode-slug> [--render <file>]`, 2);
}
const render = renderAt >= 0 ? path.resolve(argv[renderAt + 1] ?? "") : path.join(rendersDir, `${slug}-high.mp4`);
const shown = (file) => path.relative(workspaceRoot, file).split(path.sep).join("/");
const fromRepo = (file) => path.relative(repoRoot, file).split(path.sep).join("/");

// The render must be of this episode, unchanged since, and from the sources
// as they are now.
let meta;
try {
  meta = readEpisode(slug);
} catch (error) {
  fail(error.message);
}
if (!existsSync(render)) {
  fail(`${shown(render)} does not exist; render it with 'npm run render -- --episode ${slug} --quality high -o renders/${slug}-high.mp4'`);
}
const receipt = readReceipt(render);
if (!receipt || receipt.episode !== slug) {
  fail(`${shown(render)} has no receipt for episode '${slug}'; render it again with 'npm run render -- --episode ${slug} --quality high -o ${shown(render)}'`);
}
if (receipt.video !== fileDigest(render)) {
  fail(`${shown(render)} has changed since it was rendered; render it again`);
}
syncBrand();
if (receipt.sources !== digestFiles(renderInputs(slug))) {
  fail(`the sources of '${slug}' have changed since ${shown(render)} was rendered (${receipt.rendered}); render it again`);
}
const html = readFileSync(paths.composition, "utf8");
const duration = compositionDuration(html);
const cuesFile = path.join(paths.narration, "cues.json");
if (!existsSync(cuesFile)) fail(`${shown(cuesFile)} does not exist; run 'npm run narrate -- ${slug}'`);
const narration = JSON.parse(readFileSync(cuesFile, "utf8"));
if (Math.abs(narration.duration - duration) > 0.001) {
  fail(`the narration lasts ${narration.duration}s and the composition ${duration}s; run 'npm run timeline -- ${slug}' and render again`);
}
let at;
try {
  at = posterTime(html, meta.poster);
} catch (error) {
  fail(`episodes/${slug}/episode.json: ${error.message}`);
}

// Written under temporary names first, then named by the cut of all three.
mkdirSync(siteMediaDir, { recursive: true });
const staging = Object.fromEntries(MEDIA.map((ext) => [ext, path.join(siteMediaDir, `${slug}.staging.${ext}`)]));
copyFileSync(render, staging.mp4);
writeFileSync(staging.vtt, toWebVtt(captionCues(narration.cues, narration.cues)));
const frame = spawnSync("ffmpeg", ["-hide_banner", "-loglevel", "error", "-y", "-ss", String(at), "-i", render, "-frames:v", "1", "-q:v", "2", staging.jpg], { encoding: "utf8" });
if (frame.status !== 0) {
  for (const file of Object.values(staging)) rmSync(file, { force: true });
  fail(`ffmpeg could not take the poster frame at ${at}s: ${frame.stderr || frame.error?.message}`);
}

const cut = cutOf(Object.fromEntries(MEDIA.map((ext) => [ext, fileDigest(staging[ext])])));
const names = mediaNames(slug, cut);
const current = new Set(Object.values(names));
const thisEpisode = new RegExp(`^${slug}-[0-9a-f]{12}\\.(${MEDIA.join("|")})$`);
for (const entry of readdirSync(siteMediaDir)) {
  if (thisEpisode.test(entry) && !current.has(entry)) {
    rmSync(path.join(siteMediaDir, entry), { force: true });
    console.log(`publish: removed the earlier cut's ${entry}`);
  }
}
for (const ext of MEDIA) renameSync(staging[ext], path.join(siteMediaDir, names[ext]));
const bytes = statSync(path.join(siteMediaDir, names.mp4)).size;

if (meta.published?.cut !== cut || meta.published?.bytes !== bytes) {
  writeEpisode(slug, { ...meta, published: { cut, bytes } });
}
const { page, original, updated } = companionUpdate(slug);
if (updated !== original) writeFileSync(page, updated);
console.log(
  `publish: '${slug}' is cut ${cut} (poster at ${at}s, ${(bytes / 1e6).toFixed(1)} MB) in ${fromRepo(siteMediaDir)}/; ` +
    `commit it with episodes/${slug}/episode.json and docs/videos/${slug}.md`,
);
