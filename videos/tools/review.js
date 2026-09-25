#!/usr/bin/env node
// Writes a local review page for an episode's render: the video in a native
// player with its WebVTT captions, the numbers that matter for hosting, and
// the transcript - the shape the companion page's player will take. The page
// is output (renders/review/<slug>.html) and never committed.
//
//   npm run review -- <slug> [--video renders/<file>.mp4]
//   npm run review -- <slug> --audio     the narration alone, to approve by ear before rendering
//
// Serve the workspace to watch it, for example:
//   python -m http.server 8765    then open http://localhost:8765/renders/review/<slug>.html
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths, rendersDir } from "./lib/layout.js";
import { listeningPage } from "./lib/listening.js";
import { parseScript } from "./lib/narration.js";
import { escapeHtml } from "./lib/snippets.js";

const args = process.argv.slice(2);
let paths;
try {
  paths = episodePaths(args.find((arg, i) => !arg.startsWith("--") && args[i - 1] !== "--video"));
} catch (error) {
  console.error(`review: ${error.message}`);
  console.error("usage: npm run review -- <episode-slug> [--video renders/<file>.mp4] [--audio]");
  process.exit(2);
}
const { slug } = paths;
const reviewDir = path.join(rendersDir, "review");
const fromReview = (file) => path.relative(reviewDir, file).split(path.sep).join("/");
// A re-render keeps its file name, so the page names the version it was
// written for: a browser that has the previous render cached fetches this one.
const versioned = (file) => `${fromReview(file)}?v=${Math.round(statSync(file).mtimeMs)}`;
const title = /^#\s+(.+?)(?:\s+-\s+script)?\s*$/m.exec(readFileSync(paths.script, "utf8"))?.[1] ?? slug;
const brandSheets = ["tokens.css", "fonts.css"].map((file) => fromReview(path.join(workspaceRoot, "shared", "brand", "site", file)));

if (args.includes("--audio")) {
  const manifestFile = path.join(paths.narration, "cues.json");
  const narration = path.join(paths.narration, "narration.wav");
  if (!existsSync(manifestFile) || !existsSync(narration)) {
    console.error(`review: '${slug}' has no mastered narration; run 'npm run narrate -- ${slug}' first`);
    process.exit(1);
  }
  const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
  mkdirSync(reviewDir, { recursive: true });
  writeFileSync(path.join(reviewDir, `${slug}-narration.html`), listeningPage({ title, manifest, audioSrc: versioned(narration), stylesheets: brandSheets }));
  console.log(`review: renders/review/${slug}-narration.html, the narration alone (${manifest.cues.length} cues)`);
  process.exit(0);
}

const chosen = args.includes("--video") ? args[args.indexOf("--video") + 1] : null;
const candidates = chosen ? [chosen] : [`renders/${slug}-high.mp4`, `renders/${slug}.mp4`, `renders/${slug}-draft.mp4`];
const video = candidates.map((file) => path.join(workspaceRoot, file)).find((file) => existsSync(file));
if (!video) {
  console.error(`review: no render of '${slug}' found (looked for ${candidates.join(", ")}); render it first`);
  process.exit(1);
}

const manifestFile = path.join(paths.narration, "cues.json");
const manifest = existsSync(manifestFile) ? JSON.parse(readFileSync(manifestFile, "utf8")) : null;
const { cues } = parseScript(readFileSync(paths.script, "utf8"), `episodes/${slug}/SCRIPT.md`);

// What the delivered file measures, from the file itself.
const probe = spawnSync("ffprobe", ["-v", "error", "-show_entries", "format=duration,size,bit_rate:stream=codec_name,width,height", "-of", "json", video], {
  encoding: "utf8",
});
const probed = probe.status === 0 ? JSON.parse(probe.stdout) : null;
const loudness = spawnSync("ffmpeg", ["-hide_banner", "-nostats", "-i", video, "-vn", "-af", "ebur128=peak=true", "-f", "null", "-"], { encoding: "utf8" });
const measured = /Integrated loudness:\s*I:\s*(-?[\d.]+)\s*LUFS[\s\S]*True peak:\s*Peak:\s*(-?[\d.]+)\s*dBFS/.exec(
  loudness.stderr.slice(loudness.stderr.lastIndexOf("Summary:")),
);

const megabytes = (statSync(video).size / 1e6).toFixed(1);
const duration = probed ? Number(probed.format.duration) : manifest?.duration;
const facts = [
  ["File", path.basename(video)],
  ["Size", `${megabytes} MB`],
  ["Length", duration ? `${Math.floor(duration / 60)}:${(duration % 60).toFixed(1).padStart(4, "0")}` : "unknown"],
  ["Average bit rate", probed ? `${Math.round(Number(probed.format.bit_rate) / 1000)} kbit/s` : "unknown"],
  ["Loudness as delivered", measured ? `${measured[1]} LUFS, true peak ${measured[2]} dBTP` : "not measured (needs ffmpeg)"],
  ["Megabytes per minute", duration ? (Number(megabytes) / (duration / 60)).toFixed(2) : "unknown"],
];

let scene;
const transcript = [];
for (const cue of cues) {
  if (cue.scene !== scene) {
    scene = cue.scene;
    transcript.push(`<h3>${escapeHtml(scene ?? "")}</h3>`);
  }
  transcript.push(`<p>${escapeHtml(cue.text)}</p>`);
}

const page = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)} - review</title>
    <link rel="stylesheet" href="${fromReview(path.join(workspaceRoot, "shared", "brand", "site", "tokens.css"))}" />
    <link rel="stylesheet" href="${fromReview(path.join(workspaceRoot, "shared", "brand", "site", "fonts.css"))}" />
    <style>
      body { margin: 0; background: var(--lt-surface); color: var(--lt-ink); font-family: var(--lv-font-sans); line-height: 1.6; }
      main { max-width: 1280px; margin: 0 auto; padding: 32px 24px 64px; }
      h1 { margin: 0 0 4px; font-size: var(--lt-text-2xl); font-weight: var(--lt-weight-display); letter-spacing: -0.02em; }
      .kicker { margin: 0 0 20px; color: var(--lt-ink-3); font-weight: var(--lt-weight-label); }
      video { display: block; width: 100%; height: auto; border: 1px solid var(--lt-rule); border-radius: var(--lt-radius-md); background: var(--lt-surface); }
      table { margin: 24px 0 8px; border-collapse: collapse; border-top: 1.5px solid var(--lt-ink); border-bottom: 1.5px solid var(--lt-ink); }
      th, td { padding: 6px 28px 6px 0; text-align: left; border-bottom: 1px solid var(--lt-rule); }
      th { color: var(--lt-ink-2); font-weight: var(--lt-weight-label); }
      td { font-family: var(--lv-font-mono); }
      h2 { margin: 40px 0 8px; padding-top: 16px; border-top: 1px solid var(--lt-rule); font-size: var(--lt-text-xl); }
      h3 { margin: 24px 0 4px; font-size: var(--lt-text-lg); }
      p { margin: 0 0 10px; max-width: 72ch; }
    </style>
  </head>
  <body>
    <main>
      <p class="kicker">Orleans.Lattice video series - local review</p>
      <h1>${escapeHtml(title)}</h1>
      <video controls preload="metadata" src="${versioned(video)}">
        <track kind="captions" srclang="en" label="English" default src="${versioned(path.join(paths.narration, "narration.vtt"))}" />
      </video>
      <table>
        ${facts.map(([name, value]) => `<tr><th>${escapeHtml(name)}</th><td>${escapeHtml(String(value))}</td></tr>`).join("\n        ")}
      </table>
      <h2>Transcript</h2>
      ${transcript.join("\n      ")}
    </main>
  </body>
</html>
`;

mkdirSync(path.join(rendersDir, "review"), { recursive: true });
const out = path.join(rendersDir, "review", `${slug}.html`);
writeFileSync(out, page);
console.log(`review: renders/review/${slug}.html, for ${path.basename(video)} (${megabytes} MB)`);
