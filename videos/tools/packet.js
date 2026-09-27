#!/usr/bin/env node
// Writes an episode's review packet, the body of its pull request
// (tools/lib/packet.js), and the review copy of its video to attach to it:
//
//   npm run packet -- <slug> [--video <url>] [--base <ref>] [--previous <file>] [-o <file>]
//   npm run packet -- <slug> --review-copy [--max-mb 9.5]
//   npm run packet -- <slug> --upload [--repo <owner>/<name>]       (needs GH_TOKEN)
//
// The packet is written to renders/packets/<slug>.md unless -o names a file.
// --video puts the uploaded review copy in it. --base is where the published
// cut is (origin/main by default), to say what a re-cut changed and which
// words the voice has said before. --previous is the pull request's current
// body, whose record of the feedback acted on the new packet carries on.
//
// --review-copy writes renders/packets/<slug>-review.mp4, small enough to
// attach to a pull request (GitHub takes a video of up to 10 MB on every
// plan): the published cut itself when it fits, or else a 720p copy of it.
// --upload attaches the review copy to the repository, as the github-pr-media
// skill does, and prints its URL for --video. Nothing here is committed.
import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { restoreTakes } from "./lib/cache.js";
import { episodePaths, listEpisodes, rendersDir } from "./lib/layout.js";
import { parseScript } from "./lib/narration.js";
import { changedCues, ledgerOf, listenAt, packetMarkdown, sectionOf } from "./lib/packet.js";
import { companionPath, formatLength, mediaNames, readEpisode, repoRoot, siteMediaDir } from "./lib/publication.js";
import { episodesOn, isEpisodeItem, itemOf, pathOf, readPlan } from "./lib/series.js";

const fail = (message, code = 1) => {
  console.error(`packet: ${message}`);
  process.exit(code);
};
const argv = process.argv.slice(2);
const VALUED = new Set(["--video", "--base", "--previous", "-o", "--max-mb", "--repo"]);
const option = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : undefined);
const slug = argv.find((arg, i) => !arg.startsWith("-") && !VALUED.has(argv[i - 1]));
let paths;
try {
  paths = episodePaths(slug);
} catch (error) {
  fail(`${error.message}\nusage: npm run packet -- <slug> [--video <url>] [--base <ref>] [--previous <file>] [-o <file>] | --review-copy | --upload`, 2);
}
const packetsDir = path.join(rendersDir, "packets");
const reviewCopy = path.join(packetsDir, `${slug}-review.mp4`);
let meta;
try {
  meta = readEpisode(slug);
} catch (error) {
  fail(error.message);
}
const run = (command, args) => spawnSync(command, args, { encoding: "utf8", windowsHide: true, cwd: repoRoot });

// The video under review: the published cut, or else the latest high-quality render.
function videoUnderReview() {
  if (meta.published) {
    const published = path.join(siteMediaDir, mediaNames(slug, meta.published.cut).mp4);
    if (existsSync(published)) return published;
  }
  const render = path.join(rendersDir, `${slug}-high.mp4`);
  if (existsSync(render)) return render;
  return fail(`'${slug}' has neither a published cut nor renders/${slug}-high.mp4 to review`);
}

if (argv.includes("--review-copy")) {
  const limit = Math.round(Number(option("--max-mb") ?? 9.5) * 1e6);
  const source = videoUnderReview();
  mkdirSync(packetsDir, { recursive: true });
  if (statSync(source).size <= limit) {
    copyFileSync(source, reviewCopy);
  } else {
    const probe = run("ffprobe", ["-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", source]);
    const seconds = Number(probe.stdout);
    if (!(seconds > 0)) fail(`ffprobe could not read the length of ${source}`);
    // The whole file within the limit, less a margin for the container.
    let videoKbps = Math.floor((limit * 8 * 0.92) / seconds / 1000) - 96;
    for (let attempt = 1; ; attempt++) {
      const encoded = run("ffmpeg", [
        "-hide_banner", "-loglevel", "error", "-y", "-i", source,
        "-vf", "scale=-2:720", "-c:v", "libx264", "-preset", "medium",
        "-b:v", `${videoKbps}k`, "-maxrate", `${Math.round(videoKbps * 1.5)}k`, "-bufsize", `${videoKbps * 2}k`,
        "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", reviewCopy,
      ]);
      if (encoded.status !== 0) fail(`ffmpeg could not make the review copy: ${encoded.stderr || encoded.error?.message}`);
      if (statSync(reviewCopy).size <= limit) break;
      if (attempt === 3) fail(`the review copy is still over ${limit / 1e6} MB after ${attempt} attempts`);
      videoKbps = Math.floor(videoKbps * 0.85);
    }
  }
  console.log(`packet: renders/packets/${slug}-review.mp4 (${(statSync(reviewCopy).size / 1e6).toFixed(1)} MB), from ${path.relative(repoRoot, source).split(path.sep).join("/")}`);
  process.exit(0);
}

if (argv.includes("--upload")) {
  const token = process.env.GH_TOKEN;
  if (!token) fail("--upload needs GH_TOKEN, the token of the account the pull request is raised as (gh auth token --user <account>)");
  if (!existsSync(reviewCopy)) fail(`there is no review copy yet: make it with 'npm run packet -- ${slug} --review-copy'`);
  let repo = option("--repo");
  if (!repo) {
    const origin = run("git", ["remote", "get-url", "origin"]).stdout.trim();
    repo = /github\.com[:/]([^/]+\/[^/]+?)(?:\.git)?$/.exec(origin)?.[1];
    if (!repo) fail("could not tell the repository from the origin remote; pass --repo <owner>/<name>");
  }
  const headers = { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" };
  const info = await fetch(`https://api.github.com/repos/${repo}`, { headers });
  const id = info.ok ? (await info.json()).id : null;
  if (!Number.isInteger(id)) fail(`could not look up the repository ${repo} (${info.status})`);
  const url = new URL("https://uploads.github.com/user-attachments/assets");
  url.searchParams.set("name", path.basename(reviewCopy));
  url.searchParams.set("content_type", "video/mp4");
  url.searchParams.set("repository_id", String(id));
  const upload = await fetch(url, { method: "POST", headers: { ...headers, "Content-Type": "application/octet-stream" }, body: readFileSync(reviewCopy) });
  const answer = await upload.json().catch(() => null);
  if (!upload.ok || !/^https:\/\//.test(answer?.url ?? "")) fail(`the upload failed (${upload.status}): ${JSON.stringify(answer)}`);
  console.log(answer.url);
  process.exit(0);
}

// The packet.
const plan = readPlan();
const manifestFile = path.join(paths.narration, "cues.json");
if (!existsSync(manifestFile)) fail(`there is no narration of '${slug}' on this machine: run 'npm run narrate -- ${slug}'`);
const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
const script = readFileSync(paths.script, "utf8");
const item = itemOf(plan, meta.items?.at(-1)) ?? plan.items.find((candidate) => isEpisodeItem(candidate) && candidate.episode === slug);
if (!item) fail(`no item in series.json makes or re-cuts '${slug}'`);

// What the voice has said before: every other published episode, and this one as published.
const base = option("--base") ?? "origin/main";
const shown = run("git", ["show", `${base}:videos/episodes/${slug}/SCRIPT.md`]);
const previous = shown.status === 0 ? shown.stdout : null;
const others = listEpisodes()
  .filter((other) => other !== slug)
  .filter((other) => {
    try {
      return Boolean(readEpisode(other).published);
    } catch {
      return false;
    }
  })
  .map((other) => readFileSync(episodePaths(other).script, "utf8"));
const known = [...others, previous ?? ""].join("\n");

const numbered = manifest.cues.map((cue) => ({ index: cue.index, text: cue.text }));
const before = previous ? parseScript(previous, `${base}:SCRIPT.md`).cues : null;
const changed = before ? changedCues(numbered, before) : new Set();
const recut = before
  ? { changed: [...changed], removed: before.filter((cue) => !numbered.some((now) => now.text === cue.text)).length }
  : null;

const sceneTitles = new Map((manifest.scenes ?? []).map((scene) => [scene.id, scene.title]));
const cues = manifest.cues.map((cue) => ({ ...cue, sceneTitle: sceneTitles.get(cue.scene) ?? cue.scene }));
const takes = manifest.cues
  .map((cue) => {
    const name = path.basename(cue.file, ".wav");
    restoreTakes(paths, name);
    const dir = path.join(paths.takes, name);
    const numbers = existsSync(dir)
      ? readdirSync(dir)
          .map((file) => /^take(\d+)\.wav$/.exec(file))
          .filter(Boolean)
          .map((match) => Number(match[1]))
          .sort((a, b) => a - b)
      : [];
    return { cue: cue.index, takes: numbers };
  })
  .filter((entry) => entry.takes.length > 0);

// The one-line idea: the companion page's first sentence (series.md, "How an episode is made").
const page = companionPath(slug);
const intro = existsSync(page)
  ? readFileSync(page, "utf8")
      .replace(/\r\n/g, "\n")
      .split("\n\n")
      .find((block) => block.trim() && !block.startsWith("#") && !block.startsWith("<!--"))
  : null;
const idea = intro ? /^[\s\S]*?[.!?](?=\s|$)/.exec(intro.replace(/\s+/g, " ").trim())?.[0] : null;

const onPath = isEpisodeItem(item) && item.path !== "front-door" ? episodesOn(plan, item.path) : null;
const bytes = meta.published?.bytes ?? (existsSync(path.join(rendersDir, `${slug}-high.mp4`)) ? statSync(path.join(rendersDir, `${slug}-high.mp4`)).size : null);
const mastered = manifest.loudness?.mastered;
const body = packetMarkdown({
  item,
  pathTitle: onPath ? pathOf(plan, item.path).title : null,
  place: onPath ? `${item.order} of ${onPath.length}` : null,
  step: `${plan.items.indexOf(item) + 1} of ${plan.items.length}`,
  idea: isEpisodeItem(item) ? idea : `A re-cut of ${item.recut}, the episode '${slug}'.`,
  facts: {
    length: formatLength(manifest.duration),
    size: bytes ? `${(bytes / 1e6).toFixed(1)} MB` : "not rendered",
    loudness: mastered ? `${mastered.integrated} LUFS, ${mastered.truePeak} dBTP` : "not measured",
    cut: meta.published?.cut ?? "not published",
  },
  video: option("--video") ?? null,
  rows: listenAt(manifest, { known, changed }),
  cues,
  takes,
  sources: sectionOf(script, "Sources"),
  recut,
  ledger: option("--previous") ? ledgerOf(readFileSync(option("--previous"), "utf8")) : null,
});

const out = option("-o") ? path.resolve(option("-o")) : path.join(packetsDir, `${slug}.md`);
mkdirSync(path.dirname(out), { recursive: true });
writeFileSync(out, body);
if (body.length > 60000) console.warn(`packet: the packet is ${body.length} characters; a pull request body takes at most 65,536`);
console.log(`packet: ${path.relative(process.cwd(), out)} for ${item.code} (${item.title}), ${body.length} characters`);
