// Publishing an episode: what its finished files are called, where they are
// kept, and the block its companion page carries so that the documentation
// site can list and play it (series.md, "Hosting - decided").
//
// The finished files - the video, its captions and its poster - are
// committed to docs-site/media/, named by a cut, a digest of the three, and
// the site plays them from its own origin. The companion page pins the cut,
// so a page and the video it plays always come from the same commit, and a
// new cut is a new file name, so nothing cached under an old name is stale.
import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./hyperframes.js";
import { episodePaths } from "./layout.js";
import { openingTags } from "./timeline.js";

/** The published documentation site. */
export const SITE_URL = "https://nsta1.github.io/Orleans.Lattice/";

/** The series' paths, in the order the site lists them (series.md, "Shape"). */
export const PATHS = Object.freeze(["front-door", "build", "evaluate", "operate", "secure", "how-it-works"]);

/** The three files of a published cut: the video, its captions, its poster. */
export const MEDIA = Object.freeze(["mp4", "vtt", "jpg"]);

/** The repository root. */
export const repoRoot = path.resolve(workspaceRoot, "..");

/** The folder of companion pages. */
export const companionsDir = path.join(repoRoot, "docs", "videos");

/** Where the published files are committed, and where the site's build reads them. */
export const siteMediaDir = path.join(repoRoot, "docs-site", "media");

/** An episode's companion page. */
export function companionPath(slug) {
  return path.join(companionsDir, `${slug}.md`);
}

/** The file names of a published cut, by extension. */
export function mediaNames(slug, cut) {
  return Object.fromEntries(MEDIA.map((ext) => [ext, `${slug}-${cut}.${ext}`]));
}

/** True for the name of a published file: <slug>-<cut>.<mp4|vtt|jpg>. */
export function isMediaName(name) {
  return new RegExp(`^[a-z0-9]+(?:-[a-z0-9]+)*-[0-9a-f]{12}\\.(${MEDIA.join("|")})$`).test(name);
}

const CUT = /^[0-9a-f]{12}$/;

/** True for a cut: 12 lower-case hex digits. */
export function isCut(value) {
  return typeof value === "string" && CUT.test(value);
}

/** The SHA-256 of a file, in hex. */
export function fileDigest(file) {
  return createHash("sha256").update(readFileSync(file)).digest("hex");
}

/**
 * The cut of a finished set: the first 12 hex digits of a SHA-256 over the
 * three files' own SHA-256 digests, so a change to any of them is a new cut.
 */
export function cutOf(digests) {
  const text = MEDIA.map((ext) => `${ext}:${digests[ext]}`).join("\n");
  return createHash("sha256").update(text).digest("hex").slice(0, 12);
}

const EPISODE_FIELDS = new Set(["path", "order", "poster", "published"]);
const POSTER_FIELDS = new Set(["scene", "beat", "offset"]);

/**
 * What is wrong with an episode's metadata (episodes/<slug>/episode.json),
 * if anything:
 *
 *   path       the series path the episode is on, one of PATHS
 *   order      its place on that path, from 1
 *   poster     the moment its poster shows: a scene id, then optionally a beat
 *              of that scene (from 0) and an offset in seconds from it
 *   published  written by `npm run publish`: the cut the companion page pins,
 *              and the size of its video in bytes
 */
export function episodeProblems(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return ["expected a JSON object"];
  const problems = [];
  for (const key of Object.keys(meta)) {
    if (!EPISODE_FIELDS.has(key)) problems.push(`unknown field '${key}'`);
  }
  if (!PATHS.includes(meta.path)) problems.push(`path must be one of ${PATHS.join(", ")}`);
  if (!Number.isInteger(meta.order) || meta.order < 1) problems.push("order must be a whole number from 1");
  const poster = meta.poster;
  if (!poster || typeof poster !== "object" || typeof poster.scene !== "string" || poster.scene === "") {
    problems.push("poster.scene must name a scene of the composition");
  } else {
    for (const key of Object.keys(poster)) {
      if (!POSTER_FIELDS.has(key)) problems.push(`unknown field 'poster.${key}'`);
    }
    if (poster.beat !== undefined && (!Number.isInteger(poster.beat) || poster.beat < 0)) {
      problems.push("poster.beat must be a whole number from 0");
    }
    if (poster.offset !== undefined && !Number.isFinite(poster.offset)) {
      problems.push("poster.offset must be a number of seconds");
    }
  }
  if (meta.published !== undefined) {
    const published = meta.published ?? {};
    if (!isCut(published.cut)) problems.push("published.cut must be 12 lower-case hex digits");
    if (!Number.isInteger(published.bytes) || published.bytes <= 0) {
      problems.push("published.bytes must be the size of the video in bytes");
    }
  }
  return problems;
}

/** Reads and checks an episode's metadata; throws naming every problem. */
export function readEpisode(slug) {
  const file = episodePaths(slug).metadata;
  const source = `episodes/${slug}/episode.json`;
  if (!existsSync(file)) throw new Error(`${source} does not exist`);
  let meta;
  try {
    meta = JSON.parse(readFileSync(file, "utf8"));
  } catch (error) {
    throw new Error(`${source}: ${error.message}`);
  }
  const problems = episodeProblems(meta);
  if (problems.length > 0) throw new Error(`${source}: ${problems.join("; ")}`);
  return meta;
}

/** Writes an episode's metadata, after checking it. */
export function writeEpisode(slug, meta) {
  const problems = episodeProblems(meta);
  if (problems.length > 0) throw new Error(`episodes/${slug}/episode.json: ${problems.join("; ")}`);
  writeFileSync(episodePaths(slug).metadata, `${JSON.stringify(meta, null, 2)}\n`);
}

/** The root clip of a composition and its stamped duration in seconds. */
function compositionRoot(html) {
  const tags = openingTags(html);
  const root = tags.find((tag) => tag.get("data-composition-id") !== undefined);
  const duration = Number(root?.get("data-duration"));
  if (!(duration > 0)) throw new Error("the composition is not stamped with a duration; run 'npm run timeline -- <slug>'");
  return { tags, duration };
}

/** The stamped duration of a composition, in seconds. */
export function compositionDuration(html) {
  return compositionRoot(html).duration;
}

/**
 * The moment a poster shows, in seconds from the start of a stamped
 * composition: the named scene's start, plus the start of the named beat
 * within it, plus the offset.
 */
export function posterTime(html, poster) {
  const { tags, duration } = compositionRoot(html);
  const host = tags.find((tag) => tag.get("data-scene") === poster.scene && tag.get("data-composition-id") !== undefined);
  if (!host) throw new Error(`the poster's scene '${poster.scene}' is not in the composition`);
  let at = Number(host.get("data-start"));
  if (poster.beat !== undefined) {
    const values = JSON.parse(host.get("data-variable-values") ?? "{}");
    const beats = String(values.beats ?? "")
      .split(",")
      .filter(Boolean)
      .map(Number);
    if (poster.beat >= beats.length) {
      throw new Error(`scene '${poster.scene}' has ${beats.length} stamped beat(s), so it has no beat ${poster.beat}`);
    }
    at += beats[poster.beat];
  }
  at = Math.round((at + (poster.offset ?? 0)) * 1000) / 1000;
  if (!(at >= 0 && at < duration)) {
    throw new Error(`the poster falls at ${at}s, outside the episode (0 to ${duration}s)`);
  }
  return at;
}

/** An episode's length as the site shows it: minutes and seconds, "2:52". */
export function formatLength(seconds) {
  const total = Math.round(seconds);
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, "0")}`;
}

const BEGIN = /<!--\s*video:begin((?:\s+[a-z]+="[^"]*")*)\s*-->/g;
const END = "<!-- video:end -->";

/**
 * A companion page's video block. The begin marker carries what the site
 * needs to list and play the episode:
 *
 *   episode  the slug, which also names the page
 *   path     one of PATHS
 *   order    its place on that path
 *   length   "m:ss"
 *   cut      the published cut, absent until the episode is published
 *
 * Between the markers is a note for readers on github.com, linking to the
 * site's page and to the committed video. The site replaces the whole block,
 * markers included, with its player.
 */
export function videoBlock({ slug, path: seriesPath, order, length, published }, newline = "\n") {
  const attributes = [
    ["episode", slug],
    ["path", seriesPath],
    ["order", String(order)],
    ["length", length],
  ];
  if (published) attributes.push(["cut", published.cut]);
  const begin = `<!-- video:begin ${attributes.map(([key, value]) => `${key}="${value}"`).join(" ")} -->`;
  const note = published
    ? [
        "> [!NOTE]",
        `> Watch it on the [documentation site](${SITE_URL}docs/videos/${slug}.html),`,
        `> or [download it](../../docs-site/media/${mediaNames(slug, published.cut).mp4})`,
        `> (MP4, ${length}, ${(published.bytes / 1e6).toFixed(1)} MB).`,
      ]
    : ["> [!NOTE]", "> The video is not published yet."];
  return [begin, "", ...note, "", END].join(newline);
}

/**
 * The video blocks of a companion page, in order: where each starts and ends
 * (end is -1 when its end marker is missing) and its begin marker's
 * attributes. A begin marker in any other form is an error, never silently
 * skipped.
 */
export function findVideoBlocks(markdown) {
  const blocks = [];
  for (const match of markdown.matchAll(BEGIN)) {
    const close = markdown.indexOf(END, match.index);
    const attributes = Object.fromEntries([...match[1].matchAll(/([a-z]+)="([^"]*)"/g)].map((m) => [m[1], m[2]]));
    blocks.push({ start: match.index, end: close < 0 ? -1 : close + END.length, attributes });
  }
  const markers = (markdown.match(/video:begin/g) ?? []).length;
  if (markers !== blocks.length) {
    throw new Error(`${markers - blocks.length} video:begin marker(s) are not in the form <!-- video:begin key="value" ... -->`);
  }
  return blocks;
}
