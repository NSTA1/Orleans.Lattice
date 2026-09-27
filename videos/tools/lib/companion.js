// What an episode's companion page (docs/videos/<slug>.md) should say. Three
// parts of the page are written from the episode, between markers:
//
//   <!-- video:begin ... --> ... <!-- video:end -->
//       what the site needs to list and play the video, from episode.json and
//       the stamped composition, and a note for readers on github.com
//       (see publication.js, videoBlock)
//   <!-- transcript:begin --> ... <!-- transcript:end -->
//       the narration, word for word as SCRIPT.md has it
//   <!-- where-next:begin --> ... <!-- where-next:end -->
//       where to go next, from the series plan (series.js, whereNextMarkdown):
//       the next episode on its path and the deep dive it leads to, linked once
//       they are published, and the pages it introduces
//
// Everything else on the page is written by hand.
import { existsSync, readFileSync } from "node:fs";
import { episodePaths } from "./layout.js";
import { parseScript, transcriptMarkdown } from "./narration.js";
import { companionPath, compositionDuration, findVideoBlocks, formatLength, readEpisode, videoBlock } from "./publication.js";
import { isEpisodeItem, publishedEpisodes, readEpisodes, readPlan, whereNextMarkdown } from "./series.js";

const TRANSCRIPT_BEGIN = "<!-- transcript:begin -->";
const TRANSCRIPT_END = "<!-- transcript:end -->";
const WHERE_NEXT_BEGIN = "<!-- where-next:begin -->";
const WHERE_NEXT_END = "<!-- where-next:end -->";

// Replaces what lies between two markers with `body`, set off by blank lines.
function between(text, begin, end, body, newline, what) {
  const from = text.indexOf(begin);
  const to = text.indexOf(end);
  if (from < 0 || to < from) throw new Error(`has no ${begin} ... ${end} block for ${what}`);
  return text.slice(0, from + begin.length) + newline + newline + body + newline + newline + text.slice(to);
}

/**
 * An episode's companion page as it is (`original`) and as the episode says
 * it should be (`updated`). Throws when the page, the episode's metadata, its
 * composition or its place in the series plan is not in a state to write from.
 * `plan` and `published` default to the plan and the episodes as they are.
 */
export function companionUpdate(slug, { plan = readPlan(), published = publishedEpisodes(readEpisodes()) } = {}) {
  const page = companionPath(slug);
  const original = readFileSync(page, "utf8");
  const newline = original.includes("\r\n") ? "\r\n" : "\n";
  const { composition, script } = episodePaths(slug);
  const meta = readEpisode(slug);
  if (!existsSync(composition)) throw new Error(`episodes/${slug}/composition.html does not exist`);
  const length = formatLength(compositionDuration(readFileSync(composition, "utf8")));
  const item = plan.items.find((candidate) => isEpisodeItem(candidate) && candidate.episode === slug);
  if (!item) throw new Error(`no item in videos/series.json makes the episode '${slug}'`);

  let updated = original;
  const blocks = findVideoBlocks(updated);
  if (blocks.length !== 1 || blocks[0].end < 0) {
    throw new Error(`needs exactly one <!-- video:begin ... --> ... <!-- video:end --> block, and has ${blocks.length} complete or partial`);
  }
  const [block] = blocks;
  const video = videoBlock({ slug, path: meta.path, order: meta.order, length, published: meta.published }, newline);
  updated = updated.slice(0, block.start) + video + updated.slice(block.end);

  const { cues } = parseScript(readFileSync(script, "utf8"), `episodes/${slug}/SCRIPT.md`);
  updated = between(updated, TRANSCRIPT_BEGIN, TRANSCRIPT_END, transcriptMarkdown(cues).replace(/\n/g, newline), newline, "the transcript");
  const whereNext = whereNextMarkdown(plan, item.code, published).replace(/\n/g, newline);
  updated = between(updated, WHERE_NEXT_BEGIN, WHERE_NEXT_END, whereNext, newline, "where to go next");

  return { page, original, updated };
}
