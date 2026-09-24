// What an episode's companion page (docs/videos/<slug>.md) should say. Two
// parts of the page are written from the episode, between markers:
//
//   <!-- video:begin ... --> ... <!-- video:end -->
//       what the site needs to list and play the video, from episode.json and
//       the stamped composition, and a note for readers on github.com
//       (see publication.js, videoBlock)
//   <!-- transcript:begin --> ... <!-- transcript:end -->
//       the narration, word for word as SCRIPT.md has it
//
// Everything else on the page is written by hand.
import { existsSync, readFileSync } from "node:fs";
import { episodePaths } from "./layout.js";
import { parseScript, transcriptMarkdown } from "./narration.js";
import { companionPath, compositionDuration, findVideoBlocks, formatLength, readEpisode, videoBlock } from "./publication.js";

const TRANSCRIPT_BEGIN = "<!-- transcript:begin -->";
const TRANSCRIPT_END = "<!-- transcript:end -->";

/**
 * An episode's companion page as it is (`original`) and as the episode says
 * it should be (`updated`). Throws when the page, the episode's metadata or
 * its composition is not in a state to write from.
 */
export function companionUpdate(slug) {
  const page = companionPath(slug);
  const original = readFileSync(page, "utf8");
  const newline = original.includes("\r\n") ? "\r\n" : "\n";
  const { composition, script } = episodePaths(slug);
  const meta = readEpisode(slug);
  if (!existsSync(composition)) throw new Error(`episodes/${slug}/composition.html does not exist`);
  const length = formatLength(compositionDuration(readFileSync(composition, "utf8")));

  let updated = original;
  const blocks = findVideoBlocks(updated);
  if (blocks.length !== 1 || blocks[0].end < 0) {
    throw new Error(`needs exactly one <!-- video:begin ... --> ... <!-- video:end --> block, and has ${blocks.length} complete or partial`);
  }
  const [block] = blocks;
  const video = videoBlock({ slug, path: meta.path, order: meta.order, length, published: meta.published }, newline);
  updated = updated.slice(0, block.start) + video + updated.slice(block.end);

  const begin = updated.indexOf(TRANSCRIPT_BEGIN);
  const end = updated.indexOf(TRANSCRIPT_END);
  if (begin < 0 || end < begin) throw new Error(`has no ${TRANSCRIPT_BEGIN} ... ${TRANSCRIPT_END} block for the transcript`);
  const { cues } = parseScript(readFileSync(script, "utf8"), `episodes/${slug}/SCRIPT.md`);
  const transcript = transcriptMarkdown(cues).replace(/\n/g, newline);
  updated = updated.slice(0, begin + TRANSCRIPT_BEGIN.length) + newline + newline + transcript + newline + newline + updated.slice(end);

  return { page, original, updated };
}
