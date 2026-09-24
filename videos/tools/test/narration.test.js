import assert from "node:assert/strict";
import { test } from "node:test";
import {
  buildTimeline,
  captionCues,
  formatTimestamp,
  parseScript,
  splitCaption,
  toWebVtt,
} from "../lib/narration.js";

const script = [
  "# Hello, Lattice - script",
  "",
  "Status: draft",
  "",
  "## Narration",
  "",
  "### Opening",
  "Orleans.Lattice is a platform",
  "for durable state.",
  "",
  "<!-- beat: the join diagram assembles -->",
  "Writes converge.",
  "",
  "### Close",
  "That is the whole idea.",
  "",
  "## Notes",
  "",
  "Not spoken.",
].join("\n");

test("only the Narration section is spoken, one cue per paragraph, labelled by scene", () => {
  assert.deepEqual(parseScript(script), [
    { text: "Orleans.Lattice is a platform for durable state.", scene: "Opening" },
    { text: "Writes converge.", scene: "Opening" },
    { text: "That is the whole idea.", scene: "Close" },
  ]);
});

test("a script without a Narration section is rejected", () => {
  assert.throws(() => parseScript("# Title\n\nText", "x.md"), /no '## Narration' section/);
});

test("a Narration section with nothing to say is rejected", () => {
  assert.throws(() => parseScript("## Narration\n\n<!-- todo -->\n\n## Notes", "x.md"), /has no cues/);
});

test("cues are laid end to end after the lead-in, separated by the gap", () => {
  assert.deepEqual(buildTimeline([2, 1.5], { leadIn: 0.5, gap: 0.25 }), [
    { start: 0.5, end: 2.5 },
    { start: 2.75, end: 4.25 },
  ]);
});

test("captions break at sentence ends, not at a full stop inside a token", () => {
  assert.deepEqual(splitCaption("Orleans.Lattice is local-first. It scales out."), [
    "Orleans.Lattice is local-first.",
    "It scales out.",
  ]);
});

test("a long sentence breaks at word boundaries within the limit", () => {
  const pieces = splitCaption("one two three four five six", 9);
  assert.deepEqual(pieces, ["one two", "three", "four five", "six"]);
  for (const piece of pieces) assert.ok(piece.length <= 9);
});

test("caption pieces share their cue's time in proportion to their length", () => {
  const captions = captionCues([{ text: "Aaaa. Bb.", scene: null }], [{ start: 1, end: 4 }]);
  assert.deepEqual(captions, [
    { start: 1, end: 2.875, text: "Aaaa." },
    { start: 2.875, end: 4, text: "Bb." },
  ]);
});

test("timestamps are hh:mm:ss.mmm", () => {
  assert.equal(formatTimestamp(0), "00:00:00.000");
  assert.equal(formatTimestamp(3723.4567), "01:02:03.457");
});

test("WebVTT output has a header, numbered cues and escaped text", () => {
  assert.equal(
    toWebVtt([{ start: 0.5, end: 1, text: "a < b" }]),
    "WEBVTT\n\n1\n00:00:00.500 --> 00:00:01.000\na &lt; b\n",
  );
});
