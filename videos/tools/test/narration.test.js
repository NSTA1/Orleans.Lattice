import assert from "node:assert/strict";
import { test } from "node:test";
import {
  buildScenes,
  buildTimeline,
  captionCues,
  formatTimestamp,
  parseScript,
  sceneId,
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
  "<!-- pause 1.0 -->",
  "",
  "### Opening",
  "Orleans.Lattice is a platform",
  "for durable state.",
  "",
  "<!-- beat: the join diagram assembles -->",
  "Writes converge.",
  "",
  "<!-- pause 0.8 -->",
  "",
  "### The close",
  "That is the whole idea.",
  "",
  "<!-- pause 2.5 -->",
  "",
  "## Notes",
  "",
  "Not spoken.",
].join("\n");

test("only the Narration section is spoken, one cue per paragraph, labelled by scene", () => {
  assert.deepEqual(parseScript(script), {
    cues: [
      { text: "Orleans.Lattice is a platform for durable state.", scene: "Opening", pause: 1 },
      { text: "Writes converge.", scene: "Opening", pause: 0 },
      { text: "That is the whole idea.", scene: "The close", pause: 0.8 },
    ],
    tail: 2.5,
  });
});

test("a pause directive is silence before the next cue; direction notes are never spoken", () => {
  const { cues } = parseScript("## Narration\n\nOne.\n\n<!-- pause 0.5 --> <!-- pause 0.25 -->\n\n<!-- a note -->\nTwo.");
  assert.deepEqual(cues.map((cue) => [cue.text, cue.pause]), [
    ["One.", 0],
    ["Two.", 0.75],
  ]);
});

test("a script without a Narration section is rejected", () => {
  assert.throws(() => parseScript("# Title\n\nText", "x.md"), /no '## Narration' section/);
});

test("a Narration section with nothing to say is rejected", () => {
  assert.throws(() => parseScript("## Narration\n\n<!-- todo -->\n\n## Notes", "x.md"), /has no cues/);
});

test("a scene's id is its title in lower-case words joined by hyphens", () => {
  assert.equal(sceneId("Local to Global"), "local-to-global");
  assert.equal(sceneId("  What it is?  "), "what-it-is");
});

const cues = [
  { text: "a", scene: "One", pause: 0 },
  { text: "b", scene: "One", pause: 0.5 },
  { text: "c", scene: "Two", pause: 0 },
];

test("cues follow the lead-in, a cue gap within a scene, a scene gap between scenes, and their own pauses", () => {
  assert.deepEqual(buildTimeline(cues, [2, 1.5, 1], { leadIn: 0.5, cueGap: 0.25, sceneGap: 1, tail: 2 }), {
    timeline: [
      { start: 0.5, end: 2.5 },
      { start: 3.25, end: 4.75 },
      { start: 5.75, end: 6.75 },
    ],
    duration: 8.75,
  });
});

test("the timeline needs one duration per cue", () => {
  assert.throws(() => buildTimeline(cues, [1, 2]), /3 cue\(s\) but 2 duration\(s\)/);
});

test("a scene opens ahead of its first cue, never before the previous scene falls silent, and lasts until the next", () => {
  const { timeline, duration } = buildTimeline(cues, [2, 1.5, 1], { leadIn: 0.5, cueGap: 0.25, sceneGap: 1, tail: 2 });
  assert.deepEqual(buildScenes(cues, timeline, duration, { lead: 0.6 }), [
    { id: "one", title: "One", cues: [0, 1], start: 0, end: 5.15, beats: [0.5, 3.25] },
    { id: "two", title: "Two", cues: [2], start: 5.15, end: 8.75, beats: [0.6] },
  ]);
  const tight = buildScenes(cues, timeline, duration, { lead: 5 });
  assert.equal(tight[1].start, 4.75, "the second scene waits for the first scene's last cue to end");
});

test("a scene heading used twice is rejected", () => {
  const split = [...cues, { text: "d", scene: "One", pause: 0 }];
  const { timeline, duration } = buildTimeline(split, [1, 1, 1, 1]);
  assert.throws(() => buildScenes(split, timeline, duration), /scene 'One' appears twice/);
});

test("captions break at sentence ends, not at a full stop inside a token", () => {
  assert.deepEqual(splitCaption("Orleans.Lattice is local-first. It scales out."), [
    "Orleans.Lattice is local-first.",
    "It scales out.",
  ]);
});

test("a long sentence breaks at word boundaries into the fewest captions that fit", () => {
  const pieces = splitCaption("one two three four five six", { width: 9, lines: 1 });
  assert.deepEqual(pieces, ["one two", "three", "four", "five six"]);
  for (const piece of pieces) assert.ok(piece.length <= 9);
});

test("a caption has at most two lines of 42 characters, broken at a clause and never after a word that binds forward", () => {
  const text =
    "Almost every application has to remember things: what is in a basket, who may open a document, how often a page is viewed.";
  const captions = splitCaption(text);
  assert.deepEqual(captions, [
    "Almost every application has\nto remember things: what is in a basket,",
    "who may open a document,\nhow often a page is viewed.",
  ]);
  for (const caption of captions) {
    const lines = caption.split("\n");
    assert.ok(lines.length <= 2, caption);
    for (const line of lines) {
      assert.ok(line.length <= 42, line);
      assert.doesNotMatch(line, /\s(a|the|of|to)$/, line);
    }
  }
  assert.equal(captions.join(" ").replace(/\n/g, " "), text, "nothing is lost or reordered");
});

test("a caption that fits on one line stays on one line", () => {
  assert.deepEqual(splitCaption("Developers call that memory state."), ["Developers call that memory state."]);
});

test("a word longer than a line gets a line of its own", () => {
  assert.deepEqual(splitCaption("see Orleans.Lattice.Api.Replication.Grpc now", { width: 12, lines: 2 }), [
    "see\nOrleans.Lattice.Api.Replication.Grpc",
    "now",
  ]);
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
