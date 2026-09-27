import assert from "node:assert/strict";
import { test } from "node:test";
import { changedCues, clock, ledgerOf, listenAt, newWords, packetMarkdown, riskyWords, sectionOf } from "../lib/packet.js";

test("times are minutes and seconds", () => {
  assert.equal(clock(0), "0:00");
  assert.equal(clock(65.9), "1:05");
  assert.equal(clock(181), "3:01");
});

test("the words a voice may say wrongly are names, identifiers, initialisms, compounds and long words", () => {
  assert.deepEqual(riskyWords("Your code resolves ILattice, and calls GetAsync on Orleans.Lattice's CRDTs."), [
    "ILattice",
    "GetAsync",
    "Orleans.Lattice",
    "CRDTs",
  ]);
  assert.deepEqual(riskyWords("A key-value store, linearizable by default, on .NET 10."), ["key-value", "linearizable", "NET"]);
  assert.deepEqual(riskyWords("It keeps the working memory inside."), []);
});

test("a word is new when nothing the voice has said contains it", () => {
  assert.deepEqual(newWords("Resolve ILattice and call GetAsync.", "Your code resolves ILattice, and calls it."), ["GetAsync"]);
  assert.deepEqual(newWords("ORLEANS.LATTICE", "Orleans.Lattice"), [], "case does not make a word new");
});

const manifest = {
  cues: [
    { index: 1, start: 0.5, text: "Hello, Lattice.", check: { attempt: 1, verified: true } },
    { index: 2, start: 4.2, text: "Call GetAsync.", check: { attempt: 3, verified: true } },
    { index: 3, start: 9, text: "Merges are idempotent.", check: { attempt: 2, verified: false, picked: true } },
    { index: 4, start: 61, text: "It converges.", check: { attempt: 4, verified: false, differences: { "base.en": ["converge -> conversion"], "small.en": [] }, problems: [] } },
    { index: 5, start: 70, text: "You choose." },
  ],
};

test("the moments to listen to say why, cue by cue", () => {
  const rows = listenAt(manifest, { known: "Hello, Lattice.", changed: new Set([5]) });
  assert.deepEqual(
    rows.map((row) => [row.cue, row.why]),
    [
      [2, ["heard exactly on attempt 3", "new to the voice: `GetAsync`"]],
      [3, ["a take picked by ear", "new to the voice: `idempotent`"]],
      [4, ["not heard exactly (base.en heard converge -> conversion)", "new to the voice: `converges`"]],
      [5, ["changed since the published cut"]],
    ],
  );
});

test("a re-cut's changed cues are the ones the published script does not have", () => {
  const now = [
    { index: 1, text: "Same." },
    { index: 2, text: "Reworded." },
  ];
  assert.deepEqual([...changedCues(now, [{ text: "Same." }, { text: "Original." }])], [2]);
});

test("a section is read by its heading, and the ledger carries on from the last packet", () => {
  assert.equal(sectionOf("# T\n\n## Sources\n\n| a | b |\n\n## Next\n\nx", "Sources"), "| a | b |");
  assert.equal(sectionOf("# T", "Sources"), null);
  assert.equal(ledgerOf("x\n<!-- ledger:begin -->\n- 1:10 reworded cue 12\n<!-- ledger:end -->\n"), "- 1:10 reworded cue 12");
  assert.equal(ledgerOf("<!-- ledger:begin -->\nNo feedback yet.\n<!-- ledger:end -->"), null);
  assert.equal(ledgerOf("no ledger"), null);
});

test("the packet has the video, the numbers, where to listen, the script, the sources and how to respond", () => {
  const body = packetMarkdown({
    item: { code: "B1", title: "Hello, Lattice" },
    pathTitle: "Build",
    place: "1 of 7",
    step: "2 of 30",
    idea: "Register Lattice and write a key.",
    facts: { length: "3:05", size: "12.0 MB", loudness: "-16.2 LUFS, -1.9 dBTP", cut: "0123456789ab" },
    video: "https://github.com/user-attachments/assets/abc",
    rows: listenAt(manifest, { known: "" }),
    cues: manifest.cues.map((cue) => ({ ...cue, sceneTitle: "Opening" })),
    takes: [{ cue: 3, takes: [1, 2, 3, 4] }],
    sources: "| Narration | Source |\n| --- | --- |\n| hello | README |",
    recut: null,
    ledger: null,
  });
  assert.match(body, /^<!-- video-packet: B1 -->\n## B1: Hello, Lattice\n/);
  assert.match(body, /Build, episode 1 of 7\. Item 2 of 30 in the production order/);
  assert.match(body, /\nhttps:\/\/github\.com\/user-attachments\/assets\/abc\n/, "the video sits on a line of its own, so GitHub plays it");
  assert.match(body, /\| 3:05 \| 12\.0 MB \| -16\.2 LUFS, -1\.9 dBTP \| `0123456789ab` \|/);
  assert.match(body, /\| 1:01 \| 4 \| not heard exactly/);
  assert.match(body, /- Cue 3: takes 1, 2, 3, 4/);
  assert.match(body, /\| hello \| README \|/);
  assert.match(body, /`\/pick <cue> <take>`/);
  assert.match(body, /squash-merge this pull request/);
  assert.match(body, /<!-- ledger:begin -->\nNo feedback yet\.\n<!-- ledger:end -->/);
  assert.doesNotMatch(body, /Changed since the published cut/, "a new episode has no published cut to compare");
});
