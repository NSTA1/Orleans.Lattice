import assert from "node:assert/strict";
import { test } from "node:test";
import { clock, cueNotes, listeningPage, takesPage } from "../lib/listening.js";

const manifest = {
  duration: 181.8,
  loudness: { mastered: { integrated: -16.3 } },
  cues: [
    { index: 1, start: 1.5, text: "Orleans.Lattice, in three minutes.", check: { attempt: 1, verified: true } },
    { index: 2, start: 115.9, text: "Merges are <idempotent>.", check: { attempt: 3, verified: false, differences: { "base.en": ["'lock' -> 'lot'"] }, problems: [] } },
    { index: 3, start: 150.0, text: "You choose.", changed: "a new take", check: { attempt: 2, verified: true, picked: true } },
  ],
};

test("a time is minutes and seconds to a tenth", () => {
  assert.equal(clock(115.94), "1:55.9");
  assert.equal(clock(1.5), "0:01.5");
});

test("a cue's notes say what changed, which take it is, whether it was picked, and what only an ear can settle", () => {
  assert.deepEqual(cueNotes(manifest.cues[0]), []);
  assert.deepEqual(cueNotes(manifest.cues[1]), ["take 3", "listen: base.en heard 'lock' -> 'lot'"]);
  assert.deepEqual(cueNotes(manifest.cues[2]), ["changed in this run: a new take", "take 2, picked by ear"]);
  assert.deepEqual(cueNotes({ check: { attempt: null, verified: true, picked: true } }), ["picked by ear"], "a picked reading made outside the takes has no take number");
});

test("the narration page lists every cue with a time to play from, and highlights what needs an ear", () => {
  const page = listeningPage({ title: "Orleans.Lattice in three minutes", manifest, audioSrc: "../narration/introduction/narration.wav?v=1" });
  assert.equal((page.match(/<button type="button" data-t=/g) ?? []).length, 3);
  assert.match(page, /data-t="115.9">1:55.9</);
  assert.match(page, /Merges are &lt;idempotent&gt;\./, "cue text is escaped");
  assert.equal((page.match(/<li class="flag">/g) ?? []).length, 2, "the unsettled cue and the changed one");
  assert.match(page, /2 cue\(s\) are highlighted/);
  assert.match(page, /src="\.\.\/narration\/introduction\/narration\.wav\?v=1"/);
});

test("the takes page shows each take with its verdict, marks the one in use, and says how to pick", () => {
  const page = takesPage({
    title: "Orleans.Lattice in three minutes",
    slug: "introduction",
    cues: [
      {
        index: 29,
        start: 178.8,
        text: "Operate, if you are running an estate. You choose.",
        takes: [
          { take: 1, src: "../takes/introduction/abc/take1.wav?v=1", seconds: 3.03, verdict: "base.en: 'an estate' -> 'in a state'", current: true },
          { take: 2, src: "../takes/introduction/abc/take2.wav?v=1", seconds: 3.68, verdict: "heard exactly", current: false },
        ],
      },
    ],
  });
  assert.match(page, /<h2>Cue 29 at 2:58\.8<\/h2>/);
  assert.equal((page.match(/<audio controls/g) ?? []).length, 2);
  assert.equal((page.match(/in the narration now/g) ?? []).length, 1);
  assert.match(page, /npm run audition -- introduction --pick 29=2/);
  assert.match(page, /3\.0s - base\.en: 'an estate' -&gt; 'in a state'/);
  assert.match(page, /only the published cut is committed/);
});
