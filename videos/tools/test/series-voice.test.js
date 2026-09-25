import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";
import { validateLexicon } from "../lib/lexicon.js";
import {
  chatterboxArgs,
  clipNamer,
  pinnedRequirements,
  readVoice,
  speakTake,
  spokenCues,
  startChatterbox,
  verdictOf,
} from "../lib/series-voice.js";
import { seedFor } from "../lib/verify.js";

const chatterbox = {
  model: "ResembleAI/chatterbox",
  revision: "0123456789abcdef0123456789abcdef01234567",
  reference: "voice/reference.wav",
  exaggeration: 0.75,
  cfgWeight: 0.35,
  temperature: 0.8,
  trim: { thresholdDb: -50, padBeforeSeconds: 0.05, padAfterSeconds: 0.15 },
  recognisers: ["base.en", "small.en"],
  hotwords: "Orleans Lattice",
};

/** A workspace with just the files a Chatterbox clip name reads. */
function scratchRoot({ requirements = "chatterbox-tts==0.1.7\nfaster-whisper==1.2.1\n", reference = "RIFF" } = {}) {
  const root = mkdtempSync(path.join(tmpdir(), "series-voice-"));
  mkdirSync(path.join(root, "voice"));
  writeFileSync(path.join(root, "voice", "requirements.txt"), requirements);
  writeFileSync(path.join(root, "voice", "reference.wav"), reference);
  return root;
}

test("a Kokoro clip keeps the name it has always had, so earlier Kokoro clips stay valid", () => {
  const voice = { provider: "kokoro", kokoro: { voice: "bf_emma", lang: "en-gb", speed: 1 } };
  const spoken = "Then two places can change the same thing at the same moment.";
  const legacy = createHash("sha256")
    .update(JSON.stringify({ spoken, voice: "bf_emma", lang: "en-gb", speed: 1, cli: "0.8.46" }))
    .digest("hex")
    .slice(0, 16);
  assert.equal(clipNamer(voice, { cli: "0.8.46" })(spoken), legacy);
});

test("a Chatterbox clip is renamed by a new version pin or reference clip, not by a comment", () => {
  const voice = { provider: "chatterbox", chatterbox };
  const roots = [
    scratchRoot(),
    scratchRoot({ requirements: "# the voice\nfaster-whisper==1.2.1   # recognisers\n\nchatterbox-tts==0.1.7\n" }),
    scratchRoot({ requirements: "chatterbox-tts==0.1.8\nfaster-whisper==1.2.1\n" }),
    scratchRoot({ reference: "RIFF, but another voice" }),
  ];
  try {
    const [base, commented, upgraded, recast] = roots.map((root) => clipNamer(voice, { root })("Neither waits its turn."));
    assert.match(base, /^[0-9a-f]{16}$/);
    assert.equal(commented, base, "comments, blank lines and order do not rename clips");
    assert.notEqual(upgraded, base, "a new version does");
    assert.notEqual(recast, base, "a new reference clip does");
    assert.notEqual(clipNamer({ provider: "chatterbox", chatterbox: { ...chatterbox, exaggeration: 0.5 } }, { root: roots[0] })("Neither waits its turn."), base);
  } finally {
    for (const root of roots) rmSync(root, { recursive: true, force: true });
  }
});

test("the pinned requirements are the lines that pin, sorted, without comments", () => {
  assert.deepEqual(pinnedRequirements("# header\nb==2  # why\n\na==1\r\n"), ["a==1", "b==2"]);
});

test("each engine is given the lexicon's form for it, and Chatterbox reads a hyphenated compound as one breath", () => {
  const lexicon = validateLexicon([{ written: "Orleans", spoken: "Or-leens", engines: { chatterbox: "Orleens" } }]);
  const cues = [{ text: "Orleans keeps a key-value store." }];
  assert.deepEqual(spokenCues(cues, lexicon, "chatterbox"), ["Orleens keeps a key value store."]);
  assert.deepEqual(spokenCues(cues, lexicon, "kokoro"), ["Or-leens keeps a key-value store."]);
});

test("the worker is started with the voice's settings, and not at all without its Python", () => {
  const args = chatterboxArgs({ chatterbox }, { threads: 6, root: "/w" });
  assert.equal(path.basename(args[0]), "voice_worker.py");
  const value = (flag) => args[args.indexOf(flag) + 1];
  assert.equal(value("--revision"), chatterbox.revision);
  assert.equal(value("--exaggeration"), "0.75");
  assert.equal(value("--cfg-weight"), "0.35");
  assert.equal(value("--recognisers"), "base.en,small.en");
  assert.equal(value("--threads"), "6");
  assert.throws(() => startChatterbox({ chatterbox }, { env: {} }), /set VIDEOS_VOICE_PYTHON/);
});

test("voice.json must name an engine the workspace runs", () => {
  const root = mkdtempSync(path.join(tmpdir(), "series-voice-"));
  try {
    const file = path.join(root, "voice.json");
    writeFileSync(file, JSON.stringify({ provider: "piper" }));
    assert.throws(() => readVoice(file), /provider 'piper'/);
    writeFileSync(file, JSON.stringify({ provider: "chatterbox", chatterbox }));
    assert.equal(readVoice(file).provider, "chatterbox");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("take n of a clip is spoken with narration's seed for attempt n, and judged", async () => {
  const requests = [];
  const worker = {
    request: async (payload) => {
      requests.push(payload);
      return { seconds: 1.5, transcripts: { a: "Neither awaits its turn." }, generateSeconds: 30, repaired: { cutAt: 1.4 } };
    },
  };
  const take = await speakTake(worker, {
    text: "Neither waits its turn.",
    spoken: "Neither waits its turn.",
    name: "5a3ae19465122180",
    take: 3,
    output: "take3.wav",
    secondsPerWord: { min: 0.18, max: 0.75 },
  });
  assert.equal(requests[0].seed, seedFor("5a3ae19465122180", 2));
  assert.equal(take.take, 3);
  assert.equal(take.judgement.passed, false);
  assert.equal(verdictOf(take), "a: 'waits' -> 'awaits' (cut a sound after the last word at 1.4s)");
  assert.equal(verdictOf({ judgement: { passed: true, differences: {}, problems: [] }, reply: {} }), "heard exactly");
});
