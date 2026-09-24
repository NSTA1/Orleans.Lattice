#!/usr/bin/env node
// Shows how the series voice will read an episode before it is narrated: for
// each cue, the written text, the spoken form the lexicon turns it into, and
// the phonemes Kokoro's own phonemizer produces from that. Words with two
// readings (voice/heteronyms.json) are flagged, because the phonemizer picks
// one reading without looking at the sentence - it says "lives" as the plural
// of life even in "the store lives in the cluster".
//
//   npm run phonemes -- <slug>          every cue
//   npm run phonemes -- <slug> --flagged only the cues with a word to check
//
// Needs the same Python as narration (HYPERFRAMES_PYTHON, with kokoro-onnx).
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths } from "./lib/layout.js";
import { applyLexicon, heteronymsIn, loadHeteronyms, loadLexicon } from "./lib/lexicon.js";
import { parseScript } from "./lib/narration.js";

const args = process.argv.slice(2);
let paths;
try {
  paths = episodePaths(args.find((arg) => !arg.startsWith("--")));
} catch (error) {
  console.error(`phonemes: ${error.message}`);
  console.error("usage: npm run phonemes -- <episode-slug> [--flagged]");
  process.exit(2);
}
if (!existsSync(paths.script)) {
  console.error(`phonemes: episodes/${paths.slug}/SCRIPT.md does not exist`);
  process.exit(2);
}
const onlyFlagged = args.includes("--flagged");

const voice = JSON.parse(readFileSync(path.join(workspaceRoot, "voice", "voice.json"), "utf8"));
const lexicon = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
const heteronyms = loadHeteronyms(path.join(workspaceRoot, "voice", "heteronyms.json"));
const { cues } = parseScript(readFileSync(paths.script, "utf8"), `episodes/${paths.slug}/SCRIPT.md`);
const spoken = cues.map((cue) => applyLexicon(cue.text, lexicon));

// One Python process phonemizes every cue, through kokoro-onnx's tokenizer:
// exactly the path the text takes on its way to the voice.
const program = [
  "import json, sys",
  "from kokoro_onnx.tokenizer import Tokenizer",
  "tokenizer = Tokenizer()",
  "request = json.loads(sys.stdin.read())",
  "print(json.dumps([tokenizer.phonemize(text, lang=request['lang']) for text in request['texts']]))",
].join("\n");
const python = process.env.HYPERFRAMES_PYTHON || (process.platform === "win32" ? "python" : "python3");
const run = spawnSync(python, ["-c", program], {
  input: JSON.stringify({ lang: voice.lang, texts: spoken }),
  encoding: "utf8",
  env: { ...process.env, PYTHONIOENCODING: "utf-8" },
});
if (run.status !== 0) {
  console.error(`phonemes: ${python} could not phonemize the script:\n${run.stderr || run.error?.message || ""}`);
  console.error("phonemes: set HYPERFRAMES_PYTHON to the interpreter that has kokoro-onnx (the one narration uses).");
  process.exit(1);
}
const phonemes = JSON.parse(run.stdout.trim().split(/\r?\n/).at(-1));

let flagged = 0;
cues.forEach((cue, i) => {
  const words = heteronymsIn(cue.text, heteronyms);
  if (words.length > 0) flagged++;
  if (onlyFlagged && words.length === 0) return;
  console.log(`cue ${i + 1} (${cue.scene ?? "no scene"}): ${cue.text}`);
  if (spoken[i] !== cue.text) console.log(`  spoken:   ${spoken[i]}`);
  console.log(`  phonemes: ${phonemes[i]}`);
  if (words.length > 0) console.log(`  check:    ${words.join(", ")} - two readings; confirm the phonemes say the one meant`);
});
console.log(`phonemes: ${cues.length} cue(s), ${flagged} with a word to check`);
