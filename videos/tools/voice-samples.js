#!/usr/bin/env node
// Renders the voice audition set: the same sample script (voice/sample-script.txt,
// with the pronunciation lexicon applied) in every candidate voice from
// voice/candidates.json, plus an index.html with one player per voice, under
// renders/voice-samples/. Open that page to compare, then record the choice in
// voice/voice.json.
//
// Usage: npm run voice:samples [-- --voices bf_emma,bm_george]
// Needs Python with Kokoro: pip install kokoro-onnx soundfile
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { applyLexicon, loadLexicon } from "./lib/lexicon.js";
import { escapeHtml } from "./lib/snippets.js";
import { speak } from "./lib/tts.js";

const voiceDir = path.join(workspaceRoot, "voice");
const { candidates } = JSON.parse(readFileSync(path.join(voiceDir, "candidates.json"), "utf8"));
const flag = process.argv.indexOf("--voices");
const only = flag > 0 ? new Set(process.argv[flag + 1].split(",")) : null;
const selected = only ? candidates.filter((candidate) => only.has(candidate.voice)) : candidates;
if (selected.length === 0) {
  console.error("voice-samples: no candidate voices selected");
  process.exit(2);
}

const script = readFileSync(path.join(voiceDir, "sample-script.txt"), "utf8").trim();
const spoken = applyLexicon(script, loadLexicon(path.join(voiceDir, "lexicon.json")));
const outDir = path.join(workspaceRoot, "renders", "voice-samples");
mkdirSync(outDir, { recursive: true });

const results = [];
for (const candidate of selected) {
  const file = `${candidate.voice}.wav`;
  let result;
  try {
    result = await speak(spoken, { voice: candidate.voice, lang: candidate.lang, speed: 1, output: path.join(outDir, file) });
  } catch (error) {
    console.error(`voice-samples: text-to-speech failed for ${candidate.voice}: ${error.message}`);
    console.error("voice-samples: is Kokoro installed (pip install kokoro-onnx soundfile)? For a virtual environment, set HYPERFRAMES_PYTHON to its interpreter.");
    process.exit(1);
  }
  const { durationSeconds } = result;
  results.push({ ...candidate, file, durationSeconds });
  console.log(`${candidate.voice}: ${durationSeconds.toFixed(1)}s`);
}

const rows = results
  .map(
    (result) => `      <li>
        <h2>${escapeHtml(result.label)}</h2>
        <p><code>${escapeHtml(result.voice)}</code> - ${result.durationSeconds.toFixed(1)}s - ${escapeHtml(result.style)}</p>
        <audio controls preload="none" src="${result.file}"></audio>
      </li>`,
  )
  .join("\n");

const page = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Orleans.Lattice video series - voice auditions</title>
    <style>
      body { margin: 2rem auto; max-width: 52rem; padding: 0 1rem; font: 16px/1.5 system-ui, sans-serif; background: #0b0e14; color: #e6e9ef; }
      blockquote { margin: 1rem 0; padding: 0.75rem 1rem; border: 1px solid #2a3244; border-radius: 6px; background: #141925; }
      ol { padding-left: 1.25rem; }
      li { margin: 1.25rem 0; }
      h2 { margin: 0; font-size: 1.1rem; }
      p { margin: 0.25rem 0 0.5rem; color: #a3acbb; }
      audio { width: 100%; }
      code { color: #b3a4f5; }
    </style>
  </head>
  <body>
    <h1>Voice auditions</h1>
    <p>The same script in each candidate voice, generated locally with Kokoro-82M at normal speed. Pace is a separate setting (voice/voice.json) and can be tuned after the voice is chosen.</p>
    <blockquote>${escapeHtml(script)}</blockquote>
    <ol>
${rows}
    </ol>
  </body>
</html>
`;
writeFileSync(path.join(outDir, "index.html"), page);
console.log(`voice-samples: ${results.length} voice(s); open renders/voice-samples/index.html`);
