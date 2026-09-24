#!/usr/bin/env node
// Turns an episode's script into narration in the series voice
// (voice/voice.json), locally, with no account or API key:
//
//   renders/narration/<slug>/cue-NNN.wav    one clip per cue, for the timeline
//   renders/narration/<slug>/cues.json      cue start/end, scene label, written text
//   renders/narration/<slug>/narration.vtt  WebVTT captions in the written form
//   renders/narration/<slug>/narration.wav  the cues joined with the configured
//                                           gaps, for review (needs ffmpeg)
//
// Usage: npm run narrate -- <episode-slug>     (reads episodes/<slug>/SCRIPT.md)
// Needs Python with Kokoro: pip install kokoro-onnx soundfile
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { applyLexicon, loadLexicon } from "./lib/lexicon.js";
import { buildTimeline, captionCues, parseScript, toWebVtt } from "./lib/narration.js";
import { speak } from "./lib/tts.js";

// Kokoro-82M writes 24 kHz mono; the joined track and its silences match it.
const SAMPLE_RATE = 24000;

const slug = process.argv[2];
if (!slug || !/^[a-z0-9][a-z0-9-]*$/.test(slug)) {
  console.error("usage: npm run narrate -- <episode-slug>   (reads episodes/<slug>/SCRIPT.md)");
  process.exit(2);
}

const scriptFile = path.join(workspaceRoot, "episodes", slug, "SCRIPT.md");
if (!existsSync(scriptFile)) {
  console.error(`narrate: episodes/${slug}/SCRIPT.md does not exist`);
  process.exit(2);
}

const voice = JSON.parse(readFileSync(path.join(workspaceRoot, "voice", "voice.json"), "utf8"));
if (!voice.voice) {
  console.error("narrate: no series voice is set in voice/voice.json; audition with 'npm run voice:samples' and record the choice");
  process.exit(2);
}
const lexicon = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
const cues = parseScript(readFileSync(scriptFile, "utf8"), `episodes/${slug}/SCRIPT.md`);
const outDir = path.join(workspaceRoot, "renders", "narration", slug);
mkdirSync(outDir, { recursive: true });

const files = [];
const durations = [];
for (const [index, cue] of cues.entries()) {
  const file = `cue-${String(index + 1).padStart(3, "0")}.wav`;
  let result;
  try {
    result = await speak(applyLexicon(cue.text, lexicon), { ...voice, output: path.join(outDir, file) });
  } catch (error) {
    console.error(`narrate: text-to-speech failed on cue ${index + 1}: ${error.message}`);
    console.error("narrate: is Kokoro installed (pip install kokoro-onnx soundfile)? For a virtual environment, set HYPERFRAMES_PYTHON to its interpreter.");
    process.exit(1);
  }
  files.push(file);
  durations.push(result.durationSeconds);
  console.log(`cue ${index + 1}/${cues.length}: ${durations.at(-1).toFixed(2)}s`);
}

const timeline = buildTimeline(durations, { leadIn: voice.leadInSeconds, gap: voice.cueGapSeconds });
const manifest = {
  episode: slug,
  voice: voice.voice,
  speed: voice.speed,
  cues: cues.map((cue, i) => ({ index: i + 1, file: files[i], scene: cue.scene, ...timeline[i], text: cue.text })),
};
writeFileSync(path.join(outDir, "cues.json"), `${JSON.stringify(manifest, null, 2)}\n`);
writeFileSync(path.join(outDir, "narration.vtt"), toWebVtt(captionCues(cues, timeline)));

const ffmpeg = spawnSync("ffmpeg", ["-version"], { stdio: "ignore" });
if (ffmpeg.status !== 0) {
  console.log("narrate: ffmpeg not found; skipped joining narration.wav (the per-cue clips are complete)");
} else {
  const silence = (seconds, label) =>
    `aevalsrc=0:c=mono:s=${SAMPLE_RATE}:d=${seconds},aformat=sample_fmts=fltp:channel_layouts=mono[${label}]`;
  const graph = [silence(voice.leadInSeconds, "lead")];
  const order = ["[lead]"];
  files.forEach((_, i) => {
    graph.push(`[${i}:a]aresample=${SAMPLE_RATE},aformat=sample_fmts=fltp:channel_layouts=mono[c${i}]`);
    order.push(`[c${i}]`);
    if (i < files.length - 1) {
      graph.push(silence(voice.cueGapSeconds, `g${i}`));
      order.push(`[g${i}]`);
    }
  });
  graph.push(`${order.join("")}concat=n=${order.length}:v=0:a=1[out]`);
  const joined = spawnSync(
    "ffmpeg",
    [
      "-hide_banner", "-loglevel", "error", "-y",
      ...files.flatMap((file) => ["-i", path.join(outDir, file)]),
      "-filter_complex", graph.join(";"),
      "-map", "[out]", "-c:a", "pcm_s16le",
      path.join(outDir, "narration.wav"),
    ],
    { stdio: "inherit" },
  );
  if (joined.status !== 0) {
    console.error("narrate: ffmpeg failed to join narration.wav");
    process.exit(1);
  }
}

const total = timeline.at(-1).end;
console.log(`narrate: ${cues.length} cue(s), ${total.toFixed(2)}s of narration in renders/narration/${slug}/`);
