#!/usr/bin/env node
// Turns an episode's script into narration in the series voice
// (voice/voice.json), locally, with no account or API key:
//
//   renders/narration/<slug>/clips/<hash>.wav one clip per cue, named by what it
//                                           says and how, so a re-run speaks only
//                                           the cues that changed
//   renders/narration/<slug>/cues.json      the timeline: each cue's window, each
//                                           scene's window and beats, the loudness
//   renders/narration/<slug>/narration.vtt  WebVTT captions in the written form
//   renders/narration/<slug>/narration.wav  the cues joined on the timeline and
//                                           mastered to the series loudness: the
//                                           track the composition plays (needs ffmpeg)
//
// Usage: npm run narrate -- <episode-slug>     (reads episodes/<slug>/SCRIPT.md)
// Then:  npm run timeline -- <episode-slug>    (stamps the timeline into the composition)
// Needs Python with Kokoro: pip install kokoro-onnx soundfile
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths } from "./lib/layout.js";
import { applyLexicon, loadLexicon } from "./lib/lexicon.js";
import { DELIVERED_AS, gainFor, LIMITER_CEILING_DB, LOUDNESS_TARGET, masteringFilter, onTarget, parseEbur128 } from "./lib/loudness.js";
import { buildScenes, buildTimeline, captionCues, parseScript, sceneId, toWebVtt } from "./lib/narration.js";
import { speak } from "./lib/tts.js";
import { wavDuration } from "./lib/wav.js";

// Kokoro-82M writes 24 kHz mono; the joined track and its silences match it.
const SAMPLE_RATE = 24000;

let paths;
try {
  paths = episodePaths(process.argv[2]);
} catch (error) {
  console.error(`narrate: ${error.message}`);
  console.error("usage: npm run narrate -- <episode-slug>   (reads episodes/<slug>/SCRIPT.md)");
  process.exit(2);
}
const { slug } = paths;
if (!existsSync(paths.script)) {
  console.error(`narrate: episodes/${slug}/SCRIPT.md does not exist`);
  process.exit(2);
}

const voice = JSON.parse(readFileSync(path.join(workspaceRoot, "voice", "voice.json"), "utf8"));
if (!voice.voice) {
  console.error("narrate: no series voice is set in voice/voice.json; audition with 'npm run voice:samples' and record the choice");
  process.exit(2);
}
const lexicon = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
const { cues, tail } = parseScript(readFileSync(paths.script, "utf8"), `episodes/${slug}/SCRIPT.md`);
const cli = JSON.parse(readFileSync(path.join(workspaceRoot, "package.json"), "utf8")).devDependencies.hyperframes;

// Everything but the clips is rebuilt on every run. A clip is named by a hash
// of what it says and how - the spoken text, the voice settings and the pinned
// CLI that runs Kokoro - so an unchanged cue reuses its clip, and a changed
// one gets a new clip instead of overwriting one the timeline still names.
const outDir = paths.narration;
const clipsDir = path.join(outDir, "clips");
mkdirSync(clipsDir, { recursive: true });
for (const entry of readdirSync(outDir)) {
  if (entry !== "clips") rmSync(path.join(outDir, entry), { recursive: true, force: true });
}
const clipHash = (spoken) =>
  createHash("sha256")
    .update(JSON.stringify({ spoken, voice: voice.voice, lang: voice.lang, speed: voice.speed, cli }))
    .digest("hex")
    .slice(0, 16);

const files = [];
const durations = [];
let reused = 0;
for (const [index, cue] of cues.entries()) {
  const spoken = applyLexicon(cue.text, lexicon);
  const file = `clips/${clipHash(spoken)}.wav`;
  const target = path.join(outDir, file);
  const cached = existsSync(target);
  if (cached) {
    reused++;
  } else {
    // Spoken to a partial file first, so an interrupted run never leaves a
    // truncated clip that a later run would take for a finished one.
    const partial = target.replace(/\.wav$/, ".partial.wav");
    try {
      await speak(spoken, { ...voice, output: partial });
    } catch (error) {
      console.error(`narrate: text-to-speech failed on cue ${index + 1}: ${error.message}`);
      console.error("narrate: is Kokoro installed (pip install kokoro-onnx soundfile)? For a virtual environment, set HYPERFRAMES_PYTHON to its interpreter.");
      console.error("narrate: the cues spoken so far are kept; run the command again to carry on.");
      process.exit(1);
    }
    renameSync(partial, target);
  }
  files.push(file);
  durations.push(wavDuration(readFileSync(target)));
  console.log(`cue ${index + 1}/${cues.length}: ${durations.at(-1).toFixed(2)}s${cached ? " (unchanged, reused)" : ""}`);
}
const current = new Set(files.map((file) => path.basename(file)));
for (const entry of readdirSync(clipsDir)) {
  if (!current.has(entry)) rmSync(path.join(clipsDir, entry), { force: true });
}

const { timeline, duration } = buildTimeline(cues, durations, {
  leadIn: voice.leadInSeconds,
  cueGap: voice.cueGapSeconds,
  sceneGap: voice.sceneGapSeconds,
  tail,
});
const scenes = buildScenes(cues, timeline, duration, { lead: voice.sceneLeadSeconds });

const ffmpeg = (args) => spawnSync("ffmpeg", ["-hide_banner", "-nostats", ...args], { encoding: "utf8" });
let loudness = null;
if (ffmpeg(["-version"]).status !== 0) {
  console.log("narrate: ffmpeg not found; the clips are spoken, but narration.wav, which the composition plays, needs ffmpeg to join and master them");
} else {
  // Silence before each clip, so every cue lands exactly where the timeline
  // puts it, and after the last one up to the episode's end.
  const silence = (seconds, label) =>
    `aevalsrc=0:c=mono:s=${SAMPLE_RATE}:d=${seconds.toFixed(3)},aformat=sample_fmts=fltp:channel_layouts=mono[${label}]`;
  const graph = [];
  const order = [];
  let cursor = 0;
  files.forEach((_, i) => {
    const gap = timeline[i].start - cursor;
    if (gap > 0) {
      graph.push(silence(gap, `s${i}`));
      order.push(`[s${i}]`);
    }
    graph.push(`[${i}:a]aresample=${SAMPLE_RATE},aformat=sample_fmts=fltp:channel_layouts=mono[c${i}]`);
    order.push(`[c${i}]`);
    cursor = timeline[i].start + durations[i];
  });
  if (duration - cursor > 0) {
    graph.push(silence(duration - cursor, "tail"));
    order.push("[tail]");
  }
  graph.push(`${order.join("")}concat=n=${order.length}:v=0:a=1[out]`);
  const raw = path.join(outDir, "narration.raw.wav");
  const joined = ffmpeg([
    "-loglevel", "error", "-y",
    ...files.flatMap((file) => ["-i", path.join(outDir, file)]),
    "-filter_complex", graph.join(";"),
    "-map", "[out]", "-c:a", "pcm_s16le",
    raw,
  ]);
  if (joined.status !== 0) {
    console.error(`narrate: ffmpeg failed to join the cues:\n${joined.stderr}`);
    process.exit(1);
  }

  // Mastered to the series loudness with one gain and a peak limiter, then
  // measured again, as delivered (in stereo; see DELIVERED_AS). The limiter
  // takes a little loudness off the loudest passages, so a second pass makes
  // up the difference if the first falls short. The composition plays this
  // file, so what is reviewed is what ships.
  const measure = (file) =>
    parseEbur128(ffmpeg(["-i", file, "-af", `${DELIVERED_AS},ebur128=peak=true`, "-f", "null", "-"]).stderr);
  const mastered = path.join(outDir, "narration.wav");
  const before = measure(raw);
  let gainDb = gainFor(before);
  let after = null;
  for (let pass = 1; pass <= 3; pass++) {
    const master = ffmpeg([
      "-loglevel", "error", "-y", "-i", raw,
      "-af", masteringFilter(gainDb), "-ar", String(SAMPLE_RATE), "-c:a", "pcm_s16le", mastered,
    ]);
    if (master.status !== 0) {
      console.error(`narrate: ffmpeg failed to master narration.wav:\n${master.stderr}`);
      process.exit(1);
    }
    after = measure(mastered);
    if (onTarget(after)) break;
    gainDb = Math.round((gainDb + LOUDNESS_TARGET.integrated - after.integrated) * 100) / 100;
  }
  rmSync(raw, { force: true });
  if (!onTarget(after)) {
    console.error(
      `narrate: narration.wav measures ${after.integrated} LUFS with a true peak of ${after.truePeak} dBTP, ` +
        `not ${LOUDNESS_TARGET.integrated} LUFS under ${LOUDNESS_TARGET.truePeakCeiling} dBTP`,
    );
    process.exit(1);
  }
  loudness = { target: LOUDNESS_TARGET, raw: before, gainDb, limiterCeilingDb: LIMITER_CEILING_DB, mastered: after };
}

const manifest = {
  episode: slug,
  voice: voice.voice,
  speed: voice.speed,
  duration,
  narration: loudness ? "narration.wav" : null,
  loudness,
  scenes: scenes.map((scene) => ({
    id: scene.id,
    title: scene.title,
    start: scene.start,
    end: scene.end,
    beats: scene.beats,
    cues: scene.cues.map((index) => index + 1),
  })),
  cues: cues.map((cue, i) => ({
    index: i + 1,
    file: files[i],
    scene: sceneId(cue.scene ?? "Untitled"),
    start: timeline[i].start,
    end: timeline[i].end,
    text: cue.text,
  })),
};
writeFileSync(path.join(outDir, "cues.json"), `${JSON.stringify(manifest, null, 2)}\n`);
writeFileSync(path.join(outDir, "narration.vtt"), toWebVtt(captionCues(cues, timeline)));

const clock = `${Math.floor(duration / 60)}:${(duration % 60).toFixed(1).padStart(4, "0")}`;
const level = loudness
  ? `; mastered to ${loudness.mastered.integrated} LUFS, true peak ${loudness.mastered.truePeak} dBTP (raw ${loudness.raw.integrated} LUFS, gain ${loudness.gainDb >= 0 ? "+" : ""}${loudness.gainDb} dB)`
  : "";
console.log(
  `narrate: ${cues.length} cue(s) in ${scenes.length} scene(s) (${cues.length - reused} spoken, ${reused} unchanged), ` +
    `${clock}${level}, in renders/narration/${slug}/`,
);
