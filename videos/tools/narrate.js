#!/usr/bin/env node
// Turns an episode's script into narration in the series voice
// (voice/voice.json), locally, with no account or API key:
//
//   renders/narration/<slug>/clips/<hash>.wav one clip per cue, named by what it
//                                           says and how, so a re-run speaks only
//                                           the cues that changed
//   renders/narration/<slug>/clips/<hash>.json how a Chatterbox clip was checked:
//                                           its attempt, seed and transcripts
//   renders/narration/<slug>/cues.json      the timeline: each cue's window, each
//                                           scene's window and beats, the loudness
//   renders/narration/<slug>/narration.vtt  WebVTT captions in the written form
//   renders/narration/<slug>/narration.wav  the cues joined on the timeline and
//                                           mastered to the series loudness: the
//                                           track the composition plays (needs ffmpeg)
//
// Usage: npm run narrate -- <episode-slug>     (reads episodes/<slug>/SCRIPT.md)
// Then:  npm run timeline -- <episode-slug>    (stamps the timeline into the composition)
//
// The engine is voice.json's "provider":
//   chatterbox  the series voice. Chatterbox, cloned from voice/reference.wav,
//               run by tools/voice_worker.py in the Python environment that
//               VIDEOS_VOICE_PYTHON names (voice/requirements.txt). A generative
//               voice can slip, so every clip is transcribed by local speech
//               recognisers and compared with the script; one that is not heard
//               exactly is made again with the next seed (tools/lib/verify.js).
//   kokoro      the first series voice, Kokoro through the HyperFrames CLI
//               (pip install kokoro-onnx soundfile; HYPERFRAMES_PYTHON).
import { spawnSync } from "node:child_process";
import { createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths } from "./lib/layout.js";
import { loadLexicon } from "./lib/lexicon.js";
import { DELIVERED_AS, gainFor, LIMITER_CEILING_DB, LOUDNESS_TARGET, masteringFilter, onTarget, parseEbur128 } from "./lib/loudness.js";
import { buildScenes, buildTimeline, captionCues, parseScript, sceneId, toWebVtt } from "./lib/narration.js";
import { clipNamer, readVoice, speakTake, spokenCues as spokenForms, startChatterbox, verdictOf } from "./lib/series-voice.js";
import { speak } from "./lib/tts.js";
import { bestAttempt, judgeAttempt } from "./lib/verify.js";
import { wavDuration } from "./lib/wav.js";

// Both engines write 24 kHz mono; the joined track and its silences match it.
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

let voice;
try {
  voice = readVoice();
} catch (error) {
  console.error(`narrate: ${error.message}`);
  process.exit(2);
}
const engine = voice.provider;
const lexicon = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
const { cues, tail } = parseScript(readFileSync(paths.script, "utf8"), `episodes/${slug}/SCRIPT.md`);
const cli = JSON.parse(readFileSync(path.join(workspaceRoot, "package.json"), "utf8")).devDependencies.hyperframes;
const started = Date.now();

// Everything but the clips is rebuilt on every run. A clip is named by a hash
// of what it says and how - the spoken text and everything that shapes the
// voice (see clipNamer) - so an unchanged cue reuses its clip, and a changed
// one gets a new clip instead of overwriting one the timeline still names. A
// take picked by ear (npm run audition -- <slug> --pick) is installed as its
// cue's clip, so it is reused like any other.
const outDir = paths.narration;
const clipsDir = path.join(outDir, "clips");
mkdirSync(clipsDir, { recursive: true });
for (const entry of readdirSync(outDir)) {
  if (entry !== "clips") rmSync(path.join(outDir, entry), { recursive: true, force: true });
}

const kokoro = voice.kokoro ?? {};
const chatterbox = voice.chatterbox ?? {};
const clipHash = clipNamer(voice, { cli });
const spokenCues = spokenForms(cues, lexicon, engine);
const files = spokenCues.map((spoken) => `clips/${clipHash(spoken)}.wav`);
const checks = new Array(cues.length).fill(null);
const missing = files.map((file, index) => (existsSync(path.join(outDir, file)) ? -1 : index)).filter((index) => index >= 0);

// A Chatterbox clip's check record, beside it.
const sidecarOf = (index) => path.join(outDir, files[index].replace(/\.wav$/, ".json"));
const readSidecar = (index) => (existsSync(sidecarOf(index)) ? JSON.parse(readFileSync(sidecarOf(index), "utf8")) : null);
// Kept clips made before clips were inspected for sounds that are not speech.
const uninspected =
  engine === "chatterbox" ? files.map((_, index) => index).filter((index) => !missing.includes(index) && !readSidecar(index)?.inspected) : [];
let spoken = 0;
// What this run changed, cue by cue, for the listening page (npm run review -- <slug> --audio).
const changes = new Map();

if (engine === "kokoro") {
  for (const index of missing) {
    // Spoken to a partial file first, so an interrupted run never leaves a
    // truncated clip that a later run would take for a finished one.
    const target = path.join(outDir, files[index]);
    const partial = target.replace(/\.wav$/, ".partial.wav");
    try {
      await speak(spokenCues[index], { ...kokoro, output: partial });
    } catch (error) {
      console.error(`narrate: text-to-speech failed on cue ${index + 1}: ${error.message}`);
      console.error("narrate: is Kokoro installed (pip install kokoro-onnx soundfile)? For a virtual environment, set HYPERFRAMES_PYTHON to its interpreter.");
      console.error("narrate: the cues spoken so far are kept; run the command again to carry on.");
      process.exit(1);
    }
    renameSync(partial, target);
    spoken++;
    changes.set(index, "a new take");
    console.log(`cue ${index + 1}/${cues.length}: ${wavDuration(readFileSync(target)).toFixed(2)}s`);
  }
} else if (missing.length > 0 || uninspected.length > 0) {
  const log = createWriteStream(path.join(outDir, "voice.log"));
  let worker;
  try {
    worker = startChatterbox(voice, { log });
  } catch (error) {
    console.error(`narrate: ${error.message}`);
    process.exit(2);
  }
  try {
    const ready = await worker.ready;
    const versions = Object.entries(ready.versions ?? {}).map(([name, version]) => `${name} ${version}`).join(", ");
    console.log(
      `voice: Chatterbox ready in ${ready.loadSeconds}s (${versions}); ${missing.length} of ${cues.length} cue(s) to speak` +
        (uninspected.length > 0 ? `, ${uninspected.length} made before to inspect` : ""),
    );

    // Inspect the clips made before inspection existed: a sound after the last
    // word is cut off, and a clip with one inside its speech is made again,
    // from the attempt after the one that was kept.
    const toMake = missing.map((index) => [index, 0]);
    for (const index of uninspected) {
      const target = path.join(outDir, files[index]);
      const partial = target.replace(/\.wav$/, ".repaired.partial.wav");
      const reply = await worker.request({ analyse: target, output: partial });
      if (reply.repaired) renameSync(partial, target);
      const check = { ...(readSidecar(index) ?? {}), inspected: true, repaired: reply.repaired, artefacts: reply.artefacts, seconds: reply.seconds };
      writeFileSync(sidecarOf(index), `${JSON.stringify(check, null, 2)}\n`);
      const found = reply.artefacts.map((a) => `${a.kind} at ${a.start}-${a.end}s`);
      if (reply.repaired) {
        changes.set(index, `a sound after the last word cut off at ${reply.repaired.cutAt}s`);
        console.log(`cue ${index + 1}/${cues.length}: cut a sound after the last word (from ${reply.repaired.from}s, ${reply.repaired.level} dB) at ${reply.repaired.cutAt}s`);
      }
      if (found.length > 0) {
        console.log(`cue ${index + 1}/${cues.length}: ${found.join("; ")}; making it again`);
        toMake.push([index, check.attempt ?? 0]);
      }
    }
    toMake.sort((a, b) => a[0] - b[0]);

    for (const [index, first] of toMake) {
      const target = path.join(outDir, files[index]);
      const name = path.basename(target, ".wav");
      const attempts = [];
      for (let take = first + 1; take <= first + (chatterbox.attempts ?? 1); take++) {
        const output = path.join(clipsDir, `${name}.take${take}.partial.wav`);
        const attempt = await speakTake(worker, {
          text: cues[index].text,
          spoken: spokenCues[index],
          name,
          take,
          output,
          secondsPerWord: chatterbox.secondsPerWord,
        });
        attempts.push(attempt);
        console.log(
          `cue ${index + 1}/${cues.length}, attempt ${take}: ${attempt.reply.seconds.toFixed(2)}s in ${attempt.reply.generateSeconds}s, ${verdictOf(attempt)}`,
        );
        if (attempt.judgement.passed) break;
      }
      const kept = attempts[bestAttempt(attempts.map((a) => a.judgement))];
      renameSync(kept.output, target);
      for (const { output } of attempts) rmSync(output, { force: true });
      spoken++;
      changes.set(index, "a new take");
      const check = {
        attempt: kept.take,
        seed: kept.seed,
        verified: kept.judgement.passed,
        differences: kept.judgement.differences,
        problems: kept.judgement.problems,
        transcripts: kept.reply.transcripts,
        seconds: kept.reply.seconds,
        rawSeconds: kept.reply.rawSeconds,
        inspected: true,
        repaired: kept.reply.repaired,
        artefacts: kept.reply.artefacts,
      };
      writeFileSync(sidecarOf(index), `${JSON.stringify(check, null, 2)}\n`);
      if (!check.verified) {
        console.log(`cue ${index + 1}/${cues.length}: kept attempt ${check.attempt}, which did not pass every check; listen to it`);
      }
    }
  } catch (error) {
    console.error(`narrate: the voice failed: ${error.message}`);
    console.error("narrate: the cues spoken so far are kept; run the command again to carry on.");
    await worker.close().catch(() => {});
    process.exit(1);
  }
  await worker.close();
  log.end();
}

// What each Chatterbox clip's check found, whether it was made now or before:
// judged again from its stored transcripts, so the verdict follows the rules
// as they are now. A take picked by ear stands whatever the recognisers
// heard; what they heard is kept for the record.
if (engine === "chatterbox") {
  files.forEach((_, index) => {
    const check = readSidecar(index);
    if (!check) return;
    const judgement = judgeAttempt(cues[index].text, check, { secondsPerWord: chatterbox.secondsPerWord });
    checks[index] = {
      ...check,
      verified: judgement.passed || Boolean(check.picked),
      differences: judgement.differences,
      problems: judgement.problems,
    };
  });
}

const durations = files.map((file) => wavDuration(readFileSync(path.join(outDir, file))));
const reused = cues.length - spoken;
if (engine === "kokoro") {
  files.forEach((_, index) => {
    if (!missing.includes(index)) console.log(`cue ${index + 1}/${cues.length}: ${durations[index].toFixed(2)}s (unchanged, reused)`);
  });
}
const current = new Set(files.flatMap((file) => [path.basename(file), path.basename(file).replace(/\.wav$/, ".json")]));
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
  engine,
  voice:
    engine === "kokoro"
      ? { voice: kokoro.voice, lang: kokoro.lang, speed: kokoro.speed }
      : {
          model: `${chatterbox.model}@${chatterbox.revision}`,
          reference: chatterbox.reference,
          exaggeration: chatterbox.exaggeration,
          cfgWeight: chatterbox.cfgWeight,
          temperature: chatterbox.temperature,
        },
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
    ...(changes.has(i) ? { changed: changes.get(i) } : {}),
    ...(checks[i]
      ? {
          check: {
            attempt: checks[i].attempt,
            verified: checks[i].verified,
            ...(checks[i].picked ? { picked: true } : {}),
            differences: checks[i].differences,
            problems: checks[i].problems,
          },
        }
      : {}),
  })),
};
writeFileSync(path.join(outDir, "cues.json"), `${JSON.stringify(manifest, null, 2)}\n`);
writeFileSync(path.join(outDir, "narration.vtt"), toWebVtt(captionCues(cues, timeline)));

const clock = `${Math.floor(duration / 60)}:${(duration % 60).toFixed(1).padStart(4, "0")}`;
const level = loudness
  ? `; mastered to ${loudness.mastered.integrated} LUFS, true peak ${loudness.mastered.truePeak} dBTP (raw ${loudness.raw.integrated} LUFS, gain ${loudness.gainDb >= 0 ? "+" : ""}${loudness.gainDb} dB)`
  : "";
const minutes = ((Date.now() - started) / 60000).toFixed(1);
console.log(
  `narrate: ${cues.length} cue(s) in ${scenes.length} scene(s) (${cues.length - reused} spoken, ${reused} unchanged), ` +
    `${clock}${level}, in renders/narration/${slug}/, in ${minutes} min`,
);
const unverified = checks.map((check, index) => (check && !check.verified ? index + 1 : null)).filter(Boolean);
if (unverified.length > 0) {
  console.log(`narrate: no recogniser heard cue(s) ${unverified.join(", ")} exactly; cues.json has what they heard. Listen before publishing.`);
}
