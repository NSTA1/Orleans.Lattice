// The series voice as the tools use it: which engine voice/voice.json names,
// what each cue is called in the clip cache, what the engine is given to say,
// and how one take of a cue is spoken and judged. tools/narrate.js and
// tools/audition.js share it, so a take made for an audition is exactly the
// clip narration would make.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./hyperframes.js";
import { applyLexicon, readingFor } from "./lexicon.js";
import { fileDigest } from "./publication.js";
import { judgeAttempt, seedFor } from "./verify.js";
import { startVoiceWorker } from "./voice-worker.js";

/** The engines voice.json's "provider" can name. */
export const PROVIDERS = Object.freeze(["chatterbox", "kokoro"]);

/** Reads voice/voice.json and checks it names an engine the workspace runs. */
export function readVoice(file = path.join(workspaceRoot, "voice", "voice.json")) {
  const voice = JSON.parse(readFileSync(file, "utf8"));
  if (!PROVIDERS.includes(voice.provider)) {
    throw new Error(`voice/voice.json names the provider '${voice.provider}'; it must be one of ${PROVIDERS.join(", ")}`);
  }
  return voice;
}

/** The packages a requirements file pins, without its comments or blank lines, sorted. */
export function pinnedRequirements(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.replace(/#.*/, "").trim())
    .filter(Boolean)
    .sort();
}

const hash = (value) => createHash("sha256").update(JSON.stringify(value)).digest("hex").slice(0, 16);

/**
 * The function that names a cue's clip in the cache: a hash of what it says
 * (the spoken form) and of everything that shapes the voice. An unchanged cue
 * keeps its name, so its clip is reused; a change to the text or the voice
 * gives a new name. For Kokoro the name is what it has always been, so earlier
 * Kokoro clips stay valid; for Chatterbox it covers the model and its
 * revision, the reference clip's content, the reading settings, the trim, and
 * the pinned packages of the voice's environment (only a change of version,
 * not a comment, renames the clips).
 */
export function clipNamer(voice, { cli, root = workspaceRoot } = {}) {
  if (voice.provider === "kokoro") {
    const kokoro = voice.kokoro ?? {};
    return (spoken) => hash({ spoken, voice: kokoro.voice, lang: kokoro.lang, speed: kokoro.speed, cli });
  }
  const chatterbox = voice.chatterbox ?? {};
  const identity = {
    engine: voice.provider,
    model: chatterbox.model,
    revision: chatterbox.revision,
    reference: fileDigest(path.join(root, chatterbox.reference)),
    exaggeration: chatterbox.exaggeration,
    cfgWeight: chatterbox.cfgWeight,
    temperature: chatterbox.temperature,
    trim: chatterbox.trim,
    requirements: pinnedRequirements(readFileSync(path.join(root, "voice", "requirements.txt"), "utf8")),
  };
  return (spoken) => hash({ spoken, ...identity });
}

/** What the engine is given for each cue: the lexicon's spoken forms for it, then its reading rules (readingFor). */
export function spokenCues(cues, lexicon, engine) {
  return cues.map((cue) => readingFor(applyLexicon(cue.text, lexicon, engine), engine));
}

/** The arguments that start tools/voice_worker.py with the series voice's settings. */
export function chatterboxArgs(voice, { threads = "8", root = workspaceRoot } = {}) {
  const chatterbox = voice.chatterbox ?? {};
  const trim = chatterbox.trim ?? {};
  return [
    path.join(root, "tools", "voice_worker.py"),
    "--model", chatterbox.model,
    "--revision", chatterbox.revision,
    "--reference", path.join(root, chatterbox.reference),
    "--exaggeration", String(chatterbox.exaggeration),
    "--cfg-weight", String(chatterbox.cfgWeight),
    "--temperature", String(chatterbox.temperature),
    "--trim-threshold-db", String(trim.thresholdDb ?? -50),
    "--trim-pad-before", String(trim.padBeforeSeconds ?? 0.05),
    "--trim-pad-after", String(trim.padAfterSeconds ?? 0.15),
    "--recognisers", (chatterbox.recognisers ?? []).join(","),
    "--hotwords", chatterbox.hotwords ?? "",
    "--threads", String(threads),
  ];
}

/**
 * Starts the voice worker in the Python environment VIDEOS_VOICE_PYTHON names
 * (see voice/requirements.txt); throws, saying so, when it is not set.
 */
export function startChatterbox(voice, { log, env = process.env } = {}) {
  const python = env.VIDEOS_VOICE_PYTHON;
  if (!python) {
    throw new Error(
      "set VIDEOS_VOICE_PYTHON to the Python 3.11 interpreter of an environment with voice/requirements.txt installed (README.md, 'The series voice')",
    );
  }
  return startVoiceWorker(python, chatterboxArgs(voice, { threads: env.VIDEOS_VOICE_THREADS ?? "8" }), { log });
}

/**
 * Speaks take `take` (from 1) of a cue: the seed comes from the clip's name
 * and the take, so take n of a clip is the same audio as narration's attempt
 * n on the same machine. Resolves with the worker's reply and its judgement.
 */
export async function speakTake(worker, { text, spoken, name, take, output, secondsPerWord }) {
  const seed = seedFor(name, take - 1);
  const reply = await worker.request({ text: spoken, seed, output });
  return { take, seed, output, reply, judgement: judgeAttempt(text, reply, { secondsPerWord }) };
}

/** A take's verdict, as a line: heard exactly, or what the recognisers heard and what else was found. */
export function verdictOf({ judgement, reply }) {
  const heard = Object.entries(judgement.differences)
    .filter(([, differences]) => differences.length > 0)
    .map(([recogniser, differences]) => `${recogniser}: ${differences.join(", ")}`);
  const cut = reply?.repaired ? ` (cut a sound after the last word at ${reply.repaired.cutAt}s)` : "";
  return (judgement.passed ? "heard exactly" : [...heard, ...judgement.problems].join("; ")) + cut;
}
