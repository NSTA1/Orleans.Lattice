#!/usr/bin/env node
// Takes of a line, to pick by ear. A generative voice reads a line a little
// differently on every take: one says a word wrongly, another rises at the
// end like a question. When a line's reading matters, make several takes and
// pick one:
//
//   npm run audition -- <slug> <cue> [<cue> ...] [--takes N]   make N takes (4 by default) of each cue
//   npm run audition -- <slug> --pick <cue>=<take> [...]       use that take in the narration
//
// Cues are numbered from 1, as cues.json and the listening page number them
// (npm run review -- <slug> --audio). Take n is spoken with the seed narration
// uses for its nth attempt, so the take narration kept is one of them and is
// reused rather than made again. Each take is checked like any narration clip
// (heard back, inspected) and the page shows what the checks found:
// renders/review/<slug>-takes.html.
//
// Picking copies the take into the clip cache as its cue's clip, marked as
// picked, so it stands whatever the recognisers heard; then run
// 'npm run narrate -- <slug>' to master the track again. Takes live under
// renders/takes/ and picks in the clip cache: neither is ever committed. What
// is committed is the published cut.
import { copyFileSync, createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { episodePaths, rendersDir } from "./lib/layout.js";
import { loadLexicon } from "./lib/lexicon.js";
import { takesPage } from "./lib/listening.js";
import { parseScript } from "./lib/narration.js";
import { fileDigest } from "./lib/publication.js";
import { clipNamer, readVoice, speakTake, spokenCues, startChatterbox, verdictOf } from "./lib/series-voice.js";

const fail = (message, code = 1) => {
  console.error(`audition: ${message}`);
  process.exit(code);
};
const usage = "usage: npm run audition -- <slug> <cue> [<cue> ...] [--takes N]   or   npm run audition -- <slug> --pick <cue>=<take> [...]";

const argv = process.argv.slice(2);
let paths;
try {
  paths = episodePaths(argv[0]);
} catch (error) {
  fail(`${error.message}\n${usage}`, 2);
}
const { slug } = paths;
const picking = argv.includes("--pick");
const takesAt = argv.indexOf("--takes");
const takeCount = takesAt >= 0 ? Number(argv[takesAt + 1]) : 4;
if (!Number.isInteger(takeCount) || takeCount < 1 || takeCount > 12) fail(`--takes must be a whole number from 1 to 12\n${usage}`, 2);
const words = argv.slice(1).filter((arg, i, all) => !arg.startsWith("--") && all[i - 1] !== "--takes");

const voice = readVoice();
if (voice.provider !== "chatterbox") fail(`takes are for the Chatterbox voice; voice/voice.json names '${voice.provider}'`, 2);
const settings = voice.chatterbox;
const lexicon = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
const { cues } = parseScript(readFileSync(paths.script, "utf8"), `episodes/${slug}/SCRIPT.md`);
const spoken = spokenCues(cues, lexicon, voice.provider);
const nameOf = clipNamer(voice);
const names = spoken.map((text) => nameOf(text));
const clipsDir = path.join(paths.narration, "clips");
const takeDir = (index) => path.join(paths.takes, names[index]);
const takeFile = (index, take) => path.join(takeDir(index), `take${take}.wav`);
const cueOf = (word) => {
  const number = Number(word);
  if (!Number.isInteger(number) || number < 1 || number > cues.length) fail(`'${word}' is not a cue of '${slug}' (1 to ${cues.length})`, 2);
  return number - 1;
};

if (picking) {
  if (words.length === 0) fail(usage, 2);
  mkdirSync(clipsDir, { recursive: true });
  for (const word of words) {
    const [cueWord, takeWord] = word.split("=");
    const index = cueOf(cueWord);
    const take = Number(takeWord);
    const source = takeFile(index, take);
    if (!existsSync(source)) fail(`cue ${index + 1} has no take ${takeWord}; make it with 'npm run audition -- ${slug} ${index + 1} --takes ${take}'`);
    const record = JSON.parse(readFileSync(source.replace(/\.wav$/, ".json"), "utf8"));
    const target = path.join(clipsDir, `${names[index]}.wav`);
    copyFileSync(source, target);
    writeFileSync(target.replace(/\.wav$/, ".json"), `${JSON.stringify({ ...record, picked: true }, null, 2)}\n`);
    console.log(`audition: cue ${index + 1} now uses take ${take}`);
  }
  console.log(`audition: run 'npm run narrate -- ${slug}' to master the track with the picked take(s)`);
  process.exit(0);
}

if (words.length === 0) fail(usage, 2);
const wanted = [...new Set(words.map(cueOf))].sort((a, b) => a - b);

// Takes already made are kept; narration's kept clip is take n when it was attempt n.
const toMake = [];
for (const index of wanted) {
  mkdirSync(takeDir(index), { recursive: true });
  const clip = path.join(clipsDir, `${names[index]}.wav`);
  const clipRecord = existsSync(clip.replace(/\.wav$/, ".json")) ? JSON.parse(readFileSync(clip.replace(/\.wav$/, ".json"), "utf8")) : null;
  for (let take = 1; take <= takeCount; take++) {
    if (existsSync(takeFile(index, take))) continue;
    if (clipRecord && !clipRecord.picked && !clipRecord.repaired && clipRecord.attempt === take) {
      copyFileSync(clip, takeFile(index, take));
      writeFileSync(takeFile(index, take).replace(/\.wav$/, ".json"), `${JSON.stringify(clipRecord, null, 2)}\n`);
      console.log(`cue ${index + 1}, take ${take}: the take narration kept`);
      continue;
    }
    toMake.push([index, take]);
  }
}

if (toMake.length > 0) {
  mkdirSync(paths.takes, { recursive: true });
  const log = createWriteStream(path.join(paths.takes, "voice.log"));
  let worker;
  try {
    worker = startChatterbox(voice, { log });
  } catch (error) {
    fail(error.message, 2);
  }
  try {
    const ready = await worker.ready;
    console.log(`voice: Chatterbox ready in ${ready.loadSeconds}s; ${toMake.length} take(s) to make`);
    for (const [index, take] of toMake) {
      const output = takeFile(index, take).replace(/\.wav$/, ".partial.wav");
      const made = await speakTake(worker, { text: cues[index].text, spoken: spoken[index], name: names[index], take, output, secondsPerWord: settings.secondsPerWord });
      const record = {
        attempt: take,
        seed: made.seed,
        verified: made.judgement.passed,
        differences: made.judgement.differences,
        problems: made.judgement.problems,
        transcripts: made.reply.transcripts,
        seconds: made.reply.seconds,
        rawSeconds: made.reply.rawSeconds,
        inspected: true,
        repaired: made.reply.repaired,
        artefacts: made.reply.artefacts,
      };
      writeFileSync(takeFile(index, take).replace(/\.wav$/, ".json"), `${JSON.stringify(record, null, 2)}\n`);
      renameSync(output, takeFile(index, take));
      console.log(`cue ${index + 1}, take ${take}: ${made.reply.seconds.toFixed(2)}s in ${made.reply.generateSeconds}s, ${verdictOf(made)}`);
    }
  } catch (error) {
    await worker.close().catch(() => {});
    fail(`the voice failed: ${error.message}; the takes made so far are kept, run the command again to carry on`);
  }
  await worker.close();
  log.end();
}

// The page: every cue with takes of its current reading, in cue order.
const manifestFile = path.join(paths.narration, "cues.json");
const manifest = existsSync(manifestFile) ? JSON.parse(readFileSync(manifestFile, "utf8")) : null;
const title = /^#\s+(.+?)(?:\s+-\s+script)?\s*$/m.exec(readFileSync(paths.script, "utf8"))?.[1] ?? slug;
const reviewDir = path.join(rendersDir, "review");
const fromReview = (file) => `${path.relative(reviewDir, file).split(path.sep).join("/")}?v=${Math.round(statSync(file).mtimeMs)}`;
const listed = cues
  .map((cue, index) => ({ cue, index }))
  .filter(({ index }) => existsSync(takeDir(index)))
  .map(({ cue, index }) => {
    const clip = path.join(clipsDir, `${names[index]}.wav`);
    const inUse = existsSync(clip) ? fileDigest(clip) : null;
    const takes = readdirSync(takeDir(index))
      .map((file) => /^take(\d+)\.wav$/.exec(file))
      .filter(Boolean)
      .map((match) => Number(match[1]))
      .sort((a, b) => a - b)
      .map((take) => {
        const file = takeFile(index, take);
        const record = JSON.parse(readFileSync(file.replace(/\.wav$/, ".json"), "utf8"));
        const judgement = { passed: record.verified, differences: record.differences ?? {}, problems: record.problems ?? [] };
        return { take, src: fromReview(file), seconds: record.seconds, verdict: verdictOf({ judgement, reply: record }), current: inUse === fileDigest(file) };
      });
    return { index: index + 1, start: manifest?.cues?.[index]?.start ?? null, text: cue.text, takes };
  })
  .filter((entry) => entry.takes.length > 0);
mkdirSync(reviewDir, { recursive: true });
writeFileSync(path.join(reviewDir, `${slug}-takes.html`), takesPage({ title, slug, cues: listed, stylesheets: brandSheets(reviewDir) }));
console.log(`audition: renders/review/${slug}-takes.html (${listed.length} cue(s)); pick with 'npm run audition -- ${slug} --pick <cue>=<take>'`);

function brandSheets(from) {
  return ["tokens.css", "fonts.css"].map((file) => path.relative(from, path.join(workspaceRoot, "shared", "brand", "site", file)).split(path.sep).join("/"));
}
