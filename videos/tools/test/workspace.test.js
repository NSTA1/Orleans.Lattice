import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { cliEntryPoint, guardArguments, lastJsonObject, quietEnvironment, workspaceRoot } from "../lib/hyperframes.js";
import { loadLexicon } from "../lib/lexicon.js";

const readJson = (relative) => JSON.parse(readFileSync(path.join(workspaceRoot, relative), "utf8"));

test("snapshot never sends frames to a vision model unless explicitly asked", () => {
  assert.deepEqual(guardArguments(["snapshot", "."]), ["snapshot", ".", "--describe", "false"]);
  assert.deepEqual(guardArguments(["snapshot", ".", "--describe", "Is the mark visible?"]), [
    "snapshot",
    ".",
    "--describe",
    "Is the mark visible?",
  ]);
  assert.deepEqual(guardArguments(["snapshot", "--describe=false"]), ["snapshot", "--describe=false"]);
  assert.deepEqual(guardArguments(["render", "."]), ["render", "."]);
});

test("every CLI run is quiet: no telemetry, no update check, no skill install", () => {
  assert.equal(quietEnvironment.HYPERFRAMES_NO_TELEMETRY, "1");
  assert.equal(quietEnvironment.HYPERFRAMES_NO_UPDATE_CHECK, "1");
  assert.equal(quietEnvironment.HYPERFRAMES_SKIP_SKILLS, "1");
  assert.equal(quietEnvironment.DO_NOT_TRACK, "1");
});

test("the CLI resolves from node_modules at the version package.json pins", () => {
  assert.ok(existsSync(cliEntryPoint()));
  const pinned = readJson("package.json").devDependencies.hyperframes;
  assert.match(pinned, /^\d+\.\d+\.\d+$/, "the CLI must be pinned to an exact version");
  const installed = JSON.parse(readFileSync(path.join(path.dirname(cliEntryPoint()), "..", "package.json"), "utf8"));
  assert.equal(installed.version, pinned);
});

test("the last JSON line of CLI output is taken as the result", () => {
  assert.deepEqual(lastJsonObject('progress\n{"ok":false}\nmore\n{"ok":true,"durationSeconds":5.4}\n'), {
    ok: true,
    durationSeconds: 5.4,
  });
  assert.throws(() => lastJsonObject("no json here"), /found none/);
});

test("the pronunciation lexicon is valid", () => {
  assert.ok(loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json")).length > 0);
});

test("every audition candidate names a voice, a language and a description", () => {
  const { candidates } = readJson("voice/candidates.json");
  assert.ok(candidates.length > 0);
  for (const candidate of candidates) {
    assert.match(candidate.voice, /^[a-z]{2}_[a-z]+$/);
    assert.ok(["en-gb", "en-us"].includes(candidate.lang), candidate.voice);
    assert.ok(candidate.label && candidate.style, candidate.voice);
  }
});

test("the series voice is local, on an engine the workspace runs, with each engine's settings complete", () => {
  const voice = readJson("voice/voice.json");
  const { candidates } = readJson("voice/candidates.json");
  assert.ok(["chatterbox", "kokoro"].includes(voice.provider), voice.provider);
  assert.ok(voice[voice.provider], `voice.json has no settings for its provider '${voice.provider}'`);

  const { kokoro } = voice;
  assert.equal(typeof kokoro.speed, "number");
  const candidate = candidates.find((c) => c.voice === kokoro.voice);
  assert.ok(candidate, `the Kokoro voice ${kokoro.voice} is one of the auditioned candidates`);
  assert.equal(kokoro.lang, candidate.lang);

  const { chatterbox } = voice;
  assert.match(chatterbox.revision, /^[0-9a-f]{40}$/, "the Chatterbox model is pinned to one revision");
  assert.ok(existsSync(path.join(workspaceRoot, chatterbox.reference)), "the voice's reference clip is in the repository");
  for (const setting of ["exaggeration", "cfgWeight", "temperature"]) assert.equal(typeof chatterbox[setting], "number", setting);
  assert.ok(Number.isInteger(chatterbox.attempts) && chatterbox.attempts >= 1);
  assert.ok(chatterbox.recognisers.length > 0, "every clip is heard back by at least one recogniser");
  assert.ok(chatterbox.secondsPerWord.min < chatterbox.secondsPerWord.max);
  assert.ok(existsSync(path.join(workspaceRoot, "voice", "requirements.txt")), "the voice's Python environment is pinned");
});
