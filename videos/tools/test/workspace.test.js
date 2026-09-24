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

test("the series voice is local, and is either not yet chosen or one of the auditioned candidates", () => {
  const voice = readJson("voice/voice.json");
  const { candidates } = readJson("voice/candidates.json");
  assert.equal(voice.provider, "kokoro");
  assert.equal(typeof voice.speed, "number");
  assert.ok(voice.voice === null || candidates.some((candidate) => candidate.voice === voice.voice));
  if (voice.voice !== null) {
    assert.equal(voice.lang, candidates.find((candidate) => candidate.voice === voice.voice).lang);
  }
});
