import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, test } from "node:test";
import { restoreClip, restoreTakes, saveClip, saveTake } from "../lib/cache.js";
import { cachedChrome, keepWarm } from "../lib/warm.js";

const scratch = mkdtempSync(path.join(tmpdir(), "videos-cache-"));
after(() => rmSync(scratch, { recursive: true, force: true }));
let n = 0;
// An episode's paths as layout.js gives them, with the workspace and the state directory in two places.
const layout = () => {
  const root = path.join(scratch, String(++n));
  return {
    narration: path.join(root, "checkout", "renders", "narration", "ep"),
    takes: path.join(root, "checkout", "renders", "takes", "ep"),
    clipCache: path.join(root, "state", "clips", "ep"),
    takeCache: path.join(root, "state", "takes", "ep"),
  };
};
const write = (file, text) => {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, text);
};

test("a clip and its check record are copied out to the cache, and back into a fresh checkout", () => {
  const made = layout();
  write(path.join(made.narration, "clips", "abc.wav"), "audio");
  write(path.join(made.narration, "clips", "abc.json"), '{"picked":true}');
  saveClip(made, "abc");
  assert.equal(readFileSync(path.join(made.clipCache, "abc.json"), "utf8"), '{"picked":true}');

  const fresh = { ...made, narration: path.join(scratch, "fresh", "narration") };
  assert.equal(restoreClip(fresh, "abc"), true);
  assert.equal(readFileSync(path.join(fresh.narration, "clips", "abc.wav"), "utf8"), "audio");
  assert.equal(readFileSync(path.join(fresh.narration, "clips", "abc.json"), "utf8"), '{"picked":true}', "a pick survives the checkout");
  assert.equal(restoreClip(fresh, "abc"), false, "a clip the checkout has is left alone");
  assert.equal(restoreClip(fresh, "missing"), false);
});

test("takes are copied out as they are made, and back in without overwriting any", () => {
  const made = layout();
  write(path.join(made.takes, "abc", "take1.wav"), "one");
  write(path.join(made.takes, "abc", "take1.json"), "{}");
  write(path.join(made.takes, "abc", "take2.wav"), "two");
  saveTake(made, "abc", 1);
  saveTake(made, "abc", 2);
  const fresh = { ...made, takes: path.join(scratch, "fresh-takes") };
  write(path.join(fresh.takes, "abc", "take2.wav"), "kept");
  assert.equal(restoreTakes(fresh, "abc"), 1);
  assert.ok(existsSync(path.join(fresh.takes, "abc", "take1.json")));
  assert.equal(readFileSync(path.join(fresh.takes, "abc", "take2.wav"), "utf8"), "kept");
  assert.equal(restoreTakes(fresh, "none"), 0);
});

test("the cached headless Chrome is found wherever the CLI put it", () => {
  const root = path.join(scratch, "chrome");
  const name = process.platform === "win32" ? "chrome-headless-shell.exe" : "chrome-headless-shell";
  write(path.join(root, "chrome-headless-shell", "win64-152.0.1", "chrome-headless-shell-win64", name), "");
  assert.equal(cachedChrome(root), path.join(root, "chrome-headless-shell", "win64-152.0.1", "chrome-headless-shell-win64", name));
  assert.equal(cachedChrome(path.join(scratch, "no-chrome")), null);
});

test("warming holds one idle FFmpeg and probes each binary until it is stopped", async () => {
  const started = [];
  const fake = (command, args) => {
    const child = new EventEmitter();
    child.exitCode = null;
    child.killed = false;
    child.kill = () => {
      child.killed = true;
    };
    started.push({ command, args, child });
    return child;
  };
  const stop = keepWarm({ ffmpeg: "ff", ffprobe: "fp", chrome: "ch", everyMs: 10, spawnProcess: fake });
  await new Promise((resolve) => setTimeout(resolve, 35));
  stop();
  const [idle, ...probes] = started;
  assert.equal(idle.command, "ff");
  assert.ok(idle.args.includes("anullsrc=r=8000:cl=mono"));
  assert.ok(probes.filter((p) => p.command === "fp").length >= 2, "ffprobe is probed repeatedly");
  assert.ok(probes.some((p) => p.command === "ch" && p.args[0] === "--version"));
  assert.equal(idle.child.killed, true, "stopping ends the idle FFmpeg");
  const count = started.length;
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.equal(started.length, count, "and the probes");
});
