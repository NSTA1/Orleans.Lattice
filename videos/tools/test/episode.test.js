import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, test } from "node:test";
import { defaultIndexBackup, recoverWorkspaceIndex, takeEpisodeArgument, withEpisode } from "../lib/episode.js";
import { workspaceRoot } from "../lib/hyperframes.js";
import { rendersDir } from "../lib/layout.js";

const scratch = mkdtempSync(path.join(tmpdir(), "videos-episode-"));
after(() => rmSync(scratch, { recursive: true, force: true }));

/** A stand-in workspace: its own index.html, an episode composition, and where the backup goes. */
function workspace(name) {
  const dir = path.join(scratch, name);
  mkdirSync(path.join(dir, "episodes", "pilot"), { recursive: true });
  const files = {
    index: path.join(dir, "index.html"),
    composition: path.join(dir, "episodes", "pilot", "composition.html"),
    backup: path.join(dir, "renders", ".workspace-index.html"),
  };
  writeFileSync(files.index, "workspace");
  writeFileSync(files.composition, "episode");
  return files;
}

const read = (file) => readFileSync(file, "utf8");

test("--episode is taken out of the arguments in either form, and the rest pass through in order", () => {
  assert.deepEqual(takeEpisodeArgument(["check", ".", "--episode", "introduction", "--json"]), {
    slug: "introduction",
    args: ["check", ".", "--json"],
  });
  assert.deepEqual(takeEpisodeArgument(["snapshot", "--episode=introduction", "--at", "3"]), {
    slug: "introduction",
    args: ["snapshot", "--at", "3"],
  });
  assert.deepEqual(takeEpisodeArgument(["lint", "."]), { slug: null, args: ["lint", "."] });
});

test("--episode needs a value, and only one", () => {
  assert.throws(() => takeEpisodeArgument(["check", "--episode"]), /needs an episode slug/);
  assert.throws(() => takeEpisodeArgument(["check", "--episode", "--json"]), /needs an episode slug/);
  assert.throws(() => takeEpisodeArgument(["check", "--episode", "a", "--episode=b"]), /given once/);
});

test("the backup waits in renders/, never at the project root where lint would see a second composition", () => {
  assert.equal(path.dirname(defaultIndexBackup), rendersDir);
  assert.notEqual(path.dirname(defaultIndexBackup), workspaceRoot);
});

test("the episode stands in as index.html for the task, and the workspace's index.html is back afterwards", async () => {
  const files = workspace("swap");
  const seen = await withEpisode("pilot", async () => read(files.index), files);
  assert.equal(seen, "episode");
  assert.equal(read(files.index), "workspace");
  assert.ok(!existsSync(files.backup), "the backup is removed once restored");
});

test("the workspace's index.html is restored when the task fails", async () => {
  const files = workspace("failing");
  await assert.rejects(
    withEpisode("pilot", async () => {
      throw new Error("render failed");
    }, files),
    /render failed/,
  );
  assert.equal(read(files.index), "workspace");
  assert.ok(!existsSync(files.backup));
});

test("an episode without a composition is refused before anything is touched", async () => {
  const files = workspace("missing");
  rmSync(files.composition);
  await assert.rejects(withEpisode("pilot", async () => "ran", files), /has no composition/);
  assert.equal(read(files.index), "workspace");
  assert.ok(!existsSync(files.backup));
});

test("a workspace index.html left swapped out by an interrupted run is put back", async () => {
  const files = workspace("interrupted");
  mkdirSync(path.dirname(files.backup), { recursive: true });
  writeFileSync(files.backup, "workspace");
  writeFileSync(files.index, "episode left behind");
  assert.equal(recoverWorkspaceIndex(files), true);
  assert.equal(read(files.index), "workspace");
  assert.equal(recoverWorkspaceIndex(files), false, "nothing to put back the second time");

  writeFileSync(files.backup, "workspace");
  writeFileSync(files.index, "episode left behind");
  const seen = await withEpisode("pilot", async () => read(files.index), files);
  assert.equal(seen, "episode");
  assert.equal(read(files.index), "workspace", "the original survives a swap that starts over a stale one");
});
