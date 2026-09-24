import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, test } from "node:test";
import { workspaceRoot } from "../lib/hyperframes.js";
import {
  brandSiteDir,
  compositionFiles,
  componentsDir,
  episodePaths,
  episodesDir,
  isSlug,
  listEpisodes,
  listFiles,
  sharedDir,
  workspaceIndex,
} from "../lib/layout.js";

const scratch = mkdtempSync(path.join(tmpdir(), "videos-layout-"));
after(() => rmSync(scratch, { recursive: true, force: true }));

test("an episode slug is kebab-case", () => {
  for (const slug of ["introduction", "hello-lattice", "crdt-90s"]) assert.ok(isSlug(slug), slug);
  for (const slug of ["Introduction", "hello_lattice", "-intro", "intro-", "a--b", "", undefined, "../x"]) {
    assert.ok(!isSlug(slug), String(slug));
  }
});

test("everything an episode owns sits in its own folder, and its narration under renders/", () => {
  const paths = episodePaths("introduction");
  assert.equal(paths.dir, path.join(episodesDir, "introduction"));
  for (const file of [paths.brief, paths.script, paths.storyboard, paths.composition, paths.assets]) {
    assert.equal(path.dirname(file), paths.dir, file);
  }
  assert.equal(path.basename(paths.composition), "composition.html");
  assert.equal(paths.narration, path.join(workspaceRoot, "renders", "narration", "introduction"));
  assert.throws(() => episodePaths("../escape"), /not an episode slug/);
});

test("the shared material lives under shared/, and the site copy inside the brand seam", () => {
  assert.equal(sharedDir, path.join(workspaceRoot, "shared"));
  assert.equal(componentsDir, path.join(sharedDir, "components"));
  assert.equal(brandSiteDir, path.join(sharedDir, "brand", "site"));
});

test("episodes are the slug-named folders under episodes/, in order", () => {
  const root = path.join(scratch, "episodes");
  for (const dir of ["hello-lattice", "introduction", "Not_A_Slug"]) mkdirSync(path.join(root, dir), { recursive: true });
  writeFileSync(path.join(root, "notes.md"), "not an episode");
  assert.deepEqual(listEpisodes(root), ["hello-lattice", "introduction"]);
  assert.deepEqual(listEpisodes(path.join(scratch, "absent")), []);
});

test("files are listed recursively and in order, skipping node_modules", () => {
  const root = path.join(scratch, "files");
  for (const file of ["b.html", "a/c.html", "a/d.txt", "node_modules/e.html"]) {
    mkdirSync(path.dirname(path.join(root, file)), { recursive: true });
    writeFileSync(path.join(root, file), "");
  }
  assert.deepEqual(
    listFiles(root, (file) => file.endsWith(".html")).map((file) => path.relative(root, file).split(path.sep).join("/")),
    ["a/c.html", "b.html"],
  );
  assert.deepEqual(listFiles(path.join(scratch, "absent"), () => true), []);
});

test("the compositions are the smoke test, the shared components and each episode's composition", () => {
  const files = compositionFiles();
  assert.equal(files[0], workspaceIndex);
  assert.ok(files.includes(path.join(componentsDir, "title-card.html")));
  assert.ok(files.includes(path.join(componentsDir, "join-diagram.html")));
  for (const slug of listEpisodes()) {
    const { composition } = episodePaths(slug);
    assert.equal(files.includes(composition), existsSync(composition), slug);
  }
});

// The root holds the workspace's own files; an episode's material goes in its
// folder, and anything shared goes in shared/ (README.md, "Layout").
const ROOT_FILES = new Set([
  "README.md",
  "series.md",
  "frame.md",
  "index.html",
  "hyperframes.json",
  "meta.json",
  "package.json",
  "package-lock.json",
]);
const ROOT_DIRECTORIES = new Set(["episodes", "shared", "tools", "voice"]);
const IGNORED = new Set(["node_modules", "renders", "snapshots"]);

test("the workspace root holds workspace files only: no episode material and no media", () => {
  const strays = readdirSync(workspaceRoot, { withFileTypes: true })
    .filter((entry) => !entry.name.startsWith(".") && !IGNORED.has(entry.name))
    .filter((entry) => !(entry.isDirectory() ? ROOT_DIRECTORIES : ROOT_FILES).has(entry.name))
    .map((entry) => entry.name);
  assert.deepEqual(strays, [], "put episode material in episodes/<slug>/ and anything reused in shared/");
});

test("every episode folder holds at least its brief and its script", () => {
  for (const slug of listEpisodes()) {
    const { brief, script } = episodePaths(slug);
    assert.ok(existsSync(brief), `episodes/${slug}/BRIEF.md`);
    assert.ok(existsSync(script), `episodes/${slug}/SCRIPT.md`);
  }
});
