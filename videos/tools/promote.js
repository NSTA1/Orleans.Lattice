#!/usr/bin/env node
// Copies the approved episodes of the video series from main onto a release
// line, so that the documentation site built from that line shows them
// (.github/workflows/promote-videos.yml). Run it from the root of a checkout
// of the line, naming the ref to promote from:
//
//   node <checkout of main>/videos/tools/promote.js --from <ref> [--through <code>] [--message <file>]
//
// An item is approved when a published episode on <ref> lists it: merging an
// episode's pull request is the approval. Promoted are the approved items up
// to and including --through (all of them when it is not given), a gapless
// run from the start of the production order, so the site shows every path
// from its start. For each episode whose every item is promoted it copies,
// whole, the episode's folder, its companion page and its published cut, and
// removes the line's earlier cut; the rest of videos/ is made the same as
// <ref>'s, so the line's workspace can check what it holds; an episode not
// promoted keeps whatever the line had of it; and every companion page is
// written again, so each links to the episodes the line has. It copies rather
// than cherry-picks, because a cherry-pick of an episode's pull request
// conflicts wherever main has moved since the line was cut.
//
// It stages the result and, with --message, writes a commit message for it.
// It commits and pushes nothing: the workflow does, after building the site.
import { spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { episodeProblems, MEDIA, mediaNames } from "./lib/publication.js";
import { doneItems, itemOf, orderProblems } from "./lib/series.js";

const argv = process.argv.slice(2);
const option = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : undefined);
const fail = (message) => {
  console.error(`promote: ${message}`);
  process.exit(1);
};
const run = (args) => spawnSync("git", args, { cwd: root, encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
const git = (...args) => {
  const result = run(args);
  if (result.status !== 0) fail(`git ${args.join(" ")} failed: ${result.stderr.trim()}`);
  return result.stdout;
};
const lines = (text) => text.split("\n").map((line) => line.trim()).filter(Boolean);

const top = spawnSync("git", ["rev-parse", "--show-toplevel"], { encoding: "utf8" });
if (top.status !== 0) fail("run it from inside a checkout of the release line");
const root = top.stdout.trim();
const from = option("--from");
const through = option("--through") || null;
if (!from) fail("name the ref to promote from with --from, such as origin/main");
const fromSha = git("rev-parse", "--verify", `${from}^{commit}`).trim();

// What main has approved, read from the ref itself.
const plan = JSON.parse(git("show", `${fromSha}:videos/series.json`));
const slugsAt = (ref) => {
  const listed = run(["ls-tree", "-d", "--name-only", `${ref}:videos/episodes`]);
  return listed.status === 0 ? lines(listed.stdout) : [];
};
const episodesAt = (ref) =>
  new Map(
    slugsAt(ref).map((slug) => {
      const shown = run(["show", `${ref}:videos/episodes/${slug}/episode.json`]);
      if (shown.status !== 0) return [slug, { meta: null, problems: ["does not exist"] }];
      const meta = JSON.parse(shown.stdout);
      return [slug, { meta, problems: episodeProblems(meta) }];
    }),
  );
const onMain = episodesAt(fromSha);
const problems = orderProblems(plan, onMain);
if (problems.length > 0) fail(`${from} is not in a state to promote from:\n  ${problems.join("\n  ")}`);
const approved = plan.items.filter((item) => doneItems(onMain).has(item.code)).map((item) => item.code);
let promotable = approved;
if (through) {
  if (!itemOf(plan, through)) fail(`'${through}' is not an item of the series plan`);
  const at = approved.indexOf(through);
  if (at < 0) fail(`${through} is not approved on ${from}: its episode's pull request has not merged`);
  promotable = approved.slice(0, at + 1);
}
const promotableSet = new Set(promotable);
const promoted = [...onMain]
  .filter(([, { meta }]) => meta?.published && meta.items.every((code) => promotableSet.has(code)))
  .map(([slug]) => slug);
const onLine = episodesAt("HEAD");

// The workspace as main has it, but for the episodes not promoted.
git("checkout", fromSha, "--", "videos");
const mainFiles = new Set(lines(git("ls-tree", "-r", "--name-only", fromSha, "--", "videos")));
for (const file of lines(git("ls-files", "--", "videos"))) {
  if (!mainFiles.has(file)) git("rm", "-q", "--", file);
}
for (const slug of onMain.keys()) {
  if (promoted.includes(slug)) continue;
  git("rm", "-r", "-q", "--", `videos/episodes/${slug}`);
  if (onLine.has(slug)) git("checkout", "HEAD", "--", `videos/episodes/${slug}`);
}

// Each promoted episode's companion page and published cut, whole.
for (const slug of promoted) {
  git("checkout", fromSha, "--", `docs/videos/${slug}.md`);
  const cut = onMain.get(slug).meta.published.cut;
  const current = Object.values(mediaNames(slug, cut));
  const earlier = new RegExp(`^${slug}-[0-9a-f]{12}\\.(${MEDIA.join("|")})$`);
  for (const file of lines(git("ls-files", "--", "docs-site/media"))) {
    const name = path.posix.basename(file);
    if (earlier.test(name) && !current.includes(name)) git("rm", "-q", "--", file);
  }
  git("checkout", fromSha, "--", ...current.map((name) => `docs-site/media/${name}`));
}

// Every companion page again, with the line's own copy of the tools, so each
// page's Where next links only to episodes the line has.
const companions = spawnSync(process.execPath, [path.join(root, "videos", "tools", "companions.js")], { cwd: path.join(root, "videos"), stdio: "inherit" });
if (companions.status !== 0) fail("the companion pages could not be written again");
git("add", "-A", "--", "videos", "docs/videos", "docs-site/media");

const changed = promoted.filter((slug) => onLine.get(slug)?.meta?.published?.cut !== onMain.get(slug).meta.published.cut);
const staged = lines(git("diff", "--cached", "--name-only"));
const summary = [
  `Promoted from ${from} (${fromSha.slice(0, 12)}): ${promotable.length > 0 ? `${promotable.join(", ")}` : "nothing approved yet"}.`,
  ...promoted.map((slug) => {
    const meta = onMain.get(slug).meta;
    const was = onLine.get(slug)?.meta?.published?.cut;
    const state = !was ? "new on the line" : was === meta.published.cut ? "unchanged" : `re-cut, was ${was}`;
    return `- ${slug} (${meta.items.join(", ")}): cut ${meta.published.cut}, ${state}`;
  }),
  `${staged.length} file(s) staged.`,
];
console.log(summary.join("\n"));

const message = option("--message");
if (message) {
  const scope = through ? `every approved item through ${through}` : "every approved item";
  const subject =
    changed.length > 0
      ? `docs(videos): promote ${changed.map((slug) => onMain.get(slug).meta.items.at(-1)).join(", ")} to the site`
      : `docs(videos): bring the video workspace in step with main`;
  writeFileSync(
    message,
    [
      subject,
      "",
      `Copies the video series' approved episodes (${scope}) from`,
      "main onto this line, so that the documentation site shows them",
      "(.github/workflows/promote-videos.yml). From main at",
      `${fromSha}:`,
      "",
      ...summary.slice(1, -1),
      "",
    ].join("\n"),
  );
}
