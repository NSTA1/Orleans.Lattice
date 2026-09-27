import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { PATHS } from "../lib/publication.js";
import {
  doneItems,
  endingProblems,
  endingText,
  endingTitles,
  expectedCode,
  linksOf,
  markdownProblems,
  nextItem,
  orderProblems,
  planFile,
  planProblems,
  publishedEndingProblems,
  readEpisodes,
  readPlan,
  tablesOf,
  whereNextMarkdown,
} from "../lib/series.js";

// A small plan with every kind of item: a front door and its re-cut, two
// paths, a deep dive, and a hold at the end.
const THEN = {
  build: { for: "if you build", then: { title: "Samples", site: "samples/index.html" } },
  evaluate: { for: "if you evaluate", then: { path: "secure" } },
  operate: { for: "if you operate", then: { path: "secure" } },
  secure: { then: { title: "Security", page: "docs/lattice/security.md" } },
};
const makePlan = () => ({
  paths: PATHS.map((id) => ({ id, title: id[0].toUpperCase() + id.slice(1), ...(THEN[id] ?? {}) })),
  holds: { later: { label: "the console", reason: "it is not ready" } },
  items: [
    { code: "F", episode: "front", title: "Front", path: "front-door", order: 1 },
    { code: "B1", episode: "b-one", title: "Build one", path: "build", order: 1, introduces: [{ title: "Quick start", page: "README.md#quick-start" }] },
    { code: "E1", episode: "e-one", title: "Evaluate one", path: "evaluate", order: 1, introduces: [{ title: "Consistency", page: "docs/lattice/consistency.md" }] },
    { code: "F2", recut: "F", title: "Front, naming them", names: ["B1", "E1"] },
    { code: "B2", episode: "b-two", title: "Build two", path: "build", order: 2, leadsTo: "H1" },
    { code: "H1", episode: "h-one", title: "Deep one", path: "how-it-works", order: 1 },
    { code: "O1", episode: "o-one", title: "Operate one", path: "operate", order: 1, hold: "later", introduces: [{ title: "WAL", page: "docs/lattice/wal.md" }] },
    { code: "S1", episode: "s-one", title: "Secure one", path: "secure", order: 1, hold: "later" },
  ],
});

// Episodes as readEpisodes returns them: slug -> { meta, problems }.
const episodes = (...entries) =>
  new Map(
    entries.map(([slug, path, order, items, published = true]) => [
      slug,
      { meta: { path, order, items, poster: { scene: "opening" }, ...(published ? { published: { cut: "0123456789ab", bytes: 1 } } : {}) }, problems: [] },
    ]),
  );

test("the series plan, series.md and the published episodes agree", () => {
  const plan = readPlan();
  assert.deepEqual(markdownProblems(plan, readFileSync(planFile, "utf8")), []);
  const found = readEpisodes();
  assert.deepEqual(orderProblems(plan, found), []);
  assert.deepEqual(publishedEndingProblems(plan, found), []);
  assert.ok(doneItems(found).has("F"), "the introduction is published as F");
});

test("a small plan with every kind of item has no problems", () => {
  assert.deepEqual(planProblems(makePlan()), []);
});

test("an episode's code is its path's letter and its place on the path", () => {
  assert.equal(expectedCode("front-door", 1), "F");
  assert.equal(expectedCode("build", 3), "B3");
  assert.equal(expectedCode("how-it-works", 2), "H2");
  const plan = makePlan();
  plan.items[1].code = "B9";
  assert.match(planProblems(plan).join("\n"), /number 1 on build is coded B1/);
});

test("the plan keeps its order rules", () => {
  const swapped = makePlan();
  [swapped.items[4], swapped.items[5]] = [swapped.items[5], swapped.items[4]];
  assert.match(planProblems(swapped).join("\n"), /H1 must come straight after B2, which leads to it/);

  const early = makePlan();
  early.items.splice(3, 1);
  early.items.splice(1, 0, { code: "F2", recut: "F", title: "too soon", names: ["B1", "E1"] });
  assert.match(planProblems(early).join("\n"), /F2 names B1, so it must come after it/);

  const backwards = makePlan();
  backwards.items[1].order = 2;
  backwards.items[1].code = "B2";
  backwards.items[4].order = 1;
  backwards.items[4].code = "B1";
  backwards.items[3].names = ["B2", "E1"];
  assert.match(planProblems(backwards).join("\n"), /build: its episodes must be made in the order they are watched/);

  const orphan = makePlan();
  delete orphan.items[4].leadsTo;
  assert.match(planProblems(orphan).join("\n"), /H1: no episode leads to it/);
});

test("the plan names real pages, known holds, and each episode once", () => {
  const plan = makePlan();
  plan.items[1].introduces.push({ title: "Nowhere", page: "docs/nowhere.md" });
  plan.items[7].hold = "someday";
  plan.items[2].episode = "b-one";
  const problems = planProblems(plan).join("\n");
  assert.match(problems, /docs\/nowhere\.md does not exist/);
  assert.match(problems, /hold 'someday' is not one of the plan's holds/);
  assert.match(problems, /the episode 'b-one' is made by two items/);
});

test("what is done is what the published episodes list, and the next item is the first that is not", () => {
  const plan = makePlan();
  const found = episodes(["front", "front-door", 1, ["F"]], ["b-one", "build", 1, ["B1"]], ["e-one", "evaluate", 1, ["E1"], false]);
  assert.deepEqual([...doneItems(found)], ["F", "B1"], "an unpublished episode completes nothing");
  assert.deepEqual(orderProblems(plan, found), []);
  assert.equal(nextItem(plan, doneItems(found)).item.code, "E1");
});

test("the next item can be held, and nothing is next when everything is done", () => {
  const plan = makePlan();
  const next = nextItem(plan, new Set(["F", "B1", "E1", "F2", "B2", "H1"]));
  assert.equal(next.item.code, "O1");
  assert.equal(next.hold.label, "the console");
  assert.equal(nextItem(plan, new Set(plan.items.map((item) => item.code))), null);
});

test("an item published ahead of an earlier one breaks the order", () => {
  const plan = makePlan();
  const found = episodes(["front", "front-door", 1, ["F"]], ["e-one", "evaluate", 1, ["E1"]]);
  const problems = orderProblems(plan, found).join("\n");
  assert.match(problems, /E1 \(Evaluate one\) is published, but B1 \(Build one\) comes before it/);
});

test("a re-cut is done when its episode's published cut lists it", () => {
  const plan = makePlan();
  const found = episodes(["front", "front-door", 1, ["F", "F2"]], ["b-one", "build", 1, ["B1"]], ["e-one", "evaluate", 1, ["E1"]]);
  assert.deepEqual(orderProblems(plan, found), []);
  assert.equal(nextItem(plan, doneItems(found)).item.code, "B2");
});

test("a held item cannot be published, and an episode must be where the plan puts it", () => {
  const plan = makePlan();
  plan.items = plan.items.filter((item) => item.code !== "S1");
  plan.items.splice(1, 0, plan.items.splice(plan.items.findIndex((item) => item.code === "O1"), 1)[0]);
  const held = orderProblems(plan, episodes(["front", "front-door", 1, ["F"]], ["o-one", "operate", 1, ["O1"]])).join("\n");
  assert.match(held, /O1 is published while it is held for the console/);

  const misplaced = orderProblems(makePlan(), episodes(["front", "build", 1, ["F"]])).join("\n");
  assert.match(misplaced, /says build number 1, and series\.json has F as front-door number 1/);

  const unlisted = orderProblems(makePlan(), episodes(["front", "front-door", 1, ["B1"]])).join("\n");
  assert.match(unlisted, /lists B1, which is an item of 'b-one'/);
  assert.match(unlisted, /is published, so its items must include F/);

  const stranger = orderProblems(makePlan(), episodes(["stray", "build", 9, ["B1"]])).join("\n");
  assert.match(stranger, /episodes\/stray\/ is not an episode of the plan/);
});

test("series.md's tables are read for codes, titles, pages, leads and the production order", () => {
  const [table] = tablesOf("| # | Episode |\n| --- | --- |\n| B1 | **Build one** (first) |\n\nprose");
  assert.deepEqual(table, { header: ["#", "Episode"], rows: [["B1", "**Build one** (first)"]] });
  assert.deepEqual(linksOf("[Quick start](../README.md#quick-start), [API](../docs/lattice/api.md)"), [
    { title: "Quick start", page: "README.md#quick-start" },
    { title: "API", page: "docs/lattice/api.md" },
  ]);

  const markdown = [
    "## Episodes",
    "",
    "| # | Episode | Idea | Introduces | Leads to |",
    "| --- | --- | --- | --- | --- |",
    "| F | **Front** (published) | x | | |",
    "| B1 | **Build one** (first) | x | [Quick start](../README.md#quick-start) | |",
    "| E1 | Evaluate one | x | [Consistency](../docs/lattice/consistency.md) | |",
    "| B2 | Build two | x | | H1 |",
    "| O1 | Operate one | x | [WAL](../docs/lattice/wal.md) | |",
    "| S1 | Secure one | x | | |",
    "",
    "| # | Episode | Idea | Introduces | Reached from |",
    "| --- | --- | --- | --- | --- |",
    "| H1 | Deep one | x | | B2 |",
    "",
    "## Production order",
    "",
    "| Step | Items | Completes |",
    "| --- | --- | --- |",
    "| Pilot | F | |",
    "| 1 | B1, E1, F2 | |",
    "| 2 | B2, H1 | |",
    "| Held for the console | O1, S1 | |",
    "",
  ].join("\n");
  assert.deepEqual(markdownProblems(makePlan(), markdown), []);

  const drifted = markdown
    .replace("| Build two |", "| Build 2 |")
    .replace("| H1 | Deep one | x | | B2 |", "| H1 | Deep one | x | | E1 |")
    .replace("| 2 | B2, H1 |", "| 2 | H1, B2 |")
    .replace("| Held for the console | O1, S1 |", "| 3 | O1, S1 |");
  const problems = markdownProblems(makePlan(), drifted).join("\n");
  assert.match(problems, /B2: series\.md calls it 'Build 2' and series\.json 'Build two'/);
  assert.match(problems, /H1: series\.md says it is reached from E1, and series\.json from B2/);
  assert.match(problems, /series\.md's production order is F, B1, E1, F2, H1, B2/);
  assert.match(problems, /O1: series\.md does not hold it, and series\.json holds it for the console/);
});

test("a companion page's Where next links an episode once it is published", () => {
  const plan = makePlan();
  assert.equal(
    whereNextMarkdown(plan, "B1", new Set()),
    "- **Next on Build:** Build two\n- **The pages it introduces:** [Quick start](../../README.md#quick-start)",
  );
  assert.equal(
    whereNextMarkdown(plan, "B1", new Set(["b-two"])),
    "- **Next on Build:** [Build two](b-two.md)\n- **The pages it introduces:** [Quick start](../../README.md#quick-start)",
  );
  assert.equal(
    whereNextMarkdown(plan, "B2", new Set(["h-one"])),
    "- **Next:** the documentation site's [Samples](https://nsta1.github.io/Orleans.Lattice/samples/index.html) page\n- **The deep dive:** [Deep one](h-one.md)",
  );
  assert.equal(
    whereNextMarkdown(plan, "E1", new Set()),
    "- **Next:** Secure, starting with Secure one\n- **The pages it introduces:** [Consistency](../lattice/consistency.md)",
  );
  assert.equal(whereNextMarkdown(plan, "S1", new Set()), "- **Next:** [Security](../lattice/security.md)");
});

test("the front door's Where next names the three ways in, and each first episode once it is out", () => {
  const plan = makePlan();
  assert.equal(
    whereNextMarkdown(plan, "F", new Set(["b-one"])),
    [
      "- **Build**, if you build: watch [Build one](b-one.md), then read [Quick start](../../README.md#quick-start).",
      "- **Evaluate**, if you evaluate: read [Consistency](../lattice/consistency.md).",
      "- **Operate**, if you operate: read [WAL](../lattice/wal.md).",
    ].join("\n"),
  );
});

test("a deep dive sends the viewer back to the path that led to it", () => {
  const plan = makePlan();
  plan.items.splice(6, 0, { code: "B3", episode: "b-three", title: "Build three", path: "build", order: 3 });
  assert.equal(whereNextMarkdown(plan, "H1", new Set()), "- **Back to Build:** Build three");
  assert.equal(endingText(plan, "H1"), "Back on Build, next: Build three. The links are on this episode's page in the documentation.");
});

test("an ending names what comes next and the deep dive, and the front door what its re-cuts added", () => {
  const plan = makePlan();
  assert.deepEqual(endingTitles(plan, "B2"), ["Samples", "Deep one"]);
  assert.deepEqual(endingTitles(plan, "F", ["F"]), [], "the first cut of the front door names no episode");
  assert.deepEqual(endingTitles(plan, "F", ["F", "F2"]), ["Build one", "Evaluate one"]);
  assert.equal(
    endingText(plan, "F", ["F", "F2"]),
    "The documentation has three ways in. Build, if you build: start with Build one. Evaluate, if you evaluate: start with Evaluate one. Operate, if you operate. You choose.",
  );
  assert.equal(
    endingText(plan, "B1"),
    "Next on Build: Build two. The links are on this episode's page in the documentation.",
  );
});

test("a published episode's closing scene must name what the plan says it leads to", () => {
  const plan = makePlan();
  const script = (closing) => `# x\n\n## Narration\n\n### Opening\n\nHello.\n\n### Where next\n\n${closing}\n`;
  assert.deepEqual(endingProblems(plan, "B2", script("Next, the Samples page. And the deep dive: Deep one.")), []);
  assert.deepEqual(endingProblems(plan, "F", script("Build, Evaluate or Operate. You choose."), { items: ["F"] }), []);
  const problems = endingProblems(plan, "F", script("Build starts with Build one."), { items: ["F", "F2"], source: "front/SCRIPT.md" });
  assert.deepEqual(problems, ["front/SCRIPT.md: its closing scene ('Where next') does not name 'Evaluate one', which the plan says it leads to"]);
});
