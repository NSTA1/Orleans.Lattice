import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { workspaceRoot } from "../lib/hyperframes.js";
import { homePage, homeScript, plainText, textParts } from "../lib/home.js";
import { homeFixture } from "./fixtures.js";

test("text keeps its code spans as parts and loses every other tag", () => {
  assert.deepEqual(textParts("Writing code against <code>ILattice</code>"), [
    ["Writing code against ", false],
    ["ILattice", true],
  ]);
  assert.equal(plainText('<a href="x">Membership</a>, with <a href="y">OIDC</a> &amp; more '), "Membership, with OIDC & more");
});

test("the home page yields its thesis, ways in, journey and seams lede", () => {
  const home = homePage(homeFixture);
  assert.equal(home.thesis, "State that lives in your cluster & converges.");
  assert.deepEqual(home.ways.map((way) => [way.anchor, way.name]), [
    ["build", "Build"],
    ["evaluate", "Evaluate"],
    ["operate", "Operate"],
  ]);
  assert.deepEqual(home.ways[0].for, [
    ["Writing code against ", false],
    ["ILattice", true],
  ]);
  assert.deepEqual(home.paths, { title: "Three ways in" });
  assert.deepEqual(home.journey.stages, [
    {
      name: "Local",
      caption: "One machine, no cloud account.",
      items: [
        { text: "File write-ahead log", inProgress: false },
        { text: "Explorer console", inProgress: true },
      ],
    },
    { name: "Team", caption: "A shared cluster.", items: [{ text: "Membership, with OIDC or Entra ID", inProgress: false }] },
  ]);
  assert.deepEqual(home.journey.invariant, { label: "Programming model", code: "ILattice", note: "unchanged at every stage" });
  assert.deepEqual(home.seams, { title: "A core plus seams", lede: "Storage and identity are companion packages." });
});

test("the script keeps anything in progress off camera", () => {
  const sandbox = {};
  new Function("window", homeScript(homeFixture, ""))(sandbox);
  assert.deepEqual(sandbox.LatticeHome.journey.stages[0].items, ["File write-ahead log"]);
  assert.ok(Object.isFrozen(sandbox.LatticeHome));
});

test("a home page that has changed shape fails loudly instead of putting stale text on camera", () => {
  assert.throws(() => homePage(homeFixture.replace('id="lt-hero-title"', 'id="hero"')), /no longer has the thesis heading/);
  assert.throws(() => homePage(homeFixture.replace(/<a class="lt-way" href="#operate">[^\n]*\n/, "")), /2 way\(s\) in, not three/);
  assert.throws(() => homePage(homeFixture.replace('<ol class="lt-stages">', "<ol>")), /no longer has the deployment journey/);
  assert.throws(() => homePage(homeFixture.replace('class="lt-invariant"', 'class="x"')), /the journey's invariant/);
});

test("the site's own home page reads cleanly, and says what the introduction narrates", () => {
  const home = homePage(readFileSync(path.join(workspaceRoot, "..", "docs-site", "pages", "index.md"), "utf8"));
  assert.ok(home.thesis.length > 20);
  assert.deepEqual(home.ways.map((way) => way.name), ["Build", "Evaluate", "Operate"]);
  assert.deepEqual(home.journey.stages.map((stage) => stage.name), ["Local", "Team", "Global"]);
  assert.equal(home.journey.invariant.code, "ILattice");
  for (const stage of home.journey.stages) {
    assert.ok(stage.items.some((item) => !item.inProgress), `${stage.name} has something released to show`);
  }
});
