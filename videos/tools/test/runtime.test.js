import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { brandDir, componentsDir } from "../lib/layout.js";

// The browser runtime the compositions load (shared/brand/*.js), run in a
// sandbox that stands in for the page.

const load = (file, globals) => {
  const window = { ...globals };
  new Function("window", "document", readFileSync(path.join(brandDir, file), "utf8"))(window, globals.document);
  return window;
};

function sceneRuntime(extra = {}) {
  return load("scene.js", { LatticeMotion: { duration: { fast: 0.4, base: 0.6, slow: 0.9 } }, ...extra }).LatticeScene;
}

test("a scene's beats and end are read from its stamped variables", () => {
  const scene = sceneRuntime();
  const timing = scene.cue({ beats: "0.6, 3.66,13.055", end: 35.19 });
  assert.deepEqual(timing.beats, [0.6, 3.66, 13.055]);
  assert.equal(timing.at(1, 99), 3.66);
  assert.equal(timing.at(5, 99), 99, "a beat the script does not reach falls back");
  assert.equal(timing.end, 35.19);
  assert.deepEqual(scene.cue({}).beats, [], "an untimed scene has no beats");
  assert.equal(scene.cue({ end: 0 }).end, null);
  assert.throws(() => scene.cue({ beats: "1,two" }), /beats must be numbers/);
});

test("a scene fades out just before its clip ends, and an untimed scene does not", () => {
  const scene = sceneRuntime();
  const calls = [];
  const tl = { to: (targets, vars, at) => calls.push({ targets, vars, at }) };
  const root = { children: [{ tagName: "DIV" }, { tagName: "STYLE" }, { tagName: "SCRIPT" }, { tagName: "SVG" }] };
  scene.exit(tl, root, { end: 10 });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].targets.map((element) => element.tagName), ["DIV", "SVG"]);
  assert.equal(calls[0].vars.opacity, 0);
  assert.equal(calls[0].at, 9.6);
  scene.exit(tl, root, { end: null });
  assert.equal(calls.length, 1);
});

test("a site: variable is read from the site's own words, anything else is returned as written", () => {
  const scene = sceneRuntime({ LatticeHome: { thesis: "State that lives in your cluster.", seams: { title: "A core plus seams" } } });
  assert.equal(scene.site("site:home.thesis"), "State that lives in your cluster.");
  assert.equal(scene.site("site:home.seams.title"), "A core plus seams");
  assert.equal(scene.site("Plain words"), "Plain words");
  assert.throws(() => scene.site("site:home.missing"), /the site has no 'home.missing'/);
  assert.throws(() => scene.site("site:packages.sections"), /the site has no 'packages.sections'/, "an unloaded source fails loudly");
});

test("ways shows first-episode titles where supplied and keeps the site's audience elsewhere", () => {
  const html = readFileSync(path.join(componentsDir, "ways.html"), "utf8");
  const script = /<script>([\s\S]*?)<\/script>/.exec(html)[1];
  const siteWays = [
    { name: "Build", for: "Writing code" },
    { name: "Evaluate", for: "Deciding whether it fits" },
    { name: "Operate", for: "Running an estate" },
  ];
  for (const episodes of ["", "Hello, Lattice|When Lattice fits, and when it doesn't"]) {
    const elements = new Map();
    const element = () => ({
      textContent: "",
      querySelector(selector) {
        if (!this.children.has(selector)) this.children.set(selector, element());
        return this.children.get(selector);
      },
      children: new Map(),
    });
    const items = [];
    const root = {
      querySelector(selector) {
        if (!elements.has(selector)) elements.set(selector, element());
        return elements.get(selector);
      },
      querySelectorAll: () => items,
    };
    root.querySelector(".wy-ways").appendChild = (item) => items.push(item);
    const timeline = { from() {}, to() {}, set() {} };
    const window = {
      __hyperframes: { getVariables: () => ({ episodes }) },
      LatticeScene: {
        cue: () => ({ at: (index) => index }),
        site: (value) => value === "site:home.ways" ? siteWays : value,
        parts: (target, value) => { target.textContent = value; },
        exit() {},
      },
      LatticeMotion: { duration: { base: 0.6, slow: 0.9 } },
    };
    const document = { querySelector: () => root, createElement: element };
    new Function("window", "document", "gsap", "getComputedStyle", script)(
      window, document, { timeline: () => timeline }, () => ({ getPropertyValue: () => "ink" }),
    );
    assert.deepEqual(items.map((item) => item.querySelector(".wy-way-name").textContent), siteWays.map((way) => way.name));
    assert.deepEqual(
      items.map((item) => item.querySelector(".wy-way-for").textContent),
      episodes ? [...episodes.split("|"), siteWays[2].for] : siteWays.map((way) => way.for),
    );
  }
});

/** Just enough of a DOM element for the code colourer: text in, HTML out. */
const codeElement = (text) => ({ textContent: text, innerHTML: "" });

test("code is coloured in the site's syntax roles: keywords, types, strings and numbers", () => {
  const { LatticeCode } = load("code.js", {});
  const element = codeElement('var user = await lattice.GetAsync<User>("user/42");\nawait lattice.SetAsync("user/42", new User("Ada", 36));');
  LatticeCode.highlight(element);
  const html = element.innerHTML;
  assert.match(html, /^<span class="lv-syn-keyword">var<\/span> user = <span class="lv-syn-keyword">await<\/span> lattice\.GetAsync&lt;<span class="lv-syn-type">User<\/span>&gt;/);
  assert.match(html, /<span class="lv-syn-string">"user\/42"<\/span>/);
  assert.match(html, /<span class="lv-syn-keyword">new<\/span> <span class="lv-syn-type">User<\/span>\(/, "a name after new is a type");
  assert.match(html, /<span class="lv-syn-number">36<\/span>\)\);$/);
  assert.ok(html.includes("\n"), "line breaks survive");
});

test("comments and attributes take their roles, and text is escaped", () => {
  const { LatticeCode } = load("code.js", {});
  const element = codeElement('[GenerateSerializer] // a < b & c\nvar x = "<tag>";');
  LatticeCode.highlight(element);
  assert.match(element.innerHTML, /^\[<span class="lv-syn-meta">GenerateSerializer<\/span>\] /);
  assert.match(element.innerHTML, /<span class="lv-syn-comment">\/\/ a &lt; b &amp; c<\/span>/);
  assert.match(element.innerHTML, /<span class="lv-syn-string">"&lt;tag&gt;"<\/span>/);
});
