import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { componentsDir } from "../lib/layout.js";

const html = readFileSync(path.join(componentsDir, "choice-list.html"), "utf8");
const script = /<script>([\s\S]*?)<\/script>/.exec(html)[1];

function run(items, { beats = [0.6, 4.2, 8.1], heading = "Choose" } = {}) {
  const element = () => ({
    children: [],
    textContent: "",
    className: "",
    appendChild(child) { this.children.push(child); },
  });
  const headingNode = element();
  const list = element();
  const root = {
    querySelector: (selector) => selector === ".lv-heading" ? headingNode : list,
  };
  const calls = [];
  const timeline = {
    from(target, vars, at) { calls.push({ method: "from", target, vars, at }); },
    to(target, vars, at) { calls.push({ method: "to", target, vars, at }); },
  };
  const window = {
    __hyperframes: { getVariables: () => ({ heading, items: JSON.stringify(items) }) },
    LatticeScene: {
      cue: () => ({ beats, at: (index, fallback) => beats[index] ?? fallback }),
      site: (value) => value === "site:home.thesis" ? "The site's words" : value,
      exit: (tl, target) => { assert.equal(tl, timeline); assert.equal(target, root); },
    },
    LatticeMotion: { duration: { slow: 0.9, base: 0.6 } },
  };
  new Function("window", "document", "gsap", "getComputedStyle", script)(
    window,
    {
      getElementById: (id) => {
        assert.equal(id, "choice-list-root");
        return root;
      },
      createElement: element,
    },
    { timeline: () => timeline },
    () => ({ getPropertyValue: (name) => name === "--lt-marker" ? " marker " : " surface " }),
  );
  return { window, calls, list, headingNode, timeline };
}

test("choice-list reveals choices on narrated beats and moves the current marker", () => {
  const { window, calls, list, timeline } = run([
    { title: "Grow-only", detail: "Keep every member", beat: 0 },
    { title: "Add-wins", detail: "Concurrent add survives", beat: 1 },
    { title: "Remove-wins", detail: "Concurrent removal wins", beat: 2 },
  ]);
  assert.equal(window.__timelines["choice-list"], timeline);
  assert.deepEqual(list.children.map((row) => row.children[1].textContent),
    ["Grow-only", "Add-wins", "Remove-wins"]);
  assert.deepEqual(calls.filter((call) => call.method === "from").slice(1).map((call) => call.at),
    [0.6, 4.2, 8.1]);
  const colours = calls.filter((call) => call.vars.backgroundColor);
  assert.deepEqual(colours.map((call) => [call.vars.backgroundColor, call.at]),
    [["marker", 0.6], ["surface", 4.2], ["marker", 4.2], ["surface", 8.1], ["marker", 8.1]]);
  assert.equal(colours[0].target, list.children[0].children[0]);
});

test("choice-list reads site words and keeps text as text rather than HTML", () => {
  const { list, headingNode } = run([
    { title: "<not markup>", detail: "site:home.thesis", beat: 0 },
  ], { heading: "site:home.thesis" });
  assert.equal(headingNode.textContent, "The site's words");
  assert.equal(list.children[0].children[1].textContent, "<not markup>");
  assert.equal(list.children[0].children[2].textContent, "The site's words");
});

test("choice-list rejects an empty, oversized or malformed choice list", () => {
  for (const items of [null, {}, [], Array(4).fill({ title: "A", detail: "B", beat: 0 })]) {
    assert.throws(() => run(items), /supply one to three items/);
  }
  for (const item of [
    { title: "A", beat: 0 },
    { title: "A", detail: "B", beat: -1 },
    { title: "A", detail: "B", beat: 0.5 },
    { title: "A", detail: "B", beat: 3 },
  ]) {
    assert.throws(() => run([item]), /each item needs title, detail and an in-range beat/);
  }
});

test("choice-list supports an untimed component preview", () => {
  const { calls } = run([
    { title: "First", detail: "", beat: 0 },
    { title: "Second", detail: "", beat: 1 },
  ], { beats: [] });
  assert.deepEqual(calls.filter((call) => call.method === "from").slice(1).map((call) => call.at),
    [0.6, 3.6]);
});
