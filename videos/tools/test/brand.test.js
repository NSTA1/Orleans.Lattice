import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, test } from "node:test";
import {
  brandCommands,
  cameraStacks,
  faceFamily,
  fontFaces,
  fontSources,
  joinFiguresScript,
  primaryFamily,
  syncBrand,
} from "../lib/brand.js";
import { homeFixture, packagesFixture } from "./fixtures.js";

const scratch = mkdtempSync(path.join(tmpdir(), "videos-brand-"));
after(() => rmSync(scratch, { recursive: true, force: true }));

const mainCss = `
@font-face {
  font-family: "Recursive Sans Linear";
  src: url("fonts/recursive-sans-linear.woff2") format("woff2");
  font-weight: 300 850;
  font-display: swap;
}
body { color: red; }
@font-face {
  font-family: "Lattice Mono";
  src: url("fonts/cascadia-mono.woff2") format("woff2");
}
`;

const tokensCss = `:root {
  --lt-font-sans: "Recursive Sans Linear", "Segoe UI", Arial, sans-serif;
  --lt-font-mono: "Lattice Mono", "Cascadia Mono", Menlo, monospace;
  --lt-ink: #15191f;
}
`;

const figuresJson = JSON.stringify({ figures: [{ id: "gcounter", layout: "diamond" }] });

/**
 * A complete docs-site stand-in, inside its own repository root that also
 * holds PACKAGES.md; `omit` drops files, `extra` adds or replaces them.
 */
function docsSite(name, { omit = [], extra = {} } = {}) {
  const files = {
    "template/public/tokens.css": tokensCss,
    "template/public/main.css": mainCss,
    "template/public/lattice-mark.svg": "<svg/>",
    "template/public/fonts/recursive-sans-linear.woff2": "font",
    "template/public/fonts/cascadia-mono.woff2": "font",
    "template/public/fonts/OFL-Recursive.txt": "licence",
    "figures/join-figures.json": figuresJson,
    "pages/index.md": homeFixture,
    "../PACKAGES.md": packagesFixture,
    ...extra,
  };
  const dir = path.join(scratch, name, "docs-site");
  for (const [file, content] of Object.entries(files)) {
    if (omit.includes(file)) continue;
    mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
    writeFileSync(path.join(dir, file), content);
  }
  return dir;
}

test("only the @font-face rules are taken, and every one renders with font-display: block", () => {
  const faces = fontFaces(mainCss);
  assert.equal(faces.length, 2);
  assert.ok(faces.every((face) => face.includes("font-display: block")));
  assert.ok(!faces.join("").includes("swap"));
  assert.ok(!faces.join("").includes("color: red"));
  assert.deepEqual(faces.map(faceFamily), ["Recursive Sans Linear", "Lattice Mono"]);
});

test("the font files a rule loads are listed", () => {
  assert.deepEqual(fontSources(fontFaces(mainCss)), ["fonts/recursive-sans-linear.woff2", "fonts/cascadia-mono.woff2"]);
});

test("a stack's primary family is its first, unquoted", () => {
  assert.equal(primaryFamily(tokensCss, "--lt-font-sans"), "Recursive Sans Linear");
  assert.throws(() => primaryFamily(tokensCss, "--lt-font-serif"), /do not define --lt-font-serif/);
});

test("the camera stacks keep the site's self-hosted family and a generic, and drop every fallback", () => {
  assert.deepEqual(cameraStacks(tokensCss, fontFaces(mainCss)), {
    "--lv-font-sans": '"Recursive Sans Linear", sans-serif',
    "--lv-font-mono": '"Lattice Mono", monospace',
  });
});

test("a site stack led by a family it does not self-host fails loudly", () => {
  assert.throws(() => cameraStacks(tokensCss, fontFaces(mainCss).slice(0, 1)), /leads with "Lattice Mono"/);
});

test("the join figures become a script that defines the scenarios before any component mounts", () => {
  const script = joinFiguresScript(figuresJson);
  const sandbox = {};
  new Function("window", script)(sandbox);
  assert.equal(sandbox.LatticeJoinFigures.figures[0].id, "gcounter");
  assert.ok(Object.isFrozen(sandbox.LatticeJoinFigures));
  assert.throws(() => joinFiguresScript('{"figures": []}'), /lists no figures/);
});

test("the design system is copied: tokens, font rules and camera stacks, fonts and licences, the mark, the figures, the packages", () => {
  const to = path.join(scratch, "complete-out");
  const result = syncBrand({ docsSite: docsSite("complete"), target: to });
  assert.deepEqual({ faces: result.faces, files: result.files }, { faces: 2, files: 3 });
  assert.equal(readFileSync(path.join(to, "tokens.css"), "utf8"), tokensCss);
  const generated = readFileSync(path.join(to, "fonts.css"), "utf8");
  assert.match(generated, /font-family: "Lattice Mono"/);
  assert.match(generated, /--lv-font-sans: "Recursive Sans Linear", sans-serif;/);
  assert.ok(!generated.includes("Segoe"), "no fallback family may reach the renderer");
  for (const file of ["fonts/recursive-sans-linear.woff2", "fonts/OFL-Recursive.txt", "lattice-mark.svg", "join-figures.js", "packages.js", "home.js"]) {
    assert.ok(existsSync(path.join(to, file)), file);
  }
  const sandbox = {};
  new Function("window", readFileSync(path.join(to, "packages.js"), "utf8"))(sandbox);
  assert.deepEqual(sandbox.LatticePackages.sections, [{ name: "Core", lede: "The core package.", packages: ["Orleans.Lattice"] }]);
  new Function("window", readFileSync(path.join(to, "home.js"), "utf8"))(sandbox);
  assert.equal(sandbox.LatticeHome.thesis, "State that lives in your cluster & converges.");
});

test("a stale copy is replaced, not merged", () => {
  const to = path.join(scratch, "fresh-out");
  mkdirSync(path.join(to, "fonts"), { recursive: true });
  writeFileSync(path.join(to, "fonts", "removed-upstream.woff2"), "old");
  syncBrand({ docsSite: docsSite("fresh"), target: to });
  assert.ok(!existsSync(path.join(to, "fonts", "removed-upstream.woff2")));
});

test("every command that loads a composition syncs the design system first; the others do not", () => {
  for (const command of ["preview", "lint", "check", "snapshot", "render"]) {
    assert.ok(brandCommands.has(command), command);
  }
  for (const command of ["tts", "transcribe", "doctor", "browser", "skills"]) {
    assert.ok(!brandCommands.has(command), command);
  }
});

test("every missing part of the design system fails loudly instead of rendering off-brand", () => {
  for (const part of [
    "template/public/tokens.css",
    "template/public/main.css",
    "template/public/lattice-mark.svg",
    "figures/join-figures.json",
    "pages/index.md",
    "../PACKAGES.md",
  ]) {
    const site = docsSite(`missing-${path.basename(part)}`, { omit: [part] });
    assert.throws(() => syncBrand({ docsSite: site, target: path.join(scratch, "x") }), /does not exist/, part);
  }
});

test("a rule that loads a missing font file fails loudly", () => {
  const site = docsSite("broken-font", { omit: ["template/public/fonts/recursive-sans-linear.woff2"] });
  assert.throws(() => syncBrand({ docsSite: site, target: path.join(scratch, "y") }), /recursive-sans-linear\.woff2, which does not exist/);
});