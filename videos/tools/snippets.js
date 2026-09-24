#!/usr/bin/env node
// Keeps the code shown on screen identical to the compiled snippets on each
// episode's companion page. A companion page (docs/videos/<episode>.md) names a
// snippet with `<!-- video-snippet: <id> -->` directly above a column-0
// ```csharp verify fence, which the repository's Roslyn harness compiles; a
// composition shows it with <code data-snippet="<id>"></code>.
//
//   npm run snippets          rewrite every composition from the pages
//   npm run snippets:check    fail if any composition has drifted (CI)
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { applySnippets, collectSnippets, listFiles } from "./lib/snippets.js";

const check = process.argv.includes("--check");
const repoRoot = path.resolve(workspaceRoot, "..");
const relative = (file) => path.relative(repoRoot, file).split(path.sep).join("/");

const pagesDir = path.join(repoRoot, "docs", "videos");
const pages = existsSync(pagesDir) ? listFiles(pagesDir, (file) => file.endsWith(".md")) : [];
const snippets = collectSnippets(pages.map((file) => ({ source: relative(file), text: readFileSync(file, "utf8") })));

const compositions = [
  path.join(workspaceRoot, "index.html"),
  ...listFiles(path.join(workspaceRoot, "compositions"), (file) => file.endsWith(".html")),
];

const problems = [];
const used = new Set();
let rewritten = 0;
for (const file of compositions) {
  const original = readFileSync(file, "utf8");
  const result = applySnippets(original, snippets);
  result.used.forEach((id) => used.add(id));
  for (const id of result.missing) {
    problems.push(`${relative(file)}: shows snippet '${id}', which no page under docs/videos/ defines`);
  }
  if (result.output !== original) {
    if (check) {
      problems.push(`${relative(file)}: shows code that differs from its companion page; run 'npm run snippets'`);
    } else {
      writeFileSync(file, result.output);
      rewritten++;
    }
  }
}

const unused = [...snippets.keys()].filter((id) => !used.has(id));
console.log(
  `${snippets.size} snippet(s) on ${pages.length} companion page(s); ${used.size} shown on screen` +
    (check ? "" : `; ${rewritten} composition(s) rewritten`),
);
for (const id of unused) {
  console.log(`note: snippet '${id}' (${snippets.get(id).source}) is not shown by any composition`);
}
for (const problem of problems) {
  console.error(`error: ${problem}`);
}
process.exitCode = problems.length === 0 ? 0 : 1;
