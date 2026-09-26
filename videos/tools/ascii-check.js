#!/usr/bin/env node
// Fails on any non-ASCII character in the workspace's text files. Plain ASCII
// is the house rule, and the repository's hygiene gates reject em-dashes and
// mojibake in every tracked file. The usual sources are imported HyperFrames
// blocks, generated agent files and model-written scripts. When a symbol must
// appear on screen, write it as an HTML entity (&#8852; for a join) so the
// source stays ASCII.
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { brandSiteDir } from "./lib/layout.js";

const TEXT_EXTENSIONS = new Set([".html", ".css", ".js", ".json", ".md", ".txt", ".vtt", ".srt", ".jsonl", ".py"]);
const SKIPPED_DIRECTORIES = new Set(["node_modules", "renders", "snapshots"]);
// The copy of the site's design system (tools/lib/brand.js) is ignored by git;
// its source files are already held to these rules by the repository's gates.
const SKIPPED_PATHS = new Set([brandSiteDir]);

const files = [];
const walk = (directory) => {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      if (!SKIPPED_DIRECTORIES.has(entry.name) && !SKIPPED_PATHS.has(full)) walk(full);
    } else if (TEXT_EXTENSIONS.has(path.extname(entry.name).toLowerCase())) {
      files.push(full);
    }
  }
};
walk(workspaceRoot);

const violations = [];
for (const file of files) {
  const lines = readFileSync(file, "utf8").split(/\r?\n/);
  lines.forEach((line, index) => {
    for (let column = 0; column < line.length; column++) {
      const code = line.charCodeAt(column);
      if (code > 0x7e || (code < 0x20 && code !== 0x09)) {
        const hex = code.toString(16).toUpperCase().padStart(4, "0");
        const where = path.relative(workspaceRoot, file).split(path.sep).join("/");
        violations.push(`${where}:${index + 1}:${column + 1}: U+${hex}`);
      }
    }
  });
}

if (files.length === 0) {
  console.error("ascii-check: scanned no files; the workspace walk is broken");
  process.exitCode = 1;
} else if (violations.length > 0) {
  console.error(`ascii-check: ${violations.length} non-ASCII character(s) in ${files.length} file(s):`);
  for (const violation of violations) console.error(`  ${violation}`);
  process.exitCode = 1;
} else {
  console.log(`ascii-check: ${files.length} file(s), all plain ASCII`);
}
