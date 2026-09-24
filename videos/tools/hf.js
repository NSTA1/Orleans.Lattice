#!/usr/bin/env node
// Runs the pinned HyperFrames CLI (node_modules, never a registry download)
// from the workspace root with telemetry, update checks and skill installs
// switched off. Before any command that loads a composition it copies the
// documentation site's design system into shared/brand/site/ (see
// shared/brand/brand.css). Every npm script goes through this; see README.md.
//
// `--episode <slug>` runs a project command (preview, lint, check, snapshot,
// render, ...) on episodes/<slug>/composition.html instead of the smoke test;
// see tools/lib/episode.js. An episode render that names its output with -o
// also gets a receipt of what it was rendered from, which `npm run publish`
// checks; see tools/lib/receipt.js.
import { existsSync, rmSync } from "node:fs";
import path from "node:path";
import { brandCommands, syncBrand } from "./lib/brand.js";
import { recoverWorkspaceIndex, takeEpisodeArgument, withEpisode } from "./lib/episode.js";
import { runHyperframes, workspaceRoot } from "./lib/hyperframes.js";
import { digestFiles, receiptPath, renderInputs, renderOutput, writeReceipt } from "./lib/receipt.js";

let slug;
let args;
try {
  ({ slug, args } = takeEpisodeArgument(process.argv.slice(2)));
  if (slug !== null && !brandCommands.has(args[0])) {
    throw new Error(`--episode applies only to the commands that load a composition (${[...brandCommands].join(", ")})`);
  }
} catch (error) {
  console.error(error.message);
  process.exit(2);
}

let receipt = null;
try {
  if (recoverWorkspaceIndex()) {
    console.log("episode: put back the workspace index.html that an interrupted run left swapped out");
  }
  if (brandCommands.has(args[0])) {
    const { docsSite, faces, files } = syncBrand();
    console.log(`brand: design system read from ${docsSite} (${faces} font face(s), ${files} font file(s))`);
  }
  if (slug !== null && args[0] === "render") {
    const output = renderOutput(args);
    if (output === null) {
      console.log("render: no -o/--output, so no receipt is written, and 'npm run publish' will not take this render");
    } else {
      const file = path.resolve(workspaceRoot, output);
      rmSync(receiptPath(file), { force: true });
      receipt = { file, sources: digestFiles(renderInputs(slug)) };
    }
  }
} catch (error) {
  console.error(error.message);
  process.exit(1);
}

const run = () => runHyperframes(args);
try {
  const { code } = slug === null ? await run() : await withEpisode(slug, run);
  process.exitCode = code;
  if (code === 0 && receipt && existsSync(receipt.file)) {
    writeReceipt(receipt.file, { episode: slug, sources: receipt.sources });
    console.log(`render: receipt written to ${path.relative(workspaceRoot, receiptPath(receipt.file))}`);
  }
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
