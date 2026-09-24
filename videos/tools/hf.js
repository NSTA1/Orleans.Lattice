#!/usr/bin/env node
// Runs the pinned HyperFrames CLI (node_modules, never a registry download)
// from the workspace root with telemetry, update checks and skill installs
// switched off. Before any command that loads a composition it copies the
// documentation site's design system into assets/brand/site/ (see
// assets/brand/brand.css). Every npm script goes through this; see README.md.
import { brandCommands, syncBrand } from "./lib/brand.js";
import { runHyperframes } from "./lib/hyperframes.js";

const args = process.argv.slice(2);
if (brandCommands.has(args[0])) {
  try {
    const { docsSite, faces, files } = syncBrand();
    console.log(`brand: design system read from ${docsSite} (${faces} font face(s), ${files} font file(s))`);
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}

const { code } = await runHyperframes(args);
process.exitCode = code;
