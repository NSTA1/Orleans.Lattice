// A render's receipt: what it was rendered from. hf.js writes one beside
// every episode render that names its output, and `npm run publish` publishes
// a render only when its receipt still matches the sources, so a published
// video always shows what the repository says.
import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./hyperframes.js";
import { episodePaths, listFiles, sharedDir } from "./layout.js";
import { fileDigest } from "./publication.js";
import { openingTags } from "./timeline.js";

/**
 * Every file an episode's render reads, sorted: its composition and assets,
 * everything shared (including the copy of the site's design system), the
 * audio its composition plays, and the workspace files that pin the renderer.
 */
export function renderInputs(slug) {
  const { composition, assets } = episodePaths(slug);
  const audio = openingTags(readFileSync(composition, "utf8"))
    .filter((tag) => tag.name === "audio" && tag.get("src"))
    .map((tag) => path.join(workspaceRoot, tag.get("src")));
  const files = [
    composition,
    ...listFiles(assets, () => true),
    ...listFiles(sharedDir, () => true),
    ...audio,
    path.join(workspaceRoot, "hyperframes.json"),
    path.join(workspaceRoot, "package-lock.json"),
  ];
  return [...new Set(files)].sort();
}

/** One digest of many files: each file's path, relative to `root`, and its SHA-256. */
export function digestFiles(files, root = workspaceRoot) {
  const hash = createHash("sha256");
  for (const file of [...files].sort()) {
    const relative = path.relative(root, file).split(path.sep).join("/");
    if (!existsSync(file)) throw new Error(`receipt: ${relative}, which the render reads, does not exist`);
    hash.update(`${relative}\0${fileDigest(file)}\n`);
  }
  return hash.digest("hex");
}

/** The output a render command names with -o or --output, or null. */
export function renderOutput(args) {
  for (let i = 0; i < args.length; i++) {
    if ((args[i] === "-o" || args[i] === "--output") && i + 1 < args.length) return args[i + 1];
    if (args[i].startsWith("--output=")) return args[i].slice("--output=".length);
  }
  return null;
}

/** Where the receipt of a render is kept: beside it. */
export function receiptPath(output) {
  return `${output}.receipt.json`;
}

/** Writes the receipt of a finished render: its episode, the digest of its sources, and its own digest. */
export function writeReceipt(output, { episode, sources }) {
  const receipt = { episode, sources, video: fileDigest(output), rendered: new Date().toISOString() };
  writeFileSync(receiptPath(output), `${JSON.stringify(receipt, null, 2)}\n`);
  return receipt;
}

/** The receipt of a render, or null when it has none. */
export function readReceipt(output) {
  const file = receiptPath(output);
  return existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : null;
}
