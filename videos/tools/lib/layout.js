import { existsSync, readdirSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./hyperframes.js";

// The workspace layout (README.md, "Layout"), defined once so that every tool
// agrees on it. The root holds workspace files only. Anything more than one
// episode can use lives once under shared/. Everything a single episode owns
// lives under episodes/<slug>/, named by the slug that also names its renders,
// its narration and its companion page.

/** Everything more than one episode uses. */
export const sharedDir = path.join(workspaceRoot, "shared");

/** The brand seam: brand.css, motion.js and the other runtime helpers. */
export const brandDir = path.join(sharedDir, "brand");

/** The copy of the docs site's design system that brand.js writes; ignored by git. */
export const brandSiteDir = path.join(brandDir, "site");

/** The variable-driven scenes episodes are built from. */
export const componentsDir = path.join(sharedDir, "components");

/** One folder per episode. */
export const episodesDir = path.join(workspaceRoot, "episodes");

/**
 * The workspace smoke test. It is also the only file the CLI's project
 * commands (preview, lint, check, snapshot) open, which is why an episode is
 * swapped in over it to run them (see tools/lib/episode.js).
 */
export const workspaceIndex = path.join(workspaceRoot, "index.html");

/** Output: renders, narration and snapshots. Ignored by git. */
export const rendersDir = path.join(workspaceRoot, "renders");

const SLUG = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

/** True for a kebab-case episode slug. */
export function isSlug(value) {
  return typeof value === "string" && SLUG.test(value);
}

/** The files one episode owns, and where its narration is written. */
export function episodePaths(slug) {
  if (!isSlug(slug)) {
    throw new Error(`'${slug}' is not an episode slug: use kebab-case, for example 'introduction'`);
  }
  const dir = path.join(episodesDir, slug);
  return {
    slug,
    dir,
    brief: path.join(dir, "BRIEF.md"),
    script: path.join(dir, "SCRIPT.md"),
    storyboard: path.join(dir, "STORYBOARD.md"),
    composition: path.join(dir, "composition.html"),
    metadata: path.join(dir, "episode.json"),
    assets: path.join(dir, "assets"),
    narration: path.join(rendersDir, "narration", slug),
  };
}

/** The slug of every episode folder under `root`, sorted. */
export function listEpisodes(root = episodesDir) {
  if (!existsSync(root)) return [];
  return readdirSync(root, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && isSlug(entry.name))
    .map((entry) => entry.name)
    .sort();
}

/** Every file under `directory` (recursively, skipping node_modules) that satisfies `accept`, sorted. */
export function listFiles(directory, accept) {
  const found = [];
  const walk = (current) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        if (entry.name !== "node_modules") walk(full);
      } else if (accept(full)) {
        found.push(full);
      }
    }
  };
  if (existsSync(directory)) walk(directory);
  return found.sort();
}

/**
 * Every composition in the workspace: the smoke test, the shared components,
 * and each episode's composition.html.
 */
export function compositionFiles() {
  return [
    workspaceIndex,
    ...listFiles(componentsDir, (file) => file.endsWith(".html")),
    ...listEpisodes()
      .map((slug) => episodePaths(slug).composition)
      .filter((file) => existsSync(file)),
  ];
}
