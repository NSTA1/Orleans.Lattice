// The series plan as data (videos/series.json): its paths, the production
// order a scheduled automation works through, and the holds that stop it.
// series.md is the prose - who each episode is for, its idea, the reasons for
// the order - and markdownProblems() keeps the two in step.
//
// What is done is not recorded in the plan. An item is done when a published
// episode lists it in its episode.json "items": merging an episode's pull
// request is what approves it, so the episodes on main are the record. The
// rule that keeps every path followable is that the done items are always a
// gapless run from the start of the production order, so an episode cannot
// be approved ahead of the one before it (orderProblems).
//
// This module uses Node's built-ins only, so CI checks the order without
// installing the workspace (.github/workflows/ci.yml, "content-gates").
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./hyperframes.js";
import { episodesDir, episodePaths, isSlug, listEpisodes } from "./layout.js";
import { parseScript } from "./narration.js";
import { episodeProblems, PATHS, repoRoot, SITE_URL } from "./publication.js";

/** The plan as data. */
export const seriesFile = path.join(workspaceRoot, "series.json");

/** The plan as prose. */
export const planFile = path.join(workspaceRoot, "series.md");

/** The letter each path's items are coded with: B1 is Build's first episode. */
export const PATH_LETTERS = Object.freeze({
  "front-door": "F",
  build: "B",
  evaluate: "E",
  operate: "O",
  secure: "S",
  "how-it-works": "H",
});

/** The paths the front door sends a viewer to, in the order it names them. */
export const WAYS_IN = Object.freeze(["build", "evaluate", "operate"]);

const CODE = /^[FBEOSH](?:[1-9]\d*)?$/;
const ITEM_FIELDS = new Set(["code", "episode", "title", "path", "order", "introduces", "leadsTo", "hold", "recut", "names"]);

/** True for an item that makes an episode; the others re-cut one. */
export const isEpisodeItem = (item) => item.recut === undefined;

/** The code an episode item must have: its path's letter and its place on the path, and plain F for the front door. */
export function expectedCode(pathId, order) {
  const letter = PATH_LETTERS[pathId];
  return pathId === "front-door" ? (order === 1 ? letter : `${letter}${order}`) : `${letter}${order}`;
}

const pageFile = (page) => page.split("#")[0];

/**
 * What is wrong with a plan, if anything: its shape, its codes and its
 * relations, and the rules its production order keeps. A path is made in the
 * order it is watched; a deep dive comes straight after the episode that leads
 * to it; and a re-cut comes after its episode and after every episode it
 * names.
 */
export function planProblems(plan, { root = repoRoot } = {}) {
  if (!plan || typeof plan !== "object" || Array.isArray(plan)) return ["expected a JSON object"];
  const problems = [];

  const paths = Array.isArray(plan.paths) ? plan.paths : [];
  const pathIds = paths.map((entry) => entry?.id);
  if (pathIds.join(",") !== PATHS.join(",")) {
    problems.push(`paths must be ${PATHS.join(", ")}, in that order: the site's order (tools/lib/publication.js, PATHS)`);
  }
  const holds = plan.holds && typeof plan.holds === "object" ? plan.holds : {};
  for (const [id, hold] of Object.entries(holds)) {
    if (typeof hold?.label !== "string" || !hold.label || typeof hold.reason !== "string" || !hold.reason) {
      problems.push(`hold '${id}' needs a label (series.md reads "Held for <label>") and a reason`);
    }
  }
  const pageProblem = (page, where) => {
    if (typeof page !== "string" || !page || page.startsWith("/") || page.includes("..")) {
      return `${where}: '${page}' must be a path from the repository root`;
    }
    return existsSync(path.join(root, pageFile(page))) ? null : `${where}: ${pageFile(page)} does not exist`;
  };
  for (const entry of paths) {
    if (!entry || typeof entry.title !== "string" || !entry.title) problems.push(`path '${entry?.id}' needs a title`);
    const then = entry?.then;
    if (then === undefined) continue;
    const kinds = ["path", "page", "site"].filter((key) => then[key] !== undefined);
    if (kinds.length !== 1) {
      problems.push(`path '${entry.id}': 'then' names one of another path, a page or a site page`);
    } else if (then.path !== undefined && !PATHS.includes(then.path)) {
      problems.push(`path '${entry.id}': then.path '${then.path}' is not a path`);
    } else if (then.page !== undefined) {
      const problem = pageProblem(then.page, `path '${entry.id}'`);
      if (problem) problems.push(problem);
    }
    if (then.path === undefined && (typeof then.title !== "string" || !then.title)) {
      problems.push(`path '${entry.id}': 'then' needs the title of the page it hands on to`);
    }
  }

  const items = plan.items;
  if (!Array.isArray(items) || items.length === 0) return [...problems, "items must list the production order"];
  const byCode = new Map();
  const bySlug = new Map();
  const slots = new Set();
  for (const [index, item] of items.entries()) {
    const at = `items[${index}]${item?.code ? ` (${item.code})` : ""}`;
    if (!item || typeof item !== "object") {
      problems.push(`${at}: expected an object`);
      continue;
    }
    for (const key of Object.keys(item)) {
      if (!ITEM_FIELDS.has(key)) problems.push(`${at}: unknown field '${key}'`);
    }
    if (typeof item.code !== "string" || !CODE.test(item.code)) problems.push(`${at}: code must be a path letter and a number, such as B1`);
    else if (byCode.has(item.code)) problems.push(`${at}: the code ${item.code} is used twice`);
    else byCode.set(item.code, item);
    if (typeof item.title !== "string" || !item.title) problems.push(`${at}: needs a title`);
    if (item.hold !== undefined && !holds[item.hold]) problems.push(`${at}: hold '${item.hold}' is not one of the plan's holds`);
    if (!isEpisodeItem(item)) {
      for (const key of ["episode", "path", "order", "introduces", "leadsTo"]) {
        if (item[key] !== undefined) problems.push(`${at}: a re-cut takes its '${key}' from the episode it re-cuts`);
      }
      if (!Array.isArray(item.names) || item.names.length === 0) problems.push(`${at}: a re-cut names the episodes it adds to the ending`);
      continue;
    }
    if (!isSlug(item.episode)) problems.push(`${at}: episode must be a kebab-case slug`);
    else if (bySlug.has(item.episode)) problems.push(`${at}: the episode '${item.episode}' is made by two items`);
    else bySlug.set(item.episode, item);
    if (!PATHS.includes(item.path)) problems.push(`${at}: path must be one of ${PATHS.join(", ")}`);
    if (!Number.isInteger(item.order) || item.order < 1) problems.push(`${at}: order must be a whole number from 1`);
    if (PATHS.includes(item.path) && Number.isInteger(item.order)) {
      const slot = `${item.path}#${item.order}`;
      if (slots.has(slot)) problems.push(`${at}: two items are number ${item.order} on ${item.path}`);
      slots.add(slot);
      if (item.code !== expectedCode(item.path, item.order)) {
        problems.push(`${at}: number ${item.order} on ${item.path} is coded ${expectedCode(item.path, item.order)}`);
      }
    }
    if (item.introduces !== undefined) {
      if (!Array.isArray(item.introduces)) {
        problems.push(`${at}: introduces lists the pages the episode is the way into`);
      } else {
        for (const page of item.introduces) {
          if (typeof page?.title !== "string" || !page.title) problems.push(`${at}: each page it introduces needs a title`);
          const problem = pageProblem(page?.page, at);
          if (problem) problems.push(problem);
        }
      }
    }
  }

  // Relations, now that every code is known.
  const position = new Map(items.map((item, index) => [item?.code, index]));
  const ledTo = new Map();
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    if (item.leadsTo !== undefined) {
      const target = byCode.get(item.leadsTo);
      if (!target || !isEpisodeItem(target) || target.path !== "how-it-works") {
        problems.push(`${item.code}: leadsTo '${item.leadsTo}' must be a How it works episode`);
      } else if (ledTo.has(item.leadsTo)) {
        problems.push(`${item.leadsTo} is led to by both ${ledTo.get(item.leadsTo)} and ${item.code}`);
      } else {
        ledTo.set(item.leadsTo, item.code);
      }
    }
    if (!isEpisodeItem(item)) {
      const target = byCode.get(item.recut);
      if (!target || !isEpisodeItem(target)) problems.push(`${item.code}: recut '${item.recut}' must be an episode`);
      else if (position.get(item.code) < position.get(target.code)) problems.push(`${item.code} re-cuts ${target.code}, so it must come after it`);
      for (const name of item.names ?? []) {
        const named = byCode.get(name);
        if (!named || !isEpisodeItem(named)) problems.push(`${item.code}: names '${name}', which is not an episode`);
        else if (position.get(item.code) < position.get(name)) problems.push(`${item.code} names ${name}, so it must come after it`);
      }
    }
  }
  for (const item of items) {
    if (!item || !isEpisodeItem(item) || item.path !== "how-it-works") continue;
    const lead = ledTo.get(item.code);
    if (!lead) problems.push(`${item.code}: no episode leads to it (leadsTo)`);
    else if (position.get(item.code) !== position.get(lead) + 1) problems.push(`${item.code} must come straight after ${lead}, which leads to it`);
  }
  for (const pathId of PATHS) {
    const onPath = items.filter((item) => item && isEpisodeItem(item) && item.path === pathId && Number.isInteger(item.order));
    const orders = onPath.map((item) => item.order).sort((a, b) => a - b);
    if (orders.some((order, index) => order !== index + 1)) problems.push(`${pathId}: its episodes must be numbered 1 to ${orders.length}`);
    // How it works is standalone, made as each lead is; every other path is
    // watched in order, so it is made in that order.
    if (pathId === "how-it-works") continue;
    const made = onPath.map((item) => item.order);
    if (made.some((order, index) => index > 0 && order < made[index - 1])) {
      problems.push(`${pathId}: its episodes must be made in the order they are watched`);
    }
  }
  return problems;
}

/** Reads and checks the plan; throws naming every problem. */
export function readPlan(file = seriesFile, options) {
  let plan;
  try {
    plan = JSON.parse(readFileSync(file, "utf8"));
  } catch (error) {
    throw new Error(`series.json: ${error.message}`);
  }
  const problems = planProblems(plan, options);
  if (problems.length > 0) throw new Error(`series.json:\n  ${problems.join("\n  ")}`);
  return plan;
}

/** An item by its code. */
export function itemOf(plan, code) {
  return plan.items.find((item) => item.code === code) ?? null;
}

/** The episode an item makes, or re-cuts. */
export function episodeOf(plan, item) {
  return isEpisodeItem(item) ? item.episode : itemOf(plan, item.recut)?.episode;
}

/** A path by its id. */
export function pathOf(plan, id) {
  return plan.paths.find((entry) => entry.id === id) ?? null;
}

/** A path's episodes, in the order they are watched. */
export function episodesOn(plan, pathId) {
  return plan.items.filter((item) => isEpisodeItem(item) && item.path === pathId).sort((a, b) => a.order - b.order);
}

/**
 * Every episode folder's metadata, by slug: { meta, problems }, where meta is
 * null when episode.json is missing or not JSON.
 */
export function readEpisodes(root = episodesDir) {
  const episodes = new Map();
  for (const slug of listEpisodes(root)) {
    const file = path.join(root, slug, "episode.json");
    if (!existsSync(file)) {
      episodes.set(slug, { meta: null, problems: ["does not exist"] });
      continue;
    }
    try {
      const meta = JSON.parse(readFileSync(file, "utf8"));
      episodes.set(slug, { meta, problems: episodeProblems(meta) });
    } catch (error) {
      episodes.set(slug, { meta: null, problems: [error.message] });
    }
  }
  return episodes;
}

/** The slugs of the episodes that have a published cut. */
export function publishedEpisodes(episodes) {
  return new Set([...episodes].filter(([, { meta }]) => meta?.published).map(([slug]) => slug));
}

/** The codes of the items the published episodes complete. */
export function doneItems(episodes) {
  const done = new Set();
  for (const { meta } of episodes.values()) {
    if (!meta?.published) continue;
    for (const code of meta.items ?? []) done.add(code);
  }
  return done;
}

/**
 * What is wrong with the episodes, against the plan: each one must be an
 * episode of the plan and sit where the plan puts it; a published one must
 * list the items it completes; and the items done must be a gapless run from
 * the start of the production order, none of them held.
 */
export function orderProblems(plan, episodes) {
  const problems = [];
  for (const [slug, { meta, problems: found }] of episodes) {
    const source = `episodes/${slug}/episode.json`;
    if (!meta || found.length > 0) {
      problems.push(`${source}: ${found.join("; ")}`);
      continue;
    }
    const item = plan.items.find((candidate) => isEpisodeItem(candidate) && candidate.episode === slug);
    if (!item) {
      problems.push(`episodes/${slug}/ is not an episode of the plan: no item in series.json makes it`);
      continue;
    }
    if (meta.path !== item.path || meta.order !== item.order) {
      problems.push(`${source}: says ${meta.path} number ${meta.order}, and series.json has ${item.code} as ${item.path} number ${item.order}`);
    }
    for (const code of meta.items ?? []) {
      const listed = itemOf(plan, code);
      if (!listed) problems.push(`${source}: lists ${code}, which is not an item of series.json`);
      else if (episodeOf(plan, listed) !== slug) problems.push(`${source}: lists ${code}, which is an item of '${episodeOf(plan, listed)}'`);
    }
    if (meta.published && !(meta.items ?? []).includes(item.code)) {
      problems.push(`${source}: is published, so its items must include ${item.code}`);
    }
  }
  const done = doneItems(episodes);
  const open = plan.items.findIndex((item) => !done.has(item.code));
  if (open >= 0) {
    const first = plan.items[open];
    for (const item of plan.items.slice(open + 1)) {
      if (done.has(item.code)) {
        problems.push(
          `${item.code} (${item.title}) is published, but ${first.code} (${first.title}) comes before it in the production order and is not: ` +
            "the series is made and approved in order (series.md, 'Production order')",
        );
      }
    }
  }
  for (const item of plan.items) {
    if (item.hold && done.has(item.code)) {
      problems.push(`${item.code} is published while it is held for ${plan.holds[item.hold].label}; release the hold in series.md and series.json first`);
    }
  }
  return problems;
}

/**
 * The next item to make: the first in the production order that is not done,
 * with the hold that stops the queue there, if any. Null when everything is
 * done.
 */
export function nextItem(plan, done) {
  const item = plan.items.find((candidate) => !done.has(candidate.code));
  if (!item) return null;
  return { item, hold: item.hold ? { id: item.hold, ...plan.holds[item.hold] } : null };
}

// ---------------------------------------------------------------------------
// series.md, read for the structure its tables state.

function section(markdown, heading) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const start = lines.findIndex((line) => line.trim() === `## ${heading}`);
  if (start < 0) return null;
  const end = lines.findIndex((line, index) => index > start && /^##\s/.test(line));
  return lines.slice(start + 1, end < 0 ? lines.length : end).join("\n");
}

const cellsOf = (line) =>
  line
    .trim()
    .replace(/^\|/, "")
    .replace(/\|$/, "")
    .split("|")
    .map((cell) => cell.trim());

/** Every table in a piece of markdown: its header cells and its rows' cells. */
export function tablesOf(markdown) {
  const lines = markdown.split("\n");
  const tables = [];
  for (let i = 0; i < lines.length - 1; i++) {
    if (!/^\s*\|/.test(lines[i]) || !/^\s*\|\s*-{3,}/.test(lines[i + 1])) continue;
    const table = { header: cellsOf(lines[i]), rows: [] };
    let j = i + 2;
    for (; j < lines.length && /^\s*\|/.test(lines[j]); j++) table.rows.push(cellsOf(lines[j]));
    tables.push(table);
    i = j - 1;
  }
  return tables;
}

/** The links in a table cell, as pages from the repository root (series.md sits in videos/). */
export function linksOf(cell) {
  return [...cell.matchAll(/\[([^\]]+)\]\(([^)\s]+)\)/g)].map(([, title, href]) => {
    const [file, anchor] = href.split("#");
    const page = path.posix.normalize(path.posix.join("videos", file));
    return { title, page: anchor ? `${page}#${anchor}` : page };
  });
}

const titleOf = (cell) =>
  cell
    .replace(/\*\*/g, "")
    .replace(/\s*\((?:first|published)\)\s*$/, "")
    .trim();
const shown = (pages) => (pages.length === 0 ? "nothing" : pages.map((page) => `${page.title} (${page.page})`).join(", "));

/**
 * Where series.md and series.json disagree: an episode's title, the pages it
 * introduces, the deep dive it leads to, or the production order and its
 * holds. The prose around the tables is series.md's alone.
 */
export function markdownProblems(plan, markdown) {
  const problems = [];
  const episodes = section(markdown, "Episodes");
  if (episodes === null) return ["series.md has no '## Episodes' section"];
  const rows = new Map();
  for (const table of tablesOf(episodes)) {
    const column = (name) => table.header.findIndex((cell) => cell.toLowerCase() === name.toLowerCase());
    const [codeAt, titleAt, introducesAt, leadsAt, reachedAt] = ["#", "Episode", "Introduces", "Leads to", "Reached from"].map(column);
    if (codeAt < 0 || titleAt < 0) continue;
    for (const cells of table.rows) {
      rows.set(cells[codeAt], {
        title: titleOf(cells[titleAt] ?? ""),
        introduces: introducesAt >= 0 ? linksOf(cells[introducesAt] ?? "") : null,
        leadsTo: leadsAt >= 0 ? cells[leadsAt] || null : undefined,
        reachedFrom: reachedAt >= 0 ? cells[reachedAt] || null : undefined,
      });
    }
  }
  const episodeItems = plan.items.filter(isEpisodeItem);
  for (const item of episodeItems) {
    const row = rows.get(item.code);
    if (!row) {
      problems.push(`${item.code} (${item.title}) is in series.json but has no row in series.md's episode tables`);
      continue;
    }
    if (row.title !== item.title) problems.push(`${item.code}: series.md calls it '${row.title}' and series.json '${item.title}'`);
    if (row.introduces !== null && JSON.stringify(row.introduces) !== JSON.stringify(item.introduces ?? [])) {
      problems.push(`${item.code}: series.md says it introduces ${shown(row.introduces)}, and series.json ${shown(item.introduces ?? [])}`);
    }
    if (row.leadsTo !== undefined && row.leadsTo !== (item.leadsTo ?? null)) {
      problems.push(`${item.code}: series.md says it leads to ${row.leadsTo ?? "nothing"}, and series.json ${item.leadsTo ?? "nothing"}`);
    }
    if (row.reachedFrom !== undefined) {
      const lead = plan.items.find((candidate) => candidate.leadsTo === item.code)?.code ?? null;
      if (row.reachedFrom !== lead) problems.push(`${item.code}: series.md says it is reached from ${row.reachedFrom}, and series.json from ${lead}`);
    }
  }
  for (const code of rows.keys()) {
    if (!episodeItems.some((item) => item.code === code)) problems.push(`${code} has a row in series.md's episode tables but is not an episode in series.json`);
  }

  const order = section(markdown, "Production order");
  const table = order === null ? null : tablesOf(order).find((candidate) => candidate.header.some((cell) => cell.toLowerCase() === "items"));
  if (!table) return [...problems, "series.md's 'Production order' has no table with an Items column"];
  const stepAt = table.header.findIndex((cell) => cell.toLowerCase() === "step");
  const itemsAt = table.header.findIndex((cell) => cell.toLowerCase() === "items");
  const sequence = table.rows.flatMap((cells) => {
    const held = /^Held for (.+)$/.exec(cells[stepAt] ?? "");
    return (cells[itemsAt] ?? "")
      .split(",")
      .map((code) => code.trim())
      .filter(Boolean)
      .map((code) => ({ code, hold: held ? held[1].trim() : null }));
  });
  const expected = plan.items.map((item) => item.code);
  if (sequence.map((entry) => entry.code).join(",") !== expected.join(",")) {
    problems.push(`series.md's production order is ${sequence.map((entry) => entry.code).join(", ")}, and series.json's is ${expected.join(", ")}`);
  }
  for (const { code, hold } of sequence) {
    const item = itemOf(plan, code);
    if (!item) continue;
    const label = item.hold ? plan.holds[item.hold].label : null;
    if (label !== hold) {
      problems.push(`${code}: series.md ${hold ? `holds it for ${hold}` : "does not hold it"}, and series.json ${label ? `holds it for ${label}` : "does not"}`);
    }
  }
  return problems;
}

// ---------------------------------------------------------------------------
// Where an episode leads: what its ending names and its companion page links.

/** What comes after an episode on its path: the next episode, or what the path hands on to at its end. */
export function continuation(plan, item) {
  const from = pathOf(plan, item.path);
  const next = episodesOn(plan, item.path).find((candidate) => candidate.order === item.order + 1);
  if (next) return { kind: "episode", path: from, item: next };
  const then = from?.then;
  if (!then) return null;
  if (then.path) return { kind: "path", from, path: pathOf(plan, then.path), item: episodesOn(plan, then.path)[0] ?? null };
  return { kind: "page", from, title: then.title, page: then.page ?? null, site: then.site ?? null };
}

/**
 * Where a viewer goes from an episode. The front door names the three ways
 * in, each with its first episode; a How it works episode sends the viewer
 * back to the path that led to it; every other episode names what comes next
 * on its path and the deep dive it leads to.
 */
export function destinations(plan, code) {
  const item = itemOf(plan, code);
  if (!item || !isEpisodeItem(item)) throw new Error(`series: '${code}' is not an episode of the plan`);
  if (item.path === "front-door") {
    return { item, ways: WAYS_IN.map((id) => ({ path: pathOf(plan, id), first: episodesOn(plan, id)[0] ?? null })) };
  }
  if (item.path === "how-it-works") {
    const lead = plan.items.find((candidate) => candidate.leadsTo === item.code) ?? null;
    return { item, lead, back: lead ? continuation(plan, lead) : null };
  }
  return { item, next: continuation(plan, item), deepDive: item.leadsTo ? itemOf(plan, item.leadsTo) : null };
}

const joinList = (parts) => (parts.length <= 1 ? parts.join("") : `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`);

/** A repository page as a link from a companion page (docs/videos/). */
export function pageLink(page) {
  const [file, anchor] = page.page.split("#");
  const relative = path.posix.relative("docs/videos", file);
  return `[${page.title}](${anchor ? `${relative}#${anchor}` : relative})`;
}

const episodeLink = (item, published) => (published.has(item.episode) ? `[${item.title}](${item.episode}.md)` : item.title);

function continuationLine(label, step, published) {
  if (!step) return null;
  if (step.kind === "episode") return `- **${label} ${step.path.title}:** ${episodeLink(step.item, published)}`;
  if (step.kind === "path") {
    return `- **Next:** ${step.path.title}${step.item ? `, starting with ${episodeLink(step.item, published)}` : ""}`;
  }
  if (step.site) return `- **Next:** the documentation site's [${step.title}](${SITE_URL}${step.site}) page`;
  return `- **Next:** ${pageLink({ title: step.title, page: step.page })}`;
}

/**
 * The "Where next" list of an episode's companion page, from the plan. An
 * episode is linked once it is published (`published` holds the slugs that
 * are) and named without a link until then, so a page gains its link to the
 * next episode when that episode ships, and never points at a page the site
 * leaves out.
 */
export function whereNextMarkdown(plan, code, published) {
  const route = destinations(plan, code);
  const lines = [];
  if (route.ways) {
    for (const { path: way, first } of route.ways) {
      const pages = joinList((first?.introduces ?? []).map(pageLink));
      const watch = first && published.has(first.episode) ? `watch ${episodeLink(first, published)}, then ` : "";
      lines.push(`- **${way.title}**, ${way.for}: ${watch}read ${pages}.`);
    }
    return lines.join("\n");
  }
  const step = route.next ?? route.back;
  const line = continuationLine(route.back !== undefined ? "Back to" : "Next on", step, published);
  if (line) lines.push(line);
  if (route.deepDive) lines.push(`- **The deep dive:** ${episodeLink(route.deepDive, published)}`);
  if (route.item.introduces?.length) lines.push(`- **The pages it introduces:** ${joinList(route.item.introduces.map(pageLink))}`);
  return lines.join("\n");
}

/**
 * The titles an episode's closing scene must name, from the plan: what comes
 * next and the deep dive it leads to; for the front door, the first episodes
 * that the re-cuts among its `items` added.
 */
export function endingTitles(plan, code, items = [code]) {
  const route = destinations(plan, code);
  if (route.ways) {
    return items
      .map((listed) => itemOf(plan, listed))
      .filter((listed) => listed && listed.recut === code)
      .flatMap((recut) => recut.names.map((name) => itemOf(plan, name).title));
  }
  const titles = [];
  const step = route.next ?? route.back;
  if (step?.item) titles.push(step.item.title);
  else if (step?.title) titles.push(step.title);
  if (route.deepDive) titles.push(route.deepDive.title);
  return titles;
}

const plain = (text) => text.replace(/`/g, "");

/**
 * The ending the plan gives an episode, as narration to start its closing
 * scene from. The words are a suggestion; the titles are not, and
 * endingProblems holds a published episode to them.
 */
export function endingText(plan, code, items = [code]) {
  const route = destinations(plan, code);
  if (route.ways) {
    const named = new Set(endingTitles(plan, code, items));
    const ways = route.ways.map(({ path: way, first }) =>
      first && named.has(first.title) ? `${way.title}, ${plain(way.for)}: start with ${first.title}.` : `${way.title}, ${plain(way.for)}.`,
    );
    return ["The documentation has three ways in.", ...ways, "You choose."].join(" ");
  }
  const sentences = [];
  const step = route.next ?? route.back;
  if (step?.kind === "episode") {
    sentences.push(route.back !== undefined ? `Back on ${step.path.title}, next: ${step.item.title}.` : `Next on ${step.path.title}: ${step.item.title}.`);
  } else if (step?.kind === "path") {
    sentences.push(`That is the end of the ${step.from.title} path. Next: ${step.path.title}${step.item ? `, starting with ${step.item.title}` : ""}.`);
  } else if (step) {
    sentences.push(`That is the end of the ${step.from.title} path. Next, the ${step.title} page in the documentation.`);
  }
  if (route.deepDive) sentences.push(`The deep dive on how this works is ${route.deepDive.title}.`);
  sentences.push("The links are on this episode's page in the documentation.");
  return sentences.join(" ");
}

const normalise = (text) =>
  text
    .toLowerCase()
    .replace(/['\u2019]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

/** Where an episode's closing scene fails to name what the plan says it leads to. */
export function endingProblems(plan, code, script, { items = [code], source = "SCRIPT.md" } = {}) {
  const { cues } = parseScript(script, source);
  const scene = cues.at(-1).scene;
  const closing = normalise(cues.filter((cue) => cue.scene === scene).map((cue) => cue.text).join(" "));
  return endingTitles(plan, code, items)
    .filter((title) => !closing.includes(normalise(title)))
    .map((title) => `${source}: its closing scene ('${scene ?? "untitled"}') does not name '${title}', which the plan says it leads to`);
}

/** Every published episode's ending problems. */
export function publishedEndingProblems(plan, episodes) {
  const problems = [];
  for (const [slug, { meta }] of episodes) {
    if (!meta?.published) continue;
    const item = plan.items.find((candidate) => isEpisodeItem(candidate) && candidate.episode === slug);
    const { script } = episodePaths(slug);
    if (!item || !existsSync(script)) continue;
    problems.push(...endingProblems(plan, item.code, readFileSync(script, "utf8"), { items: meta.items ?? [item.code], source: `episodes/${slug}/SCRIPT.md` }));
  }
  return problems;
}
