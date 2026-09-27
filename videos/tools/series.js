#!/usr/bin/env node
// The series plan (series.json and series.md) and where production stands:
//
//   npm run series -- check                the plan, series.md in step with it, and the order of what is published (CI)
//   npm run series -- status               every item: done, next, held, or to make
//   npm run series -- next [--json]        the next item to make, or the hold that stops the queue
//   npm run series -- ending <code|slug>   the ending the plan gives an episode, as narration
//   npm run series -- endings              fail if a published episode's closing scene misses what the plan says it leads to
//   npm run series -- lease take --owner <id> [--minutes 90]
//   npm run series -- lease renew --token <n> [--minutes 90]
//   npm run series -- lease release --token <n>
//   npm run series -- lease status
//
// `check` uses Node's built-ins only, so CI runs it without installing the
// workspace: `node videos/tools/series.js check`.
import { readFileSync } from "node:fs";
import { leaseStatus, releaseLease, renewLease, takeLease } from "./lib/lease.js";
import {
  doneItems,
  endingText,
  episodeOf,
  isEpisodeItem,
  itemOf,
  markdownProblems,
  nextItem,
  orderProblems,
  planFile,
  publishedEndingProblems,
  publishedEpisodes,
  readEpisodes,
  readPlan,
  whereNextMarkdown,
} from "./lib/series.js";

const usage = "usage: npm run series -- check | status | next [--json] | ending <code|slug> | endings | lease take|renew|release|status";
const argv = process.argv.slice(2);
const [command, ...rest] = argv;
const option = (name) => {
  const at = rest.indexOf(name);
  return at >= 0 ? rest[at + 1] : undefined;
};
const fail = (message, code = 1) => {
  console.error(`series: ${message}`);
  process.exit(code);
};

let plan;
try {
  plan = readPlan();
} catch (error) {
  fail(error.message);
}
const episodes = readEpisodes();
const done = doneItems(episodes);
const published = publishedEpisodes(episodes);

if (command === "check") {
  const problems = [...markdownProblems(plan, readFileSync(planFile, "utf8")), ...orderProblems(plan, episodes)];
  for (const problem of problems) console.error(`error: ${problem}`);
  const next = nextItem(plan, done);
  console.log(
    `series: ${plan.items.length} item(s), ${done.size} done` +
      (next ? `; next ${next.item.code} (${next.item.title})${next.hold ? `, held for ${next.hold.label}` : ""}` : "; all done"),
  );
  process.exit(problems.length === 0 ? 0 : 1);
}

if (command === "status") {
  const next = nextItem(plan, done);
  for (const item of plan.items) {
    const state = done.has(item.code)
      ? "done"
      : item === next?.item
        ? next.hold
          ? `next, held for ${next.hold.label}`
          : "next"
        : item.hold
          ? `held for ${plan.holds[item.hold].label}`
          : "to make";
    const what = isEpisodeItem(item) ? `${item.episode}` : `re-cut of ${item.recut} (${episodeOf(plan, item)})`;
    console.log(`${item.code.padEnd(3)} ${state.padEnd(28)} ${item.title}  [${what}]`);
  }
  process.exit(0);
}

if (command === "next") {
  const next = nextItem(plan, done);
  if (rest.includes("--json")) {
    const item = next?.item ?? null;
    const code = item ? (isEpisodeItem(item) ? item.code : item.recut) : null;
    const recutItems = item && !isEpisodeItem(item) ? [...(episodes.get(episodeOf(plan, item))?.meta?.items ?? []), item.code] : null;
    console.log(
      JSON.stringify(
        next
          ? {
              state: next.hold ? "held" : "ready",
              hold: next.hold,
              item,
              episode: episodeOf(plan, item),
              ending: endingText(plan, code, recutItems ?? [code]),
              whereNext: whereNextMarkdown(plan, code, published),
            }
          : { state: "done" },
        null,
        2,
      ),
    );
    process.exit(0);
  }
  if (!next) console.log("series: every item is done");
  else if (next.hold) console.log(`series: the next item is ${next.item.code} (${next.item.title}), held for ${next.hold.label}: ${next.hold.reason}`);
  else console.log(`series: the next item is ${next.item.code} (${next.item.title}), episode '${episodeOf(plan, next.item)}'`);
  process.exit(0);
}

if (command === "ending") {
  const wanted = rest[0];
  const item = itemOf(plan, wanted) ?? plan.items.find((candidate) => isEpisodeItem(candidate) && candidate.episode === wanted);
  if (!item) fail(`'${wanted}' is neither an item code nor an episode of the plan\n${usage}`, 2);
  const code = isEpisodeItem(item) ? item.code : item.recut;
  const items = isEpisodeItem(item) ? (episodes.get(item.episode)?.meta?.items ?? [code]) : [...(episodes.get(episodeOf(plan, item))?.meta?.items ?? []), item.code];
  console.log(endingText(plan, code, items));
  process.exit(0);
}

if (command === "endings") {
  const problems = publishedEndingProblems(plan, episodes);
  for (const problem of problems) console.error(`error: ${problem}`);
  console.log(`series: ${published.size} published episode(s); ${problems.length === 0 ? "every ending names what the plan says it leads to" : `${problems.length} ending problem(s)`}`);
  process.exit(problems.length === 0 ? 0 : 1);
}

if (command === "lease") {
  const [verb] = rest;
  const minutes = option("--minutes") === undefined ? 90 : Number(option("--minutes"));
  const token = Number(option("--token"));
  let result;
  try {
    if (verb === "take") result = takeLease({ owner: option("--owner"), minutes });
    else if (verb === "renew") result = renewLease({ token, minutes });
    else if (verb === "release") result = releaseLease({ token });
    else if (verb === "status") result = leaseStatus();
    else fail(usage, 2);
  } catch (error) {
    fail(error.message);
  }
  console.log(JSON.stringify(result, null, 2));
  process.exit(result.granted === false || result.released === false ? 3 : 0);
}

fail(usage, 2);
