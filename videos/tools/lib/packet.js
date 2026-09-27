// The review packet: what an episode's pull request says, so that its
// reviewer can approve it - by merging - without running anything. It holds
// the video (a review copy attached to the pull request), the moments worth
// listening to and why, the script with its timings, the claims and their
// sources, what changed since the published cut, the takes there are to pick
// from, and the feedback the automation has acted on so far.
//
// Everything here is a pure function of what narration, the script and the
// plan already record; tools/packet.js gathers those and writes the packet.

/** Seconds as the review packet shows them: minutes and seconds, "1:05". */
export function clock(seconds) {
  const total = Math.max(0, Math.floor(seconds));
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, "0")}`;
}

const TOKEN = /[A-Za-z][A-Za-z0-9]*(?:[.'-][A-Za-z0-9]+)*/g;

/**
 * The words in a line a voice is most likely to say wrongly: names and
 * identifiers (capitals inside a word, dots, digits), initialisms, hyphenated
 * compounds, and long words - nine letters or more, which is where the
 * introduction's one mispronunciation, "idempotent", sits. A possessive 's is
 * not part of the word.
 */
export function riskyWords(text) {
  const found = [];
  for (const [token] of text.matchAll(TOKEN)) {
    const word = token.replace(/'s$/, "");
    const risky = /[a-z][A-Z]/.test(word) || /^[A-Z]{2,}/.test(word) || /\d/.test(word) || /[.-]/.test(word) || word.length >= 9;
    if (risky && !found.includes(word)) found.push(word);
  }
  return found;
}

/** The risky words of a line that `known` - what the voice has already said - never contains. */
export function newWords(text, known) {
  const heard = new Set([...known.matchAll(TOKEN)].map(([token]) => token.replace(/'s$/, "").toLowerCase()));
  return riskyWords(text).filter((word) => !heard.has(word.toLowerCase()));
}

/**
 * The moments to listen to, cue by cue, from narration's manifest (cues.json):
 * a take picked by ear, a clip no recogniser heard exactly, one made more
 * than once before it was, words the series voice has not said before, and
 * lines that changed since the published cut. `changed` holds cue numbers.
 */
export function listenAt(manifest, { known = "", changed = new Set() } = {}) {
  const rows = [];
  for (const cue of manifest.cues) {
    const why = [];
    const check = cue.check;
    if (check?.picked) {
      why.push("a take picked by ear");
    } else if (check && !check.verified) {
      const heard = Object.entries(check.differences ?? {})
        .filter(([, differences]) => differences.length > 0)
        .map(([recogniser, differences]) => `${recogniser} heard ${differences.join(", ")}`);
      why.push(`not heard exactly (${[...heard, ...(check.problems ?? [])].join("; ") || "see cues.json"})`);
    } else if (check?.attempt > 1) {
      why.push(`heard exactly on attempt ${check.attempt}`);
    }
    const words = newWords(cue.text, known);
    if (words.length > 0) why.push(`new to the voice: ${words.map((word) => `\`${word}\``).join(", ")}`);
    if (changed.has(cue.index)) why.push("changed since the published cut");
    if (why.length > 0) rows.push({ cue: cue.index, at: cue.start, why });
  }
  return rows;
}

/** The numbers of the cues whose text the previous script does not have. */
export function changedCues(cues, previous) {
  const before = new Set(previous.map((cue) => cue.text));
  return new Set(cues.filter((cue) => !before.has(cue.text)).map((cue) => cue.index));
}

/** A section of a markdown file, by its level-2 heading, without the heading; null when there is none. */
export function sectionOf(markdown, heading) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const start = lines.findIndex((line) => line.trim() === `## ${heading}`);
  if (start < 0) return null;
  const end = lines.findIndex((line, index) => index > start && /^##\s/.test(line));
  return lines
    .slice(start + 1, end < 0 ? lines.length : end)
    .join("\n")
    .trim();
}

const LEDGER_BEGIN = "<!-- ledger:begin -->";
const LEDGER_END = "<!-- ledger:end -->";
const NO_FEEDBACK = "No feedback yet.";

/** The feedback ledger of an earlier packet, so a new packet carries it on; null when it has none. */
export function ledgerOf(body) {
  const text = body.replace(/\r\n/g, "\n");
  const from = text.indexOf(LEDGER_BEGIN);
  const to = text.indexOf(LEDGER_END);
  if (from < 0 || to < from) return null;
  const ledger = text.slice(from + LEDGER_BEGIN.length, to).trim();
  return ledger === "" || ledger === NO_FEEDBACK ? null : ledger;
}

const cell = (text) => String(text).replace(/\|/g, "\\|").replace(/\s+/g, " ").trim();

/**
 * The packet itself, as the body of the pull request. `item` is the plan's
 * item the pull request completes, `step` its place in the production order
 * (such as "1 of 30"), `facts` the cut's numbers, and `video` the review
 * copy's URL, which GitHub plays in place when it sits on a line of its own.
 */
export function packetMarkdown({ item, pathTitle, place, step, idea, facts, video, rows, cues, takes, sources, recut, ledger }) {
  const out = [];
  out.push(`<!-- video-packet: ${item.code} -->`);
  out.push(`## ${item.code}: ${item.title}`, "");
  const where = pathTitle ? `${pathTitle}, episode ${place}. ` : "";
  out.push(`${where}Item ${step} in the production order (videos/series.md, "Production order").${idea ? ` ${idea}` : ""}`, "");
  out.push(video ? video : "_No review copy is attached: run `npm run packet -- <slug> --upload`._", "");
  out.push("| Length | Size | Loudness | Cut |", "| --- | --- | --- | --- |");
  out.push(`| ${facts.length} | ${facts.size} | ${facts.loudness} | \`${facts.cut}\` |`, "");

  out.push("### Listen at", "");
  if (rows.length === 0) {
    out.push("Every cue was heard exactly on its first attempt, and no word is new to the series voice.");
  } else {
    out.push("| Time | Cue | Why |", "| --- | --- | --- |");
    for (const row of rows) out.push(`| ${clock(row.at)} | ${row.cue} | ${cell(row.why.join("; "))} |`);
  }
  out.push("");

  if (recut) {
    out.push("### Changed since the published cut", "");
    out.push(recut.changed.length === 0 ? "No line of narration changed since the published cut." : `Cue(s) ${recut.changed.join(", ")} changed, and ${recut.removed} line(s) were removed.`);
    out.push("");
  }

  out.push("### Script", "");
  out.push("| Cue | Time | Scene | Narration |", "| --- | --- | --- | --- |");
  for (const cue of cues) out.push(`| ${cue.index} | ${clock(cue.start)} | ${cell(cue.sceneTitle ?? "")} | ${cell(cue.text)} |`);
  out.push("");

  if (takes.length > 0) {
    out.push("### Takes to pick from", "");
    for (const { cue, takes: numbers } of takes) out.push(`- Cue ${cue}: take${numbers.length === 1 ? "" : "s"} ${numbers.join(", ")}`);
    out.push("");
  }

  out.push("### Claims and sources", "");
  out.push(sources ?? "_The script has no Sources section._", "");

  out.push("### How to respond", "");
  out.push(
    "- **To approve, squash-merge this pull request.** Merging is the approval; the required check refuses an episode merged ahead of the one before it.",
    "- **To ask for changes, comment in plain words**, with a time where it helps (\"1:10: 'linearizable' is said wrong\"), or one of these on a line of its own:",
    "  - `/retake <cue>`: make new takes of a cue and keep the best-heard one;",
    "  - `/pick <cue> <take>`: use that take of a cue (the takes are listed above);",
    "  - `/reword <cue> <new text>`: change what a cue says.",
    "- The next run acts on every comment it has not answered, re-cuts the episode, updates this description, and records what it did below.",
    "",
  );

  out.push("### Feedback acted on", "", LEDGER_BEGIN, ledger ?? NO_FEEDBACK, LEDGER_END, "");
  return out.join("\n");
}
