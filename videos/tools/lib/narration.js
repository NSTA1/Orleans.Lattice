/** The id a scene heading is addressed by: lower-case words joined by hyphens. */
export function sceneId(title) {
  return title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

const PAUSE = /^<!--\s*pause\s+(\d+(?:\.\d+)?)\s*-->$/;

/**
 * Parses an episode's SCRIPT.md into narration cues. Only the "## Narration"
 * section is spoken, and it ends at the next level-2 heading. Each paragraph
 * is one cue, and "### <scene>" labels the cues that follow it. A
 * `<!-- pause N -->` comment adds N seconds of silence before the next cue,
 * or, after the last cue, before the episode ends; every other HTML comment is
 * a direction note and is never spoken.
 *
 * Returns { cues: [{ text, scene, pause }], tail }, where `pause` is the extra
 * silence before a cue and `tail` the silence after the last one.
 */
export function parseScript(markdown, source = "SCRIPT.md") {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const start = lines.findIndex((line) => /^##\s+Narration\s*$/.test(line));
  if (start < 0) {
    throw new Error(`${source}: no '## Narration' section`);
  }
  let end = lines.findIndex((line, index) => index > start && /^##\s/.test(line));
  if (end < 0) end = lines.length;

  // Pause directives become placeholder lines so they keep their position;
  // every other comment is removed before paragraphs are split.
  const section = lines
    .slice(start + 1, end)
    .join("\n")
    .replace(/<!--[\s\S]*?-->/g, (comment) => (PAUSE.test(comment.trim()) ? `\n\n${comment.trim()}\n\n` : ""));

  const cues = [];
  let scene = null;
  let pending = 0;
  for (const block of section.split(/\n\s*\n/)) {
    const blockLines = block.split("\n").map((line) => line.trim()).filter(Boolean);
    const spoken = [];
    for (const line of blockLines) {
      const heading = /^###\s+(.+)$/.exec(line);
      const pause = PAUSE.exec(line);
      if (heading) {
        scene = heading[1].trim();
      } else if (pause) {
        pending += Number(pause[1]);
      } else {
        spoken.push(line);
      }
    }
    const text = spoken.join(" ").replace(/\s+/g, " ").trim();
    if (text) {
      cues.push({ text, scene, pause: pending });
      pending = 0;
    }
  }
  if (cues.length === 0) {
    throw new Error(`${source}: the Narration section has no cues`);
  }
  return { cues, tail: pending };
}

const round = (seconds) => Math.round(seconds * 1000) / 1000;

/**
 * Lays cues end to end. The first starts after `leadIn` seconds; each later
 * cue starts `cueGap` seconds after the previous one ends, or `sceneGap`
 * seconds when it opens a new scene; and a cue's own `pause` is added before
 * it. Returns the cues' { start, end } and the episode's total duration: the
 * last cue's end plus `tail`.
 */
export function buildTimeline(cues, durations, { leadIn = 0.5, cueGap = 0.35, sceneGap = 1.2, tail = 0 } = {}) {
  if (cues.length !== durations.length) {
    throw new Error(`timeline: ${cues.length} cue(s) but ${durations.length} duration(s)`);
  }
  let cursor = 0;
  const timeline = cues.map((cue, index) => {
    const gap = index === 0 ? leadIn : cue.scene !== cues[index - 1].scene ? sceneGap : cueGap;
    const start = cursor + gap + (cue.pause ?? 0);
    const end = start + durations[index];
    cursor = end;
    return { start: round(start), end: round(end) };
  });
  return { timeline, duration: round(cursor + tail) };
}

/**
 * The scenes of a timed script, in order: each scene's id and title, the
 * indices of its cues, and its window on the episode timeline. A scene opens
 * `lead` seconds before its first cue (or when the previous scene's last cue
 * ends, if that is later) and lasts until the next scene opens; the first
 * opens at 0 and the last runs to the end of the episode.
 */
export function buildScenes(cues, timeline, duration, { lead = 0.6 } = {}) {
  const scenes = [];
  cues.forEach((cue, index) => {
    const title = cue.scene ?? "Untitled";
    const last = scenes.at(-1);
    if (last && last.title === title) {
      last.cues.push(index);
    } else {
      if (scenes.some((scene) => scene.title === title)) {
        throw new Error(`timeline: scene '${title}' appears twice; give each scene one heading`);
      }
      scenes.push({ id: sceneId(title), title, cues: [index] });
    }
  });
  scenes.forEach((scene, k) => {
    const first = timeline[scene.cues[0]].start;
    const previousEnd = k === 0 ? 0 : timeline[scenes[k - 1].cues.at(-1)].end;
    scene.start = k === 0 ? 0 : round(Math.max(previousEnd, first - lead));
    scene.beats = scene.cues.map((index) => round(timeline[index].start - scene.start));
  });
  scenes.forEach((scene, k) => {
    scene.end = k === scenes.length - 1 ? duration : scenes[k + 1].start;
  });
  return scenes;
}

/** At most two lines of 42 characters a caption: the common broadcast limit. */
export const CAPTION_LAYOUT = Object.freeze({ width: 42, lines: 2 });

// A reader expects a pause after these, so a caption or a line may end there.
const CLAUSE_END = /[,;:]$/;
// Words that open a clause or a phrase: breaking just before one reads well.
const CLAUSE_START = new Set([
  "and", "but", "or", "nor", "so", "that", "which", "who", "whose", "where", "when", "while",
  "because", "if", "with", "without", "by", "as", "in", "on", "at", "for", "from", "into",
  "across", "inside", "beside",
]);
// Words that belong with the word after them, so nothing ends on one.
const BINDS_FORWARD = new Set([
  "a", "an", "the", "of", "to", "in", "on", "at", "by", "for", "from", "with", "into",
  "and", "or", "nor", "but", "so", "that", "which", "who", "whose", "where", "when", "while",
  "because", "if", "as", "than", "not", "its", "their", "your", "our", "my", "his", "her",
  "each", "every", "no",
]);

// What a break costs, between two lines of one caption and between two
// captions: a caption that ends mid-clause leaves the reader waiting.
const LINE_BREAK = Object.freeze({ clause: 0, opening: 10, word: 30, binds: 100 });
const CAPTION_BREAK = Object.freeze({ clause: 0, opening: 60, word: 150, binds: 200 });

/** What breaking between words[k - 1] and words[k] costs, at the given weights. */
function breakCost(words, k, weights) {
  const before = words[k - 1];
  const base = CLAUSE_END.test(before) ? weights.clause : CLAUSE_START.has(words[k].toLowerCase()) ? weights.opening : weights.word;
  return base + (BINDS_FORWARD.has(before.toLowerCase()) ? weights.binds : 0);
}

/** The length of words[from..to) joined by single spaces. */
function span(words, from, to) {
  let length = to - from - 1;
  for (let k = from; k < to; k++) length += words[k].length;
  return length;
}

/**
 * The cheapest way to set words[from..to) in at most `lines` lines of at most
 * `width` characters: fuller, even lines and natural breaks cost least. A word
 * longer than `width` gets a line of its own. Null when the words do not fit.
 */
function setLines(words, from, to, { width, lines }) {
  // best[m].get(k): the cheapest way to set words[from..k) in exactly m lines.
  const best = [new Map([[from, { cost: 0, breaks: [] }]])];
  for (let m = 1; m <= lines; m++) {
    const row = new Map();
    for (const [s, previous] of best[m - 1]) {
      for (let k = s + 1; k <= to; k++) {
        const length = span(words, s, k);
        if (length > width && k - s > 1) break;
        const cost = previous.cost + (width - length) ** 2 / 40 + (s > from ? breakCost(words, s, LINE_BREAK) : 0);
        if (!row.has(k) || cost < row.get(k).cost) {
          row.set(k, { cost, breaks: s > from ? [...previous.breaks, s] : previous.breaks });
        }
      }
    }
    best.push(row);
  }
  let found = null;
  for (let m = 1; m <= lines; m++) {
    const candidate = best[m].get(to);
    if (candidate && (!found || candidate.cost < found.cost)) found = candidate;
  }
  return found;
}

/**
 * Splits cue text into captions of at most `layout.lines` lines of at most
 * `layout.width` characters, each caption's lines joined by "\n". A sentence
 * end always ends a caption; within a sentence the fewest captions are used,
 * and every break, between captions or between lines, falls where a reader
 * expects one: after a comma, colon or semicolon, and never after a word that
 * belongs with the next ("a", "the", "of"). A full stop inside a token
 * ("Orleans.Lattice") is not a sentence end, because a sentence end must be
 * followed by whitespace.
 */
export function splitCaption(text, layout = CAPTION_LAYOUT) {
  const captions = [];
  for (const sentence of text.split(/(?<=[.!?])\s+/).filter(Boolean)) {
    const words = sentence.split(/\s+/).filter(Boolean);
    // best[k]: the cheapest way to caption words[0..k).
    const best = [{ cost: 0, captions: [] }];
    for (let k = 1; k <= words.length; k++) {
      for (let s = k - 1; s >= 0; s--) {
        if (!best[s]) continue;
        const set = setLines(words, s, k, layout);
        if (!set) {
          if (span(words, s, k) > layout.width * layout.lines) break;
          continue;
        }
        const cost = best[s].cost + 1000 + set.cost + (k < words.length ? breakCost(words, k, CAPTION_BREAK) : 0);
        if (!best[k] || cost < best[k].cost) {
          const bounds = [s, ...set.breaks, k];
          const lines = bounds.slice(1).map((end, i) => words.slice(bounds[i], end).join(" "));
          best[k] = { cost, captions: [...best[s].captions, lines.join("\n")] };
        }
      }
    }
    captions.push(...best[words.length].captions);
  }
  return captions;
}

/**
 * Caption cues for a narration track: each narration cue is split into
 * captions (see splitCaption) whose share of the cue's time is proportional
 * to their length. This is an approximation of speech timing, not word
 * alignment.
 */
export function captionCues(cues, timeline, layout = CAPTION_LAYOUT) {
  const captions = [];
  cues.forEach((cue, index) => {
    const { start, end } = timeline[index];
    const pieces = splitCaption(cue.text, layout);
    const total = pieces.reduce((sum, piece) => sum + piece.length, 0);
    let cursor = start;
    pieces.forEach((piece, k) => {
      const pieceEnd = k === pieces.length - 1 ? end : cursor + (end - start) * (piece.length / total);
      captions.push({ start: round(cursor), end: round(pieceEnd), text: piece });
      cursor = pieceEnd;
    });
  });
  return captions;
}

const pad = (value, width) => String(value).padStart(width, "0");

/**
 * The narration as a transcript for an episode's companion page: each scene's
 * title as a heading at `level`, then its cues as paragraphs, in the written
 * form the captions use.
 */
export function transcriptMarkdown(cues, level = 3) {
  const blocks = [];
  let scene;
  for (const cue of cues) {
    if (cue.scene !== scene) {
      scene = cue.scene;
      if (scene) blocks.push(`${"#".repeat(level)} ${scene}`);
    }
    blocks.push(cue.text);
  }
  return blocks.join("\n\n");
}

/** A WebVTT timestamp (hh:mm:ss.mmm) for a time in seconds. */
export function formatTimestamp(seconds) {
  const total = Math.round(seconds * 1000);
  const hours = Math.floor(total / 3_600_000);
  const minutes = Math.floor(total / 60_000) % 60;
  const secs = Math.floor(total / 1000) % 60;
  return `${pad(hours, 2)}:${pad(minutes, 2)}:${pad(secs, 2)}.${pad(total % 1000, 3)}`;
}

const escapeCueText = (text) => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** Serialises caption cues ([{ start, end, text }]) as a WebVTT document. */
export function toWebVtt(captions) {
  const blocks = captions.map(
    (caption, index) =>
      `${index + 1}\n${formatTimestamp(caption.start)} --> ${formatTimestamp(caption.end)}\n${escapeCueText(caption.text)}`,
  );
  return `WEBVTT\n\n${blocks.join("\n\n")}\n`;
}
