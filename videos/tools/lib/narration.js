/**
 * Parses an episode's SCRIPT.md into narration cues. Only the "## Narration"
 * section is spoken, and it ends at the next level-2 heading. Each paragraph
 * is one cue; a "### <scene>" heading labels the cues that follow it; HTML
 * comments are direction notes and are never spoken.
 */
export function parseScript(markdown, source = "SCRIPT.md") {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const start = lines.findIndex((line) => /^##\s+Narration\s*$/.test(line));
  if (start < 0) {
    throw new Error(`${source}: no '## Narration' section`);
  }
  let end = lines.findIndex((line, index) => index > start && /^##\s/.test(line));
  if (end < 0) end = lines.length;

  const section = lines.slice(start + 1, end).join("\n").replace(/<!--[\s\S]*?-->/g, "");
  const cues = [];
  let scene = null;
  for (const block of section.split(/\n\s*\n/)) {
    const blockLines = block.split("\n").map((line) => line.trim()).filter(Boolean);
    const spoken = [];
    for (const line of blockLines) {
      const heading = /^###\s+(.+)$/.exec(line);
      if (heading) {
        scene = heading[1].trim();
      } else {
        spoken.push(line);
      }
    }
    const text = spoken.join(" ").replace(/\s+/g, " ").trim();
    if (text) cues.push({ text, scene });
  }
  if (cues.length === 0) {
    throw new Error(`${source}: the Narration section has no cues`);
  }
  return cues;
}

const round = (seconds) => Math.round(seconds * 1000) / 1000;

/**
 * Lays cues end to end: the first starts after `leadIn` seconds, and each
 * later cue starts `gap` seconds after the previous one ends.
 */
export function buildTimeline(durations, { leadIn = 0.5, gap = 0.35 } = {}) {
  let cursor = leadIn;
  return durations.map((duration) => {
    const start = cursor;
    const end = start + duration;
    cursor = end + gap;
    return { start: round(start), end: round(end) };
  });
}

/**
 * Splits cue text into caption-sized pieces of at most `maxChars` characters
 * (two 42-character lines by default): at sentence ends first, then at word
 * boundaries. A full stop inside a token ("Orleans.Lattice") is not a
 * sentence end, because a sentence end must be followed by whitespace.
 */
export function splitCaption(text, maxChars = 84) {
  const pieces = [];
  for (const sentence of text.split(/(?<=[.!?])\s+/).filter(Boolean)) {
    if (sentence.length <= maxChars) {
      pieces.push(sentence);
      continue;
    }
    let current = "";
    for (const word of sentence.split(/\s+/)) {
      if (current && current.length + 1 + word.length > maxChars) {
        pieces.push(current);
        current = word;
      } else {
        current = current ? `${current} ${word}` : word;
      }
    }
    if (current) pieces.push(current);
  }
  return pieces;
}

/**
 * Caption cues for a narration track: each narration cue is split into
 * caption pieces whose share of the cue's time is proportional to their
 * length. This is an approximation of speech timing, not word alignment.
 */
export function captionCues(cues, timeline, maxChars = 84) {
  const captions = [];
  cues.forEach((cue, index) => {
    const { start, end } = timeline[index];
    const pieces = splitCaption(cue.text, maxChars);
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
