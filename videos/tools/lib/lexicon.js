import { readFileSync } from "node:fs";

/**
 * Validates lexicon entries: each maps a non-empty `written` form to a
 * non-empty `spoken` form, and no written form appears twice.
 */
export function validateLexicon(entries) {
  if (!Array.isArray(entries)) {
    throw new Error("lexicon: 'entries' must be an array");
  }
  const seen = new Set();
  for (const [index, entry] of entries.entries()) {
    if (typeof entry?.written !== "string" || entry.written.length === 0) {
      throw new Error(`lexicon: entry ${index} has no 'written' form`);
    }
    if (typeof entry.spoken !== "string" || entry.spoken.length === 0) {
      throw new Error(`lexicon: entry '${entry.written}' has no 'spoken' form`);
    }
    if (seen.has(entry.written)) {
      throw new Error(`lexicon: '${entry.written}' is defined twice`);
    }
    seen.add(entry.written);
  }
  return entries;
}

/** Loads and validates voice/lexicon.json ({ "entries": [{ written, spoken, note? }] }). */
export function loadLexicon(file) {
  return validateLexicon(JSON.parse(readFileSync(file, "utf8")).entries);
}

const escapeForRegex = (text) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Rewrites written forms to spoken forms for text-to-speech. One pass over the
 * text, longest written form first, so a replacement is never matched again and
 * "CRDTs" is never read as "CRDT" followed by "s". Matching is case-sensitive
 * and whole-token: a match may not touch a letter, digit or underscore on
 * either side. Captions keep the written form; only the audio uses this.
 */
export function applyLexicon(text, entries) {
  if (entries.length === 0) {
    return text;
  }
  const spokenFor = new Map(entries.map((entry) => [entry.written, entry.spoken]));
  const alternation = [...spokenFor.keys()]
    .sort((a, b) => b.length - a.length)
    .map(escapeForRegex)
    .join("|");
  const pattern = new RegExp(`(?<![A-Za-z0-9_])(?:${alternation})(?![A-Za-z0-9_])`, "g");
  return text.replace(pattern, (match) => spokenFor.get(match));
}

/** Loads voice/heteronyms.json ({ "words": [...] }): words spelt the same but said two ways. */
export function loadHeteronyms(file) {
  const { words } = JSON.parse(readFileSync(file, "utf8"));
  if (!Array.isArray(words) || words.some((word) => typeof word !== "string" || !/^[a-z]+$/.test(word))) {
    throw new Error("heteronyms: 'words' must be an array of lower-case words");
  }
  return words;
}

/**
 * The heteronyms a text contains, in order of first appearance, matched as
 * whole words and without regard to case, so their reading can be checked.
 */
export function heteronymsIn(text, words) {
  const wanted = new Set(words);
  const found = [];
  for (const [word] of text.matchAll(/[A-Za-z]+/g)) {
    const lower = word.toLowerCase();
    if (wanted.has(lower) && !found.includes(lower)) found.push(lower);
  }
  return found;
}
