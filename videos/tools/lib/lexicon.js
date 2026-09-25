import { readFileSync } from "node:fs";

/** The speech engines a lexicon entry can give its own spoken form. */
export const ENGINES = Object.freeze(["kokoro", "chatterbox"]);

/**
 * Validates lexicon entries: each maps a non-empty `written` form to a
 * non-empty `spoken` form, and no written form appears twice. An entry may
 * also carry `engines`, a spoken form for particular engines: a string, or
 * null to leave that engine the written form. Respellings are made for how an
 * engine reads text, and engines read differently: Kokoro's phonemizer needs
 * "livs" to say the verb, whereas Chatterbox reads "lives" from context.
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
    if (entry.engines !== undefined) {
      if (entry.engines === null || typeof entry.engines !== "object" || Array.isArray(entry.engines)) {
        throw new Error(`lexicon: entry '${entry.written}' has 'engines' that is not an object`);
      }
      for (const [engine, spoken] of Object.entries(entry.engines)) {
        if (!ENGINES.includes(engine)) {
          throw new Error(`lexicon: entry '${entry.written}' names an unknown engine '${engine}' (known: ${ENGINES.join(", ")})`);
        }
        if (spoken !== null && (typeof spoken !== "string" || spoken.length === 0)) {
          throw new Error(`lexicon: entry '${entry.written}' gives '${engine}' neither a spoken form nor null`);
        }
      }
    }
    if (seen.has(entry.written)) {
      throw new Error(`lexicon: '${entry.written}' is defined twice`);
    }
    seen.add(entry.written);
  }
  return entries;
}

/** Loads and validates voice/lexicon.json ({ "entries": [{ written, spoken, engines?, note? }] }). */
export function loadLexicon(file) {
  return validateLexicon(JSON.parse(readFileSync(file, "utf8")).entries);
}

/**
 * The text an engine is given, after the lexicon: Chatterbox pauses at a
 * hyphen ("active... active", "Or... Leens"), so for it a hyphen between two
 * letters becomes a space, and a compound is read as one breath. Other engines
 * get the text unchanged.
 */
export function readingFor(text, engine) {
  return engine === "chatterbox" ? text.replace(/(?<=[A-Za-z])-(?=[A-Za-z])/g, " ") : text;
}

/** What an entry says for an engine: its own form for that engine, the written form for null, else the default. */
export function spokenFor(entry, engine) {
  if (engine !== undefined && entry.engines && Object.hasOwn(entry.engines, engine)) {
    return entry.engines[engine] ?? entry.written;
  }
  return entry.spoken;
}

const escapeForRegex = (text) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Rewrites written forms to spoken forms for text-to-speech, as `engine`
 * reads them (see spokenFor; omit it for the default forms). One pass over the
 * text, longest written form first, so a replacement is never matched again and
 * "CRDTs" is never read as "CRDT" followed by "s". Matching is case-sensitive
 * and whole-token: a match may not touch a letter, digit or underscore on
 * either side. Captions keep the written form; only the audio uses this.
 */
export function applyLexicon(text, entries, engine) {
  if (entries.length === 0) {
    return text;
  }
  const forms = new Map(entries.map((entry) => [entry.written, spokenFor(entry, engine)]));
  const alternation = [...forms.keys()]
    .sort((a, b) => b.length - a.length)
    .map(escapeForRegex)
    .join("|");
  const pattern = new RegExp(`(?<![A-Za-z0-9_])(?:${alternation})(?![A-Za-z0-9_])`, "g");
  return text.replace(pattern, (match) => forms.get(match));
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
