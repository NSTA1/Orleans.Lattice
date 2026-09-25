// Checks that a generated clip says what the script says. A generative voice
// can drop, repeat or add a word, or slip in a sound ("Neither awaits its
// turn"), so each clip is transcribed by local speech recognisers and the
// transcripts are compared with the cue's written text, word for word. The
// comparison forgives what a recogniser cannot know (capitals, punctuation,
// numerals, spelling variants, and its guesses at the product's names), and
// nothing else.

const NUMBER_WORDS = [
  "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
  "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen", "twenty",
];

// British spellings the script uses, as a recogniser trained on American
// English writes them.
const SPELLINGS = new Map([
  ["centre", "center"], ["centres", "centers"], ["behaviour", "behavior"], ["colour", "color"],
  ["favour", "favor"], ["catalogue", "catalog"], ["licence", "license"], ["defence", "defense"],
  ["programme", "program"], ["metre", "meter"], ["travelled", "traveled"],
]);

// Words that sound the same, as one spelling each. A recogniser can only
// guess which of them was said ("a read" comes back as "a reed", "clusters
// write" as "clusters right"), and a listener cannot tell them apart either,
// so treating them as one word hides nothing that can be heard.
const HOMOPHONES = new Map([
  ["reed", "read"], ["right", "write"], ["rite", "write"], ["wright", "write"], ["rights", "writes"],
  ["righting", "writing"], ["tear", "tier"], ["tears", "tiers"], ["won", "one"], ["knew", "new"],
  ["four", "for"], ["their", "there"], ["theyre", "there"], ["too", "to"], ["two", "to"],
  ["whole", "hole"], ["know", "no"], ["buy", "by"], ["bye", "by"], ["weight", "wait"], ["weights", "waits"],
]);

// What a recogniser writes for the product's names, joined into one token.
// Each rule applies to the words of both the script and the transcript.
const NAMES = [
  [/\bor\s+(?:leans|leens|lean|leen|lines|lance)\b/g, "orleans"],
  [/\b(?:orleens|orlean|orleen|orleanz|orlins)\b/g, "orleans"],
  [/\b(?:i|eye)\s+lattice\b/g, "ilattice"],
  [/\bidem\s+potent\b/g, "idempotent"],
];

/**
 * The words of a text, as the comparison sees them: lower case, apostrophes
 * dropped ("other's" and "others" are one word to the ear), every other
 * character that is not a letter or digit a word break (so "key-value" is two
 * words and "Orleans.Lattice" is "orleans lattice"), numerals up to twenty as
 * words, British spellings as American, homophones as one spelling, and the
 * product's names as one token.
 */
export function comparableWords(text) {
  let words = text
    .toLowerCase()
    .replace(/['\u2019]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((word) => (/^\d+$/.test(word) && Number(word) < NUMBER_WORDS.length ? NUMBER_WORDS[Number(word)] : word))
    .map((word) => (/isation(s?)$/.test(word) ? word.replace(/isation(s?)$/, "ization$1") : word))
    .map((word) => SPELLINGS.get(word) ?? word)
    .map((word) => HOMOPHONES.get(word) ?? word)
    .join(" ");
  for (const [pattern, name] of NAMES) words = words.replace(pattern, name);
  return words.split(" ").filter(Boolean);
}

/**
 * The differences between two word lists, as the smallest set of edits that
 * turns `expected` into `heard`, with neighbouring edits read as one ("its"
 * heard as "it is"): each { expected, heard }, joined words, where either side
 * may be empty (a dropped or an added word). Empty when they match.
 */
export function wordDifferences(expected, heard) {
  const n = expected.length;
  const m = heard.length;
  // cost[i][j]: edits to turn expected[i..] into heard[j..].
  const cost = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
  for (let i = n; i >= 0; i--) {
    for (let j = m; j >= 0; j--) {
      if (i === n) cost[i][j] = m - j;
      else if (j === m) cost[i][j] = n - i;
      else if (expected[i] === heard[j]) cost[i][j] = cost[i + 1][j + 1];
      else cost[i][j] = 1 + Math.min(cost[i + 1][j + 1], cost[i + 1][j], cost[i][j + 1]);
    }
  }
  const runs = [];
  let run = null;
  const edit = (from, to) => {
    run ??= { expected: [], heard: [] };
    if (from !== undefined) run.expected.push(from);
    if (to !== undefined) run.heard.push(to);
  };
  let i = 0;
  let j = 0;
  while (i < n || j < m) {
    if (i < n && j < m && expected[i] === heard[j]) {
      if (run) runs.push(run);
      run = null;
      i++;
      j++;
    } else if (i < n && j < m && cost[i][j] === 1 + cost[i + 1][j + 1]) {
      edit(expected[i++], heard[j++]);
    } else if (i < n && cost[i][j] === 1 + cost[i + 1][j]) {
      edit(expected[i++], undefined);
    } else {
      edit(undefined, heard[j++]);
    }
  }
  if (run) runs.push(run);
  // A word joined or split differently ("autoscaling", "auto scaling") is
  // spelt differently, not said differently.
  return runs
    .map((r) => ({ expected: r.expected.join(" "), heard: r.heard.join(" ") }))
    .filter((r) => r.expected.replace(/ /g, "") !== r.heard.replace(/ /g, ""));
}

/** A difference as a reader sees it: 'waits' -> 'awaits', or a dropped or added word. */
export function describeDifference({ expected, heard }) {
  if (!heard) return `dropped '${expected}'`;
  if (!expected) return `added '${heard}'`;
  return `'${expected}' -> '${heard}'`;
}

/**
 * How one attempt at a clip did: its differences from the script by
 * recogniser, their total, whether every recogniser heard it exactly, and
 * whether its pace is plausible for its length (`secondsPerWord`, { min, max },
 * catches a clip that trails off, stalls on a word or is cut short, which a
 * transcript can miss).
 */
export function judgeAttempt(text, { seconds, transcripts }, { secondsPerWord } = {}) {
  const expected = comparableWords(text);
  const byRecogniser = {};
  let total = 0;
  for (const [name, transcript] of Object.entries(transcripts)) {
    const differences = wordDifferences(expected, comparableWords(transcript)).map(describeDifference);
    byRecogniser[name] = differences;
    total += differences.length;
  }
  const pace = seconds / Math.max(1, expected.length);
  const problems = [];
  if (secondsPerWord?.max !== undefined && pace > secondsPerWord.max) {
    problems.push(`${pace.toFixed(2)}s a word is slower than ${secondsPerWord.max}s: a stall, a long pause or trailing sound`);
  }
  if (secondsPerWord?.min !== undefined && pace < secondsPerWord.min) {
    problems.push(`${pace.toFixed(2)}s a word is faster than ${secondsPerWord.min}s: words are missing or rushed`);
  }
  const heardExactly = Object.keys(transcripts).length > 0 && total === 0;
  return { heardExactly, passed: heardExactly && problems.length === 0, differences: byRecogniser, total, problems };
}

/**
 * The attempt to keep: the first that passed, or else the one with the fewest
 * differences and pace problems, the earlier on a tie. Returns its index.
 */
export function bestAttempt(judgements) {
  const passed = judgements.findIndex((judgement) => judgement.passed);
  if (passed >= 0) return passed;
  let best = 0;
  const score = (judgement) => judgement.total + judgement.problems.length;
  judgements.forEach((judgement, index) => {
    if (score(judgement) < score(judgements[best])) best = index;
  });
  return best;
}

/** The seed for an attempt at a clip: derived from the clip's name, so it is the same on every run. */
export function seedFor(clipHash, attempt) {
  return (Number.parseInt(clipHash.slice(0, 8), 16) + attempt * 7919) % 2147483647;
}
