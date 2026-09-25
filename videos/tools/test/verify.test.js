import assert from "node:assert/strict";
import { test } from "node:test";
import { bestAttempt, comparableWords, describeDifference, judgeAttempt, seedFor, wordDifferences } from "../lib/verify.js";

test("words are compared without capitals, punctuation, apostrophes or hyphens", () => {
  assert.deepEqual(comparableWords("Each merges the other's update: a key-value store."), [
    "each", "merges", "the", "others", "update", "a", "key", "value", "store",
  ]);
});

test("numerals are words, and British spellings American, as a recogniser writes them", () => {
  assert.deepEqual(comparableWords("Cluster A adds 3, then 5."), comparableWords("Cluster A adds three, then five."));
  assert.deepEqual(comparableWords("At its centre, authorisation."), ["at", "its", "center", "authorization"]);
});

test("words that sound the same, or are joined or split differently, are one word, because nobody can hear which was written", () => {
  const same = (script, heard) => assert.deepEqual(wordDifferences(comparableWords(script), comparableWords(heard)), [], heard);
  same("So a read is a grain call.", "So a reed is a grain call.");
  same("Two clusters write at the same time.", "2 clusters right at the same time.");
  same("to a separate database tier", "to a separate database tear");
  same("with backup and an autoscaling signal", "with backup and an auto scaling signal");
  same("a key-value store", "a keyvalue store");
  assert.deepEqual(
    wordDifferences(comparableWords("Neither waits its turn."), comparableWords("Neither awaits its turn.")),
    [{ expected: "waits", heard: "awaits" }],
    "a sound that is not the same is still a difference",
  );
});

test("a recogniser's guesses at the product's names are the names", () => {
  const expected = comparableWords("Orleans.Lattice takes a different approach, on Microsoft Orleans.");
  for (const heard of [
    "Orleans Lattice takes a different approach, on Microsoft Orleans.",
    "Or, Lean's lattice takes a different approach, on Microsoft Orleens.",
    "Orlean's lattice takes a different approach, on Microsoft Or leans.",
  ]) {
    assert.deepEqual(wordDifferences(expected, comparableWords(heard)), [], heard);
  }
  assert.deepEqual(comparableWords("Your code resolves ILattice."), comparableWords("Your code resolves I Lattice."));
  assert.deepEqual(comparableWords("merges are idempotent"), comparableWords("merges are idem potent"));
});

test("a slipped, dropped or added word is a difference, and neighbouring slips read as one", () => {
  const expected = comparableWords("Neither waits its turn.");
  assert.deepEqual(wordDifferences(expected, comparableWords("Neither awaits its turn.")), [{ expected: "waits", heard: "awaits" }]);
  assert.deepEqual(wordDifferences(expected, comparableWords("Neither its turn.")), [{ expected: "waits", heard: "" }]);
  assert.deepEqual(wordDifferences(expected, comparableWords("Neither waits its own turn.")), [{ expected: "", heard: "own" }]);
  assert.deepEqual(
    wordDifferences(expected, comparableWords("Either way it's turn.")),
    [{ expected: "neither waits", heard: "either way" }],
    "neighbouring edits are one run",
  );
  assert.equal(describeDifference({ expected: "waits", heard: "awaits" }), "'waits' -> 'awaits'");
  assert.equal(describeDifference({ expected: "waits", heard: "" }), "dropped 'waits'");
  assert.equal(describeDifference({ expected: "", heard: "own" }), "added 'own'");
});

test("an attempt passes only when every recogniser hears it exactly, at a plausible pace", () => {
  const text = "Neither waits its turn.";
  const pace = { secondsPerWord: { min: 0.18, max: 0.75 } };
  const exact = judgeAttempt(text, { seconds: 1.5, transcripts: { a: "Neither waits its turn.", b: "neither waits its turn" } }, pace);
  assert.equal(exact.passed, true);
  assert.equal(exact.total, 0);

  const oneMisheard = judgeAttempt(text, { seconds: 1.5, transcripts: { a: "Neither waits its turn.", b: "Neither awaits its turn." } }, pace);
  assert.equal(oneMisheard.passed, false);
  assert.deepEqual(oneMisheard.differences, { a: [], b: ["'waits' -> 'awaits'"] });

  const stalled = judgeAttempt(text, { seconds: 6, transcripts: { a: text } }, pace);
  assert.equal(stalled.heardExactly, true);
  assert.equal(stalled.passed, false, "heard exactly, but four words in six seconds is a stall");
  assert.match(stalled.problems[0], /slower than 0.75s/);

  assert.equal(judgeAttempt(text, { seconds: 1.5, transcripts: {} }).passed, false, "an attempt nobody heard is not verified");

  const squealed = judgeAttempt(
    text,
    { seconds: 1.5, transcripts: { a: text }, artefacts: [{ kind: "a burst louder than the speech", start: 0.9, end: 1.1, level: -5 }] },
    pace,
  );
  assert.equal(squealed.heardExactly, true, "recognisers ignore a squeal");
  assert.equal(squealed.passed, false, "the inspection does not");
  assert.deepEqual(squealed.problems, ["a burst louder than the speech at 0.9-1.1s"]);
});

test("the first attempt that passed is kept; otherwise the one with the fewest problems, the earlier on a tie", () => {
  const judge = (total, problems = 0, passed = false) => ({ total, problems: new Array(problems).fill("x"), passed });
  assert.equal(bestAttempt([judge(2), judge(0, 0, true), judge(0, 0, true)]), 1);
  assert.equal(bestAttempt([judge(3), judge(1), judge(1), judge(2)]), 1);
  assert.equal(bestAttempt([judge(0, 1), judge(1)]), 0);
});

test("a clip's seeds come from its name, differ by attempt, and are the same on every run", () => {
  const name = "5a3ae19465122180";
  assert.equal(seedFor(name, 0), seedFor(name, 0));
  assert.notEqual(seedFor(name, 0), seedFor(name, 1));
  assert.notEqual(seedFor(name, 0), seedFor("9b438cca26dc8f19", 0));
  for (let attempt = 0; attempt < 4; attempt++) {
    const seed = seedFor("ffffffffffffffff", attempt);
    assert.ok(Number.isInteger(seed) && seed >= 0 && seed < 2147483647, String(seed));
  }
});
