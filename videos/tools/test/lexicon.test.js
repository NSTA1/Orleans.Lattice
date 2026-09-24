import assert from "node:assert/strict";
import path from "node:path";
import { test } from "node:test";
import { workspaceRoot } from "../lib/hyperframes.js";
import { applyLexicon, heteronymsIn, loadHeteronyms, loadLexicon, validateLexicon } from "../lib/lexicon.js";

const entries = validateLexicon([
  { written: "CRDT", spoken: "C R D T" },
  { written: "CRDTs", spoken: "C R D Tees" },
  { written: "Orleans.Lattice", spoken: "Orleans Lattice" },
  { written: "B+ tree", spoken: "B plus tree" },
  { written: "WAL", spoken: "wall" },
]);

test("the longest written form wins, so a plural is not read as its singular plus 's'", () => {
  assert.equal(applyLexicon("Two CRDTs and one CRDT.", entries), "Two C R D Tees and one C R D T.");
});

test("matching is whole-token and case-sensitive", () => {
  assert.equal(applyLexicon("WALK past the WAL, not the wal.", entries), "WALK past the wall, not the wal.");
});

test("a replacement is never matched again", () => {
  const chained = validateLexicon([
    { written: "A", spoken: "B" },
    { written: "B", spoken: "C" },
  ]);
  assert.equal(applyLexicon("A B", chained), "B C");
});

test("punctuation inside a written form is matched literally", () => {
  assert.equal(applyLexicon("Orleans.Lattice uses a B+ tree.", entries), "Orleans Lattice uses a B plus tree.");
  assert.equal(applyLexicon("OrleansXLattice", entries), "OrleansXLattice");
});

test("an empty lexicon leaves text unchanged", () => {
  assert.equal(applyLexicon("CRDT", []), "CRDT");
});

test("a written form defined twice is rejected", () => {
  assert.throws(
    () => validateLexicon([{ written: "X", spoken: "x" }, { written: "X", spoken: "y" }]),
    /defined twice/,
  );
});

test("an entry without a spoken form is rejected", () => {
  assert.throws(() => validateLexicon([{ written: "X", spoken: "" }]), /no 'spoken' form/);
});

test("the series lexicon says the verb 'lives', not the plural of 'life'", () => {
  const series = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
  assert.equal(applyLexicon("First: the store lives in the cluster.", series), "First: the store livs in the cluster.");
  assert.equal(applyLexicon("where the state lives.", series), "where the state livs.");
  assert.equal(applyLexicon("It delivers.", series), "It delivers.", "only the whole word is respelt");
});

test("heteronyms are found as whole words, whatever their case, once each and in order", () => {
  const words = ["lives", "read", "separate"];
  assert.deepEqual(heteronymsIn("Read it: the store lives here, and Lives there. A read.", words), ["read", "lives"]);
  assert.deepEqual(heteronymsIn("It delivers, reads and separates.", words), []);
});

test("the series heteronym list is valid and covers the reading that went wrong", () => {
  const words = loadHeteronyms(path.join(workspaceRoot, "voice", "heteronyms.json"));
  assert.ok(words.includes("lives"));
  assert.equal(new Set(words).size, words.length, "no word is listed twice");
});

test("entries must be an array", () => {
  assert.throws(() => validateLexicon(undefined), /must be an array/);
});
