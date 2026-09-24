import assert from "node:assert/strict";
import { test } from "node:test";
import { applyLexicon, validateLexicon } from "../lib/lexicon.js";

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

test("entries must be an array", () => {
  assert.throws(() => validateLexicon(undefined), /must be an array/);
});
