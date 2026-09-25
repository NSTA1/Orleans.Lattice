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

test("the series lexicon says the verb 'lives', not the plural of 'life', for Kokoro", () => {
  const series = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
  assert.equal(applyLexicon("First: the store lives in the cluster.", series), "First: the store livs in the cluster.");
  assert.equal(applyLexicon("where the state lives.", series, "kokoro"), "where the state livs.");
  assert.equal(applyLexicon("It delivers.", series), "It delivers.", "only the whole word is respelt");
});

test("an engine gets its own spoken form, or the written form when its entry is null", () => {
  const lexicon = validateLexicon([
    { written: "Orleans", spoken: "Or-leens", engines: { chatterbox: "Orleens" } },
    { written: "lives", spoken: "livs", engines: { chatterbox: null } },
    { written: "CRDT", spoken: "C R D T" },
  ]);
  const text = "Orleans lives in a CRDT.";
  assert.equal(applyLexicon(text, lexicon), "Or-leens livs in a C R D T.", "no engine: the default forms");
  assert.equal(applyLexicon(text, lexicon, "kokoro"), "Or-leens livs in a C R D T.", "an engine with no form of its own: the default");
  assert.equal(applyLexicon(text, lexicon, "chatterbox"), "Orleens lives in a C R D T.");
});

test("the series lexicon gives Chatterbox one-word names and leaves it the words it reads from context", () => {
  const series = loadLexicon(path.join(workspaceRoot, "voice", "lexicon.json"));
  assert.equal(
    applyLexicon("Orleans.Lattice runs on Microsoft Orleans. The store lives in the cluster.", series, "chatterbox"),
    "Orleens Lattice runs on Microsoft Orleens. The store lives in the cluster.",
  );
  assert.equal(applyLexicon("Cluster A adds three; merges are idempotent.", series, "chatterbox"), "Cluster A adds three; merges are idempotent.");
  assert.equal(applyLexicon("Your code resolves ILattice.", series, "chatterbox"), "Your code resolves I Lattice.");
  for (const entry of series) {
    for (const spoken of Object.values(entry.engines ?? {})) {
      if (spoken !== null) assert.doesNotMatch(spoken, /-/, `'${entry.written}': a hyphen makes Chatterbox pause`);
    }
  }
});

test("an engine the workspace does not run, or an empty form for one, is rejected", () => {
  assert.throws(() => validateLexicon([{ written: "X", spoken: "x", engines: { piper: "ex" } }]), /unknown engine 'piper'/);
  assert.throws(() => validateLexicon([{ written: "X", spoken: "x", engines: { chatterbox: "" } }]), /neither a spoken form nor null/);
  assert.throws(() => validateLexicon([{ written: "X", spoken: "x", engines: ["chatterbox"] }]), /not an object/);
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
