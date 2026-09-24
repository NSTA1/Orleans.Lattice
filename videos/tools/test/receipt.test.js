import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";
import { digestFiles, receiptPath, renderOutput } from "../lib/receipt.js";

test("a render's output is the value of -o or --output, in either form", () => {
  assert.equal(renderOutput(["render", ".", "-o", "renders/a.mp4"]), "renders/a.mp4");
  assert.equal(renderOutput(["render", ".", "--output", "renders/b.mp4", "--quality", "high"]), "renders/b.mp4");
  assert.equal(renderOutput(["render", ".", "--output=renders/c.mp4"]), "renders/c.mp4");
  assert.equal(renderOutput(["render", ".", "--quality", "high"]), null);
  assert.equal(renderOutput(["render", "-o"]), null, "a flag with no value names nothing");
});

test("a receipt sits beside its render", () => {
  assert.equal(receiptPath("renders/introduction-high.mp4"), "renders/introduction-high.mp4.receipt.json");
});

test("the digest of a render's sources follows their content and their paths, not the order they are listed in", () => {
  const root = mkdtempSync(path.join(tmpdir(), "receipt-"));
  try {
    const a = path.join(root, "a.html");
    const b = path.join(root, "b.css");
    writeFileSync(a, "<div></div>");
    writeFileSync(b, "x");
    const first = digestFiles([a, b], root);
    assert.equal(digestFiles([b, a], root), first, "order does not matter");
    writeFileSync(a, "<div>changed</div>");
    const changed = digestFiles([a, b], root);
    assert.notEqual(changed, first, "a changed file changes the digest");
    writeFileSync(path.join(root, "c.css"), "x");
    assert.notEqual(digestFiles([a, path.join(root, "c.css")], root), changed, "a renamed file changes the digest");
    assert.throws(() => digestFiles([path.join(root, "gone.css")], root), /gone\.css, which the render reads, does not exist/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
