import assert from "node:assert/strict";
import { test } from "node:test";
import { applySnippets, collectSnippets, escapeHtml, extractSnippets } from "../lib/snippets.js";

const page = [
  "# Hello, Lattice",
  "",
  "<!-- video-snippet: hello/register -->",
  "```csharp verify",
  "siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));",
  "```",
  "",
  "```csharp verify",
  "var unmarked = 1;",
  "```",
].join("\n");

test("a marked verify fence becomes a snippet and an unmarked one is ignored", () => {
  const snippets = extractSnippets(page, "docs/videos/hello.md");
  assert.equal(snippets.length, 1);
  assert.equal(snippets[0].id, "hello/register");
  assert.equal(snippets[0].line, 3);
  assert.equal(snippets[0].code, "siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));");
});

test("CRLF line endings are normalised", () => {
  const [snippet] = extractSnippets(page.replace(/\n/g, "\r\n"), "page.md");
  assert.ok(!snippet.code.includes("\r"));
});

test("a marker must be followed immediately by a column-0 csharp verify fence", () => {
  const cases = [
    "<!-- video-snippet: a -->\n\n```csharp verify\nx\n```",
    "<!-- video-snippet: a -->\n```csharp\nx\n```",
    "<!-- video-snippet: a -->\n  ```csharp verify\nx\n  ```",
  ];
  for (const markdown of cases) {
    assert.throws(() => extractSnippets(markdown, "page.md"), /must be followed immediately/);
  }
});

test("an unterminated fence is an error", () => {
  assert.throws(() => extractSnippets("<!-- video-snippet: a -->\n```csharp verify\nx", "page.md"), /no closing fence/);
});

test("an id defined twice is an error that names both places", () => {
  const document = { source: "a.md", text: page };
  assert.throws(() => collectSnippets([document, { ...document, source: "b.md" }]), /a\.md:3 and b\.md:3/);
});

test("a composition shows the snippet escaped, and the rewrite is idempotent", () => {
  const snippets = collectSnippets([
    { source: "page.md", text: "<!-- video-snippet: s -->\n```csharp verify\nif (a < b && c > d) { }\n```" },
  ]);
  const html = '<pre><code class="lv-code" data-snippet="s">stale</code></pre>';
  const first = applySnippets(html, snippets);
  assert.equal(first.output, '<pre><code class="lv-code" data-snippet="s">if (a &lt; b &amp;&amp; c &gt; d) { }</code></pre>');
  assert.deepEqual(first.used, ["s"]);
  assert.equal(applySnippets(first.output, snippets).output, first.output);
});

test("an id that no page defines is reported and left untouched", () => {
  const html = '<code data-snippet="nope">x</code>';
  const result = applySnippets(html, new Map());
  assert.deepEqual(result.missing, ["nope"]);
  assert.equal(result.output, html);
});

test("escapeHtml escapes the characters that matter in element content", () => {
  assert.equal(escapeHtml("<a & b>"), "&lt;a &amp; b&gt;");
});
