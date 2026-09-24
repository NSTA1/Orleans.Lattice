import { readdirSync } from "node:fs";
import path from "node:path";

/**
 * The marker that names a snippet on a companion page. It must sit on the line
 * immediately above the fence it names.
 */
const MARKER = /^<!--\s*video-snippet:\s*([a-z0-9][a-z0-9/_-]*)\s*-->\s*$/;

// These two mirror the Roslyn harness (DocsSnippetCompilationTestsBase in
// test/shared) exactly: a verify fence opens at column 0 and closes at the next
// line that starts with three backticks. An indented fence is never compiled
// by the harness, so it must never be accepted here either - otherwise a video
// could show code that nothing compiled.
const VERIFY_FENCE_OPEN = /^```csharp\s+verify\s*$/;
const FENCE_CLOSE = /^```/;

/**
 * Extracts every marked snippet from one markdown document. A snippet is a
 * `<!-- video-snippet: <id> -->` line followed immediately by a
 * ```csharp verify fence, which is the fence the repository compiles.
 */
export function extractSnippets(markdown, source) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const snippets = [];
  for (let i = 0; i < lines.length; i++) {
    const marker = MARKER.exec(lines[i]);
    if (!marker) {
      continue;
    }
    const id = marker[1];
    if (i + 1 >= lines.length || !VERIFY_FENCE_OPEN.test(lines[i + 1])) {
      throw new Error(
        `${source}:${i + 1}: snippet '${id}' must be followed immediately by a column-0 \`\`\`csharp verify fence`,
      );
    }
    const body = [];
    let j = i + 2;
    while (j < lines.length && !FENCE_CLOSE.test(lines[j])) {
      body.push(lines[j]);
      j++;
    }
    if (j >= lines.length) {
      throw new Error(`${source}:${i + 2}: snippet '${id}' has no closing fence`);
    }
    snippets.push({ id, code: body.join("\n"), source, line: i + 1 });
    i = j;
  }
  return snippets;
}

/** Collects snippets from many documents ([{ source, text }]); an id may be defined once. */
export function collectSnippets(documents) {
  const byId = new Map();
  for (const { source, text } of documents) {
    for (const snippet of extractSnippets(text, source)) {
      const existing = byId.get(snippet.id);
      if (existing) {
        throw new Error(
          `snippet '${snippet.id}' is defined twice: ${existing.source}:${existing.line} and ${snippet.source}:${snippet.line}`,
        );
      }
      byId.set(snippet.id, snippet);
    }
  }
  return byId;
}

/** Escapes text for use as HTML element content. */
export function escapeHtml(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

const SNIPPET_TARGET = /(<code\b[^>]*?\bdata-snippet="([^"]+)"[^>]*>)([\s\S]*?)(<\/code>)/g;

/**
 * Rewrites the body of every `<code data-snippet="id">` element in an HTML
 * document to the named snippet, escaped. Returns the rewritten document and
 * the ids it used and could not resolve.
 */
export function applySnippets(html, snippets) {
  const used = [];
  const missing = [];
  const output = html.replace(SNIPPET_TARGET, (whole, open, id, _body, close) => {
    const snippet = snippets.get(id);
    if (!snippet) {
      missing.push(id);
      return whole;
    }
    used.push(id);
    return open + escapeHtml(snippet.code) + close;
  });
  return { output, used, missing };
}

/** Every file under `directory` (recursively, skipping node_modules) that satisfies `accept`. */
export function listFiles(directory, accept) {
  const found = [];
  const walk = (current) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        if (entry.name !== "node_modules") walk(full);
      } else if (accept(full)) {
        found.push(full);
      }
    }
  };
  walk(directory);
  return found.sort();
}
