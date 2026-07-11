---
name: markdown-editing
description: Safe editing technique for long markdown files in Orleans.Lattice. Use whenever editing a markdown file longer than ~200 lines or one with repeated near-identical sibling bullets or table rows (long observability/metric tables in particular), where patch-style edits can silently drop neighbouring lines.
---

# Editing long markdown files

Patch-style edits are unsafe on long markdown files (`docs/**/*.md`). Use deterministic byte-level replacement with a match-count assertion and a `git diff` check instead.

Patch-style edit tools that rely on `// ...existing code...` markers and similarity matching are **unsafe on long markdown files** that contain many adjacent rows or bullets with similar prefixes (e.g. several metric-to-panel table rows at adjacent line numbers that differ only in the trailing prose). The tool can silently collapse or drop neighbouring rows and the regression is invisible until a reader notices a missing entry.

**Required workflow for any edit to a markdown file longer than ~200 lines, or any edit to a file whose surrounding context contains repeated near-identical sibling bullets or table rows (long observability/metric tables in particular):**

1. **Use deterministic byte-level replacement, not patch-style edits.** Read the file via `[System.IO.File]::ReadAllText`, perform an exact `String.Replace` (or a regex with an asserted match-count of exactly 1), and write back via `[System.IO.File]::WriteAllText`. The replacement string must be the verbatim final text - no `// ...existing code...` placeholders.

2. **Pre-condition: assert the old text matches exactly once.** Before replacing, count occurrences of the old string and throw if the count is anything other than 1. A 0 means your anchor text is wrong; a > 1 means your anchor isn't unique enough.

3. **Post-condition: `git diff` the file and visually verify only the intended lines changed.** The diff must show only the bullet you meant to change. If sibling bullets, paragraph breaks, or trailer text appear in the diff with `-` markers, the edit is wrong - `git checkout HEAD -- <file>` and retry with a more precise anchor.

4. **For new content (additions, not replacements), use line-anchored insertion.** Read the file, locate the anchor line via exact match, splice the new text after it, write back. Do not rely on the patch tool to "find the right place".

Reference template (PowerShell):

```powershell
$path = 'docs/lattice.dashboards/metrics-to-panel-map.md'
$old  = '| `orleans.lattice.example.counter` | ...full exact row... |'
$new  = $old + "`n| ``orleans.lattice.example.other`` | ...new row... |"
$content = [System.IO.File]::ReadAllText((Resolve-Path $path))
$count = ([regex]::Matches($content, [regex]::Escape($old))).Count
if ($count -ne 1) { throw "expected exactly 1 match, got $count" }
[System.IO.File]::WriteAllText((Resolve-Path $path), $content.Replace($old, $new))
# then: git diff $path  - verify only the intended line(s) changed
```

This rule overrides any general preference for patch-style edits when the target is markdown. Source code files are unaffected - `edit_file` remains the right tool for `.cs` edits.