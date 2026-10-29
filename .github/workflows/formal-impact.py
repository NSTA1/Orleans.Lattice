#!/usr/bin/env python3
"""Formal-impact report: which refinement rows and abstraction gaps a diff touches.

Item 14 of the formal verification pipeline epic (#5148, #5162). The refinement
index (contract C6, written by Formal.RefinementIndexTests when
LATTICE_REFINEMENT_INDEX names a file) lists, for every TLA+ module, the
production symbols each refinement row names (behaviour the specification
models) and the production symbols each deliberate abstraction gap names
(behaviour the specification explicitly does not model). This script maps the
changed src/**/*.cs files of a diff to the types they declare and, best effort,
to the members whose lines changed, matches them against those symbols, and
writes a markdown report.

A symbol hit is one of:
  member  `Type.Member` whose member's lines changed;
  type    a bare `Type` whose declaration's lines changed;
  near    `Type.Member` whose type changed but whose member did not.
Member and type hits are direct; near hits are listed separately because a
large partial grain changes often without touching the member a row names.

The report is advisory. #5162 asked for a gating rule chosen by replaying
recent merged pull requests; the replay and the decision are recorded in the
pull request that added this file and in the GATING note below.

GATING: advisory, by measurement. Replaying the 299 pull requests merged to
main from 2026-09-29 to 2026-10-29 (the last 60 merged pull requests widened to
30 days) against the index with the #5156 gap register: 192 changed src C#,
115 (60% of those) hit a refinement row directly and 63 (33%) hit a gap. Rows
name most grain code, so a row-hit rule would demand a '## Formal impact'
section on most code pull requests and turn it into boilerplate. Gap hits are
dominated by gaps naming a whole grain (`BPlusLeafGrain` alone in 44 of the 63,
`ReplicationShipperGrain` in most of the rest); restricted to `Type.Member`
symbols they fall to 15 (5%), but that rule misses #5126, whose gap names
`TreeShardConsolidationGrain` bare. A body-reading gate also cannot be
satisfied by editing the body and re-running the job, because a re-run reuses
the original event payload. The --gate option implements the candidate rule (a
direct row or gap hit requires a '## Formal impact' section in the body) with
self-tests, so the rule can be enabled once gap symbols name members; CI does
not pass it.

Usage:
  formal-impact.py --self-test
  formal-impact.py --index FILE --base SHA --head SHA [--summary-file F]
                   [--json-out F] [--heat-days 90] [--gate --body-file F]
  formal-impact.py --replay --index FILE [--ref origin/main] [--limit 60]
                   [--days 30] [--commit SHA ...] [--json-out F]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA = 1
SRC_PREFIX = "src/"
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
GIT_TIMEOUT_SECONDS = 300
DEFAULT_HEAT_DAYS = 90
FIX_SUBJECT = re.compile(r"^fix[(:!]")
PR_SUBJECT = re.compile(r"\(#(?P<number>\d+)\)\s*$")
GAP_HEADING = "Changes code the specs explicitly do not model"
IMPACT_SECTION = re.compile(r"^##[ \t]+Formal impact[ \t]*$", re.MULTILINE | re.IGNORECASE)

TYPE_DECL = re.compile(
    r"\b(?P<kind>class|struct|interface|enum|record(?:\s+(?:class|struct))?)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)")
NAMESPACE_DECL = re.compile(r"\bnamespace\s+[A-Za-z_][\w.]*")
IDENTIFIER_BEFORE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^<>]*(?:<[^<>]*>[^<>]*)*>)?\s*$")
LEADING_ATTRIBUTES = re.compile(r"^\s*(?:\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]\s*)+")
HUNK = re.compile(r"^@@ -(?P<a>\d+)(?:,(?P<b>\d+))? \+(?P<c>\d+)(?:,(?P<d>\d+))? @@")

KEYWORDS = frozenset("""
abstract async base bool byte case catch char checked class const continue decimal default
delegate do double else enum event explicit extern false file finally fixed float for foreach
get goto if implicit in init int interface internal is lock long namespace new null object
operator out override params partial private protected public readonly record ref required
return sbyte scoped sealed set short sizeof stackalloc static string struct switch this throw
true try typeof uint ulong unchecked unsafe ushort using var virtual void volatile when where
while with yield
""".split())


# ---------------------------------------------------------------------------
# C# structure: types and members with their line spans.
# ---------------------------------------------------------------------------

def blank_comments_and_strings(text: str) -> str:
    """Returns text with comments and string/char literal contents replaced by
    spaces, preserving every newline so line numbers stay valid."""
    out: list[str] = []
    i, n = 0, len(text)

    def blank(chunk: str) -> str:
        return "".join(ch if ch == "\n" else " " for ch in chunk)

    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if ch == "/" and nxt == "/":
            end = text.find("\n", i)
            end = n if end < 0 else end
            out.append(blank(text[i:end]))
            i = end
            continue
        if ch == "/" and nxt == "*":
            end = text.find("*/", i + 2)
            end = n if end < 0 else end + 2
            out.append(blank(text[i:end]))
            i = end
            continue
        # String prefixes: $, @, $@, @$, $$ ... before a quote.
        j = i
        while j < n and text[j] in "$@":
            j += 1
        if j < n and text[j] == '"' and (j == i or j - i <= 4):
            prefix = text[i:j]
            quotes = 0
            k = j
            while k < n and text[k] == '"':
                quotes += 1
                k += 1
            if quotes >= 3:
                closer = '"' * quotes
                end = text.find(closer, k)
                end = n if end < 0 else end + quotes
            elif quotes == 2:
                end = k  # empty string ""
            else:
                verbatim = "@" in prefix
                k = j + 1
                while k < n:
                    if verbatim:
                        if text[k] == '"':
                            if k + 1 < n and text[k + 1] == '"':
                                k += 2
                                continue
                            break
                    else:
                        if text[k] == "\\":
                            k += 2
                            continue
                        if text[k] == '"' or text[k] == "\n":
                            break
                    k += 1
                end = min(n, k + 1)
            out.append(prefix + blank(text[j:end]))
            i = end
            continue
        if ch == "'" and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            k = i + 1
            while k < n and k - i <= 12:
                if text[k] == "\\":
                    k += 2
                    continue
                if text[k] in "'\n":
                    break
                k += 1
            if k < n and text[k] == "'":
                out.append(blank(text[i:k + 1]))
                i = k + 1
                continue
        out.append(ch)
        i += 1
    return "".join(out)


@dataclass
class Decl:
    kind: str  # "type" or "member"
    type_name: str  # innermost enclosing (or declared) type
    name: str
    start: int
    end: int


def member_name(header: str) -> str | None:
    """Best-effort member name of a declaration header (attributes, modifiers,
    return type, name, parameters) at type-body depth."""
    text = LEADING_ATTRIBUTES.sub("", header).strip()
    if not text or text.startswith("[") or re.match(r"^[;,]", text):
        return None
    if re.search(r"\boperator\b", text):
        return None
    depth = 0
    angle = 0
    for idx, ch in enumerate(text):
        if ch == "(":
            if depth == 0 and angle == 0:
                match = IDENTIFIER_BEFORE.search(text[:idx])
                if match and match.group(1) not in KEYWORDS:
                    return match.group(1)
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "<" and depth == 0:
            angle += 1
        elif ch == ">" and depth == 0 and angle > 0 and text[idx - 1] != "=":
            angle -= 1
        elif depth == 0 and angle == 0 and ch in "{=;,":
            match = IDENTIFIER_BEFORE.search(text[:idx])
            if match and match.group(1) not in KEYWORDS:
                return match.group(1)
            return None
    match = IDENTIFIER_BEFORE.search(text)
    if match and match.group(1) not in KEYWORDS:
        return match.group(1)
    return None


def parse_declarations(source: str) -> list[Decl]:
    """Types and members with 1-based inclusive line spans. Never raises on
    malformed input: an unbalanced file yields whatever spans closed."""
    text = blank_comments_and_strings(source)
    decls: list[Decl] = []
    # Scope stack: (kind, name, body_depth, start_line); kind in type/enum/ns/member/block.
    scopes: list[tuple[str, str, int, int]] = []
    depth = 0
    header: list[str] = []
    header_start = 0
    header_parens = 0
    expression = False  # header saw a top-level '=' or '=>': braces are an initializer

    def innermost_type() -> tuple[str, str, int, int] | None:
        for scope in reversed(scopes):
            if scope[0] in ("type", "enum"):
                return scope
            if scope[0] in ("member", "block"):
                return None
        return None

    def at_type_body() -> tuple[str, str, int, int] | None:
        if scopes and scopes[-1][0] in ("type", "enum") and scopes[-1][2] == depth:
            return scopes[-1]
        return None

    def in_member() -> bool:
        return any(scope[0] in ("member", "block") for scope in scopes)

    def in_enum_body() -> bool:
        scope = at_type_body()
        return scope is not None and scope[0] == "enum"

    def reset_header() -> None:
        nonlocal header, header_start, header_parens, expression
        header = []
        header_start = 0
        header_parens = 0
        expression = False

    line = 1
    for idx, ch in enumerate(text):
        if ch == "\n":
            line += 1
            if header:
                header.append(" ")
            continue
        if in_member() and not expression:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if scopes and scopes[-1][2] == depth + 1:
                    kind, name, _, start = scopes.pop()
                    if kind == "member":
                        owner = innermost_type()
                        decls.append(Decl("member", owner[1] if owner else "", name, start, line))
                    reset_header()
            continue

        if not header and not ch.isspace():
            header_start = line
        if not ch.isspace() or header:
            header.append(ch)
        joined = "".join(header) if ch in "=,{};" else ""

        if ch == "(":
            header_parens += 1
        elif ch == ")":
            header_parens -= 1
        elif ch == "=" and header_parens == 0 and not re.search(r"\boperator\b", joined) \
                and not in_enum_body():
            expression = True
        elif ch == "," and header_parens == 0 and not expression:
            scope = at_type_body()
            if scope and scope[0] == "enum":
                name = member_name(joined[:-1])
                if name:
                    decls.append(Decl("member", scope[1], name, header_start, line))
                reset_header()
        elif ch == "{":
            if expression:
                depth += 1
                scopes.append(("block", "", depth, line))
                # An initializer's braces belong to the pending member header.
                continue
            type_match = TYPE_DECL.search(joined) if header_parens == 0 or "(" in joined else None
            if type_match and not re.search(r"\bnew\s*$", joined[:type_match.start()]) \
                    and type_match.group("name") not in KEYWORDS:
                depth += 1
                kind = "enum" if type_match.group("kind") == "enum" else "type"
                scopes.append((kind, type_match.group("name"), depth, header_start))
                reset_header()
                continue
            if NAMESPACE_DECL.search(joined) and not at_type_body():
                depth += 1
                scopes.append(("ns", "", depth, header_start))
                reset_header()
                continue
            owner = at_type_body()
            depth += 1
            if owner:
                name = member_name(joined[:-1]) or "?"
                scopes.append(("member", name, depth, header_start))
            else:
                scopes.append(("block", "", depth, header_start))
            reset_header()
            continue
        elif ch == "}":
            depth -= 1
            if scopes and scopes[-1][2] == depth + 1:
                kind, name, _, start = scopes.pop()
                if kind in ("type", "enum"):
                    if header and kind == "enum":
                        tail = member_name(joined[:-1])
                        if tail:
                            decls.append(Decl("member", name, tail, header_start, line))
                    decls.append(Decl("type", name, name, start, line))
                    reset_header()
                elif kind == "block" and expression:
                    # Initializer closed; keep collecting until ';'.
                    pass
                else:
                    reset_header()
            continue
        elif ch == ";" and header_parens == 0:
            owner = at_type_body()
            type_match = TYPE_DECL.search(joined)
            if type_match and type_match.group("name") not in KEYWORDS and not expression:
                decls.append(Decl("type", type_match.group("name"), type_match.group("name"),
                                  header_start, line))
            elif owner:
                name = member_name(joined[:-1])
                if name:
                    decls.append(Decl("member", owner[1], name, header_start, line))
            reset_header()
    return decls


def declared_types(source: str) -> set[str]:
    return {decl.name for decl in parse_declarations(source) if decl.kind == "type"}


def touched(decls: list[Decl], lines: set[int]) -> tuple[set[str], set[str]]:
    """Types and Type.Member names whose span contains a changed line."""
    types: set[str] = set()
    members: set[str] = set()
    for decl in decls:
        if any(decl.start <= ln <= decl.end for ln in lines):
            if decl.kind == "type":
                types.add(decl.name)
            elif decl.type_name:
                members.add(f"{decl.type_name}.{decl.name}")
                types.add(decl.type_name)
    return types, members


# ---------------------------------------------------------------------------
# Git.
# ---------------------------------------------------------------------------

def git(*args: str, cwd: str | None = None, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, check=False,
        encoding="utf-8", errors="replace", timeout=GIT_TIMEOUT_SECONDS)
    if check and completed.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout


@dataclass
class FileChange:
    old_path: str | None
    new_path: str | None
    old_lines: set[int] = field(default_factory=set)
    new_lines: set[int] = field(default_factory=set)


def parse_diff(diff: str) -> list[FileChange]:
    changes: list[FileChange] = []
    current: FileChange | None = None
    for raw in diff.splitlines():
        if raw.startswith("diff --git "):
            current = FileChange(None, None)
            changes.append(current)
            continue
        if current is None:
            continue
        if raw.startswith("--- "):
            current.old_path = None if raw[4:] == "/dev/null" else raw[4:].removeprefix("a/")
        elif raw.startswith("+++ "):
            current.new_path = None if raw[4:] == "/dev/null" else raw[4:].removeprefix("b/")
        elif raw.startswith("rename from "):
            current.old_path = raw[len("rename from "):]
        elif raw.startswith("rename to "):
            current.new_path = raw[len("rename to "):]
        else:
            match = HUNK.match(raw)
            if match:
                a, b = int(match["a"]), int(match["b"] if match["b"] is not None else 1)
                c, d = int(match["c"]), int(match["d"] if match["d"] is not None else 1)
                current.old_lines.update(range(a, a + b))
                current.new_lines.update(range(c, c + d))
    return changes


def is_src_cs(path: str | None) -> bool:
    return bool(path) and path.startswith(SRC_PREFIX) and path.endswith(".cs")


@dataclass
class DiffImpact:
    files: list[str]
    types: set[str]
    members: set[str]


def read_blobs(specs: list[str], cwd: str | None) -> dict[str, str]:
    """`rev:path` -> text for every spec, in one `git cat-file --batch` call.
    A missing object maps to the empty string."""
    if not specs:
        return {}
    completed = subprocess.run(
        ["git", "cat-file", "--batch"], cwd=cwd, input=("\n".join(specs) + "\n").encode("utf-8"),
        capture_output=True, check=False, timeout=GIT_TIMEOUT_SECONDS)
    if completed.returncode != 0:
        raise RuntimeError(f"git cat-file --batch failed: {completed.stderr.decode('utf-8', 'replace')}")
    data = completed.stdout
    blobs: dict[str, str] = {}
    offset = 0
    for spec in specs:
        newline = data.index(b"\n", offset)
        header = data[offset:newline].decode("utf-8", "replace").split()
        offset = newline + 1
        if len(header) == 3 and header[1] == "blob":
            size = int(header[2])
            blobs[spec] = data[offset:offset + size].decode("utf-8", "replace").lstrip("\ufeff")
            offset += size + 1
        else:
            blobs[spec] = ""
    return blobs


def mentions(names: set[str] | None):
    """A predicate: does a text mention any of `names` as a word? None means
    every text qualifies. Skipping a file that mentions no indexed type loses
    no hit: a symbol can only match a type the file declares, by name."""
    if names is None:
        return lambda text: True
    if not names:
        return lambda text: False
    pattern = re.compile(r"\b(?:" + "|".join(sorted(map(re.escape, names))) + r")\b")
    return lambda text: bool(pattern.search(text))


def diff_impact(base: str, head: str, cwd: str | None = None,
                indexed_types: set[str] | None = None) -> DiffImpact:
    """Changed src C# files between base and head, with the types and members
    whose lines changed on either side. With indexed_types, only files that
    mention one of those type names are parsed."""
    diff = git("diff", "-U0", "-M", "--no-color", "--no-ext-diff", base, head,
               "--", ":(glob)src/**/*.cs", cwd=cwd)
    changes = [change for change in parse_diff(diff)
               if is_src_cs(change.old_path) or is_src_cs(change.new_path)]
    specs = []
    for change in changes:
        if change.old_path and change.old_lines:
            specs.append(f"{base}:{change.old_path}")
        if change.new_path and change.new_lines:
            specs.append(f"{head}:{change.new_path}")
    blobs = read_blobs(specs, cwd)
    relevant = mentions(indexed_types)
    files: list[str] = []
    types: set[str] = set()
    members: set[str] = set()
    for change in changes:
        files.append(change.new_path or change.old_path or "")
        for rev, path, lines in ((base, change.old_path, change.old_lines),
                                 (head, change.new_path, change.new_lines)):
            if not (path and lines):
                continue
            text = blobs.get(f"{rev}:{path}", "")
            if not relevant(text):
                continue
            t, m = touched(parse_declarations(text), lines)
            types |= t
            members |= m | concern_members(path, t)
    return DiffImpact(sorted(set(files)), types, members)


def indexed_type_names(modules: list[dict]) -> set[str]:
    names: set[str] = set()
    for module in modules:
        for row in module["rows"]:
            names.update(symbol.split(".")[0] for symbol in row["production"])
        for gap in module["gaps"]:
            names.update(symbol.split(".")[0] for symbol in gap["symbols"])
    return names


def concern_members(path: str, types: set[str]) -> set[str]:
    """A partial-class concern file `Type.Concern.cs` whose Type changed also
    counts as `Type.Concern`: notes name concerns that way (`ShardRootGrain.Split`)."""
    parts = Path(path).name[:-len(".cs")].split(".")
    if len(parts) >= 2 and parts[0] in types:
        return {f"{parts[0]}.{parts[1]}"}
    return set()


# ---------------------------------------------------------------------------
# Index and matching.
# ---------------------------------------------------------------------------

class IndexError_(ValueError):
    pass


def load_index(path: str) -> list[dict]:
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise IndexError_(f"cannot read refinement index {path}: {error}") from error
    if not isinstance(document, dict) or document.get("schema") != SCHEMA:
        raise IndexError_(f"refinement index {path}: expected schema {SCHEMA}")
    modules = document.get("modules")
    if not isinstance(modules, list) or not modules:
        raise IndexError_(f"refinement index {path}: 'modules' must be a non-empty list")
    for module in modules:
        if not isinstance(module, dict) or not isinstance(module.get("module"), str) \
                or not isinstance(module.get("rows"), list) or not isinstance(module.get("gaps"), list):
            raise IndexError_(f"refinement index {path}: malformed module entry")
        for row in module["rows"]:
            if not all(isinstance(row.get(key), list) for key in ("production", "detectors")) \
                    or not isinstance(row.get("label"), str):
                raise IndexError_(f"refinement index {path}: malformed row in {module['module']}")
        for gap in module["gaps"]:
            if not isinstance(gap.get("symbols"), list) or not isinstance(gap.get("title"), str):
                raise IndexError_(f"refinement index {path}: malformed gap in {module['module']}")
    return modules


def classify(symbol: str, impact: DiffImpact) -> str | None:
    """'member', 'type', 'near' or None for one index symbol."""
    parts = symbol.split(".")
    type_name = parts[0]
    if type_name not in impact.types:
        return None
    if len(parts) == 1:
        return "type"
    return "member" if f"{type_name}.{parts[1]}" in impact.members else "near"


@dataclass
class Hit:
    module: str
    note: str
    kind: str  # "row" or "gap"
    section: str
    label: str
    direct: list[str]
    near: list[str]
    detectors: list[str]


def match(modules: list[dict], impact: DiffImpact) -> list[Hit]:
    hits: list[Hit] = []
    for module in modules:
        entries = [("row", row.get("section", ""), row["label"], row["production"], row["detectors"])
                   for row in module["rows"]]
        entries += [("gap", "", gap["title"], gap["symbols"], []) for gap in module["gaps"]]
        for kind, section, label, symbols, detectors in entries:
            direct, near = [], []
            for symbol in symbols:
                verdict = classify(symbol, impact)
                if verdict in ("member", "type"):
                    direct.append(symbol)
                elif verdict == "near":
                    near.append(symbol)
            if direct or near:
                hits.append(Hit(module["module"], module.get("note", ""), kind, section, label,
                                sorted(set(direct)), sorted(set(near)), list(detectors)))
    return hits


# ---------------------------------------------------------------------------
# Gap heat.
# ---------------------------------------------------------------------------

def type_files(head: str, cwd: str | None) -> dict[str, set[str]]:
    """Every src C# file at head declaring each type name (partials included)."""
    out = git("grep", "-n", "-E",
              r"(class|struct|interface|enum|record)[[:space:]]+[A-Za-z_][A-Za-z0-9_]*",
              head, "--", ":(glob)src/**/*.cs", cwd=cwd, check=False)
    files: dict[str, set[str]] = {}
    for raw in out.splitlines():
        # rev:path:line:text
        parts = raw.split(":", 3)
        if len(parts) < 4:
            continue
        path, text = parts[1], parts[3]
        code = blank_comments_and_strings(text)
        for match_ in TYPE_DECL.finditer(code):
            name = match_.group("name")
            if name not in KEYWORDS:
                files.setdefault(name, set()).add(path)
    return files


def files_for_symbol(symbol: str, head: str, declared: dict[str, set[str]], cwd: str | None) -> set[str]:
    parts = symbol.split(".")
    candidates = declared.get(parts[0], set())
    if len(parts) < 2 or len(candidates) <= 1:
        return set(candidates)
    out = git("grep", "-l", "-w", parts[1], head, "--", *sorted(candidates), cwd=cwd, check=False)
    narrowed = {line.split(":", 1)[1] for line in out.splitlines() if ":" in line}
    return narrowed or set(candidates)


def gap_heat(modules: list[dict], head: str, days: int, cwd: str | None) -> list[tuple[str, str, int, int]]:
    """(module, gap title, fix commits in the window, files) per gap naming symbols."""
    declared = type_files(head, cwd)
    head_time = int(git("show", "-s", "--format=%ct", head, cwd=cwd).strip())
    since = head_time - days * 86400
    rows = []
    for module in modules:
        for gap in module["gaps"]:
            if not gap["symbols"]:
                continue
            paths: set[str] = set()
            for symbol in gap["symbols"]:
                paths |= files_for_symbol(symbol, head, declared, cwd)
            count = 0
            if paths:
                log = git("log", "--format=%ct%x09%s", head, "--", *sorted(paths), cwd=cwd, check=False)
                for line in log.splitlines():
                    stamp, _, subject = line.partition("\t")
                    if stamp.isdigit() and since <= int(stamp) <= head_time and FIX_SUBJECT.match(subject):
                        count += 1
            rows.append((module["module"], gap["title"], count, len(paths)))
    rows.sort(key=lambda row: (-row[2], row[0], row[1]))
    return rows


# ---------------------------------------------------------------------------
# Report and gate.
# ---------------------------------------------------------------------------

def requires_impact_section(hits: list[Hit]) -> bool:
    """The candidate gating rule: a direct row or gap hit."""
    return any(hit.direct for hit in hits)


def body_has_impact_section(body: str) -> bool:
    return bool(IMPACT_SECTION.search(body or ""))


def code(items: list[str]) -> str:
    return ", ".join(f"`{item}`" for item in items) if items else "-"


def cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render(impact: DiffImpact, hits: list[Hit], heat: list[tuple[str, str, int, int]] | None,
           base: str, head: str, heat_days: int) -> str:
    lines = ["## Formal impact", ""]
    lines.append(f"Advisory. Diff `{base[:12]}..{head[:12]}` (merge base to head): "
                 f"{len(impact.files)} changed `src/` C# file(s); {len(impact.types)} type(s) and "
                 f"{len(impact.members)} member(s) with changed lines in files that mention a type "
                 "the refinement index names.")
    lines.append("")
    rows = [hit for hit in hits if hit.kind == "row" and hit.direct]
    gaps = [hit for hit in hits if hit.kind == "gap" and hit.direct]
    near = [hit for hit in hits if not hit.direct]
    if not impact.files:
        lines.append("No `src/` C# file changed, so no refinement row or gap is affected.")
    elif not hits:
        lines.append("No refinement row or deliberate abstraction gap names a changed symbol.")
    if rows:
        modules = sorted({hit.module for hit in rows})
        lines += ["", f"### Modelled behaviour: {len(rows)} refinement row(s) in "
                  f"{len(modules)} module(s)", "",
                  "| Module | Section | Row | Changed symbols | Detectors the row cites |",
                  "|---|---|---|---|---|"]
        for hit in rows:
            lines.append(f"| {hit.module} | {cell(hit.section)} | {cell(hit.label)} | "
                         f"{code(hit.direct)} | {code(hit.detectors)} |")
    if gaps:
        lines += ["", f"### {GAP_HEADING}", "",
                  "These deliberate abstraction gaps name a changed symbol. No model checks "
                  "this behaviour; review it as unverified by TLA+.", "",
                  "| Module | Gap | Changed symbols | Note |", "|---|---|---|---|"]
        for hit in gaps:
            lines.append(f"| {hit.module} | {cell(hit.label)} | {code(hit.direct)} | `{hit.note}` |")
    if near:
        lines += ["", "<details><summary>"
                  f"{len(near)} row(s) or gap(s) name a member of a changed type whose own lines "
                  "did not change</summary>", "",
                  "| Module | Kind | Row or gap | Unchanged members of changed types |",
                  "|---|---|---|---|"]
        for hit in near:
            lines.append(f"| {hit.module} | {hit.kind} | {cell(hit.label)} | {code(hit.near)} |")
        lines += ["", "</details>"]
    if impact.members:
        shown = sorted(impact.members)
        lines += ["", "<details><summary>Changed members (best effort, from diff hunks)"
                  "</summary>", "", code(shown[:200]) + (" ..." if len(shown) > 200 else ""),
                  "", "</details>"]
    if heat is not None:
        lines += ["", f"<details><summary>Gap heat: `fix` commits in the {heat_days} days to "
                  f"head touching each gap's files</summary>", "",
                  "| Fix commits | Module | Gap | Files |", "|---|---|---|---|"]
        for module, title, count, file_count in heat:
            lines.append(f"| {count} | {module} | {cell(title)} | {file_count} |")
        lines += ["", "</details>"]
    lines.append("")
    return "\n".join(lines)


def to_json(impact: DiffImpact, hits: list[Hit]) -> dict:
    return {
        "files": impact.files,
        "types": sorted(impact.types),
        "members": sorted(impact.members),
        "rows": [hit.__dict__ for hit in hits if hit.kind == "row"],
        "gaps": [hit.__dict__ for hit in hits if hit.kind == "gap"],
        "requiresImpactSection": requires_impact_section(hits),
    }


# ---------------------------------------------------------------------------
# Replay.
# ---------------------------------------------------------------------------

def replay_commits(ref: str, limit: int, days: int, cwd: str | None) -> list[tuple[str, str, int]]:
    """(sha, subject, commit time) of first-parent squash merges on ref: the
    last `limit` pull requests, widened to every one in the last `days`."""
    log = git("log", "--first-parent", "--format=%H%x09%ct%x09%s", ref, cwd=cwd)
    entries = []
    for raw in log.splitlines():
        sha, stamp, subject = raw.split("\t", 2)
        if PR_SUBJECT.search(subject):
            entries.append((sha, subject, int(stamp)))
    if not entries:
        return []
    newest = entries[0][2]
    return [entry for index, entry in enumerate(entries)
            if index < limit or entry[2] >= newest - days * 86400]


def replay(modules: list[dict], commits: list[tuple[str, str, int]], cwd: str | None) -> list[dict]:
    indexed = indexed_type_names(modules)
    results = []
    for sha, subject, stamp in commits:
        parent = git("rev-parse", "--verify", "-q", f"{sha}^1", cwd=cwd, check=False).strip()
        impact = diff_impact(parent or EMPTY_TREE, sha, cwd, indexed)
        hits = match(modules, impact)
        number = PR_SUBJECT.search(subject)
        results.append({
            "sha": sha, "pr": int(number["number"]) if number else None, "subject": subject,
            "time": stamp, "srcFiles": len(impact.files),
            "rowHits": sum(1 for hit in hits if hit.kind == "row" and hit.direct),
            "gapHits": sum(1 for hit in hits if hit.kind == "gap" and hit.direct),
            "nearHits": sum(1 for hit in hits if not hit.direct),
            "modules": sorted({hit.module for hit in hits if hit.direct}),
            "gaps": sorted({f"{hit.module}: {hit.label}" for hit in hits if hit.kind == "gap" and hit.direct}),
            "requiresImpactSection": requires_impact_section(hits),
        })
    return results


def summarise_replay(results: list[dict]) -> str:
    total = len(results)
    src = [r for r in results if r["srcFiles"]]
    rows = [r for r in results if r["rowHits"]]
    gaps = [r for r in results if r["gapHits"]]
    gated = [r for r in results if r["requiresImpactSection"]]
    lines = [f"Replayed {total} merged pull requests; {len(src)} changed src C# files.",
             f"Direct row hit: {len(rows)} ({pct(len(rows), total)} of all, {pct(len(rows), len(src))} of src-changing).",
             f"Direct gap hit: {len(gaps)} ({pct(len(gaps), total)} of all, {pct(len(gaps), len(src))} of src-changing).",
             f"Candidate gate would require '## Formal impact': {len(gated)} ({pct(len(gated), total)}).",
             "", "| PR | src files | rows | gaps | near | modules | gaps hit |", "|---|---|---|---|---|---|---|"]
    for r in results:
        lines.append(f"| #{r['pr']} | {r['srcFiles']} | {r['rowHits']} | {r['gapHits']} | {r['nearHits']} | "
                     f"{', '.join(r['modules']) or '-'} | {cell('; '.join(r['gaps'])) or '-'} |")
    return "\n".join(lines)


def pct(part: int, whole: int) -> str:
    return f"{(100.0 * part / whole):.0f}%" if whole else "n/a"


# ---------------------------------------------------------------------------
# Self-test.
# ---------------------------------------------------------------------------

class ParserTests(unittest.TestCase):
    SOURCE = (
        "namespace Demo.Grains;\n"                                   # 1
        "\n"                                                          # 2
        "// class NotAType in a comment\n"                            # 3
        "internal sealed partial class FoldGrain(IFoo foo) : Grain, IFoldGrain\n"  # 4
        "{\n"                                                         # 5
        "    private const string Label = \"class Fake { }\";\n"      # 6
        "    private readonly Dictionary<string, int> _map = new()\n" # 7
        "    {\n"                                                     # 8
        "        [\"a\"] = 1,\n"                                      # 9
        "    };\n"                                                    # 10
        "\n"                                                          # 11
        "    [Test]\n"                                                # 12
        "    public async Task<int> FoldAsync<T>(int x = 3) where T : class\n"  # 13
        "    {\n"                                                     # 14
        "        if (x > 0) { return 1; }\n"                          # 15
        "        return 0;\n"                                         # 16
        "    }\n"                                                     # 17
        "\n"                                                          # 18
        "    public int Count => _map.Count;\n"                       # 19
        "\n"                                                          # 20
        "    public (int A, int B) Pair() => (1, 2);\n"               # 21
        "\n"                                                          # 22
        "    private sealed class Inner\n"                            # 23
        "    {\n"                                                     # 24
        "        public string Name { get; init; } = \"}\";\n"        # 25
        "    }\n"                                                     # 26
        "}\n"                                                         # 27
        "\n"                                                          # 28
        "public enum FoldState { Idle = 0, Draining, Done }\n"        # 29
        "public readonly record struct FoldStamp(long Ticks);\n"      # 30
    )

    def decls(self):
        return {(d.kind, d.type_name, d.name): (d.start, d.end) for d in parse_declarations(self.SOURCE)}

    def test_types_are_declared_with_spans_and_comments_and_strings_are_ignored(self):
        decls = self.decls()
        self.assertEqual(decls[("type", "FoldGrain", "FoldGrain")], (4, 27))
        self.assertEqual(decls[("type", "Inner", "Inner")], (23, 26))
        self.assertEqual(decls[("type", "FoldState", "FoldState")], (29, 29))
        self.assertEqual(decls[("type", "FoldStamp", "FoldStamp")], (30, 30))
        self.assertEqual(declared_types(self.SOURCE), {"FoldGrain", "Inner", "FoldState", "FoldStamp"})

    def test_members_are_found_with_spans(self):
        decls = self.decls()
        self.assertEqual(decls[("member", "FoldGrain", "Label")], (6, 6))
        self.assertEqual(decls[("member", "FoldGrain", "_map")], (7, 10))
        self.assertEqual(decls[("member", "FoldGrain", "FoldAsync")], (12, 17))
        self.assertEqual(decls[("member", "FoldGrain", "Count")], (19, 19))
        self.assertEqual(decls[("member", "FoldGrain", "Pair")], (21, 21))
        self.assertEqual(decls[("member", "Inner", "Name")], (25, 25))
        self.assertIn(("member", "FoldState", "Draining"), decls)
        self.assertIn(("member", "FoldState", "Done"), decls)

    def test_touched_maps_lines_to_types_and_members(self):
        decls = parse_declarations(self.SOURCE)
        types, members = touched(decls, {16})
        self.assertEqual(types, {"FoldGrain"})
        self.assertEqual(members, {"FoldGrain.FoldAsync"})
        types, members = touched(decls, {25})
        self.assertEqual(types, {"FoldGrain", "Inner"})
        self.assertEqual(members, {"Inner.Name"})
        self.assertEqual(touched(decls, {2}), (set(), set()))

    def test_unbalanced_input_does_not_raise(self):
        parse_declarations("class A {\n void F() {\n")
        parse_declarations("}}}\n")
        parse_declarations('var s = """\nraw " text\n""";\n')

    def test_member_name_shapes(self):
        self.assertEqual(concern_members("src/a/ShardRootGrain.Split.cs", {"ShardRootGrain"}),
                         {"ShardRootGrain.Split"})
        self.assertEqual(concern_members("src/a/ShardRootGrain.cs", {"ShardRootGrain"}), set())
        self.assertEqual(concern_members("src/a/Other.Split.cs", {"ShardRootGrain"}), set())
        self.assertEqual(member_name("public Task<int> FoldAsync<T>(int x) "), "FoldAsync")
        self.assertEqual(member_name("[Obsolete] public static int Max "), "Max")
        self.assertEqual(member_name("public (int A, int B) Pair() "), "Pair")
        self.assertIsNone(member_name("public static bool operator ==(A a, A b) "))
        self.assertIsNone(member_name("[Test]"))


class IndexTests(unittest.TestCase):
    def write(self, document) -> str:
        handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
        with handle:
            handle.write(document if isinstance(document, str) else json.dumps(document))
        self.addCleanup(os.unlink, handle.name)
        return handle.name

    def test_rejects_malformed_indexes(self):
        for bad in ("not json", {"schema": 2, "modules": []}, {"schema": 1, "modules": []},
                    {"schema": 1, "modules": [{"module": "M", "rows": [{}], "gaps": []}]},
                    {"schema": 1, "modules": [{"module": "M", "rows": [], "gaps": [{"title": "t"}]}]}):
            with self.assertRaises(IndexError_):
                load_index(self.write(bad))
        with self.assertRaises(IndexError_):
            load_index(os.path.join(tempfile.gettempdir(), "missing-formal-impact-index.json"))

    def test_classify_and_match(self):
        impact = DiffImpact(["src/a.cs"], {"FoldGrain", "Inner"}, {"FoldGrain.FoldAsync"})
        self.assertEqual(classify("FoldGrain.FoldAsync", impact), "member")
        self.assertEqual(classify("FoldGrain.Other", impact), "near")
        self.assertEqual(classify("Inner", impact), "type")
        self.assertIsNone(classify("Elsewhere.FoldAsync", impact))
        modules = [{"module": "M", "note": "spec/m/Refinement.md",
                    "rows": [{"section": "Action mapping", "label": "`Fold`",
                              "production": ["FoldGrain.FoldAsync"], "detectors": ["T.A"]},
                             {"section": "Action mapping", "label": "`Other`",
                              "production": ["FoldGrain.Other"], "detectors": []},
                             {"section": "Action mapping", "label": "`Cold`",
                              "production": ["Cold.X"], "detectors": []}],
                    "gaps": [{"title": "Consolidation", "symbols": ["Inner"]},
                             {"title": "Prose only", "symbols": []}]}]
        hits = match(modules, impact)
        self.assertEqual([(h.kind, h.label, h.direct, h.near) for h in hits],
                         [("row", "`Fold`", ["FoldGrain.FoldAsync"], []),
                          ("row", "`Other`", [], ["FoldGrain.Other"]),
                          ("gap", "Consolidation", ["Inner"], [])])
        report = render(impact, hits, [("M", "Consolidation", 2, 1)], "a" * 40, "b" * 40, 90)
        self.assertIn(f"### {GAP_HEADING}", report)
        self.assertIn("`T.A`", report)
        self.assertIn("| 2 | M | Consolidation | 1 |", report)
        self.assertTrue(requires_impact_section(hits))
        self.assertFalse(requires_impact_section([h for h in hits if not h.direct]))


class GateTests(unittest.TestCase):
    def test_impact_section_detection(self):
        self.assertTrue(body_has_impact_section("## Summary\nx\n\n## Formal impact\nNone.\n"))
        self.assertFalse(body_has_impact_section("## Summary\nFormal impact: none\n"))
        self.assertFalse(body_has_impact_section("### Formal impact\n"))
        self.assertFalse(body_has_impact_section(""))


class GitEndToEndTests(unittest.TestCase):
    """Drives the diff, heat and replay paths over a scratch repository."""

    def setUp(self):
        self.repo = tempfile.mkdtemp(prefix="formal-impact-")
        git("init", "-q", "-b", "main", cwd=self.repo)
        self.when = 1_700_000_000

    def tearDown(self):
        import shutil
        shutil.rmtree(self.repo, ignore_errors=True)

    def commit(self, files: dict[str, str | None], subject: str) -> str:
        for path, text in files.items():
            full = Path(self.repo, path)
            if text is None:
                full.unlink()
                continue
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(text, encoding="utf-8", newline="\n")
        git("add", "-A", cwd=self.repo)
        self.when += 86400
        env = dict(os.environ, GIT_AUTHOR_DATE=f"@{self.when} +0000", GIT_COMMITTER_DATE=f"@{self.when} +0000")
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q",
                        "-m", subject], cwd=self.repo, env=env, check=True, timeout=GIT_TIMEOUT_SECONDS)
        return git("rev-parse", "HEAD", cwd=self.repo).strip()

    GRAIN = ("namespace D;\ninternal sealed class FoldGrain\n{\n    public int FoldAsync()\n    {\n"
             "        return 1;\n    }\n\n    public int Retire()\n    {\n        return 2;\n    }\n}\n")
    MODULES = [{"module": "ShardOwnership", "note": "spec/so/Refinement.md",
                "rows": [{"section": "Action mapping", "label": "`Retire`",
                          "production": ["FoldGrain.Retire"], "detectors": ["FoldTests.Retires"]}],
                "gaps": [{"title": "Consolidation", "symbols": ["FoldGrain"]}]}]

    def test_diff_heat_and_replay(self):
        base = self.commit({"src/D/FoldGrain.cs": self.GRAIN, "src/D/Other.cs": "class Other { }\n",
                            "docs/x.md": "x\n"}, "feat(d): add fold (#1)")
        fix = self.commit({"src/D/FoldGrain.cs": self.GRAIN.replace("return 1;", "return 3;")},
                          "fix(d): sweep before drain (#2)")
        docs = self.commit({"docs/x.md": "y\n"}, "docs: words (#3)")
        unrelated = self.commit({"src/D/Other.cs": "class Other { int X; }\n"}, "fix(d): other (#4)")
        retire = self.commit({"src/D/FoldGrain.cs": self.GRAIN.replace("return 2;", "return 4;")},
                             "refactor(d): retire (#5)")

        impact = diff_impact(base, fix, self.repo)
        self.assertEqual(impact.files, ["src/D/FoldGrain.cs"])
        self.assertEqual(impact.members, {"FoldGrain.FoldAsync"})
        indexed = indexed_type_names(self.MODULES)
        self.assertEqual(indexed, {"FoldGrain"})
        filtered = diff_impact(base, fix, self.repo, indexed)
        self.assertEqual((filtered.types, filtered.members), (impact.types, impact.members))
        self.assertEqual(diff_impact(base, fix, self.repo, set()).types, set())
        self.assertEqual(diff_impact(base, fix, self.repo, {"Unrelated"}).files,
                         ["src/D/FoldGrain.cs"])
        blobs = read_blobs([f"{fix}:src/D/FoldGrain.cs", f"{fix}:src/D/Missing.cs", f"{base}:docs/x.md"],
                           self.repo)
        self.assertIn("return 3;", blobs[f"{fix}:src/D/FoldGrain.cs"])
        self.assertEqual(blobs[f"{fix}:src/D/Missing.cs"], "")
        self.assertEqual(blobs[f"{base}:docs/x.md"], "x\n")
        hits = match(self.MODULES, impact)
        self.assertEqual([(h.kind, h.direct, h.near) for h in hits],
                         [("row", [], ["FoldGrain.Retire"]), ("gap", ["FoldGrain"], [])])

        retire_hits = match(self.MODULES, diff_impact(unrelated, retire, self.repo))
        self.assertEqual([(h.kind, h.direct) for h in retire_hits],
                         [("row", ["FoldGrain.Retire"]), ("gap", ["FoldGrain"])])

        self.assertEqual(diff_impact(fix, docs, self.repo).files, [])
        self.assertEqual(match(self.MODULES, diff_impact(docs, unrelated, self.repo)), [])

        deleted = self.commit({"src/D/FoldGrain.cs": None}, "refactor(d): drop fold (#6)")
        gone = diff_impact(retire, deleted, self.repo)
        self.assertIn("FoldGrain", gone.types)

        heat = gap_heat(self.MODULES, retire, 90, self.repo)
        self.assertEqual(heat, [("ShardOwnership", "Consolidation", 1, 1)])
        self.assertEqual(gap_heat(self.MODULES, retire, 0, self.repo)[0][2], 0)

        commits = replay_commits("main", 3, 0, self.repo)
        self.assertEqual([c[0] for c in commits], [deleted, retire, unrelated])
        widened = replay_commits("main", 1, 30, self.repo)
        self.assertEqual(len(widened), 6)
        results = replay(self.MODULES, replay_commits("main", 10, 0, self.repo), self.repo)
        by_pr = {r["pr"]: r for r in results}
        self.assertEqual(by_pr[2]["gaps"], ["ShardOwnership: Consolidation"])
        self.assertTrue(by_pr[2]["requiresImpactSection"])
        self.assertFalse(by_pr[3]["requiresImpactSection"])
        self.assertEqual(by_pr[4]["srcFiles"], 1)
        self.assertFalse(by_pr[4]["requiresImpactSection"])
        self.assertIn("Direct gap hit: 4", summarise_replay(results))

        index = Path(self.repo, "index.json")
        index.write_text(json.dumps({"schema": 1, "modules": self.MODULES}), encoding="utf-8")
        summary = Path(self.repo, "summary.md")
        body = Path(self.repo, "body.md")
        body.write_text("## Summary\nx\n", encoding="utf-8")
        argv = ["--index", str(index), "--base", base, "--head", fix, "--summary-file", str(summary),
                "--gate", "--body-file", str(body)]
        self.assertEqual(main(argv, cwd=self.repo), 1)
        self.assertIn(GAP_HEADING, summary.read_text(encoding="utf-8"))
        body.write_text("## Summary\nx\n\n## Formal impact\nFold sweep only.\n", encoding="utf-8")
        self.assertEqual(main(argv, cwd=self.repo), 0)
        self.assertEqual(main(argv[:-3], cwd=self.repo), 0)
        missing = ["--index", str(index) + ".missing", "--base", base, "--head", fix]
        self.assertEqual(main(missing, cwd=self.repo), 2)


def self_test() -> int:
    suite = unittest.TestSuite()
    loader = unittest.defaultTestLoader
    for case in (ParserTests, IndexTests, GateTests, GitEndToEndTests):
        suite.addTests(loader.loadTestsFromTestCase(case))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    if result.testsRun == 0:
        print("formal-impact self-test ran no tests", file=sys.stderr)
        return 1
    return 0 if result.wasSuccessful() else 1


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None, cwd: str | None = None) -> int:
    parser = argparse.ArgumentParser(description="Formal-impact report (#5162).")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--index")
    parser.add_argument("--base")
    parser.add_argument("--head")
    parser.add_argument("--summary-file")
    parser.add_argument("--json-out")
    parser.add_argument("--heat-days", type=int, default=DEFAULT_HEAT_DAYS)
    parser.add_argument("--no-heat", action="store_true")
    parser.add_argument("--gate", action="store_true",
                        help="fail when a direct hit lacks a '## Formal impact' body section")
    parser.add_argument("--body-file")
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--ref", default="origin/main")
    parser.add_argument("--limit", type=int, default=60)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--commit", action="append", default=[])
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()
    if not args.index:
        parser.error("--index is required")
    try:
        modules = load_index(args.index)
    except IndexError_ as error:
        print(f"::error::{error}", file=sys.stderr)
        return 2

    if args.replay:
        if args.commit:
            commits = []
            for sha in args.commit:
                line = git("show", "-s", "--format=%H%x09%ct%x09%s", sha, cwd=cwd).strip()
                full, stamp, subject = line.split("\t", 2)
                commits.append((full, subject, int(stamp)))
        else:
            commits = replay_commits(args.ref, args.limit, args.days, cwd)
        results = replay(modules, commits, cwd)
        print(summarise_replay(results))
        if args.json_out:
            Path(args.json_out).write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        return 0

    if not (args.base and args.head):
        parser.error("--base and --head are required for a report")
    merge_base = git("merge-base", args.base, args.head, cwd=cwd).strip()
    impact = diff_impact(merge_base, args.head, cwd, indexed_type_names(modules))
    hits = match(modules, impact)
    heat = None if args.no_heat else gap_heat(modules, args.head, args.heat_days, cwd)
    report = render(impact, hits, heat, merge_base, args.head, args.heat_days)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            handle.write(report + "\n")
    else:
        print(report)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(to_json(impact, hits), indent=2) + "\n", encoding="utf-8")

    if args.gate and requires_impact_section(hits):
        body = Path(args.body_file).read_text(encoding="utf-8") if args.body_file else ""
        if not body_has_impact_section(body):
            print("::error::This pull request changes code a refinement row or deliberate abstraction "
                  "gap names; add a '## Formal impact' section to its body.", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
