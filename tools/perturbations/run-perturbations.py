#!/usr/bin/env python3
"""Run production-perturbation experiments for refinement-note detectors.

WHAT A PERTURBATION IS
----------------------
`spec/<area>/perturbations/<Name>.perturbation` (contract C7, epic #5148) is an
anchored edit to ONE production file under `src/` that breaks the behaviour a
refinement row describes, plus the detectors that row cites:

    MODULE: AtomicCommit
    ROW: DecideTx(t)
    FILE: src/lattice/BPlusTree/Grains/AtomicWriteGrain.cs
    DETECTORS: CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard
    SUMMARY: the compensate path decides as though every participant acked

    --- FIND
    <verbatim text, matched exactly once>
    --- REPLACE
    <replacement>
    --- END

The block grammar is the spec-mutation grammar: blank lines and '#' comments
may separate blocks, and any stray marker or stray text is refused rather than
skipped. The toolchain-free gate `RefinementPerturbationTests` checks every
perturbation's anchor, row and detectors on every pull request; this runner is
the expensive half and is never run per pull request.

WHAT THE RUNNER DOES
--------------------
1. Creates a scratch git worktree of `--ref` (default HEAD), so the caller's
   checkout is never edited. Perturbation files are read from the caller's
   checkout; the production code they edit is the scratch copy of `--ref`.
2. CONTROL: builds the detectors' test project(s) on the unperturbed tree and
   runs only the named detectors. Every detector must execute at least one test
   and every executed test must pass.
3. For each perturbation, in order: applies it, stamps the residue marker of
   the testing instructions on the file's first line, rebuilds, runs only its
   detectors, records the outcome, and restores the file from git. A perturbation is
   DETECTED when the perturbed tree builds and at least one detector test
   fails, and the control was green for every one of its detectors.
4. Writes a machine-readable result (`--output`) and exits 0 only when every
   selected perturbation was detected.

Detector selection uses `FullyQualifiedName~<Namespace>.<Fixture>.<Method>` and
then keeps only the TRX results of that fixture whose test name is exactly
`<Method>` or `<Method>(...)`, so a prefix-sharing sibling test can never be
credited with a detection.

USAGE
-----
    python3 tools/perturbations/run-perturbations.py --area atomic-commit --output result.json
    python3 tools/perturbations/run-perturbations.py spec/atomic-commit/perturbations/<Name>.perturbation
    python3 tools/perturbations/run-perturbations.py --all --check --json
    python3 tools/perturbations/run-perturbations.py --self-test

`--check` builds nothing: it parses each selected perturbation and applies it
in memory to the working-tree FILE, printing the parse with `--json`. Use
`--scratch DIR` to keep and reuse one scratch worktree (and its build output)
across invocations; without it a temporary worktree is created and removed.
Build and test logs and TRX files are kept beside the scratch directory, in
`<scratch>-logs`, and the result names that directory.

RESULT (schema 1)
-----------------
    {"schema": 1, "ref": "<sha>", "configuration": "Release", "ok": true,
     "scratch": "<dir>", "logs": "<dir>",
     "control": {"verdict": "passed" | "failed" | "skipped", "message": "...",
                 "detectors": {"Fixture.Method": {"passed": n, "failed": n, "other": n}},
                 "seconds": s},
     "perturbations": [
       {"name", "path", "module", "row", "file", "detectors": [...],
        "verdict": "detected" | "undetected" | "control-failed" |
                   "build-failed" | "no-tests" | "invalid" | "error",
        "failedTests": [...], "message": "...", "seconds": s}]}

Only "detected" is a pass. Exit codes: 0 every selected perturbation was
detected (or, with --check, every one is valid); 1 at least one was not;
2 usage or environment error.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA = 1
EXTENSION = ".perturbation"
DIRECTORY_NAME = "perturbations"
REQUIRED_HEADERS = ("MODULE", "ROW", "FILE", "DETECTORS", "SUMMARY")
FIND_MARKER = "--- FIND"
REPLACE_MARKER = "--- REPLACE"
END_MARKER = "--- END"
DETECTOR_SHAPE = re.compile(r"^[A-Z][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
TRX_NS = "{http://microsoft.com/schemas/VisualStudio/TeamTest/2010}"
# The residue marker of .github/instructions/testing.instructions.md, assembled
# from two halves so this file does not itself trip PerturbationResidueHygieneTests.
MARKER = "LATTICE" + "-PERTURBATION"
NAMESPACE = re.compile(r"^\s*namespace\s+([A-Za-z_][A-Za-z0-9_.]*)\s*[;{]", re.MULTILINE)
TYPE_DECLARATION = re.compile(r"\b(?:class|struct|record(?:\s+(?:class|struct))?)\s+([A-Za-z_][A-Za-z0-9_]*)")


class PerturbationError(Exception):
    """A perturbation file is malformed, does not apply, or cannot be resolved."""


@dataclass
class Perturbation:
    name: str
    path: str
    module: str
    row: str
    file: str
    detectors: list[str]
    summary: str
    edits: list[tuple[str, str]] = field(default_factory=list)

    def as_json(self) -> dict:
        return {
            "name": self.name,
            "path": self.path,
            "module": self.module,
            "row": self.row,
            "file": self.file,
            "detectors": list(self.detectors),
            "summary": self.summary,
            "edits": [{"find": f, "replace": r} for f, r in self.edits],
        }


# ---------------------------------------------------------------------------
# Format (contract C7). Mirrors RefinementPerturbation in the .NET gate, which
# compares its own parse with `--check --json` for every repository file.
# ---------------------------------------------------------------------------

def _is_marker(line: str) -> bool:
    return line.startswith(FIND_MARKER) or line.startswith(REPLACE_MARKER) or line.startswith(END_MARKER)


def _collect(lines: list[str], index: int, opening: str, closing: str, label: str) -> tuple[str, int]:
    index += 1
    body: list[str] = []
    while index < len(lines) and not lines[index].startswith(closing):
        if _is_marker(lines[index]):
            raise PerturbationError(
                f"{label} line {index + 1} is the marker '{lines[index]}' inside a '{opening}' block; "
                f"expected '{closing}' first.")
        body.append(lines[index])
        index += 1
    if index >= len(lines):
        raise PerturbationError(f"{label} has an unterminated block; expected '{closing}'.")
    return "\n".join(body), index


def parse(name: str, content: str, path: str = "") -> Perturbation:
    """Parse one perturbation from its text."""
    label = name + EXTENSION
    lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    headers: dict[str, str] = {}
    edits: list[tuple[str, str]] = []

    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith(FIND_MARKER):
            break
        if line.strip() == "" or line.startswith("#"):
            index += 1
            continue
        if _is_marker(line):
            raise PerturbationError(
                f"{label} line {index + 1} is the marker '{line}' before any block; expected "
                f"'{FIND_MARKER}' to open the first edit.")
        separator = line.find(":")
        if separator <= 0:
            raise PerturbationError(
                f"{label} line {index + 1} is neither a header ('KEY: value'), a comment, nor a block "
                f"marker: '{line}'")
        key = line[:separator].strip()
        if key not in REQUIRED_HEADERS:
            raise PerturbationError(
                f"{label} line {index + 1} declares the unknown header '{key}'. The headers are "
                f"{', '.join(REQUIRED_HEADERS)}.")
        if key in headers:
            raise PerturbationError(f"{label} line {index + 1} declares the header '{key}' twice.")
        headers[key] = line[separator + 1:].strip()
        index += 1

    while index < len(lines):
        line = lines[index]
        if line.startswith(FIND_MARKER):
            find, index = _collect(lines, index, FIND_MARKER, REPLACE_MARKER, label)
            replace, index = _collect(lines, index, REPLACE_MARKER, END_MARKER, label)
            if find == "":
                raise PerturbationError(f"{label} edit {len(edits) + 1} has an empty FIND block.")
            edits.append((find, replace))
            index += 1
            continue
        if line.strip() == "" or line.startswith("#"):
            index += 1
            continue
        if _is_marker(line):
            raise PerturbationError(
                f"{label} line {index + 1} is the marker '{line}' outside any block; expected "
                f"'{FIND_MARKER}' to open the next edit.")
        raise PerturbationError(
            f"{label} line {index + 1} is text outside any block; expected '{FIND_MARKER}', a "
            f"blank line or a '#' comment: '{line}'")

    if not edits:
        raise PerturbationError(f"{label} declares no edits.")
    for key in REQUIRED_HEADERS:
        if not headers.get(key):
            raise PerturbationError(f"{label} is missing the required header '{key}:'.")

    file = headers["FILE"]
    if ("\\" in file or not file.startswith("src/") or not file.endswith(".cs")
            or any(s in ("", ".", "..") for s in file.split("/"))):
        raise PerturbationError(
            f"{label} declares 'FILE: {file}'. FILE must be a repository-relative '.cs' path under 'src/', "
            "written with forward slashes and no '.' or '..' segments.")

    detectors = [d.strip() for d in headers["DETECTORS"].split(",")]
    for detector in detectors:
        if not DETECTOR_SHAPE.match(detector):
            raise PerturbationError(
                f"{label} declares the detector '{detector}'. Each DETECTORS entry must be "
                "'Fixture.Method', separated by commas.")
    seen: set[str] = set()
    for detector in detectors:
        if detector in seen:
            raise PerturbationError(f"{label} declares the detector '{detector}' twice.")
        seen.add(detector)

    return Perturbation(name, path, headers["MODULE"], headers["ROW"], file, detectors, headers["SUMMARY"], edits)


def apply(perturbation: Perturbation, source: str) -> str:
    """Apply every edit in order; each anchor must match exactly once. Returns LF-ended text."""
    normalised = source.replace("\r\n", "\n")
    text = normalised
    for number, (find, replace) in enumerate(perturbation.edits, start=1):
        occurrences = text.count(find)
        if occurrences != 1:
            raise PerturbationError(
                f"perturbation '{perturbation.name}' could not apply edit {number} to {perturbation.file}: "
                f"expected exactly one match, found {occurrences}.")
        text = text.replace(find, replace)
    if text == normalised:
        raise PerturbationError(f"perturbation '{perturbation.name}' leaves {perturbation.file} unchanged.")
    return text


def load(path: Path, repo: Path) -> Perturbation:
    relative = path.resolve().relative_to(repo.resolve()).as_posix()
    return parse(path.stem, path.read_text(encoding="utf-8-sig"), relative)


def discover(repo: Path, areas: list[str] | None) -> list[Path]:
    found: list[Path] = []
    for directory in sorted((repo / "spec").glob(f"*/{DIRECTORY_NAME}")):
        if areas is not None and directory.parent.name not in areas:
            continue
        found.extend(sorted(directory.glob(f"*{EXTENSION}"), key=lambda p: p.name))
    return found


# ---------------------------------------------------------------------------
# Detector resolution, build and test
# ---------------------------------------------------------------------------

def nearest_project(path: Path, repo: Path) -> Path | None:
    directory = path.parent
    while True:
        projects = sorted(directory.glob("*.csproj"))
        if projects:
            return projects[0]
        if directory == repo or directory.parent == directory:
            return None
        directory = directory.parent


def index_fixtures(repo: Path, sources: list[Path]) -> dict[str, set[tuple[str, Path]]]:
    """Map every type name declared under test/ to its (qualified name, test project) pairs."""
    index: dict[str, set[tuple[str, Path]]] = {}
    for source in sources:
        text = source.read_text(encoding="utf-8-sig", errors="replace")
        namespace = NAMESPACE.search(text)
        if namespace is None:
            continue
        project = nearest_project(source, repo)
        if project is None:
            continue
        for match in TYPE_DECLARATION.finditer(text):
            index.setdefault(match.group(1), set()).add((f"{namespace.group(1)}.{match.group(1)}", project))
    return index


def resolve_detector(repo: Path, detector: str, index: dict[str, set[tuple[str, Path]]]) -> tuple[str, Path]:
    """Return (fully qualified fixture name, test project) for 'Fixture.Method'."""
    owners = index.get(detector.split(".", 1)[0], set())
    if len(owners) != 1:
        found = ", ".join(sorted(f"{n} in {p.relative_to(repo).as_posix()}" for n, p in owners)) or "none"
        raise PerturbationError(
            f"detector '{detector}' must resolve to exactly one fixture under test/; found {found}.")
    return next(iter(owners))


def build_filter(qualified: dict[str, str]) -> str:
    return "|".join(f"FullyQualifiedName~{qualified[d]}.{d.split('.', 1)[1]}" for d in sorted(qualified))


def read_trx(trx: Path, qualified: dict[str, str]) -> dict[str, dict[str, list[str]]]:
    """Map each detector to its executed test names, bucketed 'passed', 'failed' and 'other'."""
    outcomes: dict[str, dict[str, list[str]]] = {d: {"passed": [], "failed": [], "other": []} for d in qualified}
    if not trx.exists():
        return outcomes
    root = ET.parse(trx).getroot()
    definitions: dict[str, tuple[str, set[str]]] = {}
    for unit in root.iter(f"{TRX_NS}UnitTest"):
        method = unit.find(f"{TRX_NS}TestMethod")
        if method is not None:
            definitions[unit.get("id", "")] = (method.get("className", ""), {unit.get("name", ""), method.get("name", "")})
    for result in root.iter(f"{TRX_NS}UnitTestResult"):
        class_name, names = definitions.get(result.get("testId", ""), ("", set()))
        display = result.get("testName", "")
        names = names | {display}
        for detector, fixture in qualified.items():
            method = detector.split(".", 1)[1]
            if class_name != fixture:
                continue
            if not any(n == method or n.startswith(method + "(") for n in names):
                continue
            outcome = result.get("outcome", "")
            bucket = "passed" if outcome == "Passed" else "failed" if outcome == "Failed" else "other"
            outcomes[detector][bucket].append(display)
    return outcomes


def run(command: list[str], cwd: Path, log: Path) -> int:
    with open(log, "a", encoding="utf-8") as sink:
        sink.write("$ " + " ".join(command) + "\n")
        sink.flush()
        return subprocess.run(command, cwd=cwd, stdout=sink, stderr=subprocess.STDOUT).returncode


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()


def stamp(text: str, name: str) -> str:
    """Prefix the residue marker, so an interrupted run's edit cannot pass for ordinary work."""
    return f"// {MARKER} {name}: this file is perturbed; restore it from git.\n" + text


def write_perturbed(path: Path, text: str) -> None:
    raw = path.read_bytes()
    if b"\r\n" in raw:
        text = text.replace("\n", "\r\n")
    path.write_bytes((b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b"") + text.encode("utf-8"))


@dataclass
class Arm:
    built: bool
    exit_code: int
    outcomes: dict[str, dict[str, list[str]]]


class Scratch:
    """A detached worktree of one commit, reused across arms so builds stay incremental."""

    def __init__(self, repo: Path, ref: str, directory: Path | None, keep: bool, configuration: str):
        self.repo = repo
        self.configuration = configuration
        self.keep = keep or directory is not None
        self.sha = git(repo, "rev-parse", "--verify", ref + "^{commit}")
        if directory is None:
            self.path = Path(tempfile.mkdtemp(prefix="lattice-perturb-"))
            self.path.rmdir()
        else:
            self.path = directory.resolve()
        self.logs = self.path.parent / (self.path.name + "-logs")
        self.logs.mkdir(parents=True, exist_ok=True)
        if (self.path / ".git").exists():
            git(self.path, "checkout", "--quiet", "--force", "--detach", self.sha)
            git(self.path, "reset", "--quiet", "--hard", self.sha)
        else:
            git(repo, "worktree", "add", "--quiet", "--detach", str(self.path), self.sha)
        sources = [self.path / p for p in git(self.path, "ls-files", "--", "test").splitlines() if p.endswith(".cs")]
        self.fixtures = index_fixtures(self.path, sources)

    def restore(self, file: str) -> None:
        git(self.path, "checkout", "--quiet", "--", file)

    def close(self) -> None:
        if self.keep:
            return
        try:
            git(self.repo, "worktree", "remove", "--force", str(self.path))
        except subprocess.CalledProcessError:
            shutil.rmtree(self.path, ignore_errors=True)
            git(self.repo, "worktree", "prune")

    def arm(self, label: str, groups: dict[Path, dict[str, str]]) -> Arm:
        built = True
        exit_code = 0
        outcomes: dict[str, dict[str, list[str]]] = {}
        log = self.logs / f"{label}.log"
        log.write_text("", encoding="utf-8")
        for project, qualified in sorted(groups.items()):
            code = run(["dotnet", "build", str(project), "-c", self.configuration, "--nologo", "-v", "q",
                        "-clp:ErrorsOnly"], self.path, log)
            if code != 0:
                built = False
                exit_code = exit_code or code
                continue
            trx = self.logs / f"{label}-{project.stem}.trx"
            trx.unlink(missing_ok=True)
            code = run(["dotnet", "test", str(project), "-c", self.configuration, "--no-build", "--nologo",
                        "--filter", build_filter(qualified), "--logger", f"trx;LogFileName={trx}",
                        "--blame-hang-timeout", "5m", "--blame-hang-dump-type", "none"], self.path, log)
            exit_code = exit_code or code
            outcomes.update(read_trx(trx, qualified))
        return Arm(built, exit_code, outcomes)


def execute(repo: Path, perturbations: list[Perturbation], args: argparse.Namespace) -> dict:
    scratch = Scratch(repo, args.ref, Path(args.scratch) if args.scratch else None, args.keep_scratch,
                      args.configuration)
    result: dict = {"schema": SCHEMA, "ref": scratch.sha, "configuration": args.configuration,
                    "scratch": str(scratch.path), "logs": str(scratch.logs), "ok": False,
                    "control": {"verdict": "skipped", "message": "", "detectors": {}, "seconds": 0.0},
                    "perturbations": []}
    try:
        resolved: dict[str, tuple[str, Path]] = {}
        entries: list[dict] = []
        for p in perturbations:
            entry = {"name": p.name, "path": p.path, "module": p.module, "row": p.row, "file": p.file,
                     "detectors": list(p.detectors), "verdict": "", "failedTests": [], "message": "",
                     "seconds": 0.0}
            entries.append(entry)
            try:
                apply(p, (scratch.path / p.file).read_text(encoding="utf-8-sig"))
                for d in p.detectors:
                    resolved.setdefault(d, resolve_detector(scratch.path, d, scratch.fixtures))
            except (PerturbationError, OSError) as error:
                entry["verdict"] = "invalid"
                entry["message"] = str(error)
        result["perturbations"] = entries

        def groups_for(detectors: list[str]) -> dict[Path, dict[str, str]]:
            groups: dict[Path, dict[str, str]] = {}
            for d in detectors:
                fixture, project = resolved[d]
                groups.setdefault(project, {})[d] = fixture
            return groups

        live = [(p, e) for p, e in zip(perturbations, entries) if e["verdict"] == ""]
        union = sorted({d for p, _ in live for d in p.detectors})
        green: set[str] = set()
        if union:
            started = time.monotonic()
            control = scratch.arm("control", groups_for(union))
            counts = {}
            for d in union:
                o = control.outcomes.get(d, {"passed": [], "failed": [], "other": []})
                counts[d] = {k: len(v) for k, v in o.items()}
                if control.built and o["passed"] and not o["failed"] and not o["other"]:
                    green.add(d)
            red = [d for d in union if d not in green]
            result["control"] = {
                "verdict": "failed" if red else "passed",
                "message": "" if not red else (
                    "the unperturbed tree does not build" if not control.built
                    else "not green on the unperturbed tree: " + ", ".join(red)),
                "detectors": counts,
                "seconds": round(time.monotonic() - started, 1),
            }

        for p, entry in live:
            started = time.monotonic()
            if any(d not in green for d in p.detectors):
                entry["verdict"] = "control-failed"
                entry["message"] = "a detector was not green on the unperturbed tree; see control"
                continue
            target = scratch.path / p.file
            try:
                write_perturbed(target, stamp(apply(p, target.read_text(encoding="utf-8-sig")), p.name))
                arm = scratch.arm(p.name, groups_for(p.detectors))
            finally:
                scratch.restore(p.file)
            empty = {"passed": [], "failed": [], "other": []}
            failed = [t for d in p.detectors for t in arm.outcomes.get(d, empty)["failed"]]
            silent = [d for d in p.detectors if not any(arm.outcomes.get(d, empty).values())]
            entry["failedTests"] = failed
            if not arm.built:
                entry["verdict"] = "build-failed"
                entry["message"] = "the perturbed tree does not build, which is not a detection"
            elif failed:
                entry["verdict"] = "detected"
            elif silent:
                entry["verdict"] = "no-tests"
                entry["message"] = "executed no test on the perturbed tree: " + ", ".join(silent)
            elif arm.exit_code != 0:
                entry["verdict"] = "error"
                entry["message"] = f"dotnet test exited {arm.exit_code} with no failed detector result"
            else:
                entry["verdict"] = "undetected"
                entry["message"] = "every detector stayed green with the perturbation applied"
            entry["seconds"] = round(time.monotonic() - started, 1)

        result["ok"] = bool(entries) and all(e["verdict"] == "detected" for e in entries)
        return result
    finally:
        scratch.close()


# ---------------------------------------------------------------------------
# Self-test (no dotnet, no git)
# ---------------------------------------------------------------------------

def self_test() -> int:
    failures: list[str] = []

    def expect(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    def expect_error(text: str, fragment: str) -> None:
        try:
            parse("T", text)
        except PerturbationError as error:
            expect(fragment in str(error), f"expected '{fragment}' in '{error}'")
            return
        failures.append(f"expected a parse error containing '{fragment}'")

    block = "--- FIND\nA\n--- REPLACE\nB\n--- END\n"
    header = ("MODULE: M\nROW: Act(t)\nFILE: src/a/B.cs\nDETECTORS: FTests.One, FTests.two_b\n"
              "SUMMARY: breaks it\n\n")
    good = parse("T", header + "--- FIND\nA = 1;\n--- REPLACE\nA = 2;\n--- END\n\n# c\n"
                           "--- FIND\nB = 1;\n--- REPLACE\n--- END\n")
    expect(good.detectors == ["FTests.One", "FTests.two_b"], "detectors parse")
    expect(good.edits == [("A = 1;", "A = 2;"), ("B = 1;", "")], "edits parse")
    expect(apply(good, "x\r\nA = 1;\r\nB = 1;\r\n") == "x\nA = 2;\n\n", "apply normalises and edits in order")
    crlf = parse("T", (header + "--- FIND\nA = 1;\n--- REPLACE\nA = 2;\n--- END\n").replace("\n", "\r\n"))
    expect(crlf.edits == [("A = 1;", "A = 2;")], "CRLF parses")

    expect_error(header + "--- FIND\nA\n--- REPLACE\nB\n--- FIND\nC\n--- REPLACE\nD\n--- END\n",
                 "line 11 is the marker '--- FIND' inside a '--- REPLACE' block")
    expect_error(header + block + "--- END\n", "line 12 is the marker '--- END' outside any block")
    expect_error(header + block + "trailing\n", "line 12 is text outside any block")
    expect_error(header + "--- FIND\nA\n--- REPLACE\nB\n", "unterminated block")
    expect_error(header + "--- REPLACE\nB\n--- END\n", "before any block")
    expect_error(header, "declares no edits")
    expect_error(header.replace("SUMMARY: breaks it\n", "") + block, "missing the required header 'SUMMARY:'")
    expect_error("TARGET: x\n" + header + block, "unknown header 'TARGET'")
    expect_error("ROW: y\n" + header + block, "header 'ROW' twice")
    expect_error(header.replace("src/a/B.cs", "test/a/B.cs") + block, "FILE must be")
    expect_error(header.replace("src/a/B.cs", "src/../B.cs") + block, "FILE must be")
    expect_error(header.replace("src/a/B.cs", "src/a/B.json") + block, "FILE must be")
    stamped = stamp("A\n", "T")
    expect(stamped.startswith("// ") and MARKER in stamped.splitlines()[0] and stamped.endswith("\nA\n"),
           f"stamp: {stamped!r}")
    expect_error(header.replace("FTests.One", "FTests") + block, "detector 'FTests'")
    expect_error(header.replace("FTests.two_b", "FTests.One") + block, "'FTests.One' twice")
    expect_error(header + "--- FIND\n--- REPLACE\nB\n--- END\n", "empty FIND block")

    for source, fragment in (("A = 1;\nA = 1;\nB = 1;\n", "found 2"), ("B = 1;\n", "found 0")):
        try:
            apply(good, source)
            failures.append(f"apply should refuse ({fragment})")
        except PerturbationError as error:
            expect(fragment in str(error), f"apply error '{error}' lacks '{fragment}'")
    identity = parse("T", header + "--- FIND\nA\n--- REPLACE\nA\n--- END\n")
    try:
        apply(identity, "A\n")
        failures.append("apply should refuse an edit that changes nothing")
    except PerturbationError as error:
        expect("unchanged" in str(error), f"identity error '{error}'")

    expect(build_filter({"FTests.One": "N.FTests", "GTests.two": "M.GTests"})
           == "FullyQualifiedName~N.FTests.One|FullyQualifiedName~M.GTests.two", "filter shape")

    trx = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<TestRun xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010"><Results>'
        '<UnitTestResult testId="1" testName="One" outcome="Passed" />'
        '<UnitTestResult testId="2" testName="One(True)" outcome="Failed" />'
        '<UnitTestResult testId="3" testName="One_longer" outcome="Failed" />'
        '<UnitTestResult testId="4" testName="One" outcome="Failed" />'
        '<UnitTestResult testId="5" testName="One(False)" outcome="NotExecuted" />'
        '</Results><TestDefinitions>'
        '<UnitTest id="1" name="One"><TestMethod className="N.FTests" name="One" /></UnitTest>'
        '<UnitTest id="2" name="One(True)"><TestMethod className="N.FTests" name="One" /></UnitTest>'
        '<UnitTest id="3" name="One_longer"><TestMethod className="N.FTests" name="One_longer" /></UnitTest>'
        '<UnitTest id="4" name="One"><TestMethod className="N.OtherFTests" name="One" /></UnitTest>'
        '<UnitTest id="5" name="One(False)"><TestMethod className="N.FTests" name="One" /></UnitTest>'
        '</TestDefinitions></TestRun>')
    with tempfile.TemporaryDirectory() as work:
        path = Path(work) / "r.trx"
        path.write_text(trx, encoding="utf-8")
        outcomes = read_trx(path, {"FTests.One": "N.FTests"})
        expect(outcomes["FTests.One"] == {"passed": ["One"], "failed": ["One(True)"], "other": ["One(False)"]},
               f"trx buckets: {outcomes}")
        expect(read_trx(Path(work) / "missing.trx", {"FTests.One": "N.FTests"})["FTests.One"]["passed"] == [],
               "a missing trx reports nothing executed")

        root = Path(work) / "repo"
        (root / "test" / "p").mkdir(parents=True)
        (root / "test" / "p" / "P.csproj").write_text("<Project />", encoding="utf-8")
        (root / "test" / "p" / "F.cs").write_text(
            "namespace N.Q;\n\npublic sealed partial class FTests\n{\n    private sealed class Helper { }\n}\n",
            encoding="utf-8")
        (root / "test" / "p" / "G.cs").write_text("namespace N.Q;\n\npublic class FTestsHelper\n{\n}\n",
                                                  encoding="utf-8")
        (root / "test" / "p" / "H.cs").write_text("namespace N.R;\n\ninternal class Helper\n{\n}\n",
                                                  encoding="utf-8")
        index = index_fixtures(root, sorted((root / "test").rglob("*.cs")))
        fixture, project = resolve_detector(root, "FTests.One", index)
        expect(fixture == "N.Q.FTests" and project.name == "P.csproj", f"resolve: {fixture} {project}")
        for missing in ("Missing.One", "Helper.One"):
            try:
                resolve_detector(root, missing, index)
                failures.append(f"'{missing}' must not resolve")
            except PerturbationError:
                pass

    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    print(f"run-perturbations self-test: {'FAILED' if failures else 'passed'} ({len(failures)} failure(s))")
    return 1 if failures else 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Run production-perturbation experiments (contract C7).")
    parser.add_argument("perturbations", nargs="*", help="perturbation files (repository-relative or absolute)")
    parser.add_argument("--area", action="append", help="every perturbation under spec/<AREA>/perturbations")
    parser.add_argument("--all", action="store_true", help="every perturbation in the repository")
    parser.add_argument("--repo", default=None, help="repository root (default: this script's repository)")
    parser.add_argument("--ref", default="HEAD", help="commit copied into the scratch worktree (default HEAD)")
    parser.add_argument("--configuration", default="Release")
    parser.add_argument("--scratch", default=None, help="reuse and keep this scratch worktree directory")
    parser.add_argument("--keep-scratch", action="store_true", help="keep the temporary scratch worktree")
    parser.add_argument("--output", default=None, help="write the JSON result to this file")
    parser.add_argument("--check", action="store_true", help="parse and apply in memory only; build nothing")
    parser.add_argument("--json", action="store_true", help="print the JSON result to stdout")
    parser.add_argument("--self-test", action="store_true", help="run the built-in self-test and exit")
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    repo = Path(args.repo).resolve() if args.repo else Path(__file__).resolve().parents[2]
    paths: list[Path] = []
    if args.all:
        paths.extend(discover(repo, None))
    if args.area:
        paths.extend(discover(repo, args.area))
    for item in args.perturbations:
        candidate = Path(item)
        paths.append(candidate if candidate.is_absolute() else repo / candidate)
    unique: list[Path] = []
    for path in paths:
        if all(path.resolve() != u.resolve() for u in unique):
            unique.append(path)
    if not unique:
        print("error: no perturbation selected (pass files, --area or --all)", file=sys.stderr)
        return 2

    perturbations: list[Perturbation] = []
    problems: list[str] = []
    for path in unique:
        try:
            perturbations.append(load(path, repo))
        except (PerturbationError, OSError, ValueError) as error:
            problems.append(f"{path}: {error}")

    if args.check:
        checked = []
        for p in perturbations:
            try:
                apply(p, (repo / p.file).read_text(encoding="utf-8-sig"))
                verdict, message = "valid", ""
            except (PerturbationError, OSError) as error:
                verdict, message = "invalid", str(error)
                problems.append(message)
            checked.append({**p.as_json(), "verdict": verdict, "message": message})
        result = {"schema": SCHEMA, "ok": not problems, "perturbations": checked, "problems": problems}
    else:
        if problems:
            for problem in problems:
                print(f"error: {problem}", file=sys.stderr)
            return 2
        try:
            result = execute(repo, perturbations, args)
        except (subprocess.CalledProcessError, OSError) as error:
            print(f"error: {error}", file=sys.stderr)
            return 2

    text = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    if args.json:
        print(text)
    else:
        for problem in result.get("problems", []):
            print(f"problem: {problem}")
        for entry in result["perturbations"]:
            print(f"{entry['verdict']:>14}  {entry['name']}  {entry.get('message', '')}".rstrip())
        if "control" in result:
            print(f"control: {result['control']['verdict']} {result['control'].get('message', '')}".rstrip())
        print("ok" if result["ok"] else "NOT OK")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
