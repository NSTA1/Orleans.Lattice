#!/usr/bin/env python3
"""Per-module TLC fingerprints (contract C4 of #5148, item #5160).

A module's fingerprint is a content hash that changes exactly when that
module's exhaustive TLC verdict could change. The verdict cache (#5164) keys a
receipt on it, so a pull request reruns only the modules whose fingerprint has
no trusted receipt.

    python3 .github/workflows/tlc-fingerprint.py --output <file>
    python3 .github/workflows/tlc-fingerprint.py --self-test

OUTPUT (C4). Readers must ignore keys they do not know:

    { "schema": 1,
      "harness": "<sha256>", "toolchain": "<sha256>",
      "harnessInputs": ["<path>", ...], "toolchainInputs": ["<path>", ...],
      "modules": { "<Module>": { "fingerprint": "<sha256>", "inputs": ["<path>", ...] } } }

`harnessInputs` and `toolchainInputs` are diagnostic additions to C4: they name
what the two global hashes cover, so a cache miss can be explained.

PER-MODULE INPUTS. Mirrors what `SpecModule` (test/lattice/Formal/) exposes to
the TLC gates. For module <M> in spec/<area>/:
  - every sibling `.tla` in spec/<area>/ (the harness copies all of them into
    TLC's scratch directory, so an edit to any sibling changes every module of
    that directory);
  - <M>.cfg and every variant cfg <M>.<Variant>.cfg;
  - <M>.manifest.json;
  - every file under the manifest's mutation directory.
A module is a `.tla` with a manifest beside it. `TlcFingerprintCompletenessTests`
checks that every path `SpecModule` exposes is in its module's list.

GLOBAL INPUTS. Read from `.github/workflows/tlc-inputs.json` (C1, #5149):
  - toolchain: files matching a `toolchain` glob (tools/tlc/** carries the
    canonical pin file, C3);
  - harness: files matching a `verdictInputs` glob that match no `toolchain`
    glob and that no module claims. A verdict file under spec/ that no module
    claims (a stray cfg, say) is therefore harness, and changes every module.
A `neutral` or `routing` file changes no fingerprint. Globs are
`fnmatch.fnmatchcase` over repository-relative '/' paths: '*' crosses '/', and
only literal characters and '*' are allowed, as `TlcInputClassificationTests`
enforces, so this script and the C# guards read every glob identically.

CONTENT, NEVER METADATA. Each entry is the repository-relative path and the
SHA-256 of the file's content; entries are sorted by path. Text is normalised
the way git normalises it under this repository's `.gitattributes`
(`* text=auto`, `tools/tlc/** text eol=lf`, `tools/ci-runners/** text eol=lf`):
CRLF becomes LF unless git's
auto-detection would treat the file as binary (a NUL byte or a lone CR), so a
Windows checkout and a Linux checkout fingerprint identically. Timestamps,
permissions and the order of discovery are never read.

POPULATION. In a git work tree the files are `git ls-files --cached --others
--exclude-standard` (tracked files, plus new files not yet added, never ignored
build output); a tracked file deleted from the work tree is skipped. Outside a
git work tree (the self-test and the synthetic controls) the tree is walked.

NOT COVERED, DELIBERATELY. Runtime-only drift - the JDK patch level that
setup-java resolves at run time, the runner image, the operating system - is
outside the fingerprint, because no file in the repository records it. The
nightly exhaustive TLC run in the Coverage workflow (#5153) ignores receipts
and is the backstop for that drift.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import posixpath
import re
import subprocess
import sys
import tempfile
from pathlib import Path

SCHEMA = 1
REPO_ROOT = Path(__file__).resolve().parents[2]
INPUTS_RELATIVE = ".github/workflows/tlc-inputs.json"
MANIFEST_SUFFIX = ".manifest.json"

# The repository's .gitattributes forces these paths to text (eol=lf), so they
# are normalised even when auto-detection would call them binary.
FORCED_TEXT = ("tools/tlc/*", "tools/ci-runners/*")

GLOB_ALPHABET = re.compile(r"^[A-Za-z0-9_./*-]+$")


class TlcInputs:
    """The C1 classification: verdict, neutral, routing and toolchain globs."""

    def __init__(self, verdict: list[str], neutral: list[str], routing: list[str], toolchain: list[str]):
        self.verdict = verdict
        self.neutral = neutral
        self.routing = routing
        self.toolchain = toolchain

    @staticmethod
    def _matches(path: str, globs: list[str]) -> bool:
        return any(fnmatch.fnmatchcase(path, glob) for glob in globs)

    def excluded(self, path: str) -> bool:
        """Neutral and routing files never change a fingerprint."""
        return self._matches(path, self.neutral) or self._matches(path, self.routing)

    def is_toolchain(self, path: str) -> bool:
        return self._matches(path, self.toolchain) and not self.excluded(path)

    def is_harness_candidate(self, path: str) -> bool:
        return (
            self._matches(path, self.verdict)
            and not self._matches(path, self.toolchain)
            and not self.excluded(path)
        )


def load_inputs(path: Path) -> TlcInputs:
    """Parses C1. A missing or malformed file raises: a fingerprint built from a
    classification nobody can read would certify nothing."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema") != 1:
        raise ValueError(f"{path}: unsupported schema {document.get('schema') if isinstance(document, dict) else None!r}")

    def globs(key: str) -> list[str]:
        value = document.get(key)
        if not isinstance(value, list) or not all(isinstance(glob, str) and glob for glob in value):
            raise ValueError(f"{path}: '{key}' must be a list of non-empty glob strings")
        for glob in value:
            if not GLOB_ALPHABET.match(glob):
                raise ValueError(f"{path}: '{key}' glob {glob!r} uses a character other than a literal or '*'")
        return value

    verdict = globs("verdictInputs")
    toolchain = globs("toolchain")
    if not verdict:
        raise ValueError(f"{path}: 'verdictInputs' is empty")
    if not toolchain:
        raise ValueError(f"{path}: 'toolchain' is empty, so a toolchain change would change no fingerprint")
    return TlcInputs(verdict, globs("neutral"), globs("routing"), toolchain)


def list_files(root: Path) -> list[str]:
    """Repository-relative '/' paths of every file in the population, sorted."""
    if (root / ".git").exists():
        def git_paths(*selection: str) -> set[str]:
            listed = subprocess.run(
                ["git", "-C", str(root), "ls-files", "-z", *selection],
                check=True,
                capture_output=True,
            ).stdout.decode("utf-8")
            return {path for path in listed.split("\0") if path}

        return sorted(git_paths("--cached", "--others", "--exclude-standard") - git_paths("--deleted"))

    found = []
    for directory, subdirectories, files in os.walk(root):
        subdirectories[:] = [name for name in subdirectories if name != ".git"]
        for name in files:
            found.append(Path(directory, name).relative_to(root).as_posix())
    return sorted(found)


def normalise(path: str, data: bytes) -> bytes:
    """Content as git stores it under `* text=auto`."""
    forced = any(fnmatch.fnmatchcase(path, glob) for glob in FORCED_TEXT)
    binary = b"\0" in data or data.count(b"\r") != data.count(b"\r\n")
    return data.replace(b"\r\n", b"\n") if forced or not binary else data


def file_digest(root: Path, path: str) -> str:
    return hashlib.sha256(normalise(path, (root / path).read_bytes())).hexdigest()


def set_digest(label: str, entries: dict[str, str]) -> str:
    """One hash over (path, content digest) pairs, sorted by path."""
    digest = hashlib.sha256(f"tlc-fingerprint/{SCHEMA}/{label}\n".encode("utf-8"))
    for path in sorted(entries):
        digest.update(f"{path}\0{entries[path]}\n".encode("utf-8"))
    return digest.hexdigest()


def discover_modules(root: Path, files: list[str]) -> dict[str, list[str]]:
    """Module name -> every file `SpecModule` exposes to the TLC gates for it."""
    by_area: dict[str, list[str]] = {}
    for path in files:
        parts = path.split("/")
        if len(parts) == 3 and parts[0] == "spec":
            by_area.setdefault(parts[1], []).append(parts[2])

    modules: dict[str, list[str]] = {}
    where: dict[str, str] = {}
    for area in sorted(by_area):
        names = set(by_area[area])
        base = f"spec/{area}"
        siblings = sorted(f"{base}/{name}" for name in names if name.endswith(".tla"))
        for specification in siblings:
            module = posixpath.basename(specification)[: -len(".tla")]
            manifest_name = module + MANIFEST_SUFFIX
            if manifest_name not in names:
                continue
            if module in modules:
                raise ValueError(f"module '{module}' is declared in both {where[module]}/ and {base}/")

            manifest_path = f"{base}/{manifest_name}"
            manifest = json.loads((root / manifest_path).read_text(encoding="utf-8"))
            mutations = manifest.get("mutations") if isinstance(manifest, dict) else None
            if not isinstance(mutations, str) or not mutations:
                raise ValueError(f"{manifest_path}: 'mutations' must name the module's mutation directory")
            mutation_directory = posixpath.normpath(posixpath.join(base, mutations))
            if mutation_directory.startswith("../") or mutation_directory in ("..", ".") or posixpath.isabs(mutations):
                raise ValueError(f"{manifest_path}: mutation directory '{mutations}' escapes the repository")

            claimed = set(siblings)
            claimed.add(manifest_path)
            claimed.update(
                f"{base}/{name}"
                for name in names
                if name == f"{module}.cfg" or (name.startswith(f"{module}.") and name.endswith(".cfg"))
            )
            claimed.update(path for path in files if path.startswith(mutation_directory + "/"))

            modules[module] = sorted(claimed)
            where[module] = base
    return modules


def fingerprint(root: Path, inputs_file: Path | None = None) -> dict:
    """The C4 document for the tree at `root`."""
    inputs = load_inputs(inputs_file or root / INPUTS_RELATIVE)
    files = list_files(root)
    claims = discover_modules(root, files)
    claimed = {path for paths in claims.values() for path in paths}

    digests: dict[str, str] = {}

    def digest_of(paths: list[str]) -> dict[str, str]:
        for path in paths:
            if path not in digests:
                digests[path] = file_digest(root, path)
        return {path: digests[path] for path in paths}

    toolchain_inputs = [path for path in files if inputs.is_toolchain(path)]
    harness_inputs = [path for path in files if path not in claimed and inputs.is_harness_candidate(path)]
    toolchain = set_digest("toolchain", digest_of(toolchain_inputs))
    harness = set_digest("harness", digest_of(harness_inputs))

    modules = {}
    for name in sorted(claims):
        module_inputs = [path for path in claims[name] if not inputs.excluded(path)]
        own = set_digest(f"module/{name}", digest_of(module_inputs))
        combined = hashlib.sha256(
            f"tlc-fingerprint/{SCHEMA}\nmodule\0{name}\nharness\0{harness}\ntoolchain\0{toolchain}\ninputs\0{own}\n".encode(
                "utf-8"
            )
        ).hexdigest()
        modules[name] = {"fingerprint": combined, "inputs": module_inputs}

    return {
        "schema": SCHEMA,
        "harness": harness,
        "toolchain": toolchain,
        "harnessInputs": harness_inputs,
        "toolchainInputs": toolchain_inputs,
        "modules": modules,
    }


# --------------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------------

SELF_TEST_INPUTS = {
    "_comment": ["A unit of C1 for the self-test; unknown keys are ignored."],
    "schema": 1,
    "verdictInputs": [
        "spec/**",
        "test/lattice/Formal/**",
        "tools/tlc/**",
        ".github/workflows/ci.yml",
        ".github/workflows/route.py",
    ],
    "neutral": [
        "spec/*.md",
        "spec/**/perturbations/**",
        "tools/tlc/README.md",
        "test/lattice/Formal/NeutralTests.cs",
    ],
    "routing": [".github/workflows/route.py"],
    "toolchain": ["tools/tlc/**"],
}


def _manifest(mutations: str, variants: dict | None = None) -> str:
    document = {"mutations": mutations, "refinement": "Refinement.md", "nonBehaviouralActions": [], "counts": {}}
    if variants:
        document["variants"] = variants
    return json.dumps(document)


SELF_TEST_TREE = {
    ".github/workflows/tlc-inputs.json": json.dumps(SELF_TEST_INPUTS),
    ".github/workflows/ci.yml": "jobs: {}\n",
    ".github/workflows/route.py": "print('route')\n",
    ".github/workflows/unclassified.py": "print('outside every glob')\n",
    "spec/README.md": "# Specifications\n",
    "spec/alpha/README.md": "# alpha\n",
    "spec/alpha/Refinement.md": "# notes\n",
    "spec/alpha/RefinementTwo.md": "# notes\n",
    "spec/alpha/Alpha.tla": "---- MODULE Alpha ----\n====\n",
    "spec/alpha/Alpha.cfg": "SPECIFICATION Spec\n",
    "spec/alpha/Alpha.manifest.json": _manifest("mutations"),
    "spec/alpha/mutations/AlphaBreaks.mutation": "MODULE: AlphaBreaks\n",
    "spec/alpha/mutations/README.md": "# mutations\n",
    "spec/alpha/AlphaTwo.tla": "---- MODULE AlphaTwo ----\nEXTENDS Alpha\n====\n",
    "spec/alpha/AlphaTwo.cfg": "SPECIFICATION Spec\n",
    "spec/alpha/AlphaTwo.Narrow.cfg": "SPECIFICATION Spec\nCONSTANTS Wrap = 2\n",
    "spec/alpha/AlphaTwo.manifest.json": _manifest("mutations-two", {"Narrow": {"distinctStates": 2}}),
    "spec/alpha/mutations-two/AlphaTwoBreaks.mutation": "MODULE: AlphaTwoBreaks\n",
    "spec/alpha/perturbations/Gap.perturbation": "MODULE: Alpha\n",
    "spec/beta/README.md": "# beta\n",
    "spec/beta/Beta.tla": "---- MODULE Beta ----\n====\n",
    "spec/beta/Beta.cfg": "SPECIFICATION Spec\n",
    "spec/beta/Beta.manifest.json": _manifest("mutations"),
    "spec/beta/mutations/BetaBreaks.mutation": "MODULE: BetaBreaks\n",
    "test/lattice/Formal/Harness.cs": "class Harness {}\n",
    "test/lattice/Formal/NeutralTests.cs": "class NeutralTests {}\n",
    "tools/tlc/toolchain.json": '{"jarSha256": "aa"}\n',
    "tools/tlc/README.md": "# toolchain\n",
}

ALL = ("Alpha", "AlphaTwo", "Beta")


class _Tree:
    def __init__(self, directory: Path):
        self.root = directory
        for path, content in SELF_TEST_TREE.items():
            self.write(path, content)

    def write(self, path: str, content: str | bytes) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, str):
            content = content.encode("utf-8")
        target.write_bytes(content)

    def append(self, path: str, text: str) -> None:
        self.write(path, (self.root / path).read_bytes() + text.encode("utf-8"))

    def rename(self, source: str, target: str) -> None:
        (self.root / source).rename(self.root / target)

    def fingerprints(self) -> dict:
        return fingerprint(self.root)


def _changed(before: dict, after: dict) -> set[str]:
    return {name for name in before["modules"] if before["modules"][name]["fingerprint"] != after["modules"][name]["fingerprint"]}


def self_test() -> int:
    failures: list[str] = []

    def case(title: str, edit, expected_modules, harness_changes=False, toolchain_changes=False) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            tree = _Tree(Path(scratch))
            before = tree.fingerprints()
            edit(tree)
            after = tree.fingerprints()
            changed = _changed(before, after)
            expected = set(expected_modules)
            if changed != expected:
                failures.append(f"{title}: expected {sorted(expected)} to change, got {sorted(changed)}")
            if (before["harness"] != after["harness"]) != harness_changes:
                failures.append(f"{title}: harness hash change should be {harness_changes}")
            if (before["toolchain"] != after["toolchain"]) != toolchain_changes:
                failures.append(f"{title}: toolchain hash change should be {toolchain_changes}")

    # The contract the issue names.
    case("a sibling edit changes every module in that directory", lambda t: t.append("spec/alpha/Alpha.tla", "\\* edit\n"), ["Alpha", "AlphaTwo"])
    case("an edit to another directory changes none of them", lambda t: t.append("spec/beta/Beta.tla", "\\* edit\n"), ["Beta"])
    for neutral in (
        "spec/README.md",
        "spec/alpha/README.md",
        "spec/alpha/Refinement.md",
        "spec/alpha/mutations/README.md",
        "spec/alpha/perturbations/Gap.perturbation",
        "test/lattice/Formal/NeutralTests.cs",
        "tools/tlc/README.md",
    ):
        case(f"a neutral file ({neutral}) changes nothing", lambda t, p=neutral: t.append(p, "edit\n"), [])
    case("a harness edit changes every module", lambda t: t.append("test/lattice/Formal/Harness.cs", "// edit\n"), ALL, harness_changes=True)
    case("a workflow verdict input is harness", lambda t: t.append(".github/workflows/ci.yml", "# edit\n"), ALL, harness_changes=True)
    case("a toolchain edit changes every module", lambda t: t.append("tools/tlc/toolchain.json", " "), ALL, toolchain_changes=True)

    # Routing and unclassified files are not verdict inputs.
    case("a routing file changes nothing", lambda t: t.append(".github/workflows/route.py", "# edit\n"), [])
    case("a file outside every glob changes nothing", lambda t: t.append(".github/workflows/unclassified.py", "# edit\n"), [])

    # Per-module files belong to their module only.
    case("a module's cfg changes that module only", lambda t: t.append("spec/alpha/Alpha.cfg", "\\* edit\n"), ["Alpha"])
    case("a variant cfg changes its module only", lambda t: t.append("spec/alpha/AlphaTwo.Narrow.cfg", "\\* edit\n"), ["AlphaTwo"])
    case("a manifest changes its module only", lambda t: t.append("spec/alpha/AlphaTwo.manifest.json", " "), ["AlphaTwo"])
    case("a mutation changes its module only", lambda t: t.append("spec/alpha/mutations/AlphaBreaks.mutation", "# edit\n"), ["Alpha"])
    case("a new mutation changes its module only", lambda t: t.write("spec/alpha/mutations-two/New.mutation", "MODULE: New\n"), ["AlphaTwo"])
    case("a new sibling changes every module in that directory", lambda t: t.write("spec/beta/Helper.tla", "---- MODULE Helper ----\n====\n"), ["Beta"])
    case(
        "a spec verdict file no module claims is harness",
        lambda t: t.write("spec/alpha/Stray.cfg", "SPECIFICATION Spec\n"),
        ALL,
        harness_changes=True,
    )

    # Content, never metadata.
    case(
        "CRLF line endings fingerprint as LF",
        lambda t: t.write("spec/alpha/Alpha.tla", SELF_TEST_TREE["spec/alpha/Alpha.tla"].replace("\n", "\r\n")),
        [],
    )
    case(
        "a timestamp change fingerprints identically",
        lambda t: os.utime(t.root / "spec/beta/Beta.tla", (1_000_000_000, 1_000_000_000)),
        [],
    )
    case(
        "the path is part of each entry",
        lambda t: (t.rename("spec/alpha/mutations/AlphaBreaks.mutation", "spec/alpha/mutations/Renamed.mutation")),
        ["Alpha"],
    )

    with tempfile.TemporaryDirectory() as scratch:
        tree = _Tree(Path(scratch))
        tree.write("spec/beta/Beta.cfg", b"x\ry\r\n")
        lone = tree.fingerprints()
        tree.write("spec/beta/Beta.cfg", b"x\ry\n")
        if _changed(lone, tree.fingerprints()) != {"Beta"}:
            failures.append("a file with a lone CR is binary to git, so its CRLF must not normalise")
        tree.write("tools/tlc/blob.bin", b"\0a\r\nb")
        first = tree.fingerprints()
        tree.write("tools/tlc/blob.bin", b"\0a\nb")
        if first["toolchain"] != tree.fingerprints()["toolchain"]:
            failures.append("tools/tlc/** is forced text by .gitattributes, so CRLF in it must normalise")
        tree.write("spec/beta/Data.cfg", b"\0a\r\nb")
        binary = tree.fingerprints()
        tree.write("spec/beta/Data.cfg", b"\0a\nb")
        if binary["harness"] == tree.fingerprints()["harness"]:
            failures.append("a binary file must be hashed byte for byte")

    # Shape and inputs.
    with tempfile.TemporaryDirectory() as scratch:
        tree = _Tree(Path(scratch))
        document = tree.fingerprints()
        again = tree.fingerprints()
        if json.dumps(document, sort_keys=True) != json.dumps(again, sort_keys=True):
            failures.append("two runs over one tree must agree")
        if sorted(document["modules"]) != sorted(ALL):
            failures.append(f"modules should be {sorted(ALL)}, got {sorted(document['modules'])}")
        expected_inputs = {
            "Alpha": [
                "spec/alpha/Alpha.cfg",
                "spec/alpha/Alpha.manifest.json",
                "spec/alpha/Alpha.tla",
                "spec/alpha/AlphaTwo.tla",
                "spec/alpha/mutations/AlphaBreaks.mutation",
            ],
            "AlphaTwo": [
                "spec/alpha/Alpha.tla",
                "spec/alpha/AlphaTwo.Narrow.cfg",
                "spec/alpha/AlphaTwo.cfg",
                "spec/alpha/AlphaTwo.manifest.json",
                "spec/alpha/AlphaTwo.tla",
                "spec/alpha/mutations-two/AlphaTwoBreaks.mutation",
            ],
        }
        for name, inputs in expected_inputs.items():
            if document["modules"][name]["inputs"] != inputs:
                failures.append(f"{name} inputs: expected {inputs}, got {document['modules'][name]['inputs']}")
        if document["harnessInputs"] != [".github/workflows/ci.yml", "test/lattice/Formal/Harness.cs"]:
            failures.append(f"harness inputs: got {document['harnessInputs']}")
        if document["toolchainInputs"] != ["tools/tlc/toolchain.json"]:
            failures.append(f"toolchain inputs: got {document['toolchainInputs']}")
        for name, module in document["modules"].items():
            if not re.fullmatch(r"[0-9a-f]{64}", module["fingerprint"]):
                failures.append(f"{name}: fingerprint is not a sha256")
        if len({module["fingerprint"] for module in document["modules"].values()}) != len(ALL):
            failures.append("distinct modules must have distinct fingerprints")

    # Globs: '*' crosses '/'.
    inputs = TlcInputs(["spec/*.tla"], ["spec/*.md"], [], ["tools/*"])
    if not inputs.is_harness_candidate("spec/a/b/C.tla") or inputs.is_harness_candidate("spec/a/README.md"):
        failures.append("'*' must cross '/', as fnmatch.fnmatchcase does")

    # Malformed inputs fail loudly.
    with tempfile.TemporaryDirectory() as scratch:
        bad = Path(scratch, "inputs.json")
        for document, why in (
            ({"schema": 2, "verdictInputs": ["a"], "neutral": [], "routing": [], "toolchain": ["a"]}, "schema 2"),
            ({"schema": 1, "verdictInputs": [], "neutral": [], "routing": [], "toolchain": ["a"]}, "empty verdictInputs"),
            ({"schema": 1, "verdictInputs": ["a"], "neutral": [], "routing": [], "toolchain": []}, "empty toolchain"),
            ({"schema": 1, "verdictInputs": ["a"], "routing": [], "toolchain": ["a"]}, "missing neutral"),
            ({"schema": 1, "verdictInputs": ["a"], "neutral": [], "toolchain": ["a"]}, "missing routing"),
            ({"schema": 1, "verdictInputs": ["spec/?.tla"], "neutral": [], "routing": [], "toolchain": ["a"]}, "a '?' glob"),
            ({"schema": 1, "verdictInputs": ["a"], "neutral": [], "routing": "x", "toolchain": ["a"]}, "a non-list routing"),
        ):
            bad.write_text(json.dumps(document), encoding="utf-8")
            try:
                load_inputs(bad)
                failures.append(f"a classification with {why} must be refused")
            except ValueError:
                pass

    with tempfile.TemporaryDirectory() as scratch:
        tree = _Tree(Path(scratch))
        tree.write("spec/beta/Beta.manifest.json", json.dumps({"refinement": "Refinement.md"}))
        try:
            tree.fingerprints()
            failures.append("a manifest with no mutation directory must be refused")
        except ValueError:
            pass
        tree.write("spec/beta/Beta.manifest.json", _manifest("../../../outside"))
        try:
            tree.fingerprints()
            failures.append("a mutation directory outside the repository must be refused")
        except ValueError:
            pass
        tree.write("spec/beta/Beta.manifest.json", _manifest("mutations"))
        tree.write("spec/gamma/Alpha.tla", "---- MODULE Alpha ----\n====\n")
        tree.write("spec/gamma/Alpha.manifest.json", _manifest("mutations"))
        try:
            tree.fingerprints()
            failures.append("a module name declared in two directories must be refused")
        except ValueError:
            pass

    # In a git work tree: tracked and new files count; ignored and deleted ones do not.
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as scratch:
        tree = _Tree(Path(scratch))
        tree.write(".gitignore", "*.ignored.tla\n")
        subprocess.run(["git", "-C", scratch, "init", "-q"], check=True, capture_output=True)
        subprocess.run(["git", "-C", scratch, "-c", "core.autocrlf=false", "add", "-A"], check=True, capture_output=True)
        (tree.root / "spec/alpha/mutations/AlphaBreaks.mutation").unlink()
        tree.write("spec/alpha/mutations/Untracked.mutation", "MODULE: Untracked\n")
        tree.write("spec/beta/Build.ignored.tla", "generated\n")
        document = tree.fingerprints()
        if document["modules"]["Alpha"]["inputs"][-1:] != ["spec/alpha/mutations/Untracked.mutation"]:
            failures.append(f"git mode: Alpha inputs {document['modules']['Alpha']['inputs']}")
        if any("ignored" in path for path in document["modules"]["Beta"]["inputs"]):
            failures.append("git mode: an ignored file must not be an input")

    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    if failures:
        print(f"tlc-fingerprint self-test: {len(failures)} failure(s)", file=sys.stderr)
        return 1
    print("tlc-fingerprint self-test: OK")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", help="write the C4 document to this file")
    parser.add_argument("--root", default=str(REPO_ROOT), help="the repository root (default: this script's repository)")
    parser.add_argument("--inputs", help=f"the C1 classification (default: <root>/{INPUTS_RELATIVE})")
    parser.add_argument("--self-test", action="store_true", help="run the self-test and exit")
    arguments = parser.parse_args(argv)

    if arguments.self_test:
        return self_test()
    if not arguments.output:
        parser.error("--output is required")

    root = Path(arguments.root).resolve()
    document = fingerprint(root, Path(arguments.inputs).resolve() if arguments.inputs else None)
    Path(arguments.output).write_text(json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(
        f"tlc-fingerprint: {len(document['modules'])} module(s), harness {document['harness'][:12]} "
        f"({len(document['harnessInputs'])} file(s)), toolchain {document['toolchain'][:12]} "
        f"({len(document['toolchainInputs'])} file(s))"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
