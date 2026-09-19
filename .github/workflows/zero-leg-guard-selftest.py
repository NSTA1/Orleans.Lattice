#!/usr/bin/env python3
"""Self-test for the zero-leg vacuity guard (issue #3002).

The guard fires precisely when NO test legs run, so the run it protects is by
definition a run in which nothing else executed. It therefore cannot be
exercised as a side effect of ordinary work: on every healthy pull request it
takes its first early return and says so, and on the one run where it matters
there is nothing else running to notice whether it was right. A guard that is
never driven is indistinguishable from a clean one - it reports nothing either
way - so it needs an executable exercise of its own.

This drives the real `zero-leg-guard.sh`, unmodified, over both halves of its
predicate, and then re-drives the whole suite against a mutation set in which
each mutation names the defect it reintroduces and the case that must change
verdict. A mutation that changes no verdict is a hole in these cases, not a
harmless edit, and is reported as a failure.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile

GUARD = pathlib.Path(__file__).resolve().parent / "zero-leg-guard.sh"

PASS = "pass"
FAIL = "fail"


def resolve_bash() -> str:
    """Locate a POSIX bash.

    On CI this is simply `bash`. On a Windows developer machine the `bash` on
    PATH is frequently the WSL launcher, which fails with an execvpe error
    rather than running the script - a failure that is easy to misread as the
    guard firing, since both surface as a non-zero exit. Prefer an explicit
    override, then Git for Windows' bash, then whatever is on PATH.
    """
    override = os.environ.get("BASH")
    if override:
        return override

    if os.name == "nt":
        for candidate in (
            pathlib.Path(r"C:\Program Files\Git\bin\bash.exe"),
            pathlib.Path(r"C:\Program Files (x86)\Git\bin\bash.exe"),
        ):
            if candidate.is_file():
                return str(candidate)

    return "bash"


BASH = resolve_bash()


def run_classify(script: pathlib.Path, paths: list[str]) -> str:
    """Return the classifier's verdict for a changed-path set."""
    proc = subprocess.run(
        [BASH, str(script), "classify"],
        input="\n".join(paths) + "\n",
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"classify exited {proc.returncode} for {paths}: {proc.stderr}"
        )
    return proc.stdout.strip()


def run_verdict(
    script: pathlib.Path,
    leg_count: str,
    package_source_changed: str,
    version_only: str,
) -> tuple[str, str]:
    """Return (`pass`/`fail`, disclosure) for one verdict invocation."""
    proc = subprocess.run(
        [
            BASH,
            str(script),
            "verdict",
            "--leg-count",
            leg_count,
            "--package-source-changed",
            package_source_changed,
            "--version-only",
            version_only,
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return PASS, proc.stdout.strip()
    if proc.returncode == 1:
        # A crashing script also exits non-zero. Requiring the guard's own
        # annotation is what separates "the guard fired" from "the script
        # broke", which would otherwise read identically and let this suite
        # report a pass while testing nothing.
        if "::error::" not in proc.stderr:
            raise AssertionError(
                "verdict exited 1 without emitting its ::error:: annotation, "
                "so the script failed rather than the guard firing: "
                f"{proc.stderr.strip()!r}"
            )
        return FAIL, proc.stderr.strip()
    raise AssertionError(
        f"verdict exited {proc.returncode} (usage error) for "
        f"legs={leg_count!r} pkg={package_source_changed!r} "
        f"ver={version_only!r}: {proc.stderr}"
    )


# ---------------------------------------------------------------------------
# Classification cases. Each states what the path set represents, because the
# distinction being drawn is "does this change seed a package", not "is this
# file interesting".
# ---------------------------------------------------------------------------
CLASSIFY_CASES: list[tuple[str, list[str], str]] = [
    (
        "a library source file seeds its package",
        ["src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs"],
        "true",
    ),
    (
        "a test source file seeds its package",
        ["test/lattice/BPlusTree/Grains/BPlusLeafGrainTests.cs"],
        "true",
    ),
    (
        "a csproj under src seeds its package",
        ["src/lattice.vector/Orleans.Lattice.Vector.csproj"],
        "true",
    ),
    (
        "the shared testing library seeds (it is consumed by every package)",
        ["test/shared/Orleans.Lattice.Testing/Hygiene/HygieneFiles.cs"],
        "true",
    ),
    (
        "one seeding file among many non-seeding ones still seeds",
        ["docs/lattice/index.md", "README.md", "src/lattice/Primitives/GSet.cs"],
        "true",
    ),
    (
        "documentation seeds nothing",
        ["docs/lattice/index.md", "docs/crdt/overview.md"],
        "false",
    ),
    (
        "a markdown note UNDER src seeds nothing - the paths filters exclude it",
        ["src/lattice/README.md"],
        "false",
    ),
    (
        "a markdown note UNDER test seeds nothing",
        ["test/lattice/README.md"],
        "false",
    ),
    (
        "a text file under src seeds nothing - the paths filters exclude *.txt",
        ["src/lattice/notes.txt"],
        "false",
    ),
    (
        "the azure-throughput silo runs its own lane and seeds nothing here",
        ["test/azure-throughput-silo/Program.cs"],
        "false",
    ),
    (
        "the explorer UI tests run their own lane and seed nothing here",
        ["test/lattice.explorer.uitests/ShellTests.cs"],
        "false",
    ),
    (
        "samples seed nothing",
        ["samples/AgentBacklog/template/backlog-protocol.md", "samples/Demo/Program.cs"],
        "false",
    ),
    (
        "workflow files seed nothing (the selector's fallback covers them)",
        [".github/workflows/ci.yml"],
        "false",
    ),
    (
        "a path merely CONTAINING src/ does not seed - the match is anchored",
        ["benchmark/azure-throughput/src/Runner.cs", "apps/src/Main.cs"],
        "false",
    ),
    (
        "an empty changed set seeds nothing and must not error",
        [],
        "false",
    ),
]


# ---------------------------------------------------------------------------
# Verdict cases: (label, leg_count, package_source_changed, version_only,
# expected, expected disclosure fragment).
#
# The disclosure is asserted, not merely the exit code. Several arms of the
# guard agree on the verdict and differ only in the reason they give, so the
# exit code alone cannot tell them apart - and on a zero-leg run the reason IS
# the output, since nothing else ran to explain what happened. Asserting only
# the code would let an arm be deleted with no case changing, which is exactly
# what the mutation suite reported before these fragments existed.
# ---------------------------------------------------------------------------
VERDICT_CASES: list[tuple[str, str, str, str, str, str]] = [
    (
        "POSITIVE CONTROL: zero legs while package source changed is the defect",
        "0",
        "true",
        "false",
        FAIL,
        "ZERO packages",
    ),
    (
        "NEGATIVE CONTROL: zero legs for a docs-only change is legitimate",
        "0",
        "false",
        "false",
        PASS,
        "no package source or test file changed",
    ),
    (
        "CARVE-OUT: a release version bump reaches zero legs legitimately",
        "0",
        "true",
        "true",
        PASS,
        "release version bump only",
    ),
    (
        "PUSH LANE: no classification ran, so there is nothing to judge",
        "0",
        "",
        "",
        PASS,
        "not a pull request",
    ),
    (
        "REACHABILITY CONTROL: a healthy multi-leg run is never failed here",
        "42",
        "true",
        "false",
        PASS,
        "42 leg(s) planned",
    ),
    (
        "a one-leg run is out of scope; the aggregate guard covers it",
        "1",
        "true",
        "false",
        PASS,
        "1 leg(s) planned",
    ),
    (
        "a healthy run with no package source change is also untouched",
        "42",
        "false",
        "false",
        PASS,
        "aggregate guard covers this run",
    ),
]


# ---------------------------------------------------------------------------
# Mutations. Each names the defect it reintroduces. A mutation that changes no
# verdict means the cases above do not pin that behaviour down.
# ---------------------------------------------------------------------------
MUTATIONS: list[tuple[str, str, str]] = [
    (
        "seeding restricted to src/ and test/ removed: every docs-only change "
        "would be classified as seeding, failing every legitimate zero-leg run",
        "  seeds=$(grep -E '^(src|test)/' \\",
        "  seeds=$(grep -E '' \\",
    ),
    (
        "markdown exclusion removed: a design note under src/ would fail the "
        "run, because a markdown-only change legitimately plans zero legs",
        "    | grep -vE '\\.md$' \\",
        "    | grep -vE '^$' \\",
    ),
    (
        "text exclusion removed: a *.txt edit under src/ would fail the run",
        "    | grep -vE '\\.txt$' \\",
        "    | grep -vE '^$' \\",
    ),
    (
        "separately-laned uitests exclusion removed: a UI-test-only change "
        "would fail, though it runs its own lane and plans zero library legs",
        "    | grep -vE '^test/lattice\\.explorer\\.uitests/' \\",
        "    | grep -vE '^$' \\",
    ),
    (
        "separately-laned azure-throughput exclusion removed: same defect on "
        "the benchmark silo's own lane",
        "    | grep -vE '^test/azure-throughput-silo/' \\",
        "    | grep -vE '^$' \\",
    ),
    (
        "version-bump carve-out removed: every coordinated release would fail "
        "build-and-test, because a version bump touches src/**/*.csproj and "
        "deliberately plans zero legs",
        '  if [ "$version_only" = "true" ]; then',
        '  if [ "$version_only" = "__never__" ]; then',
    ),
    (
        "leg-count scoping removed: the guard would fire on healthy runs that "
        "planned legs, where the aggregate guard already provides strictly "
        "better coverage",
        '  if [ "$leg_count" != "0" ]; then',
        '  if [ "$leg_count" = "__never__" ]; then',
    ),
    (
        "unclassified treated as seeding: a push lane, which runs no "
        "classifier, would be failed on a property nothing measured",
        '  if [ -z "$package_source_changed" ]; then',
        '  if [ "$package_source_changed" = "__never__" ]; then',
    ),
    (
        "the guard's own condition inverted: the defect this check exists to "
        "catch would pass and every legitimate zero-leg run would fail",
        '  if [ "$package_source_changed" != "true" ]; then',
        '  if [ "$package_source_changed" = "true" ]; then',
    ),
]


def check(script: pathlib.Path) -> list[str]:
    """Run every case against `script`, returning the failures."""
    failures: list[str] = []

    for label, paths, expected in CLASSIFY_CASES:
        try:
            actual = run_classify(script, paths)
        except AssertionError as error:
            failures.append(f"classify[{label}]: {error}")
            continue
        if actual != expected:
            failures.append(
                f"classify[{label}]: expected {expected}, got {actual} for {paths}"
            )

    for label, legs, pkg, ver, expected, fragment in VERDICT_CASES:
        try:
            actual, disclosure = run_verdict(script, legs, pkg, ver)
        except AssertionError as error:
            failures.append(f"verdict[{label}]: {error}")
            continue
        if actual != expected:
            failures.append(
                f"verdict[{label}]: expected {expected}, got {actual} "
                f"(legs={legs!r} pkg={pkg!r} ver={ver!r})"
            )
            continue
        if fragment not in disclosure:
            failures.append(
                f"verdict[{label}]: expected the disclosure to contain "
                f"{fragment!r}, got {disclosure!r}"
            )

    return failures


def main() -> int:
    if not GUARD.is_file():
        print(f"FAIL: guard script not found at {GUARD}", file=sys.stderr)
        return 1

    source = GUARD.read_text(encoding="utf-8")

    # The unmutated guard must pass every case.
    failures = check(GUARD)
    if failures:
        print("FAIL: the zero-leg guard failed its own cases:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print(
        f"ok: {len(CLASSIFY_CASES)} classification and {len(VERDICT_CASES)} "
        "verdict cases pass against the unmutated guard"
    )

    # Every mutation must be both APPLICABLE and CAUGHT. An inapplicable
    # mutation is the more dangerous of the two failures: it means the guard
    # was rewritten and this suite silently stopped testing that behaviour
    # while continuing to report a pass.
    with tempfile.TemporaryDirectory() as tmp:
        mutant_path = pathlib.Path(tmp) / "zero-leg-guard.sh"

        for label, needle, replacement in MUTATIONS:
            occurrences = source.count(needle)
            if occurrences != 1:
                print(
                    f"FAIL: mutation anchor appears {occurrences} times "
                    f"(expected exactly 1) - the guard was rewritten and this "
                    f"suite no longer tests: {label}\n"
                    f"       anchor: {needle!r}",
                    file=sys.stderr,
                )
                return 1

            mutant_path.write_text(source.replace(needle, replacement), encoding="utf-8")
            if not check(mutant_path):
                print(
                    f"FAIL: mutation changed no verdict, so nothing here pins "
                    f"down: {label}",
                    file=sys.stderr,
                )
                return 1
            print(f"ok: caught - {label}")

    print(f"PASS: zero-leg guard self-test ({len(MUTATIONS)} mutations caught)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
