#!/usr/bin/env python3
"""Self-test for the coverage lane's supersession gate.

The gate's healthy outcome for a superseded run is a SKIPPED coverage job, and
a skipped job looks exactly like a gate that has stopped working: a predicate
that skipped every run would leave the coverage report frozen with nothing red
anywhere, and a predicate that skipped none would quietly put the lane back to
measuring every commit serially. Neither is observable from the lane itself, so
the predicate needs an exercise of its own.

This drives the real `coverage-supersession.py`, unmodified, over the cases
that define it - which newer runs supersede this one and which cannot - and
over the inputs it must refuse rather than decide on. It then re-drives the
suite against a mutation set in which each mutation names the defect it
reintroduces and the case that must change verdict. Every expectation is
checked two-sided: it must hold against the real gate and must NOT hold against
the mutant, so a case that had stopped exercising its behaviour cannot let its
mutation pass for the wrong reason.

Run it with:
  python3 .github/workflows/coverage-supersession-selftest.py
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tempfile
from typing import Callable

HERE = pathlib.Path(__file__).resolve().parent
GATE = HERE / "coverage-supersession.py"
COVERAGE = HERE / "coverage.yml"
CI = HERE / "ci.yml"

BRANCH = "main"

Outcome = tuple[int, str, str]
Expectation = Callable[[Outcome], bool]


def run(number: int, status: str = "completed", conclusion: str | None = "success",
        branch: str | None = BRANCH, event: str = "push") -> dict:
    """One entry of a workflow-runs listing.

    Ids deliberately run OPPOSITE to run numbers. A predicate that compared ids
    instead of run numbers would then invert every verdict below rather than
    agree with it by coincidence.
    """
    entry = {
        "id": 900_000 - number * 7,
        "run_number": number,
        "status": status,
        "conclusion": conclusion if status == "completed" else None,
        "event": event,
        "head_sha": f"{number:040x}",
    }
    if branch is not None:
        entry["head_branch"] = branch
    return entry


def pending(number: int, branch: str = BRANCH) -> dict:
    """A run waiting on the concurrency group."""
    return run(number, status="pending", branch=branch)


def this(number: int = 10, **kwargs) -> dict:
    """The run the gate is deciding for; it is always in progress while the gate runs."""
    return run(number, status="in_progress", **kwargs)


def listing(*runs: dict) -> dict:
    return {"total_count": len(runs), "workflow_runs": list(runs)}


def invoke(gate: pathlib.Path, payload: object, run_id: int) -> Outcome:
    """Run the gate over `payload` (serialised unless it is already a string)."""
    text = payload if isinstance(payload, str) else json.dumps(payload)
    proc = subprocess.run(
        [sys.executable, str(gate), "--run-id", str(run_id), "-"],
        input=text,
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def measures(fragment: str) -> Expectation:
    return lambda o: o[0] == 0 and o[1] == "measure" and fragment in o[2]


def skips_for(number: int) -> Expectation:
    # The named run is the verdict's evidence, so it is asserted, not just the
    # word: a gate that skipped for the wrong newer run would read identically.
    return lambda o: (o[0] == 0 and o[1] == "skip"
                      and f"run #{number} (" in o[2] and "::notice" in o[2])


def refuses(fragment: str) -> Expectation:
    # Exit 2 with an empty stdout is the only refusal: a crash exits 1, and the
    # workflow treating a crash as a verdict is exactly what must not happen.
    return lambda o: o[0] == 2 and o[1] == "" and fragment in o[2]


SELF = this()
SELF_ID = SELF["id"]


# ---------------------------------------------------------------------------
# Cases: (label, payload, run id, expectation).
# ---------------------------------------------------------------------------
CASES: list[tuple[str, object, int, Expectation]] = [
    (
        "C1 the newest run measures, whatever older runs did",
        listing(SELF, run(9), run(8, conclusion="cancelled"), run(7, conclusion="failure")),
        SELF_ID,
        measures("#10 is the newest"),
    ),
    (
        "C2 a newer run waiting on the concurrency group supersedes this one",
        listing(pending(11), SELF),
        SELF_ID,
        skips_for(11),
    ),
    (
        "C3 a newer run that is already in progress supersedes this one",
        listing(run(11, status="in_progress"), SELF),
        SELF_ID,
        skips_for(11),
    ),
    (
        "C4 a newer run that already finished supersedes this one, because the "
        "queue is not guaranteed to start runs in creation order",
        listing(run(11), SELF),
        SELF_ID,
        skips_for(11),
    ),
    (
        "C5 a newer run that failed still supersedes: it measured a later commit",
        listing(run(11, conclusion="failure"), SELF),
        SELF_ID,
        skips_for(11),
    ),
    (
        "C6 a cancelled newer run does not supersede: it will never measure",
        listing(run(11, conclusion="cancelled"), SELF),
        SELF_ID,
        measures("#10 is the newest"),
    ),
    (
        "C7 a newer run that failed to start does not supersede either",
        listing(run(11, conclusion="startup_failure"), SELF),
        SELF_ID,
        measures("#10 is the newest"),
    ),
    (
        "C8 a newer run on another branch does not supersede: the concurrency "
        "group is per-ref and that run never measures this branch",
        listing(pending(11, branch="ci/validate-the-lane"), SELF),
        SELF_ID,
        measures("#10 is the newest"),
    ),
    (
        "C9 run numbers compare as numbers: run 10 is newer than run 9",
        listing(pending(10), this(9)),
        this(9)["id"],
        skips_for(10),
    ),
    (
        "C10 older runs never supersede, in any state",
        listing(SELF, pending(9), run(8, status="in_progress"), run(7)),
        SELF_ID,
        measures("#10 is the newest"),
    ),
    (
        "C11 the NEAREST superseding run is the one named",
        listing(pending(13), run(12, conclusion="cancelled"), pending(11), SELF),
        SELF_ID,
        skips_for(11),
    ),
    (
        "E1 a listing without this run is about another workflow or branch, so "
        "its run numbers - however high - are refused rather than obeyed",
        listing(pending(4000), run(3999)),
        SELF_ID,
        refuses("appears 0 time(s)"),
    ),
    (
        "E2 a listing that is not a JSON object is refused",
        [SELF],
        SELF_ID,
        refuses("not a JSON object"),
    ),
    (
        "E3 a listing with no workflow_runs array is refused",
        {"total_count": 0},
        SELF_ID,
        refuses("no workflow_runs"),
    ),
    (
        "E4 a body that is not JSON at all is refused",
        '{"workflow_runs": [',
        SELF_ID,
        refuses("cannot decide"),
    ),
    (
        "E5 a run number that is not an integer is refused",
        listing(dict(SELF, run_number="10")),
        SELF_ID,
        refuses("non-integer run_number"),
    ),
    (
        "E6 a listing entry that is not an object is refused",
        listing(SELF, "run #11"),
        SELF_ID,
        refuses("not an object"),
    ),
    (
        "E7 a run with no branch is refused: there is nothing to compare against",
        listing(this(branch=None), pending(11)),
        SELF_ID,
        refuses("no head_branch"),
    ),
]


def by_label(prefix: str) -> tuple[str, object, int, Expectation]:
    matches = [case for case in CASES if case[0].startswith(prefix + " ")]
    if len(matches) != 1:
        raise SystemExit(f"selftest bug: {len(matches)} cases carry label {prefix!r}")
    return matches[0]


# ---------------------------------------------------------------------------
# Mutations: (name, anchor, replacement, the case that must change verdict).
# ---------------------------------------------------------------------------
MUTATIONS: list[tuple[str, str, str, str]] = [
    (
        "M1 this run counted as newer than itself: every run would skip, and "
        "the newest - the one that must measure - would skip too",
        "if run_number_of(run) <= number:",
        "if run_number_of(run) < number:",
        "C1",
    ),
    (
        "M2 run numbers compared as text: run 10 would sort before run 9",
        "if run_number_of(run) <= number:",
        "if str(run_number_of(run)) <= str(number):",
        "C9",
    ),
    (
        "M3 the branch ignored: a run dispatched on a feature branch would make "
        "main's newest run skip, and nothing would measure main",
        'if run.get("head_branch") != branch:',
        "if False:",
        "C8",
    ),
    (
        "M4 a cancelled newer run trusted to measure: a cancelled run would "
        "leave the latest commit unmeasured",
        'if run.get("conclusion") in NEVER_MEASURES:',
        "if False:",
        "C6",
    ),
    (
        "M5 startup_failure forgotten: a run that never started would stand in "
        "for one that would have measured",
        'NEVER_MEASURES = frozenset({"cancelled", "startup_failure"})',
        'NEVER_MEASURES = frozenset({"cancelled"})',
        "C7",
    ),
    (
        "M6 the identity check removed: a listing that does not contain this "
        "run would no longer be refused for being about something else",
        "if len(own) != 1:",
        "if False:",
        "E1",
    ),
    (
        "M7 the farthest superseding run named instead of the nearest",
        "nearest = min(newer, key=run_number_of)",
        "nearest = max(newer, key=run_number_of)",
        "C11",
    ),
    (
        "M8 a run with no branch trusted: it would be compared against nothing",
        "if not isinstance(branch, str) or not branch:",
        "if False:",
        "E7",
    ),
    (
        "M9 a non-integer run number accepted",
        "if not isinstance(number, int) or isinstance(number, bool):",
        "if False:",
        "E5",
    ),
    (
        "M10 refusal reported as success: the workflow would read an empty "
        "verdict as a decision",
        "        return 2\n",
        "        return 0\n",
        "E4",
    ),
]


def check_cases(gate: pathlib.Path) -> list[str]:
    failures = []
    for label, payload, run_id, expected in CASES:
        outcome = invoke(gate, payload, run_id)
        if not expected(outcome):
            failures.append(f"{label}: got exit={outcome[0]} stdout={outcome[1]!r} "
                            f"stderr={outcome[2]!r}")
    return failures


def check_wiring() -> list[str]:
    failures = []
    coverage = COVERAGE.read_text(encoding="utf-8") if COVERAGE.is_file() else ""
    ci = CI.read_text(encoding="utf-8") if CI.is_file() else ""

    if "coverage-supersession.py --run-id" not in coverage:
        failures.append("coverage.yml does not invoke coverage-supersession.py with "
                        "--run-id, so the gate this fixture proves decides nothing")
    if "GITHUB_RUN_ID" not in coverage:
        failures.append("coverage.yml does not pass GITHUB_RUN_ID to the gate")
    if "coverage-supersession-selftest.py" not in ci:
        failures.append("ci.yml does not run this self-test, so nothing executes it "
                        "and a broken gate would go unnoticed")
    return failures


def main() -> int:
    if not GATE.is_file():
        print(f"FAIL: gate script not found at {GATE}", file=sys.stderr)
        return 1

    source = GATE.read_text(encoding="utf-8")

    failures = check_cases(GATE) + check_wiring()
    if failures:
        print("FAIL: the coverage supersession gate failed its own cases:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print(f"ok: {len(CASES)} cases pass against the real gate, and it is wired into "
          "coverage.yml and ci.yml")

    survivors = []
    with tempfile.TemporaryDirectory(prefix="coverage-supersession-selftest-") as work:
        for name, anchor, replacement, case in MUTATIONS:
            occurrences = source.count(anchor)
            if occurrences != 1:
                print(f"FAIL: mutation anchor for {name!r} occurs {occurrences} times "
                      "in the gate, so the mutation would not mean what it says",
                      file=sys.stderr)
                return 1

            mutant = pathlib.Path(work) / "coverage-supersession.py"
            mutant.write_text(source.replace(anchor, replacement), encoding="utf-8")

            label, payload, run_id, expected = by_label(case)
            if expected(invoke(mutant, payload, run_id)):
                survivors.append(f"{name} (expected {label} to change verdict)")

    if survivors:
        print("FAIL: these mutations changed no verdict, so the cases do not pin "
              "the behaviour they name:", file=sys.stderr)
        for survivor in survivors:
            print(f"  - {survivor}", file=sys.stderr)
        return 1

    print(f"ok: all {len(MUTATIONS)} mutations are caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
