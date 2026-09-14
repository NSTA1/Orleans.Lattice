#!/usr/bin/env python3
"""Self-test for summarise-test-legs.py, and specifically for its VACUITY GUARD.

Why this exists
---------------
`summarise-test-legs.py` decides the run's verdict, and one of the two things it
decides on is vacuity: a shard whose filter matches nothing does not fail, it
reports green having run no tests. That guard is the single point at which a
fan-out reporting "success" over an empty population is caught, and until this
file landed NOTHING in the repository exercised it. An untested guard is
indistinguishable from a clean configuration, which is the exact defect the
guard itself exists to prevent, one level up.

The unit the guard is keyed on IS the answer
--------------------------------------------
The check sums `executed` per `(package, shard)` across every tier AND every
leg, not per item and not per leg. That choice is load-bearing and is what the
three cases below pin down:

  * A single empty TIER is normal. Most packages have no Coyote models, so an
    item with `outcome: "empty"` is routine and must not fail anything.
  * A whole empty LEG is survivable, PROVIDED that leg's shards executed
    somewhere else. `run-test-leg.py` exits non-zero only for `outcome ==
    "failed"`, so an all-empty leg's own job reports success and is
    cosmetically indistinguishable from a leg that ran thousands of tests. The
    aggregate is what makes that green safe, and it can only do so because it
    is keyed on the shard rather than on the leg.
  * A shard that executed nothing in ANY tier in ANY leg is the real defect,
    and must fail.

A guard keyed on the wrong unit would not be a weak guard, it would be a guard
answering a different question, and it would read as clean forever.

The three cases are not interchangeable
---------------------------------------
  A  proves the guard FIRES, and that it fires for the right reason: exit 1
     plus the vacuity section plus the offending shard named. Asserting the
     exit status alone would also be satisfied by the script crashing, which
     is a different event with the same exit code.
  B  is the NEGATIVE CONTROL. Same shape as A with one tier executed, so A's
     red is attributable to the zero rather than to the shape. Without B, A
     establishes only that the script can exit 1.
  C  is the case the job-level question is actually about: an all-empty LEG
     whose shards ran in a different leg. It must PASS. Reduce this file to A
     alone and it proves strictly less than is already known.

Usage
-----
  python3 .github/workflows/summarise-test-legs-selftest.py

Run from the repository root. SUMMARISE_TEST_LEGS overrides the script under
test, which is what lets a negative control point this at a deliberately broken
copy and confirm these checks can fail. Point it at a copy with the `silent`
block removed and case A must go red; if it does not, this file is vacuous.

This invokes the script UNMODIFIED. The moment a self-test edits its subject to
make it testable, it stops testing the thing CI runs.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile

SCRIPT = os.environ.get("SUMMARISE_TEST_LEGS", ".github/workflows/summarise-test-legs.py")

VACUITY_HEADING = "Shards that executed no tests in any tier"
REPORT_HEADING = "## Test matrix result"

failures = 0
checks = 0


def fail(message: str) -> None:
    global failures
    failures += 1
    print(f"::error::summarise-test-legs self-test: {message}")
    print(f"FAIL: {message}", file=sys.stderr)


def passed(message: str) -> None:
    print(f"  ok: {message}")


def check() -> None:
    global checks
    checks += 1


def item(package: str, shard: str, tier: str, executed: int, outcome: str) -> dict:
    """One leg-result record, matching the schema run-test-leg.py writes."""
    return {
        "label": f"{package} / {shard} ({tier})",
        "package": package,
        "shard": shard,
        "tier": tier,
        "seeded": True,
        "estimate": 0.1,
        "leg": "?",
        "job_url": "",
        "outcome": outcome,
        "duration": 0.04,
        "executed": executed,
        "failures": [],
        "failure_count": 0,
    }


def leg(leg_id: str, items: list[dict]) -> dict:
    return {"leg": {"id": leg_id, "name": f"leg {leg_id}", "estimate": 0.1}, "items": items}


def run_aggregator(legs: list[dict], script: str | None = None) -> tuple[int, str]:
    """Invoke the unmodified script over a synthesised results directory."""
    work = tempfile.mkdtemp(prefix="summarise-selftest-")
    try:
        for payload in legs:
            path = os.path.join(work, f"{payload['leg']['id']}.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
        completed = subprocess.run(
            [sys.executable, script or SCRIPT,
             "--results-dir", work, "--expect-legs", str(len(legs))],
            capture_output=True,
            text=True,
        )
        return completed.returncode, completed.stdout
    finally:
        shutil.rmtree(work, ignore_errors=True)


def perturbed_copy(work: str, old: str, new: str) -> str | None:
    """Write a copy of the subject with one mutation applied.

    Returns None when the pattern did not match exactly once, so a pattern that
    has drifted out of date is a hard failure rather than a mutation that
    silently did nothing and left the check passing for the wrong reason.

    The original is opened only for reading. Nothing reverts it, because
    nothing modifies it - which removes the failure mode where a revert does
    not apply and a stale artefact is scored as green.
    """
    with open(SCRIPT, "r", encoding="utf-8", newline="") as handle:
        source = handle.read()

    # The subject may be stored with either line ending. A pattern written with
    # '\n' matches nothing in a CRLF checkout, so take the separator from the
    # file rather than assuming it.
    eol = "\r\n" if "\r\n" in source else "\n"
    old = old.replace("\n", eol)
    new = new.replace("\n", eol)

    if source.count(old) != 1:
        return None

    path = os.path.join(work, "perturbed.py")
    with open(path, "w", encoding="utf-8", newline="") as handle:
        handle.write(source.replace(old, new))
    return path



def main() -> int:
    if not os.path.isfile(SCRIPT):
        print(f"self-test: {SCRIPT} not found; run from the repository root.", file=sys.stderr)
        return 2

    # -----------------------------------------------------------------------
    # 0. Denominator of this self-test itself.
    #
    # Every case below is a separate invocation, so a refactor that dropped one
    # would reduce the suite silently and the survivors would still pass.
    # Assert the population before asserting anything about it.
    # -----------------------------------------------------------------------
    expected_cases = 3
    observed_cases = 0

    # -----------------------------------------------------------------------
    # A. The shard executed nothing, in any tier, in any leg. MUST FAIL.
    #
    # Three assertions, not one. Exit status alone does not separate "the guard
    # fired" from "the script crashed": both are non-zero.
    # -----------------------------------------------------------------------
    observed_cases += 1
    code, report = run_aggregator([
        leg("leg-1", [
            item("pkgA", "shardX", "chaos", 0, "empty"),
            item("pkgA", "shardX", "coyote", 0, "empty"),
        ]),
    ])
    print(f"  case A: exit={code} (expected 1)")

    check()
    if code != 1:
        fail(
            f"a shard that executed no tests in any tier was accepted (exit {code}); "
            "the aggregate would report a green run over an empty population."
        )
    else:
        passed("a shard silent in every tier and every leg fails the aggregate.")

    check()
    if VACUITY_HEADING not in report:
        fail(
            "the aggregate exited non-zero but rendered no vacuity section, so the "
            "failure is not attributable to the vacuity guard - the script may simply "
            "have crashed, which carries the same exit status."
        )
    else:
        passed("the failure is attributable: the vacuity section was rendered.")

    check()
    if "`pkgA` / `shardX`" not in report:
        fail(
            "the vacuity section does not name the offending shard, so a reader "
            "cannot tell which shard verified nothing."
        )
    else:
        passed("the vacuity section names the offending shard.")

    # -----------------------------------------------------------------------
    # B. NEGATIVE CONTROL. Same shape, one tier executed. MUST PASS.
    #
    # This is what makes case A's red attributable to the zero rather than to
    # the shape of the payload. Without it, A shows only that exit 1 is
    # reachable.
    # -----------------------------------------------------------------------
    observed_cases += 1
    code, report = run_aggregator([
        leg("leg-1", [
            item("pkgA", "shardX", "chaos", 0, "empty"),
            item("pkgA", "shardX", "deterministic", 355, "passed"),
        ]),
    ])
    print(f"  case B: exit={code} (expected 0)")

    check()
    if code != 0:
        fail(
            f"a shard with one empty tier and one populated tier was rejected (exit {code}); "
            "a single empty tier is normal and must not fail the run."
        )
    else:
        passed("an empty tier alongside a populated one is accepted.")

    check()
    if VACUITY_HEADING in report:
        fail(
            "a shard that executed 355 tests was listed as having executed none; "
            "the guard is keyed on the item rather than on the shard."
        )
    else:
        passed("no vacuity section for a shard that ran somewhere.")

    # -----------------------------------------------------------------------
    # C. An all-empty LEG whose shards executed in another leg. MUST PASS.
    #
    # The configuration the job-level question is about. `run-test-leg.py`
    # reports success for leg-2 here, and that green is safe only because the
    # aggregate sums across legs. If this case ever fails, the guard has been
    # re-keyed to the leg and will redden ordinary runs; if case A also passes
    # while this fails, the guard has been inverted.
    # -----------------------------------------------------------------------
    observed_cases += 1
    code, report = run_aggregator([
        leg("leg-1", [
            item("pkgA", "shardX", "deterministic", 355, "passed"),
            item("pkgB", "shardY", "deterministic", 210, "passed"),
        ]),
        leg("leg-2", [
            item("pkgA", "shardX", "chaos", 0, "empty"),
            item("pkgB", "shardY", "coyote", 0, "empty"),
        ]),
    ])
    print(f"  case C: exit={code} (expected 0)")

    check()
    if code != 0:
        fail(
            f"a leg whose every item was empty failed the aggregate (exit {code}) even though "
            "its shards executed in another leg; the guard has been re-keyed from the shard "
            "to the leg and will redden ordinary runs."
        )
    else:
        passed("an all-empty leg is accepted when its shards executed in another leg.")

    check()
    if REPORT_HEADING not in report:
        fail(
            "the aggregate exited zero without rendering a report, so the pass is not "
            "evidence that it examined anything."
        )
    else:
        passed("the aggregate rendered its report.")

    # -----------------------------------------------------------------------
    # D. SELF-VALIDATION. Can this suite fail at all?
    #
    # Cases A to C establish what the guard does. They do not establish that
    # this file would notice if it stopped doing it - and a self-test that
    # cannot redden is worth precisely as much as the untested guard it
    # replaced. The mutations below are the answer to "name a change that
    # would redden this assertion": they are named here, in the repository,
    # and re-checked on every run, rather than performed by hand once and
    # recorded in a commit message.
    #
    # Each mutation is applied to a COPY. The subject is never opened for
    # writing, so there is no revert and therefore no revert that silently
    # fails to apply.
    #
    # Skipped when the operator has already pointed SUMMARISE_TEST_LEGS at a
    # deliberately broken script: perturbing an already-perturbed subject
    # proves nothing.
    # -----------------------------------------------------------------------
    if os.environ.get("SUMMARISE_TEST_LEGS"):
        print("  self-validation: skipped (SUMMARISE_TEST_LEGS overrides the subject)")
    else:
        mutations = [
            (
                "the guard renders its section but does not fail the run",
                "    if silent:\n        ok = False\n",
                "    if silent:\n        ok = ok\n",
                [leg("leg-1", [
                    item("pkgA", "shardX", "chaos", 0, "empty"),
                    item("pkgA", "shardX", "coyote", 0, "empty"),
                ])],
                1,
                "case A would still have passed against a guard that never fails the run",
            ),
            (
                "the guard is re-keyed from the shard to the individual item",
                'per_shard[(item["package"], item["shard"])] += item["executed"]',
                'per_shard[(item["package"], item["shard"], item["tier"])] += item["executed"]',
                [leg("leg-1", [
                    item("pkgA", "shardX", "chaos", 0, "empty"),
                    item("pkgA", "shardX", "deterministic", 355, "passed"),
                ])],
                0,
                "case B would still have passed against a guard keyed on the wrong unit",
            ),
        ]

        work = tempfile.mkdtemp(prefix="summarise-selftest-mutate-")
        try:
            for label, old, new, payload, unmutated_code, consequence in mutations:
                check()
                broken = perturbed_copy(work, old, new)
                if broken is None:
                    fail(
                        f"the mutation '{label}' no longer matches the subject exactly "
                        "once, so this suite's ability to fail is unproven; the pattern "
                        "has drifted and must be updated alongside the script."
                    )
                    continue

                mutated_code, _ = run_aggregator(payload, script=broken)
                print(f"  self-validation ({label}): exit={mutated_code} "
                      f"(unmutated {unmutated_code})")
                if mutated_code == unmutated_code:
                    fail(f"{consequence}, so that assertion is not testing what it claims.")
                else:
                    passed(f"reintroducing '{label}' reddens this suite.")
        finally:
            shutil.rmtree(work, ignore_errors=True)

    # -----------------------------------------------------------------------
    # 0 (concluded). The population assertion, checked last so it counts every
    # case that actually ran rather than every case that was declared.
    # -----------------------------------------------------------------------
    check()
    if observed_cases != expected_cases:
        fail(
            f"ran {observed_cases} case(s) but expected {expected_cases}; "
            "a case has been dropped and the survivors cannot cover for it."
        )
    else:
        passed(f"ran all {observed_cases} cases.")

    print()
    if failures:
        print(
            f"summarise-test-legs self-test: {failures} of {checks} checks FAILED.",
            file=sys.stderr,
        )
        return 1
    print(f"summarise-test-legs self-test: all {checks} checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
