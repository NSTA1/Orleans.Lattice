#!/usr/bin/env python3
"""Supersession gate for the coverage lane (`coverage.yml`).

WHAT IT DECIDES
---------------
Whether THIS run of the coverage workflow measures, or leaves the measurement
to a newer run of the same workflow on the same branch. It reads the GitHub
"list workflow runs" response for this workflow and branch, prints exactly one
word on stdout - `measure` or `skip` - and explains the decision on stderr.

WHY THE LANE NEEDS A GATE
-------------------------
The lane used to set `cancel-in-progress: true`. A run takes about an hour and
main routinely takes merges faster than that, so most runs were cancelled
before they finished: of the 35 runs from #3327 to #3473, 22 were cancelled,
spending 615 runner-minutes measuring nothing against 708 for the 11 that
completed. A cancelled check run rolls up as a FAILURE on its commit, so main
showed a red cross on most of its commits although nothing had failed.

The lane now queues instead of cancelling (`queue: max`), so every run that
starts finishes. Queueing alone would measure EVERY commit, one after another,
and a burst of merges would leave the queue hours behind main. So a queued run
that reaches the front asks whether a newer run exists. If one does, measuring
this older commit would cost an hour and be superseded before it finished, so
it skips. The newest run never finds a newer one, so the latest state of main
is always measured - debounced on the trailing edge, with nothing cancelled.

THE PREDICATE
-------------
This run is superseded when the listing holds another run that

  - is on the same branch: the concurrency group is per-ref, and a run on
    another branch will never measure this branch's commits;
  - has a higher run number: run numbers are per-workflow and increase with
    every new run whatever its event, while a re-run keeps its number;
  - and was not cancelled and did not fail to start, because a run that will
    never measure cannot stand in for this one.

Whether that newer run is queued, in progress or complete does not matter.
Queued and running ones will measure, and a complete one has - and the queue is
first-in-first-out by the time a run starts waiting, which GitHub does not
guarantee matches the order runs were created in, so an older run can reach the
front after a newer one has finished. It must not then re-measure an older
commit over the newer one.

FAILING OPEN
------------
The two mistakes are not symmetric. Measuring needlessly costs one run of the
lane. Skipping wrongly is SILENT: a skipped job reports as skipped rather than
failed, so nothing turns red and the coverage report simply stops moving. So
anything this script cannot read is an error (exit 2), never a verdict, and the
workflow measures on any error.

That includes a listing that does not contain this run exactly once. The run is
in progress while this executes, so a listing without it is about some other
workflow or branch - and a foreign workflow's run numbers say nothing about this
one. A busier workflow's higher numbers would otherwise make every run skip.

Usage:
  coverage-supersession.py --run-id ID RUNS_JSON

  RUNS_JSON   the body of GET /repos/{owner}/{repo}/actions/workflows/{file}/runs
              filtered to this run's branch, or `-` to read it from stdin
"""

from __future__ import annotations

import argparse
import json
import sys

MEASURE = "measure"
SKIP = "skip"

# Conclusions of a run that did not measure and never will, so it cannot
# supersede an older run.
NEVER_MEASURES = frozenset({"cancelled", "startup_failure"})


class GateError(Exception):
    """Input the gate cannot decide on. The workflow measures on any of these."""


def run_number_of(run: dict) -> int:
    """The run's per-workflow run number, refusing anything but an integer."""
    number = run.get("run_number")
    # bool is an int subclass, and `true` is a malformed listing, not run 1.
    if not isinstance(number, int) or isinstance(number, bool):
        raise GateError(
            f"run {run.get('id')!r} carries a non-integer run_number {number!r}")
    return number


def describe(run: dict) -> str:
    """A one-line identification of a run for the log."""
    state = run.get("conclusion") or run.get("status") or "unknown state"
    sha = str(run.get("head_sha") or "")[:9] or "an unknown commit"
    return f"#{run_number_of(run)} ({run.get('event') or 'unknown event'} of {sha}, {state})"


def decide(payload: object, run_id: int) -> tuple[str, str]:
    """Return (verdict, reason) for the run whose id is `run_id`."""
    if not isinstance(payload, dict):
        raise GateError("the listing is not a JSON object")

    runs = payload.get("workflow_runs")
    if not isinstance(runs, list):
        raise GateError("the listing has no workflow_runs array")

    for run in runs:
        if not isinstance(run, dict):
            raise GateError("the listing holds an entry that is not an object")

    own = [run for run in runs if run.get("id") == run_id]
    if len(own) != 1:
        raise GateError(
            f"run {run_id} appears {len(own)} time(s) in the listing, so the "
            "listing is not about this run and its run numbers say nothing "
            "about it")

    number = run_number_of(own[0])
    branch = own[0].get("head_branch")
    if not isinstance(branch, str) or not branch:
        raise GateError(
            f"run {run_id} carries no head_branch, so there is no branch to "
            "compare the other runs against")

    newer = []
    for run in runs:
        if run.get("head_branch") != branch:
            continue
        if run_number_of(run) <= number:
            continue
        if run.get("conclusion") in NEVER_MEASURES:
            continue
        newer.append(run)

    if not newer:
        return MEASURE, (
            f"run #{number} is the newest run of this workflow on {branch} that "
            "has measured or will measure, so it measures")

    nearest = min(newer, key=run_number_of)
    return SKIP, (
        f"run {describe(nearest)} is newer on {branch} and measures a later "
        f"commit, so run #{number} skips rather than spend a lane run on a "
        "commit that is already superseded")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Decide whether this coverage run measures or defers to a newer run.")
    parser.add_argument("--run-id", required=True, type=int,
                        help="this run's id (GITHUB_RUN_ID)")
    parser.add_argument("runs", help="the workflow-runs listing, or - for stdin")
    args = parser.parse_args(argv)

    try:
        if args.runs == "-":
            text = sys.stdin.read()
        else:
            with open(args.runs, encoding="utf-8") as handle:
                text = handle.read()
        verdict, reason = decide(json.loads(text), args.run_id)
    except (OSError, ValueError, GateError) as error:
        # json.JSONDecodeError is a ValueError.
        print(f"coverage-supersession: cannot decide: {error}", file=sys.stderr)
        return 2

    print(verdict)
    if verdict == SKIP:
        print(f"::notice title=Coverage deferred to a newer run::{reason}", file=sys.stderr)
    else:
        print(reason, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
