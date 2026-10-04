#!/usr/bin/env python3
"""Decide which test tiers a ci.yml run executes, and say why.

THE RULE
--------
A pull request whose BASE is an integration branch (`<type>/epic/<slug>`, an
epic or a bucket) is a MEMBER pull request. It runs the deterministic tier and
skips the two tiers whose cost is dominated by exploration rather than by the
behaviour under test:

- `coyote` - systematic schedule exploration of the extracted concurrency
  cores (`[Category("Coyote")]`). Its cost is the number of schedules
  explored, not the code changed.
- `chaos` - fault-injection and stress runs (`[Category("Chaos")]`), which
  spin up multi-silo clusters, kill and restart them, and are the slowest and
  most flake-prone tier per test.

Every other run is FULLY GATED and skips nothing:

- a pull request into `main` (including an integration branch's own pull
  request into `main`) or into a `release/**` line;
- the integration-branch push lane, which evaluates the bucket's combined
  state after each member merge;
- any event or base this script does not recognise. The failure direction is
  deliberately "run more", never "run less".

WHY THE SKIPPED TIERS STILL RUN SOMEWHERE
-----------------------------------------
Every commit a member pull request brings reaches `main` only through its
integration branch, and the integration branch is evaluated with every tier
twice over: on its own push lane after each member merge, and by its pull
request into `main`, which is the required check. A Coyote or chaos regression
introduced by a member therefore still blocks the merge to `main`; what it loses
is per-member attribution, because it surfaces on the bucket with every other
member's changes beside it. That is the accepted cost of this rule.

TLC IS KEPT, DELIBERATELY
-------------------------
The `Tlc` category is also exploration-dominated - TLC model-checks every
specification module and a mutant of each definition - but it is NOT skipped. It
rides the deterministic tier, it is deterministic (the same state space yields
the same verdict every run, so there is nothing for a re-run on the bucket to
discover that the member run would not), and a specification change is the
very change it judges: skipping it would move a spec defect off the pull
request that wrote it, which is the attribution this rule otherwise preserves.

LEG CAP
-------
A member run is also planned onto at most `MEMBER_MAX_LEGS` legs rather than
the full run's `FULL_MAX_LEGS`. Every leg pays a fixed overhead (checkout, SDK,
restore, a whole-solution build, the TLA+ toolchain) of a few minutes, and a
concurrent-job cap is spent per job, so a bucket with many open members queues
on job count rather than on test time. Fewer legs per member trades a few
minutes of one member's makespan for runner slots the other members are
waiting on.

Usage:
  tier-scope.py --event EVENT [--base-ref REF]
                [--github-output FILE] [--summary-file FILE]

Prints `key=value` lines (scope, skipped_tiers, max_legs, reason) to stdout,
and appends them to --github-output when given.
"""

from __future__ import annotations

import argparse
import fnmatch
import sys

# The tiers a member pull request does not run, in plan-test-matrix.py's TIERS
# order. Changing this list changes what every member pull request verifies.
MEMBER_SKIPPED_TIERS: list[str] = ["coyote", "chaos"]

FULL_MAX_LEGS = 10
MEMBER_MAX_LEGS = 6


def is_integration_branch(ref: str) -> bool:
    """`*/epic/**` in Actions' branch-filter sense: `*` stops at `/`."""
    parts = ref.split("/")
    return len(parts) >= 3 and parts[0] != "" and parts[1] == "epic" and all(parts[2:])


def is_fully_gated_base(ref: str) -> bool:
    return ref == "main" or fnmatch.fnmatchcase(ref, "release/*")


def decide(event: str, base_ref: str) -> dict[str, str]:
    """The tier scope for one run. Anything unrecognised is fully gated."""
    full = {"scope": "full", "skipped_tiers": "", "max_legs": str(FULL_MAX_LEGS)}

    if event != "pull_request":
        full["reason"] = (
            f"a '{event or 'unknown'}' run is not a member pull request, so every tier runs"
        )
        return full

    if is_fully_gated_base(base_ref):
        full["reason"] = (
            f"a pull request into '{base_ref}' is fully gated, so every tier runs"
        )
        return full

    if not is_integration_branch(base_ref):
        full["reason"] = (
            f"the base '{base_ref or 'unknown'}' is not an integration branch, "
            "so nothing is skipped"
        )
        return full

    return {
        "scope": "member",
        "skipped_tiers": ",".join(MEMBER_SKIPPED_TIERS),
        "max_legs": str(MEMBER_MAX_LEGS),
        "reason": (
            f"member pull request into integration branch '{base_ref}': the "
            f"{' and '.join(MEMBER_SKIPPED_TIERS)} tiers are skipped here and run on that "
            "branch's own push lane and on its pull request into main"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True)
    parser.add_argument("--base-ref", default="")
    parser.add_argument("--github-output")
    parser.add_argument("--summary-file")
    args = parser.parse_args()

    decision = decide(args.event.strip(), args.base_ref.strip())
    lines = [f"{key}={decision[key]}" for key in ("scope", "skipped_tiers", "max_legs", "reason")]

    for line in lines:
        print(line)

    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")

    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            if decision["skipped_tiers"]:
                handle.write(f"### Test tiers skipped: {decision['skipped_tiers'].replace(',', ', ')}\n\n")
                handle.write(f"Reason: {decision['reason']}.\n\n")
                handle.write(
                    "These tiers were NOT executed on this run. A green check here does not "
                    "assert anything about them. They are executed on the integration branch's "
                    "push lane after this pull request merges, and by that branch's pull "
                    "request into `main`, which skips nothing.\n\n"
                )
            else:
                handle.write(f"### Test tiers: all ({decision['reason']})\n\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
