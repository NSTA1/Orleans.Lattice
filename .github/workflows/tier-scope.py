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

Every other run keeps all test tiers. Exhaustive TLC is separately scoped:

- a pull request into `main` skips exhaustive TLC only when its diff contains
  no TLC input; its base-model smoke shard still runs;
- a pull request into a `release/**` line, the integration-branch push lane,
  and nightly coverage keep exhaustive TLC;
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

EXHAUSTIVE TLC RUNS WHEN ITS INPUTS CHANGE
-------------------------------
The `Tlc` category is also exploration-dominated - TLC model-checks every
specification module and a mutant of each definition, about 40 runner-minutes
per run - and it is deterministic: the same specification, harness and toolchain
yield the same verdict every run. A pull request into `main` or an integration
branch runs the exhaustive TLC shards (test-shards.json `"tlc": true`) exactly
when its diff touches a TLC input
(`TLC_INPUTS`: the `.tla`, `.cfg`, manifest and mutation files under spec/, the
Formal harness, the CI workflows and the build files the harness compiles
against; not the refinement notes and READMEs, which TLC never reads), and
skips them otherwise. A change that touches none of those inputs cannot alter
any TLC verdict, so the skip loses nothing - not even
attribution, because a specification change is precisely a TLC input and still
runs TLC on the pull request that wrote it. The separate `"tlcSmoke": true`
base-model shard still runs on every routine CI pull request. Release-line
pull requests, pushes, nightly coverage, and any unknown event keep the
exhaustive shards. A missing or empty changed-file list runs TLC (fail towards
"run more").

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
  tier-scope.py --event EVENT [--base-ref REF] [--changed-files FILE]
                [--github-output FILE] [--summary-file FILE]

Prints `key=value` lines (scope, skipped_tiers, max_legs, reason,
skip_tlc_reason) to stdout,
and appends them to --github-output when given.
"""

from __future__ import annotations

import argparse
import fnmatch
import sys

# The tiers a member pull request does not run, in plan-test-matrix.py's TIERS
# order. Changing this list changes what every member pull request verifies.
MEMBER_SKIPPED_TIERS: list[str] = ["coyote", "chaos"]

# Paths whose change can alter a TLC verdict. A member pull request that
# touches none of them skips the "tlc" shards; see TLC RUNS WHEN ITS INPUTS
# CHANGE. Patterns are fnmatch globs over repository-relative paths, where `*`
# also crosses `/`.
TLC_INPUTS: list[str] = [
    "spec/*",
    "test/lattice/Formal/*",
    "test/lattice/*.csproj",
    ".github/workflows/*",
    "tools/tla*",
    "Directory.Build.*",
    "Directory.Packages.props",
    "global.json",
    "NuGet.config",
    "nuget.config",
]

# Matches of TLC_INPUTS that TLC never reads: the refinement notes and READMEs
# beside the specifications. The non-TLC Formal gates that do read them run on
# every member pull request regardless.
TLC_INPUT_EXCLUSIONS: list[str] = [
    "spec/*.md",
]

FULL_MAX_LEGS = 10
MEMBER_MAX_LEGS = 6


def is_integration_branch(ref: str) -> bool:
    """`*/epic/**` in Actions' branch-filter sense: `*` stops at `/`."""
    parts = ref.split("/")
    return len(parts) >= 3 and parts[0] != "" and parts[1] == "epic" and all(parts[2:])


def touches_tlc_input(changed: list[str] | None) -> bool:
    """True unless a non-empty changed-file list touches no TLC input."""
    if not changed:
        return True
    return any(
        any(fnmatch.fnmatchcase(path, pattern) for pattern in TLC_INPUTS)
        and not any(fnmatch.fnmatchcase(path, pattern) for pattern in TLC_INPUT_EXCLUSIONS)
        for path in changed
    )


def decide(event: str, base_ref: str, changed: list[str] | None = None) -> dict[str, str]:
    """The tier scope for one run. Anything unrecognised is fully gated."""
    full = {"scope": "full", "skipped_tiers": "", "max_legs": str(FULL_MAX_LEGS), "skip_tlc_reason": ""}

    if event != "pull_request":
        full["reason"] = (
            f"a '{event or 'unknown'}' run is not a member pull request, so every tier runs"
        )
        return full

    if base_ref == "main":
        full["reason"] = (
            "a pull request into 'main' keeps every tier; exhaustive TLC runs only when a TLC input changes"
        )
        if not touches_tlc_input(changed):
            full["skip_tlc_reason"] = (
                "pull request into 'main' touches no TLC input (specifications, Formal harness, workflows, build files), "
                "so no exhaustive TLC verdict can change; the base-model smoke shard still runs"
            )
        return full

    if fnmatch.fnmatchcase(base_ref, "release/*"):
        full["reason"] = (
            f"a pull request into '{base_ref}' is fully gated, including exhaustive TLC"
        )
        return full

    if not is_integration_branch(base_ref):
        full["reason"] = (
            f"the base '{base_ref or 'unknown'}' is not an integration branch, "
            "so nothing is skipped"
        )
        return full

    skip_tlc_reason = "" if touches_tlc_input(changed) else (
        f"member pull request into integration branch '{base_ref}' that touches no TLC "
        "input (specifications, Formal harness, workflows, build files), so no exhaustive TLC "
        "verdict can change; TLC runs on that branch's own push lane and on its pull "
        "request into main; the base-model smoke shard still runs"
    )
    return {
        "scope": "member",
        "skipped_tiers": ",".join(MEMBER_SKIPPED_TIERS),
        "max_legs": str(MEMBER_MAX_LEGS),
        "skip_tlc_reason": skip_tlc_reason,
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
    parser.add_argument(
        "--changed-files",
        help="File listing the diff's changed paths, one per line. Absent, unreadable "
             "or empty means TLC is not skipped.",
    )
    parser.add_argument("--github-output")
    parser.add_argument("--summary-file")
    args = parser.parse_args()

    changed: list[str] | None = None
    if args.changed_files:
        try:
            with open(args.changed_files, encoding="utf-8") as handle:
                changed = [line.strip() for line in handle if line.strip()]
        except OSError:
            changed = None

    decision = decide(args.event.strip(), args.base_ref.strip(), changed)
    lines = [
        f"{key}={decision[key]}"
        for key in ("scope", "skipped_tiers", "max_legs", "reason", "skip_tlc_reason")
    ]

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
            if decision["skip_tlc_reason"]:
                handle.write("### Exhaustive TLC shards skipped\n\n")
                handle.write(f"Reason: {decision['skip_tlc_reason']}.\n\n")
                handle.write("The base-model smoke shard still runs.\n\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
