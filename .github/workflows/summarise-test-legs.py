#!/usr/bin/env python3
"""Aggregate every test leg into one report, and decide the run's verdict.

The matrix gives concurrency; this gives back the single place to look that the
serial job used to provide. It reads the per-item records each leg wrote and
produces:

1. A FAILURES table first, naming package, shard, tier and the failing test
   names. If anything is red this is the only thing most readers need.
2. A VACUITY check. A shard whose filter matches nothing does not fail, it
   reports green having run no tests, which is the exact failure mode the
   dependency closure was introduced to remove (issue #2330 / #2329) reappearing
   one level down. A shard that executed zero tests across every tier it was
   given is therefore an error, not a curiosity. A single tier running zero
   tests is normal - most packages have no Coyote models - so the check sums
   over a shard's tiers rather than judging one item at a time. A package with
   no test project at all is skipped, not counted, so it is not flagged.
3. The measured DURATIONS, formatted as replacement rows for
   test-durations.tsv, so the estimates converge on reality instead of being
   re-guessed.
4. The CONCURRENCY RESULT: serial cost against actual makespan, which is the
   number the whole exercise exists to move.

Artifacts are scoped to the run rather than the run attempt, so a re-run leaves
the earlier attempt's results in place beside the new ones. Before any of the
above is computed the payloads are reduced to one per leg, keeping the highest
attempt, so a leg that was red and has been re-run green reports green. Two
results for one leg under the same attempt cannot arise from a correctly wired
collection and are reported as an error rather than resolved arbitrarily.

Usage:
  summarise-test-legs.py --results-dir DIR [--summary-file FILE]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import defaultdict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--summary-file")
    parser.add_argument(
        "--expect-legs",
        type=int,
        default=0,
        help="Number of legs planned. A missing leg result is an error, not an "
             "absence: a leg that died before writing its record would otherwise "
             "vanish from the report and read as nothing having gone wrong.",
    )
    return parser.parse_args()


def load(results_dir: str) -> list[dict]:
    payloads = []
    for path in sorted(glob.glob(os.path.join(results_dir, "**", "*.json"), recursive=True)):
        with open(path, encoding="utf-8") as handle:
            payloads.append(json.load(handle))
    return payloads


def attempt_of(payload: dict) -> int:
    """The run attempt a leg result was produced under; 0 when it predates the field."""
    try:
        return int(payload.get("leg", {}).get("attempt", 0))
    except (TypeError, ValueError):
        return 0


def select_latest(payloads: list[dict]) -> tuple[list[dict], list[tuple], list[tuple]]:
    """Keep one result per leg - the one from the highest run attempt.

    Artifacts are scoped to the run rather than the attempt, so re-running a
    failed leg leaves the earlier artifact in place and the run legitimately
    carries results from more than one attempt.

    Selecting per leg is what makes a re-run usable, and the reason the obvious
    alternative does not work is worth stating: narrowing the collection to the
    CURRENT attempt would discard every leg that passed first time, because
    "re-run failed jobs" does not re-run the passing legs and they therefore
    upload nothing under the new attempt. That turns a recoverable run into one
    that can never report, which is a worse failure than the one being fixed.

    Two results for one leg under the SAME attempt are a different matter. That
    cannot happen while each leg uploads once per attempt under an
    attempt-scoped name, so it means the collection plumbing has regressed, and
    it is returned as a conflict rather than silently resolved - which of the
    two survived extraction is not determined, so neither can be trusted.
    """
    best: dict[str, dict] = {}
    superseded: list[tuple[str, int, int]] = []
    conflicts: list[tuple[str, int]] = []

    for payload in payloads:
        leg_id = payload["leg"]["id"]
        attempt = attempt_of(payload)
        if leg_id not in best:
            best[leg_id] = payload
            continue
        incumbent = attempt_of(best[leg_id])
        if attempt > incumbent:
            best[leg_id] = payload
            superseded.append((leg_id, incumbent, attempt))
        elif attempt < incumbent:
            superseded.append((leg_id, attempt, incumbent))
        else:
            conflicts.append((leg_id, attempt))

    return [best[key] for key in sorted(best)], superseded, conflicts


def render(handle, payloads: list[dict], expect_legs: int) -> tuple[bool, dict]:
    payloads, superseded, conflicts = select_latest(payloads)
    items = [item for payload in payloads for item in payload["items"]]
    legs = [payload["leg"] for payload in payloads]

    ok = True

    failures = [item for item in items if item["outcome"] == "failed"]
    handle.write("## Test matrix result\n\n")

    if failures:
        ok = False
        handle.write(f"### {len(failures)} failing item(s)\n\n")
        handle.write("| Package | Shard | Tier | Seeded by this diff | Failing tests |\n")
        handle.write("| --- | --- | --- | --- | --- |\n")
        for item in failures:
            names = "<br>".join(f"`{name}`" for name in item["failures"][:10]) or "see log"
            if item.get("failure_count", 0) > 10:
                names += f"<br>and {item['failure_count'] - 10} more"
            seeded = "yes" if item["seeded"] else "no (dependency closure)"
            handle.write(
                f"| `{item['package']}` | `{item['shard']}` | {item['tier']} | {seeded} | {names} |\n"
            )
        handle.write("\n")
        handle.write(
            "A failure in a package marked *no (dependency closure)* was not edited by this "
            "pull request. It is reachable from an edited package through a ProjectReference, "
            "so the change may have broken a consumer - or the test was already red on the base.\n\n"
        )
    else:
        handle.write("All items passed.\n\n")

    if expect_legs and len(legs) != expect_legs:
        ok = False
        handle.write(
            f"**{expect_legs - len(legs)} leg(s) reported no result.** A leg that fails before "
            "writing its record is invisible here, so this is treated as a failure rather than "
            "silently reducing the denominator.\n\n"
        )

    if conflicts:
        ok = False
        handle.write("### Colliding leg results\n\n")
        handle.write(
            "Two results for the same leg carry the same run attempt. One overwrote the other "
            "when the artifacts were extracted and which one survived is not determined, so "
            "neither can be trusted. The attempt is part of each artifact name specifically to "
            "prevent this, so either that naming or the download step's `merge-multiple` "
            "setting has regressed.\n\n"
        )
        for leg_id, attempt in sorted(conflicts):
            handle.write(f"- `{leg_id}` (attempt {attempt})\n")
        handle.write("\n")

    if superseded:
        handle.write("### Superseded leg results\n\n")
        handle.write(
            "These legs were re-run. The later attempt is the result reported above; the "
            "earlier one is ignored rather than merged, so a leg that was red and is now green "
            "reports green.\n\n"
        )
        for leg_id, older, newer in sorted(superseded):
            handle.write(f"- `{leg_id}`: using attempt {newer}, ignoring attempt {older}\n")
        handle.write("\n")

    # -- Vacuity: a shard that ran nothing anywhere ---------------------------
    #
    # This is also the non-zero executed-test assertion that the
    # emulator-backed packages depend on (#3329). Fixtures categorised
    # AzureStorageEmulator call Assert.Inconclusive when Azurite is
    # unreachable, and NUnit counts an inconclusive as neither passed, failed,
    # nor skipped: the leg still prints `Passed!` with `Skipped: 0`, and only
    # the Total drops. Counting outcomes therefore cannot tell a suite that
    # ran from one that self-skipped wholesale. Summing TRX
    # ResultSummary/Counters@executed can, and does - a wholesale inconclusive
    # run sums to zero and fails here. Do not relax this to "no failures".
    per_shard: dict[tuple[str, str], int] = defaultdict(int)
    for item in items:
        if item["outcome"] in ("passed", "empty", "failed"):
            per_shard[(item["package"], item["shard"])] += item["executed"]

    silent = [key for key, executed in sorted(per_shard.items()) if executed == 0]
    if silent:
        ok = False
        handle.write("### Shards that executed no tests in any tier\n\n")
        handle.write(
            "A filter that matches nothing reports green. These shards ran zero tests across "
            "every tier, so either their `test-shards.json` prefixes are wrong or the tests "
            "they were meant to cover are gone. Either way nothing was verified.\n\n"
        )
        for package, shard in silent:
            handle.write(f"- `{package}` / `{shard}`\n")
        handle.write("\n")

    # -- Concurrency result ---------------------------------------------------
    serial = sum(item["duration"] for item in items)
    per_leg: dict[str, float] = defaultdict(float)
    for payload in payloads:
        per_leg[payload["leg"]["id"]] = sum(item["duration"] for item in payload["items"])
    makespan = max(per_leg.values(), default=0.0)

    handle.write("### Concurrency\n\n")
    handle.write(f"- Serial test time across all legs: **{serial:.1f} min**\n")
    handle.write(f"- Actual makespan (longest leg): **{makespan:.1f} min**\n")
    if makespan > 0:
        handle.write(f"- Speed-up on test time alone: **{serial / makespan:.1f}x**\n")
    handle.write(
        "\nMakespan excludes each leg's fixed overhead (checkout, SDK setup, restore, build), "
        "which the job timings on the run page include.\n\n"
    )

    handle.write("| Leg | Name | Estimated | Actual |\n")
    handle.write("| --- | --- | --- | --- |\n")
    for payload in sorted(payloads, key=lambda p: p["leg"]["id"]):
        leg = payload["leg"]
        handle.write(
            f"| `{leg['id']}` | {leg['name']} | {leg['estimate']:.1f} min "
            f"| {per_leg[leg['id']]:.1f} min |\n"
        )
    handle.write("\n")

    # -- Measured durations, ready to paste back ------------------------------
    measured = [
        item for item in items
        if item["outcome"] in ("passed", "failed") and item["duration"] >= 0.1
    ]
    if measured:
        handle.write("<details><summary>Measured durations for test-durations.tsv</summary>\n\n")
        handle.write("```\n")
        for item in sorted(measured, key=lambda i: -i["duration"]):
            handle.write(
                f"{item['package']}\t{item['shard']}\t{item['tier']}\t{item['duration']:.1f}\n"
            )
        handle.write("```\n\n</details>\n\n")

    stats = {
        "legs": len(legs),
        "items": len(items),
        "executed": sum(item["executed"] for item in items),
        "failing": len(failures),
        "superseded": len(superseded),
        "conflicts": len(conflicts),
        "silent": len(silent),
    }
    return ok, stats


def main() -> int:
    args = parse_args()
    payloads = load(args.results_dir)

    if not payloads:
        print("::error::No leg results were collected; refusing to report a green run.")
        return 1

    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            ok, stats = render(handle, payloads, args.expect_legs)
    else:
        ok, stats = render(sys.stdout, payloads, args.expect_legs)

    # The verdict goes to stdout as well as the step summary. The summary is a
    # separate artefact that a reader has to navigate to; the log is what is
    # open when a job goes red, and a required check whose reasoning is only
    # legible somewhere else is a check people learn to re-run rather than read.
    print(
        f"summarise-test-legs: {'PASS' if ok else 'FAIL'} - "
        f"{stats['legs']} leg(s), {stats['items']} item(s), "
        f"{stats['executed']} test(s) executed, {stats['failing']} failing item(s), "
        f"{stats['silent']} silent shard(s), "
        f"{stats['superseded']} superseded result(s), {stats['conflicts']} colliding result(s)."
    )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
