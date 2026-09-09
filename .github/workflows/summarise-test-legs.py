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

Usage:
  summarise-test-legs.py --results-dir DIR [--summary-file FILE]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
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


def render(handle, payloads: list[dict], expect_legs: int) -> bool:
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

    # -- Vacuity: a shard that ran nothing anywhere ---------------------------
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

    return ok


def main() -> int:
    args = parse_args()
    payloads = load(args.results_dir)

    if not payloads:
        print("::error::No leg results were collected; refusing to report a green run.")
        return 1

    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            ok = render(handle, payloads, args.expect_legs)
    else:
        import sys

        ok = render(sys.stdout, payloads, args.expect_legs)

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
