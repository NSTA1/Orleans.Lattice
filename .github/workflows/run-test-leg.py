#!/usr/bin/env python3
"""Run one leg of the test matrix and report per-item results.

A leg is a list of work items produced by plan-test-matrix.py, each of which is
one `dotnet test` invocation for a (package, shard, tier) triple. This script
runs them in order and records, per item, the outcome, the wall-clock duration,
the executed test count and the names of any failing tests.

ATTRIBUTION IS THE POINT
------------------------
Three separate mechanisms name the offending package, shard and tier, because a
failure in a 46-package fan-out is only useful if you can tell what broke
without reading a 500 KB log:

1. A GitHub ERROR ANNOTATION per failing item, titled with the item label. These
   surface at the top of the run page, above the job list.
2. A per-item RESULT RECORD written to leg-results/, collected by the aggregate
   report job into one table covering every leg.
3. A per-leg JOB SUMMARY table, so a leg is self-describing even in isolation.

The executed-test COUNT is recorded for every item including the passing ones,
and that is deliberate. A shard whose filter is wrong does not fail: it matches
nothing, runs nothing, and reports green. Recording counts is what makes that
visible, and the aggregate report flags any shard that executed zero tests
across every tier.

A tier a package has no tests for legitimately runs zero tests (most packages
have no Coyote models), so a zero count is reported rather than treated as an
error here. The judgement belongs in the aggregate view, which can see all the
tiers for a package at once.

Usage:
  run-test-leg.py --leg FILE --results-dir DIR [--summary-file FILE]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import time
import xml.etree.ElementTree as ElementTree

TRX_NAMESPACE = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}

# Matches the ci.yml test steps. The hang timeout catches a deadlocked Orleans
# cluster with a dump instead of burning the job's whole time budget.
BLAME_ARGS = [
    "--blame-hang",
    "--blame-hang-timeout",
    "10m",
    "--blame-hang-dump-type",
    "full",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leg", required=True)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--summary-file")
    return parser.parse_args()


def find_test_project(package: str) -> str | None:
    matches = sorted(glob.glob(os.path.join("test", package, "*.Tests.csproj")))
    return matches[0] if matches else None


def slug(text: str) -> str:
    return "".join(char if char.isalnum() or char in "-_." else "-" for char in text)


def describe(item: dict) -> str:
    """Human-readable identity of a work item, for annotations and logs."""
    parts = [item["package"]]
    if item["shard"] != "-":
        parts.append(f"shard {item['shard']}")
    if item["tier"] != "all":
        parts.append(f"{item['tier']} tier")
    return " / ".join(parts)


def parse_trx(path: str) -> tuple[int, list[str]]:
    """Return (executed count, failing test descriptions) from a trx file.

    A failing test is described as "Name [Category, Category]" when it carries
    categories. That suffix is what preserves tier attribution for an unsharded
    package, which runs all three tiers in one invocation and so cannot carry
    the tier in its item label: the categories on the failing test say whether
    the fault was in the deterministic surface or under fault injection.
    """
    if not os.path.exists(path):
        return 0, []
    try:
        root = ElementTree.parse(path).getroot()
    except ElementTree.ParseError:
        return 0, []

    executed = 0
    counters = root.find("t:ResultSummary/t:Counters", TRX_NAMESPACE)
    if counters is not None:
        executed = int(counters.get("executed") or counters.get("total") or 0)

    # Categories live on the test DEFINITION, keyed by id, not on the result.
    categories: dict[str, list[str]] = {}
    for definition in root.findall("t:TestDefinitions/t:UnitTest", TRX_NAMESPACE):
        test_id = definition.get("id")
        if not test_id:
            continue
        names = [
            entry.get("TestCategory", "")
            for entry in definition.findall("t:TestCategory/t:TestCategoryItem", TRX_NAMESPACE)
        ]
        names = [name for name in names if name]
        if names:
            categories[test_id] = sorted(names)

    failures = []
    for result in root.findall("t:Results/t:UnitTestResult", TRX_NAMESPACE):
        if result.get("outcome") != "Failed":
            continue
        name = result.get("testName", "<unnamed>")
        tags = categories.get(result.get("testId", ""), [])
        failures.append(f"{name} [{', '.join(tags)}]" if tags else name)
    return executed, failures


def run_item(item: dict, results_dir: str) -> dict:
    label = item["label"]
    project = find_test_project(item["package"])

    record = {
        "label": label,
        "package": item["package"],
        "shard": item["shard"],
        "tier": item["tier"],
        "seeded": item.get("seeded", False),
        "estimate": item.get("estimate", 0.0),
        "leg": os.environ.get("LEG_ID", "?"),
        "job_url": os.environ.get("JOB_URL", ""),
    }

    if project is None:
        # A selected package with no test project is normal (a src-only package).
        record.update(
            {"outcome": "skipped", "reason": "no test project", "duration": 0.0,
             "executed": 0, "failures": []}
        )
        print(f"::notice::No test project under test/{item['package']}/ - skipping {label}.")
        return record

    trx_name = f"{slug(label)}.trx"
    command = [
        "dotnet", "test", project,
        "--no-build",
        "--configuration", "Release",
    ]
    # A null filter means "run everything in this project", which is what an
    # unsharded package gets. Passing --filter with an empty value would select
    # nothing, so the argument has to be omitted rather than blanked.
    if item["filter"]:
        command += ["--filter", item["filter"]]
    command += [
        *BLAME_ARGS,
        "--logger", f"trx;LogFileName={trx_name}",
    ]

    print(f"::group::{label}")
    print(f"filter: {item['filter'] or '(none - whole project)'}")
    started = time.monotonic()
    completed = subprocess.run(command, check=False)
    duration = time.monotonic() - started
    print("::endgroup::")

    trx_candidates = sorted(glob.glob(os.path.join("**", "TestResults", trx_name), recursive=True))
    executed, failures = parse_trx(trx_candidates[0]) if trx_candidates else (0, [])

    # `dotnet test` reports a non-zero exit for a genuine failure. A filter that
    # matches nothing is not an error - most packages have no Coyote models -
    # so an empty run with a clean exit is recorded as "empty", not as a pass,
    # to keep it distinguishable in the aggregate view.
    if completed.returncode != 0:
        outcome = "failed"
    elif executed == 0:
        outcome = "empty"
    else:
        outcome = "passed"

    record.update(
        {
            "outcome": outcome,
            "duration": round(duration / 60.0, 2),
            "executed": executed,
            "failures": failures[:25],
            "failure_count": len(failures),
        }
    )

    if outcome == "failed":
        # The annotation title is what appears at the top of the run page. It
        # carries the package, the shard and the tier, which is the whole
        # question a reader has when a 46-package fan-out goes red.
        detail = "; ".join(failures[:5]) if failures else "see the job log"
        if len(failures) > 5:
            detail += f"; and {len(failures) - 5} more"
        print(
            f"::error title={label}::{len(failures) or 'unknown'} test(s) failed in "
            f"{describe(item)}: {detail}"
        )

    return record


def write_summary(path: str, leg: dict, records: list[dict]) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"### Leg `{leg['id']}`: {leg['name']}\n\n")
        handle.write("| Item | Outcome | Tests | Duration | Estimate |\n")
        handle.write("| --- | --- | --- | --- | --- |\n")
        for record in records:
            marks = {"passed": "pass", "failed": "FAIL", "empty": "empty", "skipped": "skipped"}
            handle.write(
                f"| `{record['label']}` | {marks.get(record['outcome'], record['outcome'])} "
                f"| {record['executed']} | {record['duration']:.2f} min "
                f"| {record['estimate']:.2f} min |\n"
            )
        handle.write("\n")
        actual = sum(record["duration"] for record in records)
        handle.write(f"Leg total **{actual:.1f} min** against an estimate of {leg['estimate']:.1f} min.\n\n")


def main() -> int:
    args = parse_args()

    with open(args.leg, encoding="utf-8") as handle:
        leg = json.load(handle)

    os.makedirs(args.results_dir, exist_ok=True)

    records = [run_item(item, args.results_dir) for item in leg["items"]]

    payload = {"leg": {"id": leg["id"], "name": leg["name"], "estimate": leg["estimate"]}, "items": records}
    with open(os.path.join(args.results_dir, f"{slug(leg['id'])}.json"), "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)

    if args.summary_file:
        write_summary(args.summary_file, leg, records)

    failed = [record for record in records if record["outcome"] == "failed"]
    if failed:
        print(f"::error::Leg {leg['id']} failed: " + ", ".join(record["label"] for record in failed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
