#!/usr/bin/env python3
"""Plan coverage with CI's shard partitions, duration estimates and LPT packer."""

import argparse
import importlib.util
import json
from pathlib import Path

WORKFLOWS = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("ci_matrix", WORKFLOWS / "plan-test-matrix.py")
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)

# Also applied to retries by coverage.yml, so even a newly introduced TLC shard
# cannot accidentally execute the external model checker in the coverage lane.
COVERAGE_FILTER = "TestCategory!=Chaos&TestCategory!=Coyote&TestCategory!=Tlc"
EXCLUDED_PROJECTS = {"microbench", "azure-throughput-silo"}
UI_PACKAGE = "lattice.explorer.uitests"


def plan(root: Path, max_legs: int = 10) -> list[dict]:
    projects = {}
    for project in sorted((root / "test").rglob("*Tests.csproj")):
        if project.relative_to(root / "test").parts[0] in EXCLUDED_PROJECTS:
            continue
        package = project.parent.name
        if package in projects:
            raise ValueError(f"Multiple test projects for {package}")
        projects[package] = project.relative_to(root).as_posix()
    if not projects:
        raise ValueError("No coverage test projects discovered")

    workflows = root / ".github" / "workflows"
    shards = json.loads((workflows / "test-shards.json").read_text(encoding="utf-8"))
    durations = ci.load_durations(str(workflows / "test-durations.tsv"))
    packages = [package for package in projects if package != UI_PACKAGE]
    items, _ = ci.make_items(
        packages, shards, durations, set(), ["coyote", "chaos"],
        "Coverage excludes concurrency and fault-injection tiers",
        "Coverage excludes exhaustive TLC", "Coverage excludes smoke TLC",
    )
    if UI_PACKAGE in projects:
        for shard in json.loads((workflows / "ui-test-shards.json").read_text(encoding="utf-8")):
            items.append({
                "package": UI_PACKAGE,
                "shard": shard["shard"],
                "tier": "deterministic",
                "filter": shard["filter"],
                "estimate": durations.get(
                    (UI_PACKAGE, shard["shard"], "all"), ci.DEFAULT_ESTIMATE["all"]
                ) + ci.PER_ITEM_OVERHEAD,
                "label": f"{UI_PACKAGE} / {shard['shard']}",
            })
    for item in items:
        item["project"] = projects[item["package"]]
        item["filter"] = f"({item['filter']})&({COVERAGE_FILTER})"
    legs = ci.pack(items, max_legs)
    if not legs:
        raise ValueError("No runnable coverage items")
    for leg in legs:
        leg["ui"] = any(item["package"] == UI_PACKAGE for item in leg["items"])
    return legs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=WORKFLOWS.parent.parent)
    parser.add_argument("--max-legs", type=int, default=10)
    parser.add_argument("--output-matrix", type=Path, required=True)
    args = parser.parse_args()
    if args.max_legs < 1:
        parser.error("--max-legs must be positive")
    legs = plan(args.root, args.max_legs)
    args.output_matrix.write_text(json.dumps(legs, separators=(",", ":")), encoding="utf-8")
    print(f"Planned {len(legs)} coverage legs using CI shard definitions and estimates.")


if __name__ == "__main__":
    main()
