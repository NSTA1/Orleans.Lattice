#!/usr/bin/env python3
"""Plan the CI test matrix: turn a selected package list into balanced legs.

WHY THIS EXISTS
---------------
`build-and-test` in ci.yml runs every selected package's tests serially in one
job on one two-core runner. On a change to `src/lattice` the dependency closure
(issue #2330) selects all 46 packages, and the serial run takes about 36 minutes
of test time. Almost none of that is the fan-out: 37 of the 46 packages finish in
about 0.1 minutes each, and half the total is `test/lattice` alone. The cost is
that nothing runs concurrently.

This planner turns the selection into N legs that run on N runners. The makespan
is then the longest single leg rather than the sum of everything, so the chaos
tier stops costing wall-clock at all: it runs beside the deterministic tier
instead of queueing behind it.

WORK ITEMS AND BIN PACKING
--------------------------
A work item is one `dotnet test` invocation: a (package, shard, tier) triple.
Packages listed in test-shards.json are split into shards; everything else runs
whole. Items are packed into legs with LPT (longest processing time first): sort
by estimate descending, then repeatedly put the next item in the least loaded
leg. LPT is within 4/3 of optimal for this problem, which is far inside the
error bar on the estimates themselves, so nothing fancier is warranted.

The makespan cannot go below the largest single item, so the leg count is capped
at the point where extra legs stop helping. Adding legs past that buys nothing
and costs one runner's fixed overhead (checkout, SDK setup, restore, build) per
leg.

ATTRIBUTION
-----------
Attribution is the reason the leg NAMES are derived from the dominant item
rather than being `leg-1 .. leg-N`. A leg called
`lattice / bplustree-chaos (chaos)` says what failed from the checks list alone.
run-test-leg.py additionally emits a GitHub error annotation per failing item and
records per-item results, so the aggregate report names the exact package, shard
and tier without anyone opening a log.

Usage:
  plan-test-matrix.py --packages FILE --shards FILE --durations FILE
                      [--seeded FILE] [--max-legs N]
                      --output-matrix FILE [--report-file FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Tier filters, matching the three test steps in ci.yml. The tuple order is the
# order a leg runs its items in, cheapest tier first.
TIERS: list[tuple[str, str]] = [
    ("deterministic", "TestCategory!=Chaos&TestCategory!=Coyote"),
    ("coyote", "TestCategory=Coyote"),
    ("chaos", "TestCategory=Chaos&TestCategory!=AzureStorageEmulator"),
]

# The filter for a package that runs in ONE invocation: none at all.
#
# ci.yml splits every package into three `dotnet test` runs because the job is
# serial and it wanted the fault-injection tier isolated at the end. In a matrix
# that split only pays for itself where the two halves are large enough to fill
# separate runners: `test/lattice` really does gain from running its 9-minute
# deterministic tier beside its 9-minute chaos tier. For the 37 packages that
# finish in about 0.1 minutes, splitting into three processes costs three
# process starts to save nothing, and process starts are the dominant cost at
# that size. So a package is split by tier only when it is sharded, and every
# other package runs once, unfiltered.
#
# Running unfiltered is EQUAL to what ci.yml's three tiers cover, not a
# superset, and this was measured rather than reasoned about. Enumerating
# test/lattice gives 10,525 tests in total; the deterministic, Coyote and chaos
# filters return 10,393 + 97 + 35, which is 10,525 with no test claimed by two
# tiers and none claimed by none. The three tiers already partition the suite.
#
# Do not "tighten" this into a filter that excludes the emulator-backed chaos
# tests. VSTest evaluates `TestCategory!=X` per category and existentially, so
# a test carrying both Chaos and AzureStorageEmulator satisfies
# `TestCategory!=Chaos` through its OTHER category and is already picked up by
# ci.yml's deterministic tier. Any De Morgan expression built on the intuitive
# reading of `!=` reduces to a tautology and matches everything anyway - it
# would just look like it was doing something.
UNSHARDED_FILTER: str | None = None

# Fallback estimates for any work item absent from the durations file. Small on
# purpose: the unlisted packages are the near-instant long tail, and a tier that
# a package has no tests for costs only the process start.
DEFAULT_ESTIMATE = {"deterministic": 0.15, "coyote": 0.02, "chaos": 0.05, "all": 0.2}

# Fixed cost of one `dotnet test` invocation - process start, assembly load,
# test discovery - paid even when the filter matches nothing. Added to every
# item so the bin packing sees the true cost of a leg rather than only its test
# time, which otherwise makes a leg of twenty trivial items look free.
PER_ITEM_OVERHEAD = 0.12


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packages", required=True)
    parser.add_argument("--shards", required=True)
    parser.add_argument("--durations", required=True)
    parser.add_argument("--seeded", help="Packages seeded directly by the diff.")
    parser.add_argument("--max-legs", type=int, default=10)
    parser.add_argument("--output-matrix", required=True)
    parser.add_argument("--report-file")
    return parser.parse_args()


def read_lines(path: str) -> list[str]:
    with open(path, encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def load_durations(path: str) -> dict[tuple[str, str, str], float]:
    table: dict[tuple[str, str, str], float] = {}
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 4:
                raise SystemExit(f"test-durations.tsv: expected 4 tab-separated fields: {line!r}")
            package, shard, tier, minutes = parts
            table[(package, shard, tier)] = float(minutes)
    return table


def build_shard_filters(config: dict) -> list[dict]:
    """Turn an ordered shard list into a partition of FullyQualifiedName filters.

    Shard i covers OR(its includes) AND NOT(includes of every EARLIER shard), so
    the first shard whose prefix matches a test owns it. The trailing complement
    shard covers everything no shard claimed. Together these two rules make the
    result a partition: no test is claimed twice, and none can escape.
    """
    root = config["namespaceRoot"]
    shards = config["shards"]
    if not shards:
        raise SystemExit("a sharded package must declare at least one shard")
    if not shards[-1].get("complement"):
        raise SystemExit(
            "the last shard must set \"complement\": true so a new test namespace "
            "cannot fall outside the partition and go untested"
        )

    result: list[dict] = []
    claimed: list[str] = []
    for entry in shards:
        name = entry["shard"]
        includes = [f"{root}.{prefix}" for prefix in entry.get("include", [])]

        if entry.get("complement"):
            if includes:
                raise SystemExit(f"shard {name!r} is the complement and must declare no includes")
            if not claimed:
                raise SystemExit(f"shard {name!r} is a complement of nothing")
            terms = [f"FullyQualifiedName!~{prefix}" for prefix in claimed]
            expression = "&".join(terms)
        else:
            if not includes:
                raise SystemExit(f"shard {name!r} declares no include prefixes")
            positive = "|".join(f"FullyQualifiedName~{prefix}" for prefix in includes)
            terms = [f"({positive})" if len(includes) > 1 else positive]
            # Subtract only the prefixes an earlier shard already claimed. A
            # shard that claims nothing new would silently run zero tests, so
            # say so now rather than letting it read as a passing empty leg.
            narrowing = [p for p in claimed if any(p.startswith(inc) or inc.startswith(p) for inc in includes)]
            if len(narrowing) == len(includes) and all(p in includes for p in narrowing):
                raise SystemExit(f"shard {name!r} is fully claimed by an earlier shard")
            terms += [f"FullyQualifiedName!~{prefix}" for prefix in claimed]
            expression = "&".join(terms)
            claimed.extend(includes)

        result.append({"shard": name, "filter": expression})
    return result


def make_items(
    packages: list[str],
    shard_config: dict,
    durations: dict[tuple[str, str, str], float],
    seeded: set[str],
) -> list[dict]:
    items: list[dict] = []
    for package in packages:
        config = shard_config.get(package)
        if config is None:
            # Unsharded: one unfiltered invocation covering all three tiers.
            # See the UNSHARDED_FILTER comment for the measurement showing this
            # is equal to, and not a superset of, what ci.yml runs.
            estimate = durations.get((package, "-", "all"), DEFAULT_ESTIMATE["all"])
            items.append(
                {
                    "package": package,
                    "shard": "-",
                    "tier": "all",
                    "filter": UNSHARDED_FILTER,
                    "estimate": round(estimate + PER_ITEM_OVERHEAD, 3),
                    "label": package,
                    "seeded": package in seeded,
                }
            )
            continue

        for shard in build_shard_filters(config):
            for tier, tier_filter in TIERS:
                combined = f"({shard['filter']})&({tier_filter})"
                estimate = durations.get(
                    (package, shard["shard"], tier), DEFAULT_ESTIMATE[tier]
                )
                items.append(
                    {
                        "package": package,
                        "shard": shard["shard"],
                        "tier": tier,
                        "filter": combined,
                        "estimate": round(estimate + PER_ITEM_OVERHEAD, 3),
                        "label": f"{package} / {shard['shard']} ({tier})",
                        "seeded": package in seeded,
                    }
                )
    return items


def pack(items: list[dict], max_legs: int) -> list[dict]:
    """Longest-processing-time-first bin packing into at most max_legs legs."""
    ordered = sorted(items, key=lambda item: (-item["estimate"], item["label"]))

    # More legs than the makespan floor allows is pure overhead: the largest
    # single item bounds the wall-clock no matter how many runners are used.
    total = sum(item["estimate"] for item in ordered)
    largest = max((item["estimate"] for item in ordered), default=0.0)
    useful = max(1, int(total / largest) + 1) if largest > 0 else 1
    leg_count = max(1, min(max_legs, useful, len(ordered)))

    legs: list[dict] = [
        {"id": f"leg-{index + 1}", "estimate": 0.0, "items": []}
        for index in range(leg_count)
    ]
    for item in ordered:
        target = min(legs, key=lambda leg: (leg["estimate"], leg["id"]))
        target["items"].append(item)
        target["estimate"] = round(target["estimate"] + item["estimate"], 3)

    for leg in legs:
        # Name the leg after its dominant item so the checks list is readable at
        # a glance. The remainder is reported in the leg's own job summary.
        dominant = leg["items"][0]["label"]
        extra = len(leg["items"]) - 1
        leg["name"] = dominant if extra == 0 else f"{dominant} +{extra}"
        # Run the heaviest item first. If a leg is going to fail, failing early
        # gets the annotation onto the run page sooner.
        leg["items"].sort(key=lambda item: (-item["estimate"], item["label"]))

    return [leg for leg in legs if leg["items"]]


def write_report(handle, legs: list[dict], items: list[dict], packages: list[str], seeded: set[str]) -> None:
    total = sum(item["estimate"] for item in items)
    makespan = max((leg["estimate"] for leg in legs), default=0.0)

    handle.write(f"### Test matrix plan: {len(legs)} legs, {len(items)} work items\n\n")
    handle.write(
        f"{len(packages)} package(s) selected, {len(seeded)} seeded directly by the diff.\n\n"
    )
    handle.write(
        f"Serial estimate **{total:.1f} min**, projected makespan **{makespan:.1f} min** "
        f"(the longest leg), before per-leg fixed overhead.\n\n"
    )
    handle.write("| Leg | Estimate | Items | Contents |\n")
    handle.write("| --- | --- | --- | --- |\n")
    for leg in legs:
        contents = ", ".join(f"`{item['label']}`" for item in leg["items"][:6])
        if len(leg["items"]) > 6:
            contents += f", and {len(leg['items']) - 6} more"
        handle.write(
            f"| `{leg['id']}` | {leg['estimate']:.1f} min | {len(leg['items'])} | {contents} |\n"
        )
    handle.write("\n")
    handle.write(
        "Estimates come from `.github/workflows/test-durations.tsv` and only decide "
        "how work is distributed. They never decide what runs.\n\n"
    )


def main() -> int:
    args = parse_args()

    packages = read_lines(args.packages)
    if not packages:
        print("::error::The planner was given an empty package selection.", file=sys.stderr)
        return 1

    seeded = set(read_lines(args.seeded)) if args.seeded and os.path.exists(args.seeded) else set()

    with open(args.shards, encoding="utf-8") as handle:
        shard_config = {
            key: value
            for key, value in json.load(handle).items()
            if not key.startswith("_")
        }

    durations = load_durations(args.durations)
    items = make_items(packages, shard_config, durations, seeded)
    legs = pack(items, args.max_legs)

    if not legs:
        print("::error::The planner produced no legs.", file=sys.stderr)
        return 1

    with open(args.output_matrix, "w", encoding="utf-8") as handle:
        json.dump(legs, handle, separators=(",", ":"))

    if args.report_file:
        with open(args.report_file, "a", encoding="utf-8") as handle:
            write_report(handle, legs, items, packages, seeded)
    else:
        write_report(sys.stderr, legs, items, packages, seeded)

    print(f"Planned {len(legs)} legs over {len(items)} work items.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
