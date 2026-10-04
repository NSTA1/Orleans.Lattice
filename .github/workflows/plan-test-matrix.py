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
whole. A sharded package is crossed with the tier filters unless it sets
`"crossTiers": false`, in which case each shard runs once, untiered - see the
UNSHARDED_FILTER comment for why that is the right default for a suite with no
fault-injection tier.

Items are packed into legs with LPT (longest processing time first): sort
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

SKIPPING TIERS BY POLICY
-----------------------
`--skip-tiers coyote,chaos` plans a run that executes the deterministic tier
only. ci.yml passes it for a MEMBER pull request into an integration branch and
for nothing else (see tier-scope.py, which owns that decision). It does not
drop anything silently, and that is the whole design:

- A sharded package crossed with the tiers keeps its deterministic items and
  turns each skipped-tier item into a POLICY-SKIPPED record. Those records are
  attached to a leg after packing - so they never cost a runner or move the leg
  count - and run-test-leg.py reports them without running them, so the
  aggregate report lists exactly which (package, shard, tier) did not run.
- An untiered item (an unsharded package, or a `crossTiers: false` shard) runs
  with the skipped tiers' categories excluded from its filter, and records the
  excluded tiers on the item. The exclusion for `coyote,chaos` together is the
  deterministic tier's own filter, so a member pull request runs precisely the
  surface the deterministic tier runs on a fully gated one.

The deterministic tier cannot be skipped: a run with nothing left to execute is
a green that verified nothing.

Usage:
  plan-test-matrix.py --packages FILE --shards FILE --durations FILE
                      [--seeded FILE] [--max-legs N]
                      [--skip-tiers TIER[,TIER]] [--skip-reason TEXT]
                      --output-matrix FILE [--report-file FILE]
  plan-test-matrix.py --shards FILE --emit-shard-filters PACKAGE

The second form prints one package's shard partition and plans nothing. The
apps lane in ci.yml uses it to run a covering test project shard by shard, so
it splits the suite by the same partition as the matrix instead of a copy.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

# Tier filters, matching the three test steps in ci.yml. The tuple order is the
# order a leg runs its items in, cheapest tier first.
TIERS: list[tuple[str, str]] = [
    ("deterministic", "TestCategory!=Chaos&TestCategory!=Coyote"),
    ("coyote", "TestCategory=Coyote"),
    ("chaos", "TestCategory=Chaos&TestCategory!=AzureStorageEmulator"),
]

# The category exclusion that removes one skippable tier from an UNTIERED item.
# Excluding both is exactly the deterministic tier's filter above, which is what
# keeps a member pull request's untiered packages on the same surface as the
# deterministic tier of a fully gated run. `deterministic` is deliberately not a
# key: it is the tier that is never skipped.
TIER_EXCLUSIONS: dict[str, str] = {
    "coyote": "TestCategory!=Coyote",
    "chaos": "TestCategory!=Chaos",
}

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
#
# The same argument applies to a SHARDED package whose suite has no
# fault-injection tier at all. Sharding is about splitting a long suite across
# runners, and crossing it with tiers is a separate decision that only pays off
# when the tiers are independently large: `test/lattice` gains because its
# 3.9-minute chaos tier then runs beside its 3.8-minute deterministic tier on
# another runner. A package with zero Chaos and zero Coyote tests gains nothing
# and pays two empty `dotnet test` starts per shard. Such a package sets
# `"crossTiers": false` in test-shards.json and each of its shards becomes one
# untiered invocation filtered only by FullyQualifiedName.
#
# That opt-out is deliberately a TIER-COUNT choice and not a tier ALLOW-LIST,
# because an allow-list is the shape that rots. Naming the tiers a package has
# today ("deterministic only") silently stops running the first Chaos fixture
# somebody adds to it, which is the failure mode this file exists to prevent.
# Dropping the tier filter entirely cannot do that: the shard expressions are a
# partition of FullyQualifiedName, so a new fixture lands in exactly one shard
# and runs there whatever categories it carries.
UNSHARDED_FILTER: str | None = None

# Fallback estimates for any work item absent from the durations file. Small on
# purpose: the unlisted packages are the near-instant long tail, and a tier that
# a package has no tests for costs only the process start.
DEFAULT_ESTIMATE = {"deterministic": 0.15, "coyote": 0.02, "chaos": 0.05, "all": 0.2}

# Fixed cost of one `dotnet test` invocation - process start, assembly load,
# test discovery - paid even when the filter matches nothing. Added to every
# item so the bin packing sees the true cost of a leg rather than only its test
# time, which otherwise makes a leg of twenty trivial items look free.
#
# MEASURED, not guessed: an item whose filter matches no test takes a median
# 0.04 min (2.4 s) on a CI leg, over 18 such items in each of 49 full fan-outs
# (2026-09-24 to 2026-09-28). The value was 0.12 before that measurement,
# three times the real cost, which priced every leg carrying the lattice empty
# tiers well above what it ran. The rows in test-durations.tsv already include
# each item's own process start, so for a non-empty item this double-counts
# about 2 s; that is far inside the rows' own sampling error and keeps an empty
# (0.0) row from pricing as free.
PER_ITEM_OVERHEAD = 0.04


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packages")
    parser.add_argument("--shards", required=True)
    parser.add_argument("--durations")
    parser.add_argument("--seeded", help="Packages seeded directly by the diff.")
    parser.add_argument("--max-legs", type=int, default=10)
    parser.add_argument(
        "--skip-tiers",
        default="",
        help="Comma-separated tiers not to run (a subset of: "
             + ", ".join(sorted(TIER_EXCLUSIONS)) + "). Empty runs every tier.",
    )
    parser.add_argument(
        "--skip-reason",
        default="skipped by policy",
        help="Why the tiers are skipped, recorded on every policy-skipped item.",
    )
    parser.add_argument("--output-matrix")
    parser.add_argument("--report-file")
    parser.add_argument(
        "--emit-shard-filters",
        metavar="PACKAGE",
        help="Print PACKAGE's shard partition as JSON ([{shard, filter}], or [] when "
             "the package is unsharded) and exit. Lanes that run a package's tests "
             "outside the matrix use this so they split the suite by exactly the "
             "partition the matrix uses, rather than by a second copy of it.",
    )
    args = parser.parse_args()
    if args.emit_shard_filters is None:
        missing = [
            flag for flag, value in (
                ("--packages", args.packages),
                ("--durations", args.durations),
                ("--output-matrix", args.output_matrix),
            ) if not value
        ]
        if missing:
            parser.error("the following arguments are required to plan a matrix: " + ", ".join(missing))
    args.skip_tiers = parse_skip_tiers(parser, args.skip_tiers)
    return args


def parse_skip_tiers(parser: argparse.ArgumentParser, raw: str) -> list[str]:
    """The skipped tiers in TIERS order, refusing anything that is not skippable."""
    requested = {name.strip() for name in raw.split(",") if name.strip()}
    unknown = sorted(requested - set(TIER_EXCLUSIONS))
    if unknown:
        parser.error(
            "--skip-tiers accepts only " + ", ".join(sorted(TIER_EXCLUSIONS))
            + "; refusing " + ", ".join(unknown)
            + " (the deterministic tier is never skipped)"
        )
    return [tier for tier, _ in TIERS if tier in requested]


def exclusion_filter(skip_tiers: list[str]) -> str | None:
    """The category filter that removes the skipped tiers from an untiered item."""
    if not skip_tiers:
        return None
    return "&".join(TIER_EXCLUSIONS[tier] for tier in skip_tiers)


def narrowed_tier(skip_tiers: list[str]) -> str:
    """The tier label of an untiered item once the skipped tiers are excluded."""
    remaining = [tier for tier, _ in TIERS if tier not in skip_tiers]
    return remaining[0] if len(remaining) == 1 else "all"


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
    claimed_categories: list[str] = []
    for entry in shards:
        name = entry["shard"]
        includes = [f"{root}.{prefix}" for prefix in entry.get("include", [])]
        categories = list(entry.get("includeCategories", []))
        for category in categories:
            if not re.fullmatch(r"[A-Za-z0-9_]+", category):
                raise SystemExit(f"shard {name!r} category {category!r} must be a plain identifier")
            if category in claimed_categories:
                raise SystemExit(f"shard {name!r} category {category!r} is already claimed by an earlier shard")

        exclusions = [f"FullyQualifiedName!~{prefix}" for prefix in claimed] + [
            f"TestCategory!={category}" for category in claimed_categories
        ]
        if entry.get("complement"):
            if includes or categories:
                raise SystemExit(f"shard {name!r} is the complement and must declare no includes")
            if not exclusions:
                raise SystemExit(f"shard {name!r} is a complement of nothing")
            expression = "&".join(exclusions)
        else:
            if not includes and not categories:
                raise SystemExit(f"shard {name!r} declares no include prefixes")
            # A category shard selects by TestCategory, for cases a FullName
            # prefix cannot address (a parameterized case's arguments sit
            # inside parentheses the NUnit adapter's filter parser rejects).
            positives = [f"FullyQualifiedName~{prefix}" for prefix in includes] + [
                f"TestCategory={category}" for category in categories
            ]
            terms = [f"({'|'.join(positives)})" if len(positives) > 1 else positives[0]]
            # Subtract only the prefixes an earlier shard already claimed. A
            # shard that claims nothing new would silently run zero tests, so
            # say so now rather than letting it read as a passing empty leg.
            if includes and not categories:
                narrowing = [p for p in claimed if any(p.startswith(inc) or inc.startswith(p) for inc in includes)]
                if len(narrowing) == len(includes) and all(p in includes for p in narrowing):
                    raise SystemExit(f"shard {name!r} is fully claimed by an earlier shard")
            terms += exclusions
            expression = "&".join(terms)
            claimed.extend(includes)
            claimed_categories.extend(categories)

        result.append({"shard": name, "filter": expression})
    return result


def make_items(
    packages: list[str],
    shard_config: dict,
    durations: dict[tuple[str, str, str], float],
    seeded: set[str],
    skip_tiers: list[str] | None = None,
    skip_reason: str = "skipped by policy",
) -> tuple[list[dict], list[dict]]:
    """Return (items to run, items skipped by policy).

    With no skipped tiers the second list is empty and the first is exactly
    the full plan. With skipped tiers, nothing disappears: a crossed item of a
    skipped tier moves to the second list, and an untiered item stays in the
    first with the skipped tiers' categories excluded and recorded.
    """
    skip_tiers = list(skip_tiers or [])
    exclusion = exclusion_filter(skip_tiers)
    untiered_tier = narrowed_tier(skip_tiers) if skip_tiers else "all"

    def untiered(name_filter: str | None) -> str | None:
        if exclusion is None:
            return name_filter
        if name_filter is None:
            return exclusion
        return f"({name_filter})&({exclusion})"

    def mark_excluded(item: dict) -> dict:
        if skip_tiers:
            item["excluded_tiers"] = list(skip_tiers)
            item["skip_reason"] = skip_reason
        return item

    items: list[dict] = []
    skipped: list[dict] = []
    for package in packages:
        config = shard_config.get(package)
        if config is None:
            # Unsharded: one unfiltered invocation covering all three tiers.
            # See the UNSHARDED_FILTER comment for the measurement showing this
            # is equal to, and not a superset of, what ci.yml runs.
            estimate = durations.get((package, "-", "all"), DEFAULT_ESTIMATE["all"])
            items.append(
                mark_excluded({
                    "package": package,
                    "shard": "-",
                    "tier": untiered_tier,
                    "filter": untiered(UNSHARDED_FILTER),
                    "estimate": round(estimate + PER_ITEM_OVERHEAD, 3),
                    "label": package,
                    "seeded": package in seeded,
                })
            )
            continue

        cross_tiers = config.get("crossTiers", True)
        for shard in build_shard_filters(config):
            if not cross_tiers:
                # One untiered invocation per shard, filtered only by name. See
                # the UNSHARDED_FILTER comment: this is the sharded form of the
                # same measurement, and it cannot drop a newly added fixture
                # because no tier filter stands between the shard and the test.
                estimate = durations.get(
                    (package, shard["shard"], "all"), DEFAULT_ESTIMATE["all"]
                )
                items.append(
                    mark_excluded({
                        "package": package,
                        "shard": shard["shard"],
                        "tier": untiered_tier,
                        "filter": untiered(shard["filter"]),
                        "estimate": round(estimate + PER_ITEM_OVERHEAD, 3),
                        "label": f"{package} / {shard['shard']}",
                        "seeded": package in seeded,
                    })
                )
                continue

            for tier, tier_filter in TIERS:
                combined = f"({shard['filter']})&({tier_filter})"
                estimate = durations.get(
                    (package, shard["shard"], tier), DEFAULT_ESTIMATE[tier]
                )
                item = {
                    "package": package,
                    "shard": shard["shard"],
                    "tier": tier,
                    "filter": combined,
                    "estimate": round(estimate + PER_ITEM_OVERHEAD, 3),
                    "label": f"{package} / {shard['shard']} ({tier})",
                    "seeded": package in seeded,
                }
                if tier in skip_tiers:
                    # Recorded, not run. It costs nothing (no process start),
                    # so it is priced at zero and never reaches the packer.
                    item["estimate"] = 0.0
                    item["skip"] = skip_reason
                    skipped.append(item)
                else:
                    items.append(item)
    return items, skipped


def attach_skipped(legs: list[dict], skipped: list[dict]) -> None:
    """Record each policy-skipped item on a leg, after packing.

    They are attached rather than packed so they can neither create a leg nor
    change the leg count: a leg made only of skipped items would cost a runner
    to report that it did nothing. Each goes beside a sibling of the same
    package and shard when one exists, so a leg's own summary shows a shard's
    run and skipped tiers together; otherwise to the leg with fewest items.
    """
    for item in skipped:
        home = next(
            (leg for leg in legs
             if any(other["package"] == item["package"] and other["shard"] == item["shard"]
                    for other in leg["items"])),
            None,
        )
        if home is None:
            home = min(legs, key=lambda leg: (len(leg["items"]), leg["id"]))
        home["items"].append(item)


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


def write_report(
    handle,
    legs: list[dict],
    items: list[dict],
    packages: list[str],
    seeded: set[str],
    skipped: list[dict] | None = None,
    skip_tiers: list[str] | None = None,
    skip_reason: str = "",
) -> None:
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

    if skip_tiers:
        narrowed = [item for item in items if item.get("excluded_tiers")]
        handle.write(f"### Tiers NOT run: {', '.join(skip_tiers)}\n\n")
        handle.write(f"Reason: {skip_reason}\n\n")
        handle.write(
            "This is a skip, not a pass. Nothing in the tiers above was executed on this run: "
            f"{len(skipped or [])} tiered item(s) are recorded as skipped and reported by the "
            f"aggregate, and {len(narrowed)} untiered item(s) run with those tiers' categories "
            "excluded from their filter.\n\n"
        )
        for item in sorted(skipped or [], key=lambda entry: entry["label"]):
            handle.write(f"- `{item['label']}`\n")
        if skipped:
            handle.write("\n")


def main() -> int:
    args = parse_args()

    with open(args.shards, encoding="utf-8") as handle:
        shard_config = {
            key: value
            for key, value in json.load(handle).items()
            if not key.startswith("_")
        }

    if args.emit_shard_filters is not None:
        config = shard_config.get(args.emit_shard_filters)
        partition = build_shard_filters(config) if config is not None else []
        json.dump(partition, sys.stdout, separators=(",", ":"))
        sys.stdout.write("\n")
        return 0

    packages = read_lines(args.packages)
    if not packages:
        print("::error::The planner was given an empty package selection.", file=sys.stderr)
        return 1

    seeded = set(read_lines(args.seeded)) if args.seeded and os.path.exists(args.seeded) else set()

    durations = load_durations(args.durations)
    items, skipped = make_items(
        packages, shard_config, durations, seeded, args.skip_tiers, args.skip_reason
    )
    legs = pack(items, args.max_legs)

    if not legs:
        print("::error::The planner produced no legs.", file=sys.stderr)
        return 1

    attach_skipped(legs, skipped)

    with open(args.output_matrix, "w", encoding="utf-8") as handle:
        json.dump(legs, handle, separators=(",", ":"))

    if args.report_file:
        with open(args.report_file, "a", encoding="utf-8") as handle:
            write_report(handle, legs, items, packages, seeded,
                         skipped, args.skip_tiers, args.skip_reason)
    else:
        write_report(sys.stderr, legs, items, packages, seeded,
                     skipped, args.skip_tiers, args.skip_reason)

    print(
        f"Planned {len(legs)} legs over {len(items)} work items"
        + (f", {len(skipped)} skipped by policy." if skipped else "."),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
