#!/usr/bin/env python3
"""Plan the nightly lane: coverage legs and uninstrumented exhaustive TLC legs.

Both matrices use CI's shard partitions, duration estimates and LPT packer from
plan-test-matrix.py, so a rebalance of test-shards.json moves both lanes at once.

COVERAGE LEGS run every non-chaos, non-Coyote, non-TLC suite under coverlet.

TLC LEGS run every shard test-shards.json marks "tlc" or "tlcSmoke", selected
by the Tlc category and run WITHOUT coverlet: instrumenting the harness buys no
coverage worth having and slows the JVM-bound cases. They run through
run-test-leg.py, which bounds a declared TLC-only item by the enclosing job's
timeout rather than VSTest's inactivity collector (#5012), so each TLC leg
carries a timeout sized from its measured estimate. They never read a TLC
verdict receipt: the nightly run is the backstop for drift no fingerprint can
see (JDK patch level, runner image), so every case runs every night. They do
WRITE receipts (coverage.yml's tlc-report job, through tlc-receipts.py), so
every module the night verifies on main is skipped by later pull requests.

`--tlc-modules FILE` plans TLC legs for only the named census modules, for the
receipt warmer (formal-receipts.yml, #5165), which runs on main only the
modules without a receipt. It is never used by the nightly lane.

`--tlc-case-durations DIR` is the lane's second helper: it reads the TLC legs'
trx files and writes one row per case with its outcome and duration, so TLC
pricing can be refreshed from nightly data.
"""

import argparse
import importlib.util
import json
import math
import xml.etree.ElementTree as ElementTree
from pathlib import Path

WORKFLOWS = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("ci_matrix", WORKFLOWS / "plan-test-matrix.py")
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)

# Also applied to retries by coverage.yml, so even a newly introduced TLC shard
# cannot accidentally execute the external model checker in the coverage lane.
COVERAGE_FILTER = "TestCategory!=Chaos&TestCategory!=Coyote&TestCategory!=Tlc"
# lattice.explorer.e2e drives out-of-process hosts on its own lane
# (explorer-journeys.yml); coverlet cannot instrument a process it did not start.
EXCLUDED_PROJECTS = {"microbench", "azure-throughput-silo", "lattice.explorer.e2e"}
UI_PACKAGE = "lattice.explorer.uitests"

# The package whose shards carry the TLA+ model checks, and the category every
# TLC case carries, smoke included. ANDed onto each TLC shard's own filter so a
# TLC leg can only ever run model checks, never an instrumentation-free copy of
# an ordinary suite.
TLC_PACKAGE = "lattice"
TLC_FILTER = "TestCategory=Tlc"

# Each TLC leg's job timeout: the measured estimate (test-durations.tsv)
# doubled, plus the fixed cost of checkout, build and toolchain provisioning.
# The timeout is the ONLY hang bound on a TLC-only item (#5012), so it must
# follow the estimate rather than a constant that a rebalance silently outgrows.
TLC_TIMEOUT_FACTOR = 2.0
TLC_TIMEOUT_OVERHEAD = 30
# Below GitHub's 360-minute hosted-runner ceiling, which would otherwise cut a
# leg off with no verdict of ours.
TLC_TIMEOUT_CEILING = 355
TLC_MAX_LEGS = 8

TRX_NAMESPACE = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}


def discover_projects(root: Path) -> dict[str, str]:
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
    return projects


def plan(root: Path, max_legs: int = 10) -> list[dict]:
    projects = discover_projects(root)
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


def tlc_timeout(estimate: float) -> int:
    """A TLC leg's job timeout, in whole minutes, from its packed estimate."""
    minutes = math.ceil(TLC_TIMEOUT_FACTOR * estimate + TLC_TIMEOUT_OVERHEAD)
    if minutes > TLC_TIMEOUT_CEILING:
        raise ValueError(
            f"A TLC leg estimated at {estimate:.1f} min needs a {minutes}-minute timeout, above "
            f"the {TLC_TIMEOUT_CEILING}-minute ceiling; split its shards in test-shards.json"
        )
    return minutes


def narrow_tlc_to_modules(
    items: list[dict],
    modules: set[str],
    census: dict[str, list[str]],
    categories: dict[str, list[str]],
) -> list[dict]:
    """Keep the TLC items that select a census case of one of `modules`, each
    narrowed to those modules' `TlcModule_` categories (#5165).

    Which census cases an item selects is decided by tlc-receipts.py's filter
    evaluator, the one ci.yml's planner and the receipt writer also use (#5164),
    so the three cannot disagree. An item that selects no such case is dropped
    rather than run empty: the receipt warmer verifies modules, and a TLC test
    outside the census (the smoke case, the synthetic module) earns no receipt.
    Every module must be selected by some item, or the plan would claim to
    verify a module that no leg runs.
    """
    tools = ci.load_receipt_tools()
    kept = []
    covered: set[str] = set()
    for item in items:
        selected = tools.selected_cases(item["filter"], census, categories)
        here = sorted(module for module in selected if module in modules)
        if not here:
            continue
        total = sum(len(names) for names in selected.values())
        share = sum(len(selected[module]) for module in here) / total
        clause = "|".join(f"TestCategory=TlcModule_{module}" for module in here)
        item["filter"] = f"({item['filter']})&({clause})"
        item["estimate"] = round(
            ci.PER_ITEM_OVERHEAD + max(item["estimate"] - ci.PER_ITEM_OVERHEAD, 0.0) * share, 3)
        item["tlc_modules"] = here
        covered.update(here)
        kept.append(item)
    unplanned = sorted(modules - covered)
    if unplanned:
        raise ValueError("No TLC shard selects a census case of: " + ", ".join(unplanned))
    return kept


def plan_tlc(
    root: Path,
    max_legs: int = TLC_MAX_LEGS,
    only: list[str] | None = None,
    modules: set[str] | None = None,
    census: dict[str, list[str]] | None = None,
    categories: dict[str, list[str]] | None = None,
) -> list[dict]:
    """Plan the exhaustive TLC legs: every "tlc" and "tlcSmoke" shard, or only the named ones.

    With `modules` (and the census and case categories that say which cases
    each shard selects), only those modules' cases run; an empty set plans no
    leg at all. The nightly lane never passes it: it runs every case.
    """
    projects = discover_projects(root)
    if TLC_PACKAGE not in projects:
        raise ValueError(f"No test project for {TLC_PACKAGE}, which owns the TLC shards")
    workflows = root / ".github" / "workflows"
    shards = json.loads((workflows / "test-shards.json").read_text(encoding="utf-8"))
    durations = ci.load_durations(str(workflows / "test-durations.tsv"))
    declared = [
        shard["shard"] for shard in ci.build_shard_filters(shards[TLC_PACKAGE])
        if shard["tlc"] or shard["tlcSmoke"]
    ]
    if not declared:
        raise ValueError(f"test-shards.json declares no TLC shard for {TLC_PACKAGE}")
    if only:
        unknown = sorted(set(only) - set(declared))
        if unknown:
            raise ValueError(
                "Not TLC shards of " + TLC_PACKAGE + ": " + ", ".join(unknown)
                + ". Known: " + ", ".join(declared)
            )
    selected = set(only) if only else set(declared)
    # The deterministic tier is the only one a TLC case is in; a TLC shard
    # crossed with the Coyote and Chaos tiers is empty there, so those items
    # are dropped rather than paid a process start each.
    items, _ = ci.make_items(
        [TLC_PACKAGE], shards, durations, set(), ["coyote", "chaos"],
        "Exhaustive TLC runs only the deterministic tier",
    )
    items = [
        item for item in items
        if (item.get("tlc") or item.get("tlcSmoke")) and item["shard"] in selected
    ]
    for item in items:
        item["project"] = projects[TLC_PACKAGE]
        item["filter"] = f"({item['filter']})&({TLC_FILTER})"
    planned = {item["shard"] for item in items}
    if planned != selected:
        raise ValueError("TLC shards with no runnable item: " + ", ".join(sorted(selected - planned)))
    if modules is not None:
        if census is None or categories is None:
            raise ValueError("Restricting TLC to modules needs the census and the case categories")
        unknown = sorted(modules - set(census))
        if unknown:
            raise ValueError("Not census modules: " + ", ".join(unknown))
        items = narrow_tlc_to_modules(items, modules, census, categories)
        if not items:
            return []
    legs = ci.pack(items, max_legs)
    for index, leg in enumerate(legs, start=1):
        leg["id"] = f"tlc-{index}"
        leg["timeout"] = tlc_timeout(leg["estimate"])
    return legs


def tlc_case_durations(directory: Path) -> list[tuple[str, str, float]]:
    """(case, outcome, seconds) for every result in every trx file under directory.

    The case is the fully qualified name the census and filters use: the test
    method's class name, a dot, then the result's display name.
    """
    rows = []
    for path in sorted(directory.rglob("*.trx")):
        root = ElementTree.parse(path).getroot()
        classes = {
            definition.get("id"): method.get("className", "")
            for definition in root.findall("t:TestDefinitions/t:UnitTest", TRX_NAMESPACE)
            for method in definition.findall("t:TestMethod", TRX_NAMESPACE)
        }
        for result in root.findall("t:Results/t:UnitTestResult", TRX_NAMESPACE):
            name = result.get("testName", "<unnamed>")
            owner = classes.get(result.get("testId"), "")
            hours, minutes, seconds = (result.get("duration") or "0:0:0").split(":")
            elapsed = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
            rows.append((f"{owner}.{name}" if owner else name, result.get("outcome", "?"), elapsed))
    return sorted(rows, key=lambda row: (-row[2], row[0]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=WORKFLOWS.parent.parent)
    parser.add_argument("--max-legs", type=int, default=10)
    parser.add_argument("--max-tlc-legs", type=int, default=TLC_MAX_LEGS)
    parser.add_argument("--output-matrix", type=Path)
    parser.add_argument("--output-tlc-matrix", type=Path)
    parser.add_argument(
        "--tlc-shards", default="",
        help="Comma-separated TLC shard names. Non-empty plans ONLY those TLC shards "
             "and an empty coverage matrix.",
    )
    parser.add_argument("--tlc-case-durations", type=Path, metavar="DIR",
                        help="Read trx files under DIR and write per-case TLC durations.")
    parser.add_argument("--output", type=Path, help="Output file for --tlc-case-durations.")
    parser.add_argument(
        "--tlc-modules", type=Path, metavar="FILE",
        help="Plan ONLY the TLC legs that run these census modules (one per line; an empty "
             "file plans no leg), and no coverage matrix. The receipt warmer (#5165) uses it; "
             "the nightly lane never does. Requires --output-tlc-matrix, --tlc-census and "
             "--tlc-case-categories.",
    )
    parser.add_argument("--tlc-census", type=Path, help="The TLC case census (contract C2).")
    parser.add_argument("--tlc-case-categories", type=Path,
                        help="Each census case's test categories (tlc-receipts.py, #5164).")
    args = parser.parse_args()

    if args.tlc_case_durations is not None:
        if args.output is None:
            parser.error("--tlc-case-durations requires --output")
        rows = tlc_case_durations(args.tlc_case_durations)
        with args.output.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write("# case\toutcome\tseconds\n")
            for case, outcome, seconds in rows:
                handle.write(f"{case}\t{outcome}\t{seconds:.1f}\n")
        print(f"Wrote {len(rows)} TLC case duration(s) to {args.output}.")
        return

    if args.max_legs < 1 or args.max_tlc_legs < 1:
        parser.error("--max-legs and --max-tlc-legs must be positive")

    if args.tlc_modules is not None:
        if args.output_tlc_matrix is None or args.tlc_census is None or args.tlc_case_categories is None:
            parser.error("--tlc-modules requires --output-tlc-matrix, --tlc-census and --tlc-case-categories")
        if args.output_matrix is not None or args.tlc_shards:
            parser.error("--tlc-modules plans TLC legs only; drop --output-matrix and --tlc-shards")
        tools = ci.load_receipt_tools()
        modules = {
            line.strip() for line in args.tlc_modules.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
        tlc_legs = plan_tlc(
            args.root, args.max_tlc_legs, None, modules,
            tools.load_census(args.tlc_census), tools.load_categories(args.tlc_case_categories),
        )
        args.output_tlc_matrix.write_text(json.dumps(tlc_legs, separators=(",", ":")), encoding="utf-8")
        print(f"Planned {len(tlc_legs)} TLC legs for {len(modules)} module(s) without a receipt.")
        return

    if args.output_matrix is None:
        parser.error("--output-matrix is required to plan")
    only = [name.strip() for name in args.tlc_shards.split(",") if name.strip()]
    if only and args.output_tlc_matrix is None:
        parser.error("--tlc-shards requires --output-tlc-matrix")
    legs = [] if only else plan(args.root, args.max_legs)
    args.output_matrix.write_text(json.dumps(legs, separators=(",", ":")), encoding="utf-8")
    print(f"Planned {len(legs)} coverage legs using CI shard definitions and estimates.")
    if args.output_tlc_matrix is not None:
        tlc_legs = plan_tlc(args.root, args.max_tlc_legs, only)
        args.output_tlc_matrix.write_text(json.dumps(tlc_legs, separators=(",", ":")), encoding="utf-8")
        print(f"Planned {len(tlc_legs)} exhaustive TLC legs"
              + (f" restricted to {', '.join(only)}." if only else "."))


if __name__ == "__main__":
    main()
