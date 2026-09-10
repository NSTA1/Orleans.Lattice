#!/usr/bin/env python3
"""Run the repository's content gates and prove the run was not vacuous.

The gates that police *text* - the formal refinement fixtures under
`test/lattice/Formal/`, the hygiene fixtures under `test/<pkg>/Hygiene/`, and
the documentation snippet compilations under `test/<pkg>/Docs/` - are ordinary
NUnit tests. Nothing else in the workflow enforces them, so if they do not run
they are not enforced at all, and the run still reports green.

This script runs them across the whole solution in one pass and then refuses to
report success unless the run actually exercised each family. A filter that
drifts away from the fixtures it was written for selects nothing, and selecting
nothing is exactly what a passing-but-useless gate looks like from the outside.
The families are derived from the test names the run itself produced, so this
guard needs no hand-maintained count to keep current.

Failure modes, all of which exit non-zero:

  * `dotnet test` itself failed (build error, crashed host, failing test).
  * A trx result recorded a failed outcome.
  * The run executed no tests at all.
  * The run executed no tests for one of the families it claims to gate.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

TRX_NS = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}

# The three families this gate exists to enforce. A family is attributed from
# the test's fully qualified name, which is where the repository already puts
# it: `Orleans.Lattice.Tests.Formal.*`, `*.Tests.Docs.*`, and the `*Hygiene*`
# fixtures. Membership is therefore observed from the run, never declared here.
FAMILIES = ("Formal", "Hygiene", "Docs")

PASSED = "Passed"
FAILED = "Failed"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--solution", required=True, help="Solution file to test.")
    parser.add_argument("--filter", required=True, help="VSTest --filter expression.")
    parser.add_argument(
        "--trx-name",
        default="text-gates.trx",
        help="File name each test project writes its trx log to.",
    )
    parser.add_argument("--summary-file", help="Markdown summary to append to.")
    return parser.parse_args()


def discover_trx(trx_name: str) -> list[Path]:
    return sorted(Path(".").rglob(f"TestResults/{trx_name}"))


def project_of(trx: Path) -> str:
    # `<project>/TestResults/<name>.trx`.
    resolved = trx.resolve()
    return resolved.parent.parent.name


def read_results(trx: Path) -> list[tuple[str, str]]:
    """Return (fullyQualifiedName, outcome) for every result the trx recorded.

    The `testName` on a result is a display name, which for a parameterised or
    renamed test need not carry the namespace at all. The fully qualified name
    lives on the test DEFINITION, so the two are joined by test id. Attributing
    families from the display name instead would under-count silently, which is
    the one thing this script must not do.
    """
    root = ET.parse(trx).getroot()

    full_names: dict[str, str] = {}
    for definition in root.findall(".//t:TestDefinitions/t:UnitTest", TRX_NS):
        test_id = definition.get("id")
        method = definition.find("t:TestMethod", TRX_NS)
        if test_id is None or method is None:
            continue
        class_name = method.get("className") or ""
        method_name = method.get("name") or ""
        full_names[test_id] = f"{class_name}.{method_name}"

    results = []
    for node in root.findall(".//t:UnitTestResult", TRX_NS):
        test_id = node.get("testId") or ""
        name = full_names.get(test_id) or node.get("testName") or ""
        outcome = node.get("outcome") or "Unknown"
        results.append((name, outcome))
    return results


def main() -> int:
    args = parse_args()

    # Stale logs from an earlier step would be counted as this run's evidence,
    # which is precisely the kind of false confidence this script exists to
    # prevent. Clear them first.
    for stale in discover_trx(args.trx_name):
        stale.unlink()

    command = [
        "dotnet",
        "test",
        args.solution,
        "--no-build",
        "--configuration",
        "Release",
        "--filter",
        args.filter,
        "--logger",
        f"trx;LogFileName={args.trx_name}",
    ]
    print("+ " + " ".join(command), flush=True)
    completed = subprocess.run(command)

    logs = discover_trx(args.trx_name)
    outcomes: Counter[str] = Counter()
    families: Counter[str] = Counter()
    per_project: dict[str, Counter[str]] = {}
    failures: list[str] = []

    for log in logs:
        project = project_of(log)
        counts = per_project.setdefault(project, Counter())
        for name, outcome in read_results(log):
            outcomes[outcome] += 1
            counts[outcome] += 1
            if outcome == FAILED:
                failures.append(f"{project}: {name}")
            for family in FAMILIES:
                if family in name:
                    families[family] += 1

    executed = outcomes[PASSED] + outcomes[FAILED]

    problems: list[str] = []
    if completed.returncode != 0:
        problems.append(f"dotnet test exited with {completed.returncode}.")
    if outcomes[FAILED]:
        problems.append(f"{outcomes[FAILED]} test(s) failed.")
    if executed == 0:
        problems.append(
            "No tests executed. The content gates did not run, so this pull "
            "request was not checked by them - the same defect a passing but "
            "unexecuted gate produces."
        )
    else:
        for family in FAMILIES:
            if families[family] == 0:
                problems.append(
                    f"No '{family}' tests executed. Either that family was "
                    f"renamed out from under the filter, or its test project "
                    f"stopped being built - both leave it unenforced."
                )

    lines = ["### Content gates", ""]
    if executed:
        lines.append(f"Executed **{executed}** tests across {len(per_project)} project(s).")
        lines.append("")
        lines.append("| Family | Tests |")
        lines.append("| --- | ---: |")
        for family in FAMILIES:
            lines.append(f"| {family} | {families[family]} |")
        lines.append("")
        lines.append("| Project | Passed | Failed |")
        lines.append("| --- | ---: | ---: |")
        for project in sorted(per_project):
            counts = per_project[project]
            if counts[PASSED] or counts[FAILED]:
                lines.append(f"| {project} | {counts[PASSED]} | {counts[FAILED]} |")
    else:
        lines.append("No tests executed.")

    if failures:
        lines += ["", "**Failed tests**", ""]
        lines += [f"- `{failure}`" for failure in failures[:50]]

    if problems:
        lines += ["", "**Problems**", ""]
        lines += [f"- {problem}" for problem in problems]

    report = "\n".join(lines) + "\n"
    print()
    print(report)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            handle.write(report)

    for problem in problems:
        print(f"::error::{problem}")

    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
