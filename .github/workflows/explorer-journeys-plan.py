#!/usr/bin/env python3
"""Plan, summarise and check the legs of explorer-journeys.yml (#5363).

Subcommands:

  plan         Decide which legs a run executes and which journeys need the
               screenshot provenance check. Writes `legs` and `provenance`
               (JSON arrays) to GITHUB_OUTPUT and a plan table to the summary.
  leg-summary  Print a leg's test counts, every journey's ordered screenshot
               manifest and the leg's coverage hits to the job summary, and
               fail a leg whose filter selected none of the tests it exists for.
  probe        Judge the fail-closed probe: with Azurite stopped, the smoke
               fixture must FAIL - never pass, never go inconclusive.

Legs:

  harness           every EndToEnd fixture that is neither a journey, a story
                    nor the aggregate coverage gate (smoke, fault seams,
                    parity, checkpoints, catalogue and outline checks).
  isolated-<jid>    one per folder under test/lattice.explorer.e2e/Journeys/,
                    discovered from the directory, so adding a journey edits
                    no shared file. LATTICE_E2E_MODE=isolated.
  story-<storyline> the Contoso and Fabrikam story fixtures, once they and a
                    chapter contract (F6) exist in the tree.
                    LATTICE_E2E_MODE=story. On a pull request they run on its
                    merge commit, so a broken hand-off is caught before it lands.
  story-contoso-narrow  nightly only: the Contoso story at 390x844.

Which legs run:

  pull_request edited    none (the body changed, not the code); provenance only.
  pull_request into an integration branch (`*/epic/*`):
      harness always; a change to anything but a journey folder, markdown,
      docs or an unrelated workflow file runs every leg (the Explorer, the
      harness, the hosts, a facade and the lane itself); otherwise the touched
      journeys' isolated legs and their storylines' story legs.
  pull_request into anything else (the bucket-to-main pull request), push,
  workflow_dispatch: every leg.
  schedule: every leg, plus story-contoso-narrow.
  A pull request whose changed files could not be listed runs every leg.
"""

import argparse
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

E2E = "test/lattice.explorer.e2e"
JOURNEYS = E2E + "/Journeys"
NAMESPACE = "Orleans.Lattice.Explorer.EndToEnd.Tests"
PENDING_GATE = NAMESPACE + ".Pending.PendingWrapperGateTests"

# Storylines by the chapter id's first letter: the Contoso acts (Administrators,
# Users, Crisis/response) and the Fabrikam tenancy storyline (#5401, D2/D3).
STORYLINE_BY_PREFIX = {"a": "contoso", "u": "contoso", "c": "contoso", "t": "fabrikam"}

# Each story leg, the storyline it runs and its fixture class.
STORY_LEGS = (
    ("contoso", "ContosoStoryTests"),
    ("fabrikam", "FabrikamStoryTests"),
)

# Files whose change runs every leg although they live outside the e2e tree:
# the lane itself and the browser installer it calls.
LANE_FILES = frozenset(
    ".github/workflows/" + name
    for name in (
        "explorer-journeys.yml",
        "explorer-journeys-plan.py",
        "explorer-journeys-plan-selftest.py",
        "journey-screenshot-provenance.py",
        "journey-screenshot-provenance-selftest.py",
        "install-playwright-browsers.sh",
        "run-playwright-install.py",
        "playwright_apt_mirrors.py",
    )
)

HARNESS_FILTER = (
    "TestCategory=EndToEnd&TestCategory!=Journey&TestCategory!=Story&TestCategory!=CoverageGate"
)
JOURNEY_FOLDER = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")
TIMEOUTS = {"harness": 45, "isolated": 45, "story": 150}


class PlanError(Exception):
    """A plan input the planner refuses to guess about."""


def discover_journeys(root):
    """Every journey folder under Journeys/, as (folder, jid, storyline), sorted."""
    directory = Path(root) / JOURNEYS
    if not directory.is_dir():
        return []
    journeys = []
    for entry in sorted(directory.iterdir(), key=lambda path: path.name.lower()):
        if not entry.is_dir() or entry.name.startswith(".") or entry.name in ("bin", "obj"):
            continue
        journeys.append((entry.name, jid_of(entry.name), storyline_of(entry.name)))
    return journeys


def jid_of(folder):
    """The screenshot journey id of a journey folder: its lower-cased name."""
    return folder.lower()


def storyline_of(folder):
    """The storyline a journey folder belongs to, by its chapter id's prefix."""
    if not JOURNEY_FOLDER.match(folder):
        raise PlanError(
            f"{JOURNEYS}/{folder} is not a chapter id (letters then digits, a C# namespace segment)")
    storyline = STORYLINE_BY_PREFIX.get(folder[0].lower())
    if storyline is None:
        raise PlanError(
            f"{JOURNEYS}/{folder} names no storyline: chapter ids start with "
            + ", ".join(sorted(STORYLINE_BY_PREFIX)))
    return storyline


def story_fixtures(root):
    """The story legs whose fixture exists in this tree, as (storyline, class)."""
    return [
        (storyline, fixture)
        for storyline, fixture in STORY_LEGS
        if (Path(root) / E2E / "Story" / (fixture + ".cs")).is_file()
    ]


CONTRACT_DECLARATION = re.compile(r"\bclass\s+\w+[^{;]*:[^{;]*\bIChapterContract\b")


def has_chapter_contracts(root):
    """Whether the tree implements any chapter contract (F6, #5364) outside the self-tests.

    A story run establishes every chapter it cannot run through its contract, so with no
    contract at all it fails at its first chapter by construction; the story legs wait for
    the first contract rather than turn every member pull request red before F6 lands.
    """
    base = Path(root) / E2E
    for path in base.rglob("*.cs"):
        parts = path.relative_to(base).parts
        if parts[0] in ("SelfTests", "bin", "obj"):
            continue
        if CONTRACT_DECLARATION.search(path.read_text(encoding="utf-8", errors="replace")):
            return True
    return False


def classify(path):
    """'journey:<folder>', 'neutral' or 'all' for one changed file."""
    if path.lower().endswith(".md"):
        return "neutral"
    prefix = JOURNEYS + "/"
    if path.startswith(prefix):
        rest = path[len(prefix):]
        # A file directly under Journeys/ is shared by every journey.
        return "journey:" + rest.split("/", 1)[0] if "/" in rest else "all"
    if path in LANE_FILES:
        return "all"
    if path.startswith((".github/", "docs/", "tools/", "videos/")):
        return "neutral"
    return "all"


def is_integration_branch(ref):
    return "/epic/" in (ref or "")


def isolated_leg(folder, jid, storyline):
    return {
        "name": "isolated-" + jid,
        "kind": "isolated",
        "journey": folder,
        "jid": jid,
        "storyline": storyline,
        "mode": "isolated",
        "viewport": "",
        "timeout": TIMEOUTS["isolated"],
        # The trailing dot keeps Journeys.A1 from selecting A10, A11 and A12.
        "filter": f"FullyQualifiedName~{NAMESPACE}.Journeys.{folder}.|FullyQualifiedName~{PENDING_GATE}",
        "expect": f"{NAMESPACE}.Journeys.{folder}.",
    }


def story_leg(storyline, fixture, narrow=False):
    name = "story-" + storyline + ("-narrow" if narrow else "")
    return {
        "name": name,
        "kind": "story",
        "journey": "",
        "jid": "",
        "storyline": storyline,
        "mode": "story",
        "viewport": "narrow" if narrow else "",
        "timeout": TIMEOUTS["story"],
        "filter": f"FullyQualifiedName~{NAMESPACE}.Story.{fixture}|FullyQualifiedName~{PENDING_GATE}",
        "expect": f"{NAMESPACE}.Story.{fixture}",
    }


def harness_leg():
    return {
        "name": "harness",
        "kind": "harness",
        "journey": "",
        "jid": "",
        "storyline": "",
        "mode": "isolated",
        "viewport": "",
        "timeout": TIMEOUTS["harness"],
        "filter": HARNESS_FILTER,
        "expect": NAMESPACE + ".",
    }


def plan(root, event, action="", base_ref="", changed=None):
    """The legs and provenance journeys for one run, plus the reasons, as a dict."""
    journeys = discover_journeys(root)
    stories = story_fixtures(root)
    by_folder = {folder: (folder, jid, storyline) for folder, jid, storyline in journeys}
    notes = []
    provenance = []

    if not stories:
        notes.append("No story fixture exists in this tree yet, so no story leg is planned.")
    elif not has_chapter_contracts(root):
        stories = []
        notes.append("No chapter contract (IChapterContract, F6 #5364) exists in this tree yet, so a story run "
                     "would fail at its first chapter by construction; no story leg is planned until one does.")

    def every_leg():
        return ([harness_leg()]
                + [isolated_leg(*journey) for journey in journeys]
                + [story_leg(storyline, fixture) for storyline, fixture in stories])

    touched = []
    if event == "pull_request":
        if changed is None:
            notes.append("The pull request's changed files could not be listed; running every leg.")
        else:
            classes = {classify(path) for path in changed}
            touched = sorted(
                {cls.split(":", 1)[1] for cls in classes if cls.startswith("journey:")} & set(by_folder),
                key=str.lower)

    if event == "pull_request" and action == "edited":
        legs = []
        notes.append("Body edit: no leg runs; the screenshot provenance check runs.")
    elif event == "schedule":
        legs = every_leg()
        legs += [story_leg(storyline, fixture, narrow=True) for storyline, fixture in stories
                 if storyline == "contoso"]
        notes.append("Nightly: every leg, and the Contoso story at 390x844.")
    elif event != "pull_request" or not is_integration_branch(base_ref):
        legs = every_leg()
        notes.append(f"{event} into '{base_ref or 'n/a'}': every leg.")
    elif changed is None or not changed:
        legs = every_leg()
        if changed is not None:
            notes.append("The pull request lists no changed file; running every leg.")
    elif "all" in {classify(path) for path in changed}:
        legs = every_leg()
        widening = sorted(path for path in changed if classify(path) == "all")
        notes.append("A change outside the journey folders runs every leg: " + ", ".join(widening[:10])
                     + (f" (and {len(widening) - 10} more)" if len(widening) > 10 else ""))
    else:
        legs = [harness_leg()] + [isolated_leg(*by_folder[folder]) for folder in touched]
        storylines = {by_folder[folder][2] for folder in touched}
        legs += [story_leg(storyline, fixture) for storyline, fixture in stories if storyline in storylines]
        notes.append("Journey-scoped pull request: " + (", ".join(touched) if touched else "no journey") + ".")

    if event == "pull_request" and is_integration_branch(base_ref) and touched:
        if action in ("edited", "reopened"):
            provenance = [by_folder[folder][1] for folder in touched]
        else:
            notes.append(
                "Screenshot provenance: after this run, paste the screenshot section "
                "(tools/Get-JourneyScreenshots.ps1) into the pull request body; the edit runs the check for "
                + ", ".join(by_folder[folder][1] for folder in touched) + ".")

    return {"legs": legs, "provenance": provenance, "notes": notes}


def plan_markdown(result):
    lines = ["## Explorer journeys plan", ""]
    if result["legs"]:
        lines += ["| Leg | Mode | Viewport | Filter |", "|---|---|---|---|"]
        for leg in result["legs"]:
            lines.append(f"| `{leg['name']}` | {leg['mode']} | {leg['viewport'] or 'desktop'} | "
                         f"`{leg['filter'].replace('|', chr(92) + '|')}` |")
    else:
        lines.append("No leg runs.")
    lines.append("")
    if result["provenance"]:
        lines.append("Screenshot provenance is checked for: " + ", ".join(f"`{j}`" for j in result["provenance"]))
        lines.append("")
    lines += [f"- {note}" for note in result["notes"]]
    return "\n".join(lines) + "\n"


def trx_counts(path):
    """(counters dict, list of test class names) from a .trx file."""
    tree = ET.parse(path)
    root = tree.getroot()
    namespace = {"t": root.tag[1:].split("}")[0]} if root.tag.startswith("{") else {}
    prefix = "t:" if namespace else ""
    counters = root.find(f".//{prefix}ResultSummary/{prefix}Counters", namespace)
    counts = {key: int(value) for key, value in (counters.attrib.items() if counters is not None else [])}
    classes = [method.get("className", "") for method in root.iterfind(f".//{prefix}TestMethod", namespace)]
    return counts, classes


def render_manifest(path):
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    entries = manifest.get("entries") or []
    lines = [
        f"### Screenshots: `{manifest.get('journey', Path(path).parent.name)}` "
        f"({len(entries)}, {manifest.get('viewport', '?')} {manifest.get('width', '?')}x{manifest.get('height', '?')})",
        "",
        "| # | Step | Substep | Persona | Title | Passed |",
        "|---|---|---|---|---|---|",
    ]
    for entry in sorted(entries, key=lambda item: item.get("index", 0)):
        title = str(entry.get("title", "")).replace("|", "\\|")
        lines.append(
            f"| {entry.get('index')} | `{entry.get('step')}` | `{entry.get('substep')}` | "
            f"{entry.get('persona', '')} | {title} | {'yes' if entry.get('passed') else '**no**'} |")
    return "\n".join(lines) + "\n"


def leg_summary(leg, trx, artifacts=None, expect=None):
    """The leg's summary markdown and a list of problems (non-empty fails the leg)."""
    problems = []
    lines = [f"## Leg `{leg}`", ""]
    if trx and Path(trx).is_file():
        counts, classes = trx_counts(trx)
        lines.append("| Total | Executed | Passed | Failed | Not executed |")
        lines.append("|---|---|---|---|---|")
        lines.append(f"| {counts.get('total', 0)} | {counts.get('executed', 0)} | {counts.get('passed', 0)} | "
                     f"{counts.get('failed', 0)} | {counts.get('notExecuted', 0)} |")
        if expect and not any(expect in name for name in classes):
            problems.append(
                f"the leg's filter selected no test in '{expect}': a leg that runs nothing it exists for "
                "is a vacuous green")
    else:
        problems.append(f"no test results at '{trx}': the leg ran no test")
        lines.append("No test results.")
    lines.append("")
    if artifacts is None:
        if problems:
            lines += [f"- **{problem}**" for problem in problems]
        return "\n".join(lines) + "\n", problems

    screenshots = Path(artifacts) / "screenshots"
    manifests = sorted(screenshots.glob("*/manifest.json")) if screenshots.is_dir() else []
    if manifests:
        for manifest in manifests:
            try:
                lines.append(render_manifest(manifest))
            except (ValueError, OSError) as error:
                lines.append(f"Manifest `{manifest.parent.name}` could not be read: {error}\n")
    else:
        lines.append("No screenshot manifest was written.\n")

    hits = Path(artifacts) / "coverage-hits.json"
    if hits.is_file():
        try:
            features = sorted(hit.get("feature", "") for hit in json.loads(hits.read_text(encoding="utf-8")).get("hits", []))
            lines.append(f"### Coverage hits ({len(features)})\n")
            lines.append(", ".join(f"`{feature}`" for feature in features) if features else "None.")
        except (ValueError, OSError, AttributeError) as error:
            lines.append(f"`coverage-hits.json` could not be read: {error}")
    else:
        lines.append("No `coverage-hits.json` (the coverage recorder is not in this tree, or the run wrote none).")
    lines.append("")
    if problems:
        lines += [f"- **{problem}**" for problem in problems]
    return "\n".join(lines) + "\n", problems


def probe_verdict(trx, exit_code):
    """Problems with the fail-closed probe; empty when storage loss failed the fixture."""
    problems = []
    if exit_code == 0:
        problems.append("dotnet test exited 0 with Azurite stopped")
    if not Path(trx).is_file():
        return problems + [f"no test results at '{trx}'"]
    counts, _ = trx_counts(trx)
    if counts.get("failed", 0) < 1:
        problems.append("no test failed with Azurite stopped")
    if counts.get("passed", 0) > 0:
        problems.append(f"{counts['passed']} test(s) passed with Azurite stopped")
    if counts.get("notExecuted", 0) > 0 or counts.get("inconclusive", 0) > 0:
        problems.append("a test went inconclusive or was not executed: the false green LATTICE_E2E_REQUIRED=1 forbids")
    return problems


def append(path_variable, text):
    target = os.environ.get(path_variable)
    if target:
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(text)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--root", default=".")
    p.add_argument("--event", required=True)
    p.add_argument("--action", default="")
    p.add_argument("--base-ref", default="")
    p.add_argument("--changed-files")
    s = sub.add_parser("leg-summary")
    s.add_argument("--leg", required=True)
    s.add_argument("--trx", required=True)
    s.add_argument("--artifacts")
    s.add_argument("--expect")
    r = sub.add_parser("probe")
    r.add_argument("--trx", required=True)
    r.add_argument("--exit-code", type=int, required=True)
    args = parser.parse_args(argv)

    if args.command == "plan":
        changed = None
        if args.changed_files and Path(args.changed_files).is_file():
            changed = [line.strip() for line in Path(args.changed_files).read_text(encoding="utf-8").splitlines()
                       if line.strip()]
        try:
            result = plan(args.root, args.event, args.action, args.base_ref, changed)
        except PlanError as error:
            print(f"::error::{error}")
            return 1
        print(plan_markdown(result))
        append("GITHUB_STEP_SUMMARY", plan_markdown(result))
        append("GITHUB_OUTPUT", "legs=" + json.dumps(result["legs"], separators=(",", ":")) + "\n")
        append("GITHUB_OUTPUT", "provenance=" + json.dumps(result["provenance"], separators=(",", ":")) + "\n")
        return 0

    if args.command == "leg-summary":
        markdown, problems = leg_summary(args.leg, args.trx, args.artifacts, args.expect)
        print(markdown)
        append("GITHUB_STEP_SUMMARY", markdown)
        if args.artifacts:
            # F3's JobSummary keeps the same copy: a job summary is not visible to an anonymous
            # viewer, the uploaded artifact is.
            Path(args.artifacts).mkdir(parents=True, exist_ok=True)
            with open(Path(args.artifacts) / "job-summary.md", "a", encoding="utf-8") as handle:
                handle.write(markdown)
        for problem in problems:
            print(f"::error::{problem}")
        return 1 if problems else 0

    problems = probe_verdict(args.trx, args.exit_code)
    for problem in problems:
        print(f"::error::Fail-closed probe: {problem}")
    if not problems:
        print("Fail-closed probe: with Azurite stopped the estate fixture FAILED, as LATTICE_E2E_REQUIRED=1 requires.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
