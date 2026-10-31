#!/usr/bin/env python3
"""Self-test for explorer-journeys-plan.py and its wiring in explorer-journeys.yml (#5363).

Pure Python, a few seconds. Run by the lane's `planner` job before it plans.
"""

import importlib.util
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

WORKFLOWS = Path(__file__).resolve().parent
REPOSITORY = WORKFLOWS.parents[1]
SCRIPT = WORKFLOWS / "explorer-journeys-plan.py"
WORKFLOW = WORKFLOWS / "explorer-journeys.yml"

spec = importlib.util.spec_from_file_location("explorer_journeys_plan", SCRIPT)
planner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(planner)

BUCKET = "test/epic/explorer-user-journeys"
TRX_NS = "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"


def names(result):
    return [leg["name"] for leg in result["legs"]]


class Tree:
    """A temporary repository root with journey folders and story fixtures."""

    def __init__(self, journeys=("A1", "A10", "A3", "T2"), stories=("ContosoStoryTests", "FabrikamStoryTests"),
                 contract=True, storylines=("A1", "T1")):
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)
        base = self.root / planner.JOURNEYS
        base.mkdir(parents=True)
        for journey in journeys:
            (base / journey).mkdir()
            (base / journey / f"{journey}Journey.cs").write_text("// journey\n")
        (base / "JourneyHelpers.cs").write_text("// a shared file, not a journey\n")
        (base / "bin").mkdir()
        story = self.root / planner.E2E / "Story"
        story.mkdir(parents=True)
        for fixture in stories:
            (story / f"{fixture}.cs").write_text("// story\n")
        tests = self.root / planner.E2E / "SelfTests"
        tests.mkdir(parents=True)
        (tests / "DelegateContract.cs").write_text("public sealed class DelegateContract : IChapterContract {}\n")
        if contract:
            contracts = self.root / planner.E2E / "Contracts"
            contracts.mkdir(parents=True)
            (contracts / "ChapterContract.cs").write_text(
                "public abstract class ChapterContract(string chapter) : IChapterContract\n{\n}\n")
            for chapter in storylines:
                if chapter == "A1":
                    (contracts / "A1Contract.cs").write_text(
                        "public sealed class A1Contract\n    : IChapterContract\n{\n}\n")
                else:
                    (contracts / f"{chapter}Contract.cs").write_text(
                        f"public sealed class {chapter}Contract() : ChapterContract(\"{chapter}\")\n{{\n}}\n")

    def close(self):
        self._temporary.cleanup()


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.tree = Tree()
        self.addCleanup(self.tree.close)

    def plan(self, event="pull_request", action="synchronize", base=BUCKET, changed=()):
        return planner.plan(self.tree.root, event, action, base, list(changed) if changed is not None else None)

    def test_journeys_are_discovered_from_the_directory(self):
        found = planner.discover_journeys(self.tree.root)
        self.assertEqual([("A1", "a1", "contoso"), ("A10", "a10", "contoso"), ("A3", "a3", "contoso"),
                          ("T2", "t2", "fabrikam")], found)

    def test_a_journey_only_pull_request_runs_its_leg_and_its_storyline(self):
        result = self.plan(changed=[f"{planner.JOURNEYS}/A3/A3Journey.cs"])
        self.assertEqual(["harness", "isolated-a3", "story-contoso"], names(result))
        self.assertEqual([], result["provenance"])
        self.assertTrue(any("Screenshot provenance" in note for note in result["notes"]))

    def test_a_fabrikam_journey_runs_the_fabrikam_story(self):
        result = self.plan(changed=[f"{planner.JOURNEYS}/T2/T2Journey.cs"])
        self.assertEqual(["harness", "isolated-t2", "story-fabrikam"], names(result))

    def test_an_explorer_change_runs_every_leg(self):
        for path in ("src/lattice.explorer/UI/Shell.razor", f"{planner.E2E}/Estate/JourneyEstate.cs",
                     "reference-architecture/hosts/Silo/Program.cs", "src/lattice.api.data/DataApi.cs",
                     ".github/workflows/explorer-journeys.yml", f"{planner.JOURNEYS}/JourneyHelpers.cs"):
            with self.subTest(path=path):
                result = self.plan(changed=[f"{planner.JOURNEYS}/A3/A3Journey.cs", path])
                self.assertEqual(["harness", "isolated-a1", "isolated-a10", "isolated-a3", "isolated-t2",
                                  "story-contoso", "story-fabrikam"], names(result))

    def test_markdown_docs_and_unrelated_workflows_run_only_the_harness(self):
        result = self.plan(changed=["docs/lattice.explorer/areas.md", f"{planner.E2E}/README.md",
                                    ".github/workflows/ci.yml", "tools/Get-JourneyScreenshots.ps1"])
        self.assertEqual(["harness"], names(result))

    def test_a_body_edit_runs_no_leg_and_checks_the_touched_journeys(self):
        result = self.plan(action="edited", changed=[f"{planner.JOURNEYS}/A3/x.cs", f"{planner.JOURNEYS}/T2/y.cs"])
        self.assertEqual([], names(result))
        self.assertEqual(["a3", "t2"], result["provenance"])
        self.assertEqual(["a3"], self.plan(action="reopened", changed=[f"{planner.JOURNEYS}/A3/x.cs"])["provenance"])

    def test_provenance_is_only_required_into_an_integration_branch(self):
        result = self.plan(action="edited", base="main", changed=[f"{planner.JOURNEYS}/A3/x.cs"])
        self.assertEqual([], result["provenance"])
        self.assertEqual([], names(result))

    def test_a_deleted_journey_is_neither_a_leg_nor_checked(self):
        result = self.plan(action="edited", changed=[f"{planner.JOURNEYS}/A9/gone.cs"])
        self.assertEqual([], result["provenance"])

    def test_the_bucket_to_main_pull_request_and_pushes_run_every_leg(self):
        every = ["harness", "isolated-a1", "isolated-a10", "isolated-a3", "isolated-t2", "story-contoso",
                 "story-fabrikam"]
        self.assertEqual(every, names(self.plan(base="main", changed=[f"{planner.JOURNEYS}/A3/x.cs"])))
        self.assertEqual(every, names(self.plan(base="release/1.2", changed=["src/x.cs"])))
        self.assertEqual(every, names(self.plan(event="push", action="", base="")))

    def test_the_nightly_run_adds_the_narrow_contoso_story(self):
        result = self.plan(event="schedule", action="", base="")
        self.assertEqual("story-contoso-narrow", names(result)[-1])
        narrow = result["legs"][-1]
        self.assertEqual(("story", "narrow", "contoso"), (narrow["mode"], narrow["viewport"], narrow["storyline"]))
        self.assertEqual(1, sum(1 for name in names(result) if name.endswith("-narrow")))

    def test_an_unlisted_or_empty_change_set_runs_every_leg(self):
        self.assertEqual(7, len(self.plan(changed=None)["legs"]))
        self.assertEqual(7, len(self.plan(changed=[])["legs"]))

    def test_isolated_filters_never_select_a_longer_chapter_id(self):
        leg = next(leg for leg in self.plan(changed=[f"{planner.JOURNEYS}/A1/x.cs"])["legs"] if leg["name"] == "isolated-a1")
        self.assertIn(".Journeys.A1.|", leg["filter"])
        self.assertEqual(f"{planner.NAMESPACE}.Journeys.A1.", leg["expect"])
        self.assertNotIn(f"{planner.NAMESPACE}.Journeys.A10.Fixture", [leg["expect"]])
        self.assertFalse(f"{planner.NAMESPACE}.Journeys.A10.Fixture".startswith(leg["expect"]))
        self.assertIn(planner.PENDING_GATE, leg["filter"])

    def test_leg_shapes(self):
        legs = {leg["name"]: leg for leg in self.plan(event="schedule", action="", base="")["legs"]}
        self.assertEqual(planner.HARNESS_FILTER, legs["harness"]["filter"])
        for excluded in ("TestCategory!=Journey", "TestCategory!=Story", "TestCategory!=CoverageGate", "TestCategory!=ContractConformance"):
            self.assertIn(excluded, legs["harness"]["filter"])
        self.assertEqual("isolated", legs["isolated-a3"]["mode"])
        self.assertEqual("story", legs["story-fabrikam"]["mode"])
        self.assertIn("Story.ContosoStoryTests", legs["story-contoso"]["filter"])
        for leg in legs.values():
            self.assertIsInstance(leg["timeout"], int)
            self.assertGreater(leg["timeout"], 0)
            self.assertTrue(leg["expect"])

    def test_story_legs_need_their_fixture(self):
        bare = Tree(stories=())
        self.addCleanup(bare.close)
        result = planner.plan(bare.root, "schedule", "", "", None)
        self.assertFalse([name for name in names(result) if name.startswith("story-")])
        self.assertTrue(any("No story fixture" in note for note in result["notes"]))

    def test_story_legs_wait_for_a_chapter_contract(self):
        bare = Tree(contract=False)
        self.addCleanup(bare.close)
        self.assertFalse(planner.has_chapter_contracts(bare.root), "a self-test contract does not count")
        result = planner.plan(bare.root, "schedule", "", "", None)
        self.assertFalse([name for name in names(result) if name.startswith("story-")])
        self.assertTrue(any("No chapter contract" in note for note in result["notes"]))
        self.assertTrue(planner.has_chapter_contracts(self.tree.root), "a multi-line class declaration counts")

    def test_a_story_leg_waits_for_its_own_storyline_contracts(self):
        contoso = Tree(storylines=("A1",))
        self.addCleanup(contoso.close)
        self.assertEqual({"contoso"}, planner.storylines_with_contracts(contoso.root))
        result = planner.plan(contoso.root, "schedule", "", "", None)
        self.assertIn("story-contoso", names(result))
        self.assertNotIn("story-fabrikam", names(result))
        self.assertTrue(any("No fabrikam chapter has a contract" in note for note in result["notes"]))
        self.assertEqual({"contoso", "fabrikam"}, planner.storylines_with_contracts(self.tree.root),
                         "a contract derived from the ChapterContract base counts")

    def test_a_folder_that_is_not_a_chapter_fails_the_plan(self):
        for folder in ("Z1", "a-3"):
            with self.subTest(folder=folder):
                odd = Tree(journeys=(folder,))
                self.addCleanup(odd.close)
                with self.assertRaises(planner.PlanError):
                    planner.plan(odd.root, "push", "", "", None)

    def test_no_journey_folder_yet_plans_the_harness_and_stories(self):
        empty = Tree(journeys=())
        self.addCleanup(empty.close)
        self.assertEqual(["harness", "story-contoso", "story-fabrikam"], names(planner.plan(empty.root, "push", "", "", None)))


class CommandLineTests(unittest.TestCase):
    def test_plan_writes_json_outputs(self):
        tree = Tree()
        self.addCleanup(tree.close)
        with tempfile.TemporaryDirectory() as scratch:
            output = Path(scratch) / "output"
            summary = Path(scratch) / "summary"
            changed = Path(scratch) / "changed.txt"
            changed.write_text(f"{planner.JOURNEYS}/A3/x.cs\n")
            code = planner_main(["plan", "--root", str(tree.root), "--event", "pull_request", "--action", "edited",
                                 "--base-ref", BUCKET, "--changed-files", str(changed)],
                                {"GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary)})
            self.assertEqual(0, code)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual([], json.loads(values["legs"]))
            self.assertEqual(["a3"], json.loads(values["provenance"]))
            self.assertIn("No leg runs.", summary.read_text())

    def test_a_missing_changed_files_list_runs_every_leg(self):
        tree = Tree()
        self.addCleanup(tree.close)
        with tempfile.TemporaryDirectory() as scratch:
            output = Path(scratch) / "output"
            code = planner_main(["plan", "--root", str(tree.root), "--event", "pull_request", "--action", "opened",
                                 "--base-ref", BUCKET, "--changed-files", str(Path(scratch) / "absent.txt")],
                                {"GITHUB_OUTPUT": str(output)})
            self.assertEqual(0, code)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual(7, len(json.loads(values["legs"])))

    def test_a_bad_journey_folder_fails_the_command(self):
        tree = Tree(journeys=("Q1",))
        self.addCleanup(tree.close)
        self.assertEqual(1, planner_main(["plan", "--root", str(tree.root), "--event", "push"], {}))


def planner_main(argv, environment):
    import contextlib
    import io
    import os
    saved = {key: os.environ.get(key) for key in ("GITHUB_OUTPUT", "GITHUB_STEP_SUMMARY")}
    try:
        for key in saved:
            os.environ.pop(key, None)
        os.environ.update(environment)
        # The command prints workflow commands (::error::); inside a CI job they would become
        # annotations on a green run, so the self-test swallows them.
        with contextlib.redirect_stdout(io.StringIO()):
            return planner.main(argv)
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def write_trx(path, counters, classes):
    tests = "".join(
        f'<UnitTest name="t{index}"><TestMethod className="{name}" name="Run" /></UnitTest>'
        for index, name in enumerate(classes))
    attributes = " ".join(f'{key}="{value}"' for key, value in counters.items())
    Path(path).write_text(
        f'<?xml version="1.0" encoding="utf-8"?><TestRun xmlns="{TRX_NS}"><TestDefinitions>{tests}</TestDefinitions>'
        f'<ResultSummary outcome="Completed"><Counters {attributes} /></ResultSummary></TestRun>')


class SummaryTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.root = Path(self._temporary.name)
        self.artifacts = self.root / "artifacts"
        (self.artifacts / "screenshots" / "a3").mkdir(parents=True)
        (self.artifacts / "screenshots" / "a3" / "manifest.json").write_text(json.dumps({
            "journey": "a3", "commit": "0" * 40, "viewport": "desktop", "width": 1440, "height": 900,
            "entries": [
                {"index": 2, "step": "create", "substep": "saved", "title": "Saved | done", "persona": "olivia", "passed": True},
                {"index": 1, "step": "create", "substep": "open", "title": "Opened", "persona": "olivia", "passed": False},
            ]}))
        (self.artifacts / "coverage-hits.json").write_text(json.dumps({"hits": [{"feature": "route:/data"}]}))
        self.trx = self.root / "leg.trx"

    def test_the_summary_prints_the_ordered_manifest_and_the_coverage_hits(self):
        write_trx(self.trx, {"total": 2, "executed": 2, "passed": 2, "failed": 0},
                  [f"{planner.NAMESPACE}.Journeys.A3.OrdersByStatus", planner.PENDING_GATE])
        markdown, problems = planner.leg_summary("isolated-a3", self.trx, self.artifacts, f"{planner.NAMESPACE}.Journeys.A3.")
        self.assertEqual([], problems)
        self.assertLess(markdown.index("| 1 | `create` | `open`"), markdown.index("| 2 | `create` | `saved`"))
        self.assertIn("Saved \\| done", markdown)
        self.assertIn("**no**", markdown)
        self.assertIn("`route:/data`", markdown)
        self.assertIn("| 2 | 2 | 2 | 0 |", markdown)

    def test_a_leg_that_ran_none_of_its_tests_fails(self):
        write_trx(self.trx, {"total": 1, "executed": 1, "passed": 1, "failed": 0}, [planner.PENDING_GATE])
        _, problems = planner.leg_summary("isolated-a3", self.trx, self.artifacts, f"{planner.NAMESPACE}.Journeys.A3.")
        self.assertEqual(1, len(problems))
        self.assertIn("vacuous", problems[0])

    def test_a_longer_chapter_id_does_not_satisfy_a_shorter_one(self):
        write_trx(self.trx, {"total": 1}, [f"{planner.NAMESPACE}.Journeys.A10.Fixture"])
        _, problems = planner.leg_summary("isolated-a1", self.trx, None, f"{planner.NAMESPACE}.Journeys.A1.")
        self.assertTrue(problems)

    def test_missing_results_fail_the_leg(self):
        _, problems = planner.leg_summary("harness", self.root / "absent.trx", self.artifacts, planner.NAMESPACE)
        self.assertTrue(problems)


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.trx = Path(self._temporary.name) / "probe.trx"

    def test_a_failed_fixture_is_the_expected_outcome(self):
        write_trx(self.trx, {"total": 1, "executed": 1, "passed": 0, "failed": 1}, ["Smoke"])
        self.assertEqual([], planner.probe_verdict(self.trx, 1))

    def test_a_passing_or_exit_zero_run_is_a_failure_of_the_probe(self):
        write_trx(self.trx, {"total": 1, "executed": 1, "passed": 1, "failed": 0}, ["Smoke"])
        self.assertEqual(3, len(planner.probe_verdict(self.trx, 0)))

    def test_an_inconclusive_run_is_the_false_green_the_probe_exists_for(self):
        write_trx(self.trx, {"total": 1, "executed": 0, "passed": 0, "failed": 0, "notExecuted": 1}, ["Smoke"])
        problems = planner.probe_verdict(self.trx, 1)
        self.assertTrue(any("inconclusive" in problem for problem in problems))

    def test_no_results_is_a_failure_of_the_probe(self):
        self.assertTrue(planner.probe_verdict(self.trx, 1))


def job(text, name):
    return re.search(rf"^  {re.escape(name)}:\n(.*?)(?=^  [\w-]+:\n|\Z)", text, re.MULTILINE | re.DOTALL).group(1)


class WiringTests(unittest.TestCase):
    """The lane calls what this file tests, the way this file tests it."""

    def setUp(self):
        self.text = WORKFLOW.read_text(encoding="utf-8")

    def test_the_planner_job_runs_both_self_tests_before_planning(self):
        block = job(self.text, "planner")
        plan = block.index("explorer-journeys-plan.py plan")
        self.assertLess(block.index("python3 .github/workflows/explorer-journeys-plan-selftest.py"), plan)
        self.assertLess(block.index("python3 .github/workflows/journey-screenshot-provenance-selftest.py"), plan)
        self.assertIn("legs: ${{ steps.plan.outputs.legs }}", block)
        self.assertIn("provenance: ${{ steps.plan.outputs.provenance }}", block)
        self.assertIn("pull-requests: read", block)

    def test_legs_are_a_fail_slow_matrix_over_the_plan(self):
        block = job(self.text, "legs")
        self.assertIn("include: ${{ fromJSON(needs.planner.outputs.legs) }}", block)
        self.assertIn("fail-fast: false", block)
        self.assertIn("timeout-minutes: ${{ matrix.timeout }}", block)
        self.assertIn("if: needs.planner.outputs.legs != '[]'", block)
        for variable in ("LATTICE_E2E_REQUIRED: '1'", "LATTICE_E2E_MODE: ${{ matrix.mode }}",
                         "LATTICE_E2E_VIEWPORT: ${{ matrix.viewport }}", "LATTICE_E2E_COMMIT:"):
            self.assertIn(variable, block)
        self.assertIn('--filter "$FILTER"', block)
        self.assertIn("leg-summary", block)
        self.assertIn("bash .github/workflows/install-playwright-browsers.sh", block)
        self.assertIn("name: journey-screenshots-${{ matrix.jid }}", block)
        self.assertIn("name: coverage-hits-${{ matrix.name }}", block)

    def test_every_upload_and_the_summary_survive_a_failed_leg(self):
        block = job(self.text, "legs")
        for step in re.findall(r"      - name: (Upload[^\n]*|Summarise the leg)\n(.*?)(?=      - |\Z)", block, re.DOTALL):
            self.assertIn("if: always()", step[1], step[0])

    def test_the_harness_leg_proves_a_stopped_azurite_fails(self):
        block = job(self.text, "legs")
        probe = block[block.index("Prove a stopped Azurite fails the leg"):]
        self.assertIn("matrix.kind == 'harness'", probe)
        self.assertIn("docker stop azurite", probe)
        self.assertIn("explorer-journeys-plan.py probe", probe)
        self.assertLess(probe.index("docker stop azurite"), probe.index("dotnet test"))
        # Actions runs `bash -e`: without this the expected failure ends the step before the verdict.
        self.assertLess(probe.index("set +e"), probe.index("dotnet test"))
        self.assertLess(probe.index("code=$?"), probe.index("set -e"))

    def test_the_coverage_gate_aggregates_every_leg(self):
        block = job(self.text, "coverage-gate")
        self.assertIn("needs: [plan, planner, legs]", block)
        self.assertIn("always()", block)
        self.assertIn("pattern: coverage-hits-*", block)
        self.assertIn('--filter "TestCategory=CoverageGate"', block)
        self.assertIn("LATTICE_E2E_COVERAGE_HITS", block)

    def test_the_provenance_job_is_pull_request_only(self):
        block = job(self.text, "provenance")
        condition = re.search(r"^    if: (.*)$", block, re.MULTILINE).group(1)
        self.assertIn("github.event_name == 'pull_request'", condition)
        self.assertIn("needs.planner.outputs.provenance != '[]'", condition)
        self.assertIn("python3 .github/workflows/journey-screenshot-provenance.py", block)
        self.assertIn("actions: read", block)

    def test_body_edits_trigger_the_lane_in_their_own_concurrency_group(self):
        self.assertIn("types: [opened, synchronize, reopened, edited]", self.text)
        self.assertIn("github.event.action == 'edited' && 'body' || 'code'", self.text)
        self.assertIn("schedule:", self.text)

    def test_both_triggers_watch_the_lane_scripts(self):
        for name in ("explorer-journeys-plan*.py", "journey-screenshot-provenance*.py",
                     "install-playwright-browsers.sh"):
            self.assertEqual(2, self.text.count(f"- '.github/workflows/{name}'"), name)
        for path in planner.LANE_FILES:
            self.assertTrue((REPOSITORY / path).is_file(), path)

    def test_the_planner_contract_matches_the_test_tree(self):
        e2e = REPOSITORY / planner.E2E
        if not e2e.is_dir():
            self.skipTest("no e2e project in this checkout")
        self.assertTrue((e2e / "Smoke" / "EstateSmokeTests.cs").is_file(),
                        "the fail-closed probe filters on Smoke.EstateSmokeTests")
        planner.discover_journeys(REPOSITORY)
        # The names the legs filter on are F3's (#5361): every one must still exist as spelled.
        gate = (e2e / "FeatureCoverage" / "CoverageGateTests.cs").read_text(encoding="utf-8")
        self.assertIn('[Category("CoverageGate")]', gate)
        self.assertIn("namespace " + planner.NAMESPACE + ".FeatureCoverage;", gate)
        pending = (e2e / "Pending" / "PendingWrapperGateTests.cs").read_text(encoding="utf-8")
        self.assertIn("namespace " + planner.NAMESPACE + ".Pending;", pending)
        self.assertEqual(planner.NAMESPACE + ".Pending.PendingWrapperGateTests", planner.PENDING_GATE)
        chapter = (e2e / "Story" / "JourneyChapter.cs").read_text(encoding="utf-8")
        self.assertIn('[Category("Journey")]', chapter)
        for storyline, fixture in planner.STORY_LEGS:
            source = (e2e / "Story" / (fixture + ".cs")).read_text(encoding="utf-8")
            self.assertIn('[Category("Story")]', source, fixture)
            self.assertIn("namespace " + planner.NAMESPACE + ".Story;", source, fixture)
        modes = (e2e / "Story" / "JourneyRunModes.cs").read_text(encoding="utf-8")
        self.assertIn('"LATTICE_E2E_MODE"', modes)
        hits = (e2e / "FeatureCoverage" / "CoverageHitsDocument.cs").read_text(encoding="utf-8")
        self.assertIn('"LATTICE_E2E_COVERAGE_HITS"', hits)
        self.assertIn('"coverage-hits.json"', hits)

    def test_the_leg_summary_is_also_kept_as_an_artifact(self):
        with tempfile.TemporaryDirectory() as scratch:
            trx = Path(scratch) / "leg.trx"
            write_trx(trx, {"total": 1, "executed": 1, "passed": 1}, [planner.NAMESPACE + ".Smoke.X"])
            artifacts = Path(scratch) / "artifacts"
            code = planner_main(["leg-summary", "--leg", "harness", "--trx", str(trx), "--artifacts", str(artifacts),
                                 "--expect", planner.NAMESPACE + "."], {})
            self.assertEqual(0, code)
            self.assertIn("## Leg `harness`", (artifacts / "job-summary.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() and result.testsRun > 0 else 1)
