#!/usr/bin/env python3
"""Self-test for explorer-journeys-plan.py and its wiring in explorer-journeys.yml (#5363).

Pure Python, a few seconds. Run by the lane's `planner` job before it plans.
"""

import importlib.util
import json
import os
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
# A landing pull request's head is an integration branch; any other head is an ordinary branch.
TOPIC = "fix/explorer-something"
TRX_NS = "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"


def names(result):
    return [leg["name"] for leg in result["legs"]]


class Tree:
    """A temporary repository root with journey folders and story fixtures."""

    def __init__(self, journeys=("A1", "A10", "A3", "T2"), stories=("ContosoStoryTests", "FabrikamStoryTests"),
                 contract=True, storylines=("A1", "T2"), conformance=("Contoso", "Fabrikam"), mentions=None):
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)
        base = self.root / planner.JOURNEYS
        base.mkdir(parents=True)
        mentions = mentions or {}
        for journey in journeys:
            (base / journey).mkdir()
            (base / journey / f"{journey}Journey.cs").write_text("// journey\n" + mentions.get(journey, ""))
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
        contracts = self.root / planner.CONTRACTS
        contracts.mkdir(parents=True, exist_ok=True)
        (contracts / "ContractConformanceFixture.cs").write_text("// the shared fixture, not a shard\n")
        for storyline in conformance:
            (contracts / f"{storyline}ContractConformanceTests.cs").write_text("// shard\n")

    def write(self, path, text=""):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def close(self):
        self._temporary.cleanup()


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.tree = Tree()
        self.addCleanup(self.tree.close)

    def plan(self, event="pull_request", action="synchronize", base=BUCKET, changed=(), labels=(), label="",
             head=None):
        if head is None:
            # A pull request into the bucket comes from a member branch; one into anything
            # else is a landing pull request unless a test names an ordinary head.
            head = f"{BUCKET}-a3-item" if planner.is_integration_branch(base) else (BUCKET if base else "")
        return planner.plan(self.tree.root, event, action, base, list(changed) if changed is not None else None,
                            list(labels), label, head)

    EVERY = ["harness", "isolated-a1", "isolated-a10", "isolated-a3", "isolated-t2", "story-contoso",
             "story-fabrikam"]
    EVERY_ISOLATED = ["harness", "isolated-a1", "isolated-a10", "isolated-a3", "isolated-t2"]

    def test_journeys_are_discovered_from_the_directory(self):
        found = planner.discover_journeys(self.tree.root)
        self.assertEqual([("A1", "a1", "contoso"), ("A10", "a10", "contoso"), ("A3", "a3", "contoso"),
                          ("T2", "t2", "fabrikam")], found)

    def test_conformance_shards_are_discovered_from_the_directory(self):
        self.assertEqual(["Contoso", "Fabrikam"], planner.conformance_shards(self.tree.root))

    def test_a_member_pull_request_runs_only_its_own_isolated_leg(self):
        # User decision on #5401 (2026-11-01): stories and conformance are advisory, at landing only.
        result = self.plan(changed=[f"{planner.JOURNEYS}/A3/A3Journey.cs"])
        self.assertEqual(["harness", "isolated-a3"], names(result))
        self.assertEqual([], result["conformance"])
        self.assertEqual([], result["provenance"])
        self.assertTrue(any("Screenshot provenance" in note for note in result["notes"]))
        self.assertTrue(any("no story leg and no contract conformance" in note for note in result["notes"]))

    def test_a_fabrikam_member_pull_request_runs_no_story(self):
        result = self.plan(changed=[f"{planner.JOURNEYS}/T2/T2Journey.cs"])
        self.assertEqual(["harness", "isolated-t2"], names(result))
        self.assertEqual([], result["conformance"])

    def test_a_shared_change_on_a_member_runs_every_isolated_leg_and_no_story(self):
        for path in ("src/lattice.explorer/UI/Shell.razor", "src/lattice.explorer/Core/Catalog/X.cs",
                     f"{planner.E2E}/Estate/JourneyEstate.cs", "reference-architecture/hosts/Silo/Program.cs",
                     "src/lattice.api.data/DataApi.cs", ".github/workflows/explorer-journeys.yml",
                     f"{planner.JOURNEYS}/JourneyHelpers.cs", "test/shared/Orleans.Lattice.Testing/X.cs",
                     f"{planner.AREAS}/Root.razor"):
            with self.subTest(path=path):
                result = self.plan(changed=[f"{planner.JOURNEYS}/A3/A3Journey.cs", path])
                self.assertEqual(self.EVERY_ISOLATED, names(result))
                self.assertEqual([], result["conformance"])

    def test_an_area_file_feeds_the_chapters_naming_its_area(self):
        tree = Tree(mentions={"A1": 'Goto("/schema");\n', "A10": "new SchemaAreaPage(page);\n",
                              "A3": "// Schemas and Schemata do not name the area\n"})
        self.addCleanup(tree.close)
        area_file = f"{planner.AREAS}/Schema/SchemaRuleBuilder.razor"
        tree.write(area_file, "<div>rules</div>\n")
        tree.write(f"{planner.AREAS}/Cluster/ClusterPage.razor", "<div>cluster</div>\n")
        result = planner.plan(tree.root, "pull_request", "synchronize", BUCKET,
                              [f"{planner.JOURNEYS}/T2/T2Journey.cs", area_file], [], "")
        self.assertEqual(["harness", "isolated-a1", "isolated-a10", "isolated-t2"], names(result))
        self.assertEqual([], result["conformance"])
        self.assertTrue(any("feeds the chapters naming the Schema area: A1, A10" in n for n in result["notes"]))

    def test_an_area_no_chapter_names_runs_only_the_touched_legs(self):
        tree = Tree()
        self.addCleanup(tree.close)
        area_file = f"{planner.AREAS}/Backups/BackupList.razor"
        tree.write(area_file)
        result = planner.plan(tree.root, "pull_request", "opened", BUCKET, [area_file], [], "")
        self.assertEqual(["harness"], names(result))
        self.assertTrue(any("none in this tree" in note for note in result["notes"]))

    def test_an_area_file_used_outside_its_area_is_shared(self):
        tree = Tree(mentions={"A1": 'Goto("/schema");\n'})
        self.addCleanup(tree.close)
        area_file = f"{planner.AREAS}/Cluster/ClusterTreeCatalog.cs"
        tree.write(area_file, "class ClusterTreeCatalog {}\n")
        tree.write(f"{planner.AREAS}/Schema/SchemaTreeCatalog.cs", "class S : ClusterTreeCatalog {}\n")
        result = planner.plan(tree.root, "pull_request", "opened", BUCKET, [area_file], [], "")
        self.assertEqual(self.EVERY_ISOLATED, names(result))
        # Used only inside its own area folder, it is not shared.
        tree.write(f"{planner.AREAS}/Schema/SchemaTreeCatalog.cs", "class S {}\n")
        tree.write(f"{planner.AREAS}/Cluster/ClusterPage.razor", "@inject ClusterTreeCatalog Catalog\n")
        self.assertFalse(planner.area_file_is_shared(tree.root, area_file, "Cluster"))
        result = planner.plan(tree.root, "pull_request", "opened", BUCKET, [area_file], [], "")
        self.assertEqual(["harness"], names(result))

    def test_markdown_docs_unrelated_workflows_and_other_test_projects_run_only_the_harness(self):
        result = self.plan(changed=["docs/lattice.explorer/areas.md", f"{planner.E2E}/README.md",
                                    ".github/workflows/ci.yml", "tools/Get-JourneyScreenshots.ps1",
                                    "test/lattice.explorer/UI/Areas/Schema/SchemaReadsTests.cs",
                                    "test/lattice.explorer.uitests/X.cs"])
        self.assertEqual(["harness"], names(result))
        self.assertEqual([], result["conformance"])

    def test_a_body_edit_runs_no_leg_and_checks_the_touched_journeys(self):
        result = self.plan(action="edited", changed=[f"{planner.JOURNEYS}/A3/x.cs", f"{planner.JOURNEYS}/T2/y.cs"])
        self.assertEqual([], names(result))
        self.assertEqual([], result["conformance"])
        self.assertEqual(["a3", "t2"], result["provenance"])
        self.assertEqual(["a3"], self.plan(action="reopened", changed=[f"{planner.JOURNEYS}/A3/x.cs"])["provenance"])
        landing = self.plan(action="edited", base="main", labels=[planner.FULL_LABEL])
        self.assertEqual(([], []), (names(landing), landing["conformance"]))

    def test_provenance_is_only_required_into_an_integration_branch(self):
        result = self.plan(action="edited", base="main", changed=[f"{planner.JOURNEYS}/A3/x.cs"])
        self.assertEqual([], result["provenance"])
        self.assertEqual([], names(result))

    def test_a_deleted_journey_is_neither_a_leg_nor_checked(self):
        result = self.plan(action="edited", changed=[f"{planner.JOURNEYS}/A9/gone.cs"])
        self.assertEqual([], result["provenance"])

    def test_the_landing_pull_request_pushes_and_dispatch_run_the_full_set(self):
        cases = {
            "landing pull request": self.plan(base="main", changed=[f"{planner.JOURNEYS}/A3/x.cs"]),
            "release landing pull request": self.plan(base="release/1.2", changed=["src/x.cs"]),
            "bucket push": self.plan(event="push", action="", base=""),
            "workflow_dispatch": self.plan(event="workflow_dispatch", action="", base=""),
        }
        for case, result in cases.items():
            with self.subTest(case=case):
                self.assertEqual(self.EVERY, names(result))
                self.assertEqual(["Contoso", "Fabrikam"], result["conformance"])

    def test_an_ordinary_pull_request_into_main_is_gated_like_a_member(self):
        # After phase 1 landed (#5624), only a landing pull request runs the stories: an ordinary
        # pull request into main or a release line runs its own isolated legs, no story, no shard.
        for base in ("main", "release/1.2"):
            with self.subTest(base=base):
                result = self.plan(base=base, head=TOPIC, changed=[f"{planner.JOURNEYS}/A3/A3Journey.cs"])
                self.assertEqual(["harness", "isolated-a3"], names(result))
                self.assertEqual([], result["conformance"])
                self.assertTrue(any("no story leg and no contract conformance" in n for n in result["notes"]))
                shared = self.plan(base=base, head=TOPIC, changed=["src/lattice.explorer/UI/Shell.razor"])
                self.assertEqual(self.EVERY_ISOLATED, names(shared))
                self.assertEqual([], shared["conformance"])
                docs = self.plan(base=base, head=TOPIC, changed=["docs/lattice.explorer/areas.md"])
                self.assertEqual(["harness"], names(docs))
                self.assertEqual(self.EVERY_ISOLATED, names(self.plan(base=base, head=TOPIC, changed=None)))
                labelled = self.plan(base=base, head=TOPIC, labels=[planner.FULL_LABEL],
                                     changed=[f"{planner.JOURNEYS}/A3/x.cs"])
                self.assertEqual(self.EVERY, names(labelled))

    def test_only_an_integration_head_into_a_non_integration_base_is_a_landing(self):
        self.assertTrue(planner.is_landing("pull_request", "main", BUCKET))
        self.assertTrue(planner.is_landing("pull_request", "release/1.2", "docs/epic/guide"))
        self.assertFalse(planner.is_landing("pull_request", "main", TOPIC))
        self.assertFalse(planner.is_landing("pull_request", BUCKET, f"{BUCKET}-a3-item"))
        self.assertFalse(planner.is_landing("pull_request", "main", ""))
        self.assertFalse(planner.is_landing("push", "", BUCKET))

    def test_a_member_pull_request_labelled_journeys_full_runs_the_full_set(self):
        result = self.plan(changed=[f"{planner.JOURNEYS}/A3/x.cs"], labels=["ci", planner.FULL_LABEL])
        self.assertEqual(self.EVERY, names(result))
        self.assertEqual(["Contoso", "Fabrikam"], result["conformance"])
        unlabelled = self.plan(changed=[f"{planner.JOURNEYS}/A3/x.cs"], labels=["ci", "journeys-fuller"])
        self.assertEqual(["harness", "isolated-a3"], names(unlabelled))

    def test_adding_the_label_runs_the_full_set_and_any_other_label_runs_nothing(self):
        added = self.plan(action="labeled", label=planner.FULL_LABEL, labels=[planner.FULL_LABEL],
                          changed=[f"{planner.JOURNEYS}/A3/x.cs"])
        self.assertEqual(self.EVERY, names(added))
        self.assertEqual(["Contoso", "Fabrikam"], added["conformance"])
        self.assertEqual([], added["provenance"])
        other = self.plan(action="labeled", label="ci", labels=["ci"], changed=[f"{planner.JOURNEYS}/A3/x.cs"])
        self.assertEqual(([], [], []), (names(other), other["conformance"], other["provenance"]))
        self.assertFalse(any("Screenshot provenance" in note for note in other["notes"]))

    def test_the_nightly_run_adds_the_narrow_contoso_story(self):
        result = self.plan(event="schedule", action="", base="")
        self.assertEqual("story-contoso-narrow", names(result)[-1])
        self.assertEqual(["Contoso", "Fabrikam"], result["conformance"])
        narrow = result["legs"][-1]
        self.assertEqual(("story", "narrow", "contoso"), (narrow["mode"], narrow["viewport"], narrow["storyline"]))
        self.assertEqual(1, sum(1 for name in names(result) if name.endswith("-narrow")))

    def test_an_unlisted_or_empty_change_set_runs_every_isolated_leg(self):
        for changed in (None, []):
            with self.subTest(changed=changed):
                result = self.plan(changed=changed)
                self.assertEqual(self.EVERY_ISOLATED, names(result))
                self.assertEqual([], result["conformance"])
        self.assertEqual(self.EVERY, names(self.plan(base="main", changed=None)))

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
            # runner-overflow.py sends the longest legs to the pool first, by `estimate`.
            self.assertEqual(leg["timeout"], leg["estimate"], leg["name"])
            self.assertNotIn("runsOn", leg, "the planner plans; the lane's routing step routes")

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
        self.assertEqual(["harness", "contract-a1", "contract-t2", "story-contoso", "story-fabrikam"],
                         names(planner.plan(empty.root, "push", "", "", None)),
                         "with no journey yet, every chapter with a contract is story-only")


class ContractLegTests(unittest.TestCase):
    """Story-only chapters (a contract, no journey) run in isolation through their conformance case (#5660)."""

    OUTLINE = (
        "Chapter(\"A1\", 1, \"Day one\"),\n"
        "Chapter(\"A3\", 2, \"Schema \\\"rules\\\"\", \"A1\"),\n"
        "Chapter(\"U2\", 3, \"A schema owner's week\", \"A3\"),\n"
        "Chapter(\"C1\", 4, \"Incident\", \"A1\"),\n"
        "Chapter(\"C10\", 5, \"Later\", \"C1\"),\n"
        "Chapter(\"T5\", 6, \"Tenants\"),\n")

    def setUp(self):
        self.tree = Tree(journeys=("A1", "A3"), storylines=("A1",))
        self.addCleanup(self.tree.close)
        for chapter in ("A3", "U2", "C1", "C10"):
            self.tree.write(f"{planner.CONTRACTS}/Contoso/{chapter}Contract.cs",
                            f"public sealed class {chapter}Contract() : ChapterContract(\"{chapter}\")\n{{\n}}\n")
        self.tree.write(f"{planner.CONTRACTS}/Fabrikam/T5Contract.cs",
                        "public sealed class T5Contract() : ChapterContract(\"T5\")\n{\n}\n")
        self.tree.write(planner.STORY_OUTLINE, self.OUTLINE)

    def plan(self, event="pull_request", action="synchronize", base=BUCKET, changed=(), labels=(), label=""):
        head = f"{BUCKET}-item" if planner.is_integration_branch(base) else (BUCKET if base else "")
        return planner.plan(self.tree.root, event, action, base, list(changed) if changed is not None else None,
                            list(labels), label, head)

    CONTRACTS = ["contract-c1", "contract-c10", "contract-t5", "contract-u2"]

    def contract_names(self, result):
        return [name for name in names(result) if name.startswith("contract-")]

    def test_story_only_chapters_are_discovered_from_the_tree(self):
        found = planner.story_only_contracts(self.tree.root, planner.discover_journeys(self.tree.root),
                                             planner.conformance_shards(self.tree.root), [])
        self.assertEqual([("C1", "contoso", "Contoso"), ("C10", "contoso", "Contoso"), ("T5", "fabrikam", "Fabrikam"),
                          ("U2", "contoso", "Contoso")], found, "A1 and A3 have journeys; the base class is no chapter")

    def test_a_chapter_whose_journey_lands_loses_its_contract_leg(self):
        self.tree.write(f"{planner.JOURNEYS}/U2/U2Journey.cs", "// journey\n")
        result = self.plan(event="workflow_dispatch", action="", base="")
        self.assertNotIn("contract-u2", names(result))
        self.assertIn("isolated-u2", names(result))

    def test_a_storyline_without_a_conformance_fixture_has_no_contract_leg(self):
        tree = Tree(journeys=(), storylines=("A1", "T2"), conformance=("Contoso",))
        self.addCleanup(tree.close)
        result = planner.plan(tree.root, "push", "", "", None)
        self.assertIn("contract-a1", names(result))
        self.assertNotIn("contract-t2", names(result))
        self.assertTrue(any("T2 has a contract and no journey, but no fabrikam" in n for n in result["notes"]))

    def test_landing_nightly_dispatch_push_and_journeys_full_run_every_contract_leg(self):
        cases = {
            "landing": self.plan(base="main", changed=["docs/x.md"]),
            "nightly": self.plan(event="schedule", action="", base=""),
            "dispatch": self.plan(event="workflow_dispatch", action="", base=""),
            "push": self.plan(event="push", action="", base=""),
            "member labelled": self.plan(changed=["docs/x.md"], labels=[planner.FULL_LABEL]),
            "label added": self.plan(action="labeled", label=planner.FULL_LABEL, labels=[planner.FULL_LABEL]),
        }
        for case, result in cases.items():
            with self.subTest(case=case):
                self.assertEqual(self.CONTRACTS, self.contract_names(result))
                self.assertEqual(["harness", "isolated-a1", "isolated-a3"] + self.CONTRACTS, names(result)[:7])

    def test_a_body_edit_or_another_label_runs_no_contract_leg(self):
        self.assertEqual([], names(self.plan(action="edited", changed=[f"{planner.CONTRACTS}/Contoso/U2Contract.cs"])))
        self.assertEqual([], names(self.plan(action="labeled", label="ci", labels=["ci"])))

    def test_a_shared_change_on_a_pull_request_runs_every_contract_leg(self):
        for path in (f"{planner.CONTRACTS}/Contoso/U2Contract.cs", f"{planner.CONTRACTS}/ContractCalls.cs",
                     f"{planner.E2E}/Estate/JourneyEstate.cs", "src/lattice.api.backup/BackupApi.cs",
                     "src/lattice.explorer/Core/X.cs", "test/shared/Orleans.Lattice.Testing/X.cs",
                     ".github/workflows/explorer-journeys-plan.py", "src/lattice/Unknown.cs"):
            for base, head in ((BUCKET, f"{BUCKET}-item"), ("main", TOPIC)):
                with self.subTest(path=path, base=base):
                    result = planner.plan(self.tree.root, "pull_request", "synchronize", base, [path], [], "", head)
                    self.assertEqual(self.CONTRACTS, self.contract_names(result))
                    self.assertEqual([], result["conformance"], "the conformance shard stays advisory")

    def test_journey_docs_and_unrelated_changes_run_no_contract_leg(self):
        result = self.plan(changed=[f"{planner.JOURNEYS}/A3/A3Journey.cs", "docs/lattice.explorer/areas.md",
                                    f"{planner.CONTRACTS}/README.md", ".github/workflows/ci.yml",
                                    "test/lattice/X.cs"])
        self.assertEqual(["harness", "isolated-a3"], names(result))

    def test_an_unlisted_or_empty_change_set_runs_every_contract_leg(self):
        for changed in (None, []):
            with self.subTest(changed=changed):
                self.assertEqual(self.CONTRACTS, self.contract_names(self.plan(changed=changed)))

    def test_an_area_file_runs_the_contracts_whose_consume_closure_names_it(self):
        area_file = f"{planner.AREAS}/Schema/SchemaRuleBuilder.razor"
        self.tree.write(area_file, "<div>rules</div>\n")
        # A3's contract names the area; U2 consumes A3, C1 and T5 do not.
        self.tree.write(f"{planner.CONTRACTS}/Contoso/A3Contract.cs",
                        "public sealed class A3Contract() : ChapterContract(\"A3\")\n{\n  // the /schema rules\n}\n")
        self.assertEqual(["contract-u2"], self.contract_names(self.plan(changed=[area_file])))
        # C10 consumes C1, so naming the area in C1's contract runs both.
        self.tree.write(f"{planner.CONTRACTS}/Contoso/C1Contract.cs",
                        "public sealed class C1Contract() : ChapterContract(\"C1\")\n{\n  // Schema\n}\n")
        self.assertEqual(["contract-c1", "contract-c10", "contract-u2"],
                         self.contract_names(self.plan(changed=[area_file])))

    def test_an_area_a_shared_contract_helper_names_runs_every_contract_leg(self):
        area_file = f"{planner.AREAS}/Backups/BackupList.razor"
        self.tree.write(area_file)
        self.assertEqual([], self.contract_names(self.plan(changed=[area_file])))
        self.tree.write(f"{planner.CONTRACTS}/ContosoBackups.cs", "static class ContosoBackups { /* Backups */ }\n")
        self.assertEqual(self.CONTRACTS, self.contract_names(self.plan(changed=[area_file])))

    def test_a_chapter_missing_from_the_outline_consumes_its_whole_storyline(self):
        area_file = f"{planner.AREAS}/Schema/SchemaRuleBuilder.razor"
        self.tree.write(area_file)
        self.tree.write(f"{planner.CONTRACTS}/Contoso/A3Contract.cs",
                        "public sealed class A3Contract() : ChapterContract(\"A3\")\n{\n  // Schema\n}\n")
        self.tree.write(planner.STORY_OUTLINE, "// unreadable\n")
        self.assertEqual(["contract-c1", "contract-c10", "contract-u2"],
                         self.contract_names(self.plan(changed=[area_file])))

    def test_a_contract_leg_runs_one_conformance_case_and_never_a_longer_chapter_id(self):
        legs = {leg["name"]: leg for leg in self.plan(event="workflow_dispatch", action="", base="")["legs"]}
        leg = legs["contract-c1"]
        fixture = f"{planner.NAMESPACE}.Contracts.ContosoContractConformanceTests"
        self.assertEqual(fixture, leg["expect"])
        self.assertEqual(("contract", "isolated", "contoso", ""), (leg["kind"], leg["mode"], leg["storyline"], leg["jid"]))
        self.assertEqual(
            f"(FullyQualifiedName~{fixture}.&FullyQualifiedName~_C1{planner.CONFORMANCE_CASE_SUFFIX})"
            f"|FullyQualifiedName~{planner.PENDING_GATE}", leg["filter"])
        self.assertNotIn(f"_C1{planner.CONFORMANCE_CASE_SUFFIX}", f"{fixture}.20_C10{planner.CONFORMANCE_CASE_SUFFIX}")
        self.assertIn("Fabrikam", legs["contract-t5"]["expect"])
        self.assertEqual(planner.TIMEOUTS["contract"], leg["timeout"])
        self.assertEqual(leg["timeout"], leg["estimate"])

    def test_the_outline_closure_follows_consumes_transitively(self):
        graph = planner.outline_graph(self.tree.root)
        self.assertEqual(["A1"], graph["A3"])
        self.assertEqual({"C10", "C1", "A1"}, planner.consume_closure(graph, "C10"))
        self.assertIsNone(planner.consume_closure(graph, "Z9"))


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
            self.assertEqual([], json.loads(values["conformance"]))
            self.assertEqual(["a3"], json.loads(values["provenance"]))
            self.assertIn("No leg runs.", summary.read_text())

    def test_the_labels_argument_drives_the_full_set(self):
        tree = Tree()
        self.addCleanup(tree.close)
        with tempfile.TemporaryDirectory() as scratch:
            changed = Path(scratch) / "changed.txt"
            changed.write_text(f"{planner.JOURNEYS}/A3/x.cs\n")
            for labels, legs, conformance in (('["ci","journeys-full"]', 7, ["Contoso", "Fabrikam"]),
                                              ('["ci"]', 2, []), ("null", 2, []), ("", 2, [])):
                with self.subTest(labels=labels):
                    output = Path(scratch) / f"output-{legs}-{len(labels)}"
                    code = planner_main(["plan", "--root", str(tree.root), "--event", "pull_request", "--action",
                                         "synchronize", "--base-ref", BUCKET, "--labels", labels, "--label", "",
                                         "--changed-files", str(changed)], {"GITHUB_OUTPUT": str(output)})
                    self.assertEqual(0, code)
                    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
                    self.assertEqual(legs, len(json.loads(values["legs"])))
                    self.assertEqual(conformance, json.loads(values["conformance"]))
            self.assertEqual(1, planner_main(["plan", "--root", str(tree.root), "--event", "pull_request",
                                              "--labels", "not json"], {}))

    def test_the_head_ref_argument_tells_a_landing_from_an_ordinary_pull_request_into_main(self):
        tree = Tree()
        self.addCleanup(tree.close)
        with tempfile.TemporaryDirectory() as scratch:
            changed = Path(scratch) / "changed.txt"
            changed.write_text(f"{planner.JOURNEYS}/A3/x.cs\n")
            for head, legs, conformance in ((BUCKET, 7, ["Contoso", "Fabrikam"]), (TOPIC, 2, []), ("", 2, [])):
                with self.subTest(head=head):
                    output = Path(scratch) / f"output-head-{legs}-{len(head)}"
                    code = planner_main(["plan", "--root", str(tree.root), "--event", "pull_request", "--action",
                                         "synchronize", "--base-ref", "main", "--head-ref", head,
                                         "--changed-files", str(changed)], {"GITHUB_OUTPUT": str(output)})
                    self.assertEqual(0, code)
                    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
                    self.assertEqual(legs, len(json.loads(values["legs"])))
                    self.assertEqual(conformance, json.loads(values["conformance"]))

    def test_the_legs_file_holds_the_legs_the_router_routes(self):
        tree = Tree()
        self.addCleanup(tree.close)
        with tempfile.TemporaryDirectory() as scratch:
            output = Path(scratch) / "output"
            legs_file = Path(scratch) / "legs.json"
            code = planner_main(["plan", "--root", str(tree.root), "--event", "workflow_dispatch",
                                 "--legs-file", str(legs_file)], {"GITHUB_OUTPUT": str(output)})
            self.assertEqual(0, code)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            legs = json.loads(legs_file.read_text(encoding="utf-8"))
            self.assertEqual(json.loads(values["legs"]), legs)
            self.assertEqual(7, len(legs))

            # The lane routes that file as CI routes its matrix, with ubuntu-24.04 as the
            # hosted label: unset RUNNER_POOL keeps every leg hosted.
            env = {key: value for key, value in os.environ.items() if key != "RUNNER_POOL"}
            subprocess.run([sys.executable, str(REPOSITORY / ".github/workflows/runner-overflow.py"),
                            "--matrix", str(legs_file), "--hosted", "ubuntu-24.04"],
                           check=True, capture_output=True, env=env)
            routed = json.loads(legs_file.read_text(encoding="utf-8"))
            self.assertEqual([leg["name"] for leg in legs], [leg["name"] for leg in routed])
            self.assertTrue(all(json.loads(leg["runsOn"]) == "ubuntu-24.04" and leg["pool"] is False
                                for leg in routed))

    def test_routing_sends_the_longest_legs_to_the_pool_first(self):
        overflow = load_router()
        tree = Tree()
        self.addCleanup(tree.close)
        legs = planner.plan(tree.root, "workflow_dispatch", "", "", None)["legs"]
        pool = ["self-hosted", "lattice-pool"]
        hosted, pooled = overflow.route([legs], pool, busy=13, budget=15, hosted="ubuntu-24.04")
        self.assertEqual((2, len(legs) - 2), (hosted, pooled))
        kept = sorted(leg["name"] for leg in legs if not leg["pool"])
        self.assertTrue(all(leg["name"].startswith("isolated-") for leg in legs if not leg["pool"]), kept)
        self.assertTrue(all(leg["pool"] for leg in legs if leg["kind"] in ("story", "harness")),
                        "the story and harness legs are the longest and overflow first")
        self.assertTrue(all(json.loads(leg["runsOn"]) == pool for leg in legs if leg["pool"]))

    def test_a_missing_changed_files_list_runs_every_isolated_leg(self):
        tree = Tree()
        self.addCleanup(tree.close)
        with tempfile.TemporaryDirectory() as scratch:
            output = Path(scratch) / "output"
            code = planner_main(["plan", "--root", str(tree.root), "--event", "pull_request", "--action", "opened",
                                 "--base-ref", BUCKET, "--changed-files", str(Path(scratch) / "absent.txt")],
                                {"GITHUB_OUTPUT": str(output)})
            self.assertEqual(0, code)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual(5, len(json.loads(values["legs"])))

    def test_a_bad_journey_folder_fails_the_command(self):
        tree = Tree(journeys=("Q1",))
        self.addCleanup(tree.close)
        self.assertEqual(1, planner_main(["plan", "--root", str(tree.root), "--event", "push"], {}))


def load_router():
    import importlib.util
    spec = importlib.util.spec_from_file_location("runner_overflow", REPOSITORY / ".github/workflows/runner-overflow.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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

    def test_a_leg_whose_run_was_aborted_by_a_hang_says_so(self):
        write_trx(self.trx, {"total": 0}, [planner.PENDING_GATE])
        aborted = self.trx.read_text().replace(
            "</ResultSummary>",
            "<RunInfos><RunInfo outcome=\"Error\"><Text>The active test run was aborted. Reason: Test host process crashed"
            "</Text></RunInfo></RunInfos></ResultSummary>")
        self.trx.write_text(aborted)
        _, problems = planner.leg_summary("contract-c3", self.trx, None, f"{planner.NAMESPACE}.Contracts.")
        self.assertEqual(1, len(problems))
        self.assertIn("aborted", problems[0])
        self.assertNotIn("vacuous", problems[0])

    def test_a_contract_leg_has_the_longest_hang_budget(self):
        self.assertEqual(30, planner.HANGS["contract"])
        self.assertTrue(all(planner.HANGS[kind] <= planner.HANGS["contract"] for kind in planner.HANGS))
        self.assertTrue(all(planner.HANGS[kind] < planner.TIMEOUTS[kind] for kind in planner.HANGS))

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
        self.assertLess(block.index("python3 .github/workflows/runner-overflow.py --self-test"), plan)
        self.assertIn("provenance: ${{ steps.plan.outputs.provenance }}", block)
        self.assertIn("pull-requests: read", block)

    def test_the_planner_routes_the_legs_as_ci_does(self):
        block = job(self.text, "planner")
        # The published legs are the routed ones, never the planner's raw output.
        self.assertIn("legs: ${{ steps.route.outputs.legs }}", block)
        self.assertNotIn("legs: ${{ steps.plan.outputs.legs }}", block)
        self.assertIn('--legs-file "$RUNNER_TEMP/legs.json"', block)
        route = block[block.index("- name: Route the legs"):]
        self.assertIn("id: route", route)
        self.assertIn("RUNNER_POOL: ${{ vars.RUNNER_POOL }}", route)
        self.assertIn("HOSTED_RUNNER_BUDGET: ${{ inputs.hosted_budget || vars.HOSTED_RUNNER_BUDGET }}", route)
        self.assertIn("GH_TOKEN: ${{ github.token }}", route)
        self.assertIn('runner-overflow.py --matrix "$RUNNER_TEMP/legs.json" --hosted ubuntu-24.04', route)
        # runner-overflow.py counts the hosted jobs in flight through the Actions API.
        self.assertIn("actions: read", block)
        self.assertRegex(self.text, r"(?m)^      hosted_budget:\n")

    def test_a_leg_runs_where_it_was_routed(self):
        block = job(self.text, "legs")
        self.assertIn("runs-on: ${{ fromJSON(matrix.runsOn || '\"ubuntu-24.04\"') }}", block)
        self.assertIn("name: ${{ matrix.pool && '[pool] ' || '' }}${{ matrix.name }}", block)
        self.assertNotRegex(block, r"(?m)^    (services|container):")

    def test_a_pool_leg_runs_on_what_the_pool_image_bakes_in(self):
        block = job(self.text, "legs")
        start = block[block.index("- name: Start Azurite"):block.index("- name: Wait for Azurite")]
        self.assertIn("POOL: ${{ matrix.pool }}", start)
        self.assertIn("nohup azurite", start)
        for ports in ("start azurite 10000 10001 10002", "start azurite-backup 11000 11001 11002",
                      "start azurite-east 12000 12001 12002"):
            self.assertIn(ports, start)
        self.assertIn("mcr.microsoft.com/azure-storage/azurite:3.36.0", start)
        self.assertIn("LATTICE_E2E_PROMETHEUS_BINARY: ${{ matrix.pool && '/opt/prometheus/prometheus' || '' }}", block)
        pull = block[block.index("- name: Pull Prometheus"):block.index("- name: Setup .NET")]
        self.assertIn("if: ${{ !matrix.pool }}", pull.split("- name: Check the pool image's Prometheus")[0])
        self.assertIn('-x "$LATTICE_E2E_PROMETHEUS_BINARY"', pull)
        for cache in ("- name: Cache NuGet packages\n        if: ${{ !matrix.pool }}\n",
                      "- name: Cache Playwright browsers\n        if: ${{ !matrix.pool }}\n"):
            self.assertIn(cache, block)
        probe = block[block.index("Prove a stopped Azurite fails the leg"):]
        self.assertIn('kill "$pid"', probe)
        self.assertIn('bash "$RUNNER_TEMP/azurite.sh"', probe)
        self.assertLess(probe.index('kill "$pid"'), probe.index("dotnet test"))

    def test_the_pool_image_carries_what_a_pool_leg_uses(self):
        dockerfile = (REPOSITORY / "tools/ci-runners/Dockerfile").read_text(encoding="utf-8")
        prometheus = (REPOSITORY / planner.E2E / "Estate/JourneyPrometheus.cs").read_text(encoding="utf-8")
        version = re.search(r'Image = "prom/prometheus:v([0-9.]+)"', prometheus).group(1)
        self.assertIn(f"ARG PROMETHEUS_VERSION={version}", dockerfile, "the pool's Prometheus must match the image tag")
        self.assertIn('"LATTICE_E2E_PROMETHEUS_BINARY"', prometheus)
        self.assertIn("/opt/prometheus", dockerfile)
        self.assertIn("ENV LATTICE_PLAYWRIGHT_SYSTEM_DEPS=chromium", dockerfile)
        self.assertIn("install-deps chromium", dockerfile)
        self.assertIn("restore/test/lattice.explorer.e2e/Orleans.Lattice.Explorer.EndToEnd.Tests.csproj", dockerfile)
        self.assertIn("azurite@3.36.0", dockerfile)

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

    def test_body_edits_and_labels_trigger_the_lane_in_their_own_concurrency_groups(self):
        self.assertIn("types: [opened, synchronize, reopened, edited, labeled]", self.text)
        self.assertIn("github.event.action == 'edited' && 'body' || github.event.action == 'labeled' && "
                      "format('label-{0}', github.event.label.name) || 'code'", self.text)
        self.assertIn("schedule:", self.text)
        self.assertRegex(self.text, r"(?m)^  workflow_dispatch:")

    def test_the_planner_receives_the_labels_and_head_branch(self):
        block = job(self.text, "planner")
        record = block[block.index("- name: Record the pull request's labels"):block.index("- name: Plan the legs")]
        # Pull-request context is read only behind the event guard (CiIntegrationBranchTriggerTests).
        self.assertIn("if: github.event_name == 'pull_request'", record)
        self.assertIn("LABELS: ${{ toJSON(github.event.pull_request.labels.*.name) }}", record)
        self.assertIn("LABEL: ${{ github.event.label.name }}", record)
        self.assertIn("HEAD_REF: ${{ github.head_ref }}", record)
        plan = block[block.index("- name: Plan the legs"):]
        self.assertNotIn("github.event.pull_request", plan)
        self.assertNotIn("github.head_ref", plan)
        self.assertIn('--labels "$labels" --label "$label" --head-ref "$head"', plan)
        self.assertIn("conformance: ${{ steps.plan.outputs.conformance }}", block)

    def test_contract_conformance_is_a_matrix_over_the_plan(self):
        block = job(self.text, "contract-conformance")
        self.assertIn("needs: [plan, planner]", block)
        condition = re.search(r"^    if: (.*)$", block, re.MULTILINE).group(1)
        self.assertIn("needs.plan.outputs.push_is_member != 'true'", condition)
        self.assertIn("needs.planner.outputs.conformance != '[]'", condition)
        self.assertIn("storyline: ${{ fromJSON(needs.planner.outputs.conformance) }}", block)
        self.assertIn("${{ matrix.storyline }}ContractConformanceTests", block)

    def test_both_triggers_watch_the_lane_scripts(self):
        for name in ("explorer-journeys-plan*.py", "journey-screenshot-provenance*.py",
                     "install-playwright-browsers.sh", "runner-overflow.py"):
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
        # The conformance job filters on <Storyline>ContractConformanceTests; F6's Contoso shard exists.
        self.assertIn("Contoso", planner.conformance_shards(REPOSITORY))
        self.assertIn("ContractConformanceTests", (e2e / "Contracts" / "ContosoContractConformanceTests.cs")
                      .read_text(encoding="utf-8"))
        # A contract leg selects one conformance case by its name (#5660): `<NN>_<chapter><suffix>`.
        fixture = (e2e / "Contracts" / "ContractConformanceFixture.cs").read_text(encoding="utf-8")
        self.assertIn(f'SetName($"{{index + 1:D2}}_{{chapter.Id}}{planner.CONFORMANCE_CASE_SUFFIX}")', fixture)
        self.assertIn("await session.EstablishAsync(chapter);", fixture,
                      "a lone case establishes its consumes in story order through StorySession")
        graph = planner.outline_graph(REPOSITORY)
        contracts = planner.chapter_contract_sources(REPOSITORY)[0]
        self.assertTrue(contracts, "the tree's chapter contracts are discovered")
        self.assertEqual(set(), set(contracts) - set(graph), "every chapter contract is in the parsed outline")
        notes = []
        story_only = planner.story_only_contracts(REPOSITORY, planner.discover_journeys(REPOSITORY),
                                                  planner.conformance_shards(REPOSITORY), notes)
        self.assertEqual([], notes, "every story-only chapter has a conformance fixture to run it")
        journeys = {folder.lower() for folder, _, _ in planner.discover_journeys(REPOSITORY)}
        self.assertEqual(sorted(chapter for chapter in contracts if chapter.lower() not in journeys),
                         sorted(chapter for chapter, _, _ in story_only))
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
