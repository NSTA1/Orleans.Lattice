#!/usr/bin/env python3
"""Exercise the real planner-to-runner TLC watchdog boundary without starting TLC."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

WORKFLOWS = Path(__file__).resolve().parent
REPOSITORY = WORKFLOWS.parent.parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, WORKFLOWS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


planner = load("watchdog_planner", "plan-test-matrix.py")
runner = load("watchdog_runner", "run-test-leg.py")


class TlcWatchdogTests(unittest.TestCase):
    def legacy_deterministic_step(self):
        workflow = (WORKFLOWS / "ci-serial-old.yml").read_text(encoding="utf-8")
        marker = "      - name: Test changed packages (deterministic)\n"
        self.assertEqual(1, workflow.count(marker), "the legacy deterministic step must be unique")
        start = workflow.index(marker)
        end = workflow.find("\n      - name:", start + len(marker))
        return workflow[start:] if end < 0 else workflow[start:end]

    def config(self, cross_tiers=True):
        return {
            "fixture": {
                "namespaceRoot": "Fixture.Tests",
                "crossTiers": cross_tiers,
                "shards": [
                    {"shard": "models", "tlc": True, "includeCategories": ["TlcShardProbe"],
                     "tiers": ["deterministic"]},
                    {"shard": "smoke", "tlcSmoke": True, "include": ["Formal.Smoke"],
                     "tiers": ["deterministic"]},
                    {"shard": "ordinary", "include": ["Grains"]},
                    {"shard": "rest", "complement": True},
                ],
            },
        }

    def item(self, **overrides):
        return {
            "package": "fixture",
            "shard": "ordinary",
            "tier": "deterministic",
            "label": "fixture / ordinary",
            "filter": "FullyQualifiedName~Fixture.Tests.Grains",
            **overrides,
        }

    def execute(self, item, returncode=0):
        with tempfile.TemporaryDirectory() as directory:
            trx = Path(directory) / "result.trx"
            trx.write_text(
                '<TestRun xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010">'
                '<ResultSummary><Counters executed="1" passed="1" failed="0"/>'
                '</ResultSummary></TestRun>', encoding="utf-8",
            )
            with (
                patch.object(runner, "find_test_project", return_value="test/fixture/Fixture.Tests.csproj"),
                patch.object(runner.glob, "glob", return_value=[str(trx)]),
                patch.object(runner.subprocess, "run", return_value=SimpleNamespace(returncode=returncode)) as run,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                record = runner.run_item(item, directory)
            run.assert_called_once()
            self.assertEqual("passed" if returncode == 0 else "failed", record["outcome"])
            self.assertEqual(1, record["executed"])
            return run.call_args.args[0]

    def assert_hang_watchdog(self, command):
        self.assertIn("--blame-hang", command)
        self.assertEqual("10m", command[command.index("--blame-hang-timeout") + 1])
        self.assertEqual("full", command[command.index("--blame-hang-dump-type") + 1])

    def assert_job_watchdog(self, command):
        self.assertNotIn("--blame-hang", command)
        self.assertNotIn("--blame-hang-timeout", command)
        self.assertNotIn("--blame-hang-dump-type", command)
        self.assertIn("--blame", command)
        self.assertIn("--blame-crash", command)
        self.assertEqual("full", command[command.index("--blame-crash-dump-type") + 1])

    def test_planner_preserves_both_tlc_flags_for_crossed_and_untiered_items(self):
        for cross_tiers in (True, False):
            with self.subTest(cross_tiers=cross_tiers):
                items, _ = planner.make_items(["fixture"], self.config(cross_tiers), {}, set())
                models = next(item for item in items if item["shard"] == "models")
                smoke = next(item for item in items if item["shard"] == "smoke")
                self.assertIs(True, models.get("tlc"))
                self.assertIs(False, models.get("tlcSmoke"))
                self.assertIs(False, smoke.get("tlc"))
                self.assertIs(True, smoke.get("tlcSmoke"))
                self.assert_job_watchdog(self.execute(models))
                self.assert_job_watchdog(self.execute(smoke))

    def test_real_shard_metadata_reaches_each_runner_command(self):
        config = json.loads((WORKFLOWS / "test-shards.json").read_text(encoding="utf-8"))
        items, _ = planner.make_items(["lattice"], config, {}, set(), skip_tiers=["coyote", "chaos"])
        expected = {
            shard["shard"]: (shard.get("tlc", False), shard.get("tlcSmoke", False))
            for shard in config["lattice"]["shards"]
        }
        self.assertTrue(any(any(flags) for flags in expected.values()))
        self.assertTrue(any(not any(flags) for flags in expected.values()))
        for item in items:
            with self.subTest(shard=item["shard"]):
                flags = expected[item["shard"]]
                self.assertEqual(flags, (item.get("tlc", False), item.get("tlcSmoke", False)))
                command = self.execute(item)
                (self.assert_job_watchdog if any(flags) else self.assert_hang_watchdog)(command)

    def test_legacy_serial_workflow_routes_the_selected_packages_through_planner_and_runner(self):
        step = self.legacy_deterministic_step()
        self.assertIn("set -euo pipefail", step)
        self.assertIn("for name in ${{ steps.discover.outputs.to_test }}; do", step)
        self.assertIn("plan-test-matrix.py", step)
        self.assertIn('--packages "$packages"', step)
        self.assertIn("--output-matrix /tmp/legacy-test-matrix.json", step)
        self.assertIn("run-test-leg.py", step)
        self.assertIn("--leg /tmp/legacy-test-leg.json", step)
        self.assertIn("--max-legs 1", step)
        self.assertIn("--skip-tiers coyote,chaos", step)
        self.assertNotIn("dotnet test", step)
        self.assertNotIn("--blame-hang", step)

    def test_legacy_deterministic_plan_separates_tlc_and_ordinary_watchdogs(self):
        config = json.loads((WORKFLOWS / "test-shards.json").read_text(encoding="utf-8"))
        items, skipped = planner.make_items(
            ["lattice"], config, {}, set(), skip_tiers=["coyote", "chaos"],
        )
        legs = planner.pack(items, 1)

        self.assertEqual(1, len(legs))
        self.assertEqual({"deterministic"}, {item["tier"] for item in items})
        self.assertEqual({"coyote", "chaos"}, {item["tier"] for item in skipped})
        self.assertTrue(all("TestCategory!=Chaos" in item["filter"] for item in items))
        self.assertTrue(all("TestCategory!=Coyote" in item["filter"] for item in items))
        tlc = next(item for item in items if item["tlc"] or item["tlcSmoke"])
        ordinary = next(item for item in items if not item["tlc"] and not item["tlcSmoke"])

        self.assert_job_watchdog(self.execute(tlc))
        self.assert_hang_watchdog(self.execute(ordinary))

    def test_apps_and_benchmark_reference_paths_do_not_select_the_core_test_project(self):
        core_project = REPOSITORY / "test" / "lattice" / "Orleans.Lattice.Tests.csproj"
        core_text = core_project.read_text(encoding="utf-8")
        selected_project_names = {
            project.name
            for directory in ("apps", "benchmark")
            for project in (REPOSITORY / directory).rglob("*.csproj")
        }

        self.assertTrue(selected_project_names, "apps/benchmark project discovery found nothing to check")
        referenced = sorted(name for name in selected_project_names if name in core_text)
        self.assertEqual(
            [],
            referenced,
            "the apps/benchmark lanes select covering test projects by ProjectReference basename, "
            "and the core test project unexpectedly references: " + ", ".join(referenced),
        )

    def test_declared_tlc_and_smoke_keep_sequence_and_crash_diagnostics(self):
        for flag in ("tlc", "tlcSmoke"):
            with self.subTest(flag=flag):
                self.assert_job_watchdog(self.execute(self.item(**{flag: True})))

    def test_mixed_leg_keeps_the_watchdog_on_its_ordinary_item(self):
        self.assert_job_watchdog(self.execute(self.item(tlc=True)))
        self.assert_hang_watchdog(self.execute(self.item(tlc=False, tlcSmoke=False)))
        self.assert_job_watchdog(self.execute(self.item(tlcSmoke=True)))

    def test_emitted_matrix_preserves_policy_after_packing_and_json_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            packages, shards, matrix = (root / name for name in ("packages.txt", "shards.json", "matrix.json"))
            packages.write_text("fixture\n", encoding="utf-8")
            shards.write_text(json.dumps(self.config(cross_tiers=False)), encoding="utf-8")
            subprocess.run([
                sys.executable, str(WORKFLOWS / "plan-test-matrix.py"),
                "--packages", str(packages), "--shards", str(shards),
                "--durations", str(WORKFLOWS / "test-durations.tsv"),
                "--output-matrix", str(matrix), "--max-legs", "1",
            ], capture_output=True, text=True, check=True, timeout=30)
            legs = json.loads(matrix.read_text(encoding="utf-8"))
        self.assertEqual(1, len(legs))
        self.assertEqual({"models", "smoke", "ordinary", "rest"}, {item["shard"] for item in legs[0]["items"]})
        for item in legs[0]["items"]:
            with self.subTest(shard=item["shard"]):
                command = self.execute(item)
                if item["shard"] in ("models", "smoke"):
                    self.assert_job_watchdog(command)
                else:
                    self.assert_hang_watchdog(command)

    def test_unsharded_mixed_project_keeps_its_hang_watchdog(self):
        items, _ = planner.make_items(["unsharded"], {}, {}, set())
        self.assertEqual(1, len(items))
        self.assertIsNone(items[0]["filter"])
        self.assert_hang_watchdog(self.execute(items[0]))

    def test_missing_and_nonboolean_item_flags_keep_the_hang_watchdog(self):
        self.assert_hang_watchdog(self.execute(self.item()))
        self.assert_hang_watchdog(self.execute(self.item(tlc=True, tlcSmoke="true")))
        self.assert_hang_watchdog(self.execute(self.item(tlc=1, tlcSmoke=True)))
        for flag in ("tlc", "tlcSmoke"):
            for value in (False, None, "", "true", "false", 0, 1, [], {}):
                with self.subTest(flag=flag, value=value):
                    self.assert_hang_watchdog(self.execute(self.item(**{flag: value})))

    def test_shard_name_or_filter_alone_cannot_disable_the_watchdog(self):
        self.assert_hang_watchdog(self.execute(self.item(
            shard="formal-tlc-scheduling", filter="TestCategory=TlcShardScheduling",
        )))

    def test_declared_tlc_without_a_filter_keeps_the_whole_project_watchdog(self):
        for flag in ("tlc", "tlcSmoke"):
            for expression in (None, "", " "):
                with self.subTest(flag=flag, expression=expression):
                    self.assert_hang_watchdog(self.execute(self.item(filter=expression, **{flag: True})))

    def test_aborted_tlc_is_failed_even_when_the_trx_contains_only_passes(self):
        self.assert_job_watchdog(self.execute(self.item(tlc=True), returncode=1))

    def test_policy_skipped_items_do_not_start_a_process(self):
        _, skipped = planner.make_items(
            ["fixture"], self.config(), {}, set(), skip_tlc_reason="already checked",
            skip_tlc_smoke_reason="already checked",
        )
        self.assertEqual({"models", "smoke"}, {item["shard"] for item in skipped})
        with (
            patch.object(runner, "find_test_project", return_value="fixture.Tests.csproj"),
            patch.object(runner.subprocess, "run") as run,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            for item in skipped:
                self.assertIs(True, item.get("tlc") or item.get("tlcSmoke"))
                record = runner.run_item(item, ".")
                self.assertEqual("policy-skipped", record["outcome"])
            run.assert_not_called()

    def test_planner_rejects_nonboolean_tlc_declarations(self):
        for flag in ("tlc", "tlcSmoke"):
            for value in (None, "true", "false", 0, 1, [], {}):
                with self.subTest(flag=flag, value=value):
                    config = self.config()
                    config["fixture"]["shards"][0][flag] = value
                    with self.assertRaisesRegex(SystemExit, flag + ".*boolean"):
                        planner.make_items(["fixture"], config, {}, set())


if __name__ == "__main__":
    unittest.main(verbosity=2)
