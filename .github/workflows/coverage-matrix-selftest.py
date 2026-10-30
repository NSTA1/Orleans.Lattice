#!/usr/bin/env python3
"""Exercise the coverage planner and the workflow's actual shell boundaries."""

import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest

WORKFLOWS = Path(__file__).resolve().parent
ROOT = WORKFLOWS.parent.parent
spec = importlib.util.spec_from_file_location("coverage_matrix", WORKFLOWS / "plan-coverage-matrix.py")
coverage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(coverage)
YAML = (WORKFLOWS / "coverage.yml").read_text(encoding="utf-8")


def script(name: str) -> str:
    match = re.search(
        rf"^      - name: {re.escape(name)}\n(?P<block>.*?)(?=^      - |^  \w|\Z)",
        YAML, re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"Missing workflow step: {name}")
    run = re.search(r"^        run: \|\n(?P<body>(?:^          .*\n|^\n)+)", match["block"], re.MULTILINE)
    if run is None:
        raise AssertionError(f"Missing shell body: {name}")
    return textwrap.dedent(run["body"])


def bash() -> str:
    # CreateProcess searches System32 before PATH, so a bare "bash" on Windows
    # can launch the WSL relay instead of the PATH-selected Bash (Git Bash).
    path = os.environ.get("BASH") or shutil.which("bash")
    if path is None:
        raise AssertionError("bash not found on PATH")
    return path


class CoverageMatrixTests(unittest.TestCase):
    def test_every_project_is_planned_once_per_shared_shard(self):
        legs = coverage.plan(ROOT)
        items = [item for leg in legs for item in leg["items"]]
        expected_projects = {
            project.relative_to(ROOT).as_posix()
            for project in (ROOT / "test").rglob("*Tests.csproj")
            if project.relative_to(ROOT / "test").parts[0] not in coverage.EXCLUDED_PROJECTS
        }
        self.assertEqual(expected_projects, {item["project"] for item in items})
        self.assertGreater(len(legs), 1)
        self.assertLessEqual(len(legs), 10)
        keys = [(item["project"], item["shard"]) for item in items]
        self.assertEqual(len(keys), len(set(keys)))
        shards = json.loads((WORKFLOWS / "test-shards.json").read_text(encoding="utf-8"))
        for package in {item["package"] for item in items} - {coverage.UI_PACKAGE}:
            expected = (
                {shard["shard"] for shard in shards[package]["shards"]
                 if not shard.get("tlc") and not shard.get("tlcSmoke")}
                if package in shards else {"-"}
            )
            self.assertEqual(expected, {item["shard"] for item in items if item["package"] == package})
        for item in items:
            self.assertEqual("deterministic", item["tier"])
            self.assertIn(coverage.COVERAGE_FILTER, item["filter"])
            self.assertNotIn("formal-tlc", item["shard"])

    def test_browser_items_use_the_browser_lane_partition(self):
        items = [
            item for leg in coverage.plan(ROOT) for item in leg["items"]
            if item["package"] == coverage.UI_PACKAGE
        ]
        shards = json.loads((WORKFLOWS / "ui-test-shards.json").read_text(encoding="utf-8"))
        self.assertEqual(len(shards), len(items))
        for shard in shards:
            item = next(item for item in items if item["shard"] == shard["shard"])
            self.assertEqual(f"({shard['filter']})&({coverage.COVERAGE_FILTER})", item["filter"])
        for leg in coverage.plan(ROOT):
            self.assertEqual(
                any(item["package"] == coverage.UI_PACKAGE for item in leg["items"]), leg["ui"]
            )

    def test_ci_rebalances_change_coverage_without_a_second_partition(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / ".github" / "workflows"
            workflows.mkdir(parents=True)
            for package in ("one", "two"):
                project = root / "test" / package / f"{package}.Tests.csproj"
                project.parent.mkdir(parents=True)
                project.touch()
            (workflows / "test-shards.json").write_text(json.dumps({
                "one": {
                    "namespaceRoot": "One.Tests",
                    "crossTiers": False,
                    "shards": [
                        {"shard": "first", "include": ["First."]},
                        {"shard": "rest", "complement": True},
                    ],
                },
            }), encoding="utf-8")
            durations = workflows / "test-durations.tsv"
            durations.write_text("one\tfirst\tall\t1\none\trest\tall\t1\ntwo\t-\tall\t1\n", encoding="utf-8")
            before = coverage.plan(root)
            durations.write_text("one\tfirst\tall\t20\none\trest\tall\t1\ntwo\t-\tall\t1\n", encoding="utf-8")
            after = coverage.plan(root)
            self.assertNotEqual(before, after)
            first = next(item for leg in after for item in leg["items"] if item["shard"] == "first")
            self.assertEqual(20 + coverage.ci.PER_ITEM_OVERHEAD, first["estimate"])
            self.assertIn("FullyQualifiedName~One.Tests.First.", first["filter"])
            config = json.loads((workflows / "test-shards.json").read_text())
            config["one"]["shards"][0]["include"] = ["Rebalanced."]
            (workflows / "test-shards.json").write_text(json.dumps(config), encoding="utf-8")
            rebalanced = next(
                item for leg in coverage.plan(root) for item in leg["items"] if item["shard"] == "first"
            )
            self.assertIn("FullyQualifiedName~One.Tests.Rebalanced.", rebalanced["filter"])
            self.assertNotIn("One.Tests.First.", rebalanced["filter"])

    def test_empty_project_discovery_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "No coverage test projects"):
                coverage.plan(Path(directory))


def declared_tlc_shards() -> list[str]:
    shards = json.loads((WORKFLOWS / "test-shards.json").read_text(encoding="utf-8"))
    return [
        shard["shard"] for shard in shards[coverage.TLC_PACKAGE]["shards"]
        if shard.get("tlc") or shard.get("tlcSmoke")
    ]


class TlcMatrixTests(unittest.TestCase):
    def test_every_tlc_and_smoke_shard_runs_once_uninstrumented(self):
        legs = coverage.plan_tlc(ROOT)
        items = [item for leg in legs for item in leg["items"]]
        declared = declared_tlc_shards()
        self.assertIn("formal-tlc-smoke", declared)
        self.assertGreater(len(declared), 2)
        self.assertEqual(sorted(declared), sorted(item["shard"] for item in items))
        self.assertLessEqual(len(legs), coverage.TLC_MAX_LEGS)
        project = coverage.discover_projects(ROOT)[coverage.TLC_PACKAGE]
        for item in items:
            self.assertEqual("deterministic", item["tier"])
            self.assertEqual(project, item["project"])
            self.assertTrue(item["filter"].endswith(f"&({coverage.TLC_FILTER})"), item["filter"])
            self.assertNotRegex(item["filter"], r"TestCategory!=Tlc(?![A-Za-z0-9_])")
            # run-test-leg.py drops the inactivity collector only for declared TLC items.
            self.assertTrue(item["tlc"] or item["tlcSmoke"])
            self.assertNotIn("skip", item)
        for index, leg in enumerate(legs, start=1):
            self.assertEqual(f"tlc-{index}", leg["id"])
            self.assertEqual(coverage.tlc_timeout(leg["estimate"]), leg["timeout"])
            self.assertGreaterEqual(leg["timeout"], leg["estimate"] + coverage.TLC_TIMEOUT_OVERHEAD)
            self.assertLessEqual(leg["timeout"], coverage.TLC_TIMEOUT_CEILING)

    def test_coverage_and_tlc_legs_never_share_an_artifact_or_a_shard(self):
        coverage_ids = {leg["id"] for leg in coverage.plan(ROOT)}
        tlc_ids = {leg["id"] for leg in coverage.plan_tlc(ROOT)}
        self.assertFalse(coverage_ids & tlc_ids)
        self.assertFalse(any(leg_id.startswith("leg-") for leg_id in tlc_ids))
        coverage_shards = {item["shard"] for leg in coverage.plan(ROOT) for item in leg["items"]}
        self.assertFalse(coverage_shards & set(declared_tlc_shards()))

    def test_a_restricted_plan_runs_only_the_named_shards(self):
        legs = coverage.plan_tlc(ROOT, only=["formal-tlc-smoke"])
        self.assertEqual(1, len(legs))
        self.assertEqual(["formal-tlc-smoke"], [item["shard"] for item in legs[0]["items"]])
        self.assertIn("The_atomic_commit_base_specification_holds", legs[0]["items"][0]["filter"])
        for name in ("formal-tlc-nope", "bplustree"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "Not TLC shards"):
                coverage.plan_tlc(ROOT, only=[name])

    def test_a_leg_beyond_the_ceiling_is_refused_rather_than_cut_off(self):
        self.assertEqual(31, coverage.tlc_timeout(0.24))
        ceiling = (coverage.TLC_TIMEOUT_CEILING - coverage.TLC_TIMEOUT_OVERHEAD) / coverage.TLC_TIMEOUT_FACTOR
        self.assertLessEqual(coverage.tlc_timeout(ceiling), coverage.TLC_TIMEOUT_CEILING)
        with self.assertRaisesRegex(ValueError, "ceiling"):
            coverage.tlc_timeout(ceiling + 1)

    def test_cli_restriction_plans_no_coverage_and_needs_a_tlc_matrix(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            planner = str(WORKFLOWS / "plan-coverage-matrix.py")
            result = subprocess.run(
                [sys.executable, planner, "--output-matrix", str(out / "c.json"),
                 "--output-tlc-matrix", str(out / "t.json"), "--tlc-shards", " formal-tlc-smoke "],
                text=True, capture_output=True, timeout=60,
            )
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertEqual([], json.loads((out / "c.json").read_text()))
            self.assertEqual(1, len(json.loads((out / "t.json").read_text())))
            result = subprocess.run(
                [sys.executable, planner, "--output-matrix", str(out / "c.json"),
                 "--tlc-shards", "formal-tlc-smoke"],
                text=True, capture_output=True, timeout=60,
            )
            self.assertNotEqual(0, result.returncode)

    def test_case_durations_name_every_case_by_its_full_name(self):
        trx = textwrap.dedent("""\
            <TestRun xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010">
              <Results>
                <UnitTestResult testId="a" testName="The_base_specification_holds(AtomicCommit)" outcome="Passed" duration="00:01:02.5000000"/>
                <UnitTestResult testId="b" testName="A_mutant_fails(AtomicCommit, NoAbort)" outcome="Failed" duration="01:00:00"/>
              </Results>
              <TestDefinitions>
                <UnitTest id="a"><TestMethod className="Orleans.Lattice.Tests.Formal.TlcModelCheckTests"/></UnitTest>
                <UnitTest id="b"><TestMethod className="Orleans.Lattice.Tests.Formal.TlcModelCheckTests"/></UnitTest>
              </TestDefinitions>
            </TestRun>
        """)
        with tempfile.TemporaryDirectory() as directory:
            leg = Path(directory) / "tlc-trx-1-tlc-1" / "test" / "TestResults"
            leg.mkdir(parents=True)
            (leg / "one.trx").write_text(trx, encoding="utf-8")
            rows = coverage.tlc_case_durations(Path(directory))
            self.assertEqual([
                ("Orleans.Lattice.Tests.Formal.TlcModelCheckTests.A_mutant_fails(AtomicCommit, NoAbort)", "Failed", 3600.0),
                ("Orleans.Lattice.Tests.Formal.TlcModelCheckTests.The_base_specification_holds(AtomicCommit)", "Passed", 62.5),
            ], rows)
            output = Path(directory) / "durations.tsv"
            result = subprocess.run(
                [sys.executable, str(WORKFLOWS / "plan-coverage-matrix.py"),
                 "--tlc-case-durations", directory, "--output", str(output)],
                text=True, capture_output=True, timeout=60,
            )
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            lines = output.read_text(encoding="utf-8").splitlines()
            self.assertEqual("# case\toutcome\tseconds", lines[0])
            self.assertTrue(lines[1].endswith("\tFailed\t3600.0"), lines[1])
            self.assertEqual([], coverage.tlc_case_durations(Path(directory) / "missing"))


# A synthetic census in the shape TlcCaseCensus and tlc-receipts.py write: two
# modules whose cases carry the TlcShard* category of a real category-only TLC
# shard, plus a module no TLC shard can select because its case is not a Tlc case.
FORMAL = "Orleans.Lattice.Tests.Formal.TlcModelCheckTests"
WARMER_CENSUS = {
    "Convergence": [f"{FORMAL}.The_base_specification_holds(Convergence)"],
    "Views": [f"{FORMAL}.The_base_specification_holds(Views)",
              f"{FORMAL}.A_mutation_is_caught(Views, Drop)"],
    "Stray": ["Elsewhere.Fixture.Case(Stray)"],
}
WARMER_CATEGORIES = {
    f"{FORMAL}.The_base_specification_holds(Convergence)":
        ["Tlc", "TlcShardConvergence", "TlcModule_Convergence"],
    f"{FORMAL}.The_base_specification_holds(Views)":
        ["Tlc", "TlcShardMaterialisedViews", "TlcModule_Views"],
    f"{FORMAL}.A_mutation_is_caught(Views, Drop)":
        ["Tlc", "TlcShardMaterialisedViews", "TlcModule_Views"],
    "Elsewhere.Fixture.Case(Stray)": ["Other"],
}


class TlcReceiptWarmerPlanTests(unittest.TestCase):
    """--tlc-modules: the receipt warmer's TLC legs (#5165)."""

    def plan(self, modules: set[str]) -> list[dict]:
        return coverage.plan_tlc(ROOT, modules=modules, census=WARMER_CENSUS, categories=WARMER_CATEGORIES)

    def test_only_the_shards_selecting_the_named_modules_run_narrowed_to_them(self):
        legs = self.plan({"Convergence"})
        items = [item for leg in legs for item in leg["items"]]
        self.assertEqual(["formal-tlc-convergence"], [item["shard"] for item in items])
        self.assertEqual(["Convergence"], items[0]["tlc_modules"])
        self.assertTrue(items[0]["filter"].endswith("&(TestCategory=TlcModule_Convergence)"), items[0]["filter"])
        self.assertTrue(items[0]["tlc"])
        self.assertEqual("tlc-1", legs[0]["id"])
        self.assertEqual(coverage.tlc_timeout(legs[0]["estimate"]), legs[0]["timeout"])

    def test_every_named_module_is_planned_and_the_smoke_case_is_not(self):
        items = [item for leg in self.plan({"Convergence", "Views"}) for item in leg["items"]]
        self.assertEqual(["formal-tlc-convergence", "formal-tlc-materialised-views"],
                         sorted(item["shard"] for item in items))
        self.assertNotIn("formal-tlc-smoke", [item["shard"] for item in items])

    def test_no_module_without_a_receipt_plans_no_leg(self):
        self.assertEqual([], self.plan(set()))

    def test_a_module_no_shard_selects_or_the_census_lacks_is_refused(self):
        with self.assertRaisesRegex(ValueError, "No TLC shard selects a census case of: Stray"):
            self.plan({"Stray"})
        with self.assertRaisesRegex(ValueError, "Not census modules: Nope"):
            self.plan({"Nope"})

    def test_cli_plans_tlc_legs_only_and_refuses_a_coverage_matrix(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "census.json").write_text(json.dumps(WARMER_CENSUS), encoding="utf-8")
            (out / "categories.json").write_text(json.dumps(WARMER_CATEGORIES), encoding="utf-8")
            planner = str(WORKFLOWS / "plan-coverage-matrix.py")
            common = ["--tlc-census", str(out / "census.json"),
                      "--tlc-case-categories", str(out / "categories.json")]
            for modules, expected in (("Views\n", 1), ("", 0)):
                with self.subTest(modules=modules):
                    (out / "modules.txt").write_text(modules, encoding="utf-8")
                    result = subprocess.run(
                        [sys.executable, planner, "--tlc-modules", str(out / "modules.txt"),
                         "--output-tlc-matrix", str(out / "t.json"), *common],
                        text=True, capture_output=True, timeout=60,
                    )
                    self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                    self.assertEqual(expected, len(json.loads((out / "t.json").read_text())))
            result = subprocess.run(
                [sys.executable, planner, "--tlc-modules", str(out / "modules.txt"),
                 "--output-tlc-matrix", str(out / "t.json"), "--output-matrix", str(out / "c.json"), *common],
                text=True, capture_output=True, timeout=60,
            )
            self.assertNotEqual(0, result.returncode)


class BrowserInstallTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        shutil.copyfile(WORKFLOWS / "install-playwright-browsers.sh", self.root / "install.sh")
        script = self.root / "test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1"
        script.parent.mkdir(parents=True)
        script.touch()
        tools = self.root / "bin"
        tools.mkdir()
        (tools / "pwsh").write_text(textwrap.dedent("""\
            #!/bin/bash
            count=0
            [ ! -f calls ] || count=$(cat calls)
            count=$((count + 1))
            echo "$count" > calls
            echo "$*" >> arguments
            case "$SCENARIO" in
              fail) exit 7 ;;
              retry-deps) [ "$count" -ne 1 ] || exit 8 ;;
              retry-download) [ "$count" -ne 2 ] || exit 9 ;;
              hang) trap '' TERM; /usr/bin/sleep 30 ;;
            esac
            exit 0
        """), encoding="utf-8", newline="\n")
        # Run the real GNU timeout, shortening only its clock in the test.
        (tools / "timeout").write_text(textwrap.dedent("""\
            #!/bin/bash
            [ "$1" = --kill-after=10s ] && [ "$2" = 120s ] || exit 97
            shift 2
            exec /usr/bin/timeout --kill-after=1s 1s "$@"
        """), encoding="utf-8", newline="\n")
        (tools / "sleep").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8", newline="\n")
        # These portable contract tests stub privilege/supervision. The native
        # playwright-install-selftest.py exercises real sudo and descendant cleanup.
        (tools / "sudo").write_text(
            '#!/bin/bash\n[ "$1" = --non-interactive ] && [ "$2" = -- ] || exit 96\n'
            'shift 2\nexec "$@"\n', encoding="utf-8", newline="\n",
        )
        (tools / "python3").write_text(
            '#!/bin/bash\n[[ "$1" = */run-playwright-install.py ]] && '
            '[ "$2" = 120 ] && [ "$3" = 10 ] || exit 95\n'
            'shift 3\n'
            'if [ "$1" = --apt-mirror-step ]; then echo "$2" >> mirrors; shift 2; fi\n'
            'exec timeout --kill-after=10s 120s "$@"\n',
            encoding="utf-8", newline="\n",
        )
        for tool in tools.iterdir():
            tool.chmod(0o755)
        self.env = dict(os.environ, SCENARIO="pass")

    def run_install(self, cache: bool | str, *browsers: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [bash(), "-c",
             'export PATH="$PWD/bin:$PATH"; bash install.sh "$@"', "install",
             str(cache).lower(), *browsers],
            cwd=self.root, env=self.env, text=True, capture_output=True, timeout=20,
        )

    def test_cache_hit_still_installs_os_dependencies_for_selected_engines(self):
        result = self.run_install(True, "chromium")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("1", (self.root / "calls").read_text().strip())
        self.assertIn("install-deps chromium", (self.root / "arguments").read_text())
        self.assertNotIn("firefox", (self.root / "arguments").read_text())

    def test_cache_miss_installs_dependencies_then_all_requested_engines(self):
        result = self.run_install(False, "chromium", "firefox", "webkit")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        calls = (self.root / "arguments").read_text().splitlines()
        self.assertEqual(2, len(calls))
        self.assertTrue(calls[0].endswith("install-deps chromium firefox webkit"))
        self.assertTrue(calls[1].endswith("install chromium firefox webkit"))

    def test_empty_cache_output_is_a_miss_and_downloads_selected_engines(self):
        result = self.run_install("", "chromium")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        calls = (self.root / "arguments").read_text().splitlines()
        self.assertEqual(2, len(calls))
        self.assertTrue(calls[0].endswith("install-deps chromium"))
        self.assertTrue(calls[1].endswith("install chromium"))

    def test_dependency_and_download_failures_retry_independently(self):
        for scenario, cache, calls in (("retry-deps", True, 2), ("retry-download", False, 3)):
            with self.subTest(scenario=scenario):
                for path in ("calls", "arguments"):
                    (self.root / path).unlink(missing_ok=True)
                self.env["SCENARIO"] = scenario
                result = self.run_install(cache, "chromium")
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual(str(calls), (self.root / "calls").read_text().strip())
                self.assertIn("::warning::", result.stdout)

    def test_permanent_failure_is_loud_and_never_downloads_browsers(self):
        self.env["SCENARIO"] = "fail"
        result = self.run_install(False, "chromium")
        self.assertEqual(7, result.returncode, result.stdout + result.stderr)
        self.assertEqual("3", (self.root / "calls").read_text().strip())
        self.assertIn("::error::", result.stdout)
        self.assertNotIn("install chromium", (self.root / "arguments").read_text())

    def test_hung_installer_ignoring_term_is_killed_and_exhausts_three_attempts(self):
        self.env["SCENARIO"] = "hang"
        result = self.run_install(True, "chromium")
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("3", (self.root / "calls").read_text().strip())
        self.assertIn("failed after three bounded attempts", result.stdout)
        self.assertEqual(["1", "2"], (self.root / "mirrors").read_text().splitlines())

    def test_missing_build_output_or_empty_browser_selection_fails_before_install(self):
        (self.root / "test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1").unlink()
        for browsers in ((), ("chromium",), ("unknown",)):
            result = self.run_install(True, *browsers)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("::error::", result.stdout)
            self.assertFalse((self.root / "calls").exists())

    def test_shell_tests_launch_the_path_selected_bash(self):
        launcher = bash()
        self.assertTrue(os.path.isabs(launcher), launcher)
        result = subprocess.run([launcher, "-c", 'echo "$BASH_VERSION"'], text=True,
                                capture_output=True, timeout=20)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertRegex(result.stdout, r"^\d+\.\d+")

    def test_ui_and_coverage_steps_use_the_bounded_installer(self):
        for name in ("ui-tests.yml", "coverage.yml"):
            yaml = (WORKFLOWS / name).read_text(encoding="utf-8")
            block = re.search(
                r"      - name: Install Playwright browsers\n(.*?)(?=      - name:|\Z)",
                yaml, re.DOTALL,
            )[1]
            self.assertIn("timeout-minutes: 15", block)
            self.assertIn("bash .github/workflows/install-playwright-browsers.sh", block)
            self.assertNotIn('pwsh "$script"', block)
        yaml = (WORKFLOWS / "ui-tests.yml").read_text(encoding="utf-8")
        self.assertEqual(2, yaml.count("- '.github/workflows/install-playwright-browsers.sh'"))
        self.assertEqual(2, yaml.count("- '.github/workflows/run-playwright-install.py'"))
        self.assertEqual(2, yaml.count("- '.github/workflows/playwright_apt_mirrors.py'"))
        self.assertIn("join(matrix.browsers, '-')", yaml)
        self.assertIn("join(matrix.browsers, ' ')", yaml)
        self.assertIn("key: playwright-chromium-firefox-webkit-", YAML)


class CoverageShellTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.env = dict(os.environ)
        self.env.update({
            "GITHUB_OUTPUT": "output.txt", "GITHUB_STEP_SUMMARY": "summary.txt",
            "LEG": json.dumps({"items": [{
                "project": "test/example/Example.Tests.csproj",
                "shard": "unit", "tier": "deterministic", "filter": coverage.COVERAGE_FILTER,
            }]}),
            "LEGS": json.dumps([{"id": "leg-1"}, {"id": "leg-2"}]),
        })
        workflows = self.root / ".github" / "workflows"
        workflows.mkdir(parents=True)
        shutil.copyfile(WORKFLOWS / "failed-tests-filter.py", workflows / "failed-tests-filter.py")
        tools = self.root / "bin"
        tools.mkdir()
        python = Path(sys.executable).as_posix()
        (tools / "python3").write_text(f'#!/bin/bash\nexec "{python}" "$@"\n', encoding="utf-8", newline="\n")
        (tools / "python3").chmod(0o755)
        if sys.platform == "win32":
            # Native Windows jq otherwise emits CRLF into Bash read variables.
            jq = Path(shutil.which("jq")).as_posix()
            (tools / "jq").write_text(
                f'#!/bin/bash\nexec "{jq}" --binary "$@"\n', encoding="utf-8", newline="\n"
            )
            (tools / "jq").chmod(0o755)
        (tools / "dotnet").write_text(textwrap.dedent("""\
            #!/bin/bash
            count=0
            [ ! -f calls ] || count=$(cat calls)
            count=$((count + 1))
            echo "$count" > calls
            echo "$*" >> arguments
            while [ $# -gt 0 ]; do
              case "$1" in
                --results-directory) results="$2"; shift ;;
                --logger) logger="$2"; shift ;;
              esac
              shift
            done
            mkdir -p "${results}/${count}"
            if [ "$SCENARIO" != missing ]; then
              echo '<coverage/>' > "${results}/${count}/coverage.cobertura.xml"
            fi
            if [ "$SCENARIO" = abort ] || { [ "$SCENARIO" = retry-abort ] && [ "$count" -eq 2 ]; }; then
              echo 'The active test run was aborted. Reason: Test host process crashed'
              exit 1
            fi
            if [ "$SCENARIO" = named ] || { [ "$SCENARIO" = flaky ] && [ "$count" -eq 1 ]; } || \
               { [ "$SCENARIO" = retry-abort ] && [ "$count" -eq 1 ]; }; then
              echo '<TestRun xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010"><Results><UnitTestResult outcome="Failed" testName="Failed_method"/></Results></TestRun>' > "${results}/${logger#*LogFileName=}"
              exit 1
            fi
            echo 'Passed!'
            exit 0
        """), encoding="utf-8", newline="\n")
        (tools / "dotnet").chmod(0o755)
        # Git Bash understands drive-prefixed paths in commands but its PATH is
        # colon-separated, so address the temporary tool directory relatively.
        self.prefix = 'export PATH="$PWD/bin:$PATH"\n'

    def run_script(self, name: str) -> subprocess.CompletedProcess:
        body = script(name).replace("${{ github.run_attempt }}", "1")
        path = self.root / "step.sh"
        path.write_text(self.prefix + body, encoding="utf-8", newline="\n")
        return subprocess.run(
            [bash(), "step.sh"], cwd=self.root, env=self.env,
            text=True, capture_output=True, timeout=30,
        )

    def assert_scenario(self, scenario: str, complete: bool, success: bool):
        self.env["SCENARIO"] = scenario
        result = self.run_script("Test with coverage")
        self.assertEqual(success, result.returncode == 0, result.stdout + result.stderr)
        self.assertIn(f"complete={str(complete).lower()}", (self.root / "output.txt").read_text())
        arguments = (self.root / "arguments").read_text()
        self.assertIn("TestCategory!=Tlc", arguments)
        if scenario in {"named", "flaky", "retry-abort"}:
            self.assertEqual("2", (self.root / "calls").read_text().strip())
            self.assertIn("FullyQualifiedName~Failed_method", arguments.splitlines()[1])
            self.assertIn("TestCategory!=Tlc", arguments.splitlines()[1])

    def test_complete_pass(self):
        self.assert_scenario("pass", complete=True, success=True)

    def test_named_failure_keeps_whole_report_but_fails(self):
        self.assert_scenario("named", complete=True, success=False)

    def test_retry_pass_is_visible_as_flaky(self):
        self.assert_scenario("flaky", complete=True, success=True)
        self.assertIn("Flaky suites", (self.root / "summary.txt").read_text())

    def test_aborted_host_with_a_report_withholds_upload(self):
        self.assert_scenario("abort", complete=False, success=False)

    def test_missing_report_withholds_upload_and_fails(self):
        self.assert_scenario("missing", complete=False, success=False)

    def test_aborted_retry_withholds_upload(self):
        self.assert_scenario("retry-abort", complete=False, success=False)

    def test_upload_requires_every_planned_leg(self):
        for leg in ("leg-1", "leg-2"):
            artifact = self.root / "coverage" / f"coverage-1-{leg}"
            (artifact / "complete").mkdir(parents=True)
            (artifact / "complete" / leg).touch()
            (artifact / "coverage.cobertura.xml").write_text("<coverage/>")
        result = self.run_script("Verify all coverage legs are complete")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        (self.root / "output.txt").unlink()
        (self.root / "coverage" / "coverage-1-leg-2" / "complete" / "leg-2").unlink()
        result = self.run_script("Verify all coverage legs are complete")
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("complete=true", (self.root / "output.txt").read_text() if (self.root / "output.txt").exists() else "")

    def test_upload_rejects_a_marker_without_a_report(self):
        for leg in ("leg-1", "leg-2"):
            artifact = self.root / "coverage" / f"coverage-1-{leg}" / "complete"
            artifact.mkdir(parents=True)
            (artifact / leg).touch()
        result = self.run_script("Verify all coverage legs are complete")
        self.assertNotEqual(0, result.returncode)

    def test_upload_rejects_an_empty_plan(self):
        self.env["LEGS"] = "[]"
        result = self.run_script("Verify all coverage legs are complete")
        self.assertNotEqual(0, result.returncode)


def shell_root(test: unittest.TestCase) -> Path:
    """A temporary cwd with a bin/ of tool wrappers, addressed relatively from bash."""
    temporary = tempfile.TemporaryDirectory()
    test.addCleanup(temporary.cleanup)
    root = Path(temporary.name)
    tools = root / "bin"
    tools.mkdir()
    python = Path(sys.executable).as_posix()
    (tools / "python3").write_text(f'#!/bin/bash\nexec "{python}" "$@"\n', encoding="utf-8", newline="\n")
    (tools / "python3").chmod(0o755)
    if sys.platform == "win32":
        jq = Path(shutil.which("jq")).as_posix()
        (tools / "jq").write_text(f'#!/bin/bash\nexec "{jq}" --binary "$@"\n', encoding="utf-8", newline="\n")
        (tools / "jq").chmod(0o755)
    return root


def run_step(root: Path, name: str, env: dict) -> subprocess.CompletedProcess:
    (root / "step.sh").write_text('export PATH="$PWD/bin:$PATH"\n' + script(name), encoding="utf-8", newline="\n")
    return subprocess.run([bash(), "step.sh"], cwd=root, env=env, text=True, capture_output=True, timeout=60)


class PlanShellTests(unittest.TestCase):
    def setUp(self):
        self.root = shell_root(self)
        workflows = self.root / ".github" / "workflows"
        workflows.mkdir(parents=True)
        for name in ("plan-coverage-matrix.py", "plan-test-matrix.py", "runner-overflow.py",
                     "test-shards.json", "test-durations.tsv"):
            shutil.copyfile(WORKFLOWS / name, workflows / name)
        for package, project in (("lattice", "Orleans.Lattice.Tests"), ("other", "Other.Tests")):
            path = self.root / "test" / package / f"{project}.csproj"
            path.parent.mkdir(parents=True)
            path.touch()
        self.env = dict(os.environ, GITHUB_OUTPUT="output.txt", GITHUB_STEP_SUMMARY="summary.txt",
                        DEFAULT_BRANCH="main", TLC_SHARDS="", TRACK_TLC_FAILURES="false")
        # Hermetic: no pool configured, so runner-overflow.py routes every leg hosted
        # without calling the GitHub API.
        self.env.pop("RUNNER_POOL", None)

    def plan(self, ref: str, **inputs: str) -> dict:
        self.env.update(GITHUB_REF=ref, TLC_SHARDS="", TRACK_TLC_FAILURES="false")
        self.env.update(inputs)
        result = run_step(self.root, "Plan coverage shards", self.env)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        outputs = dict(
            line.split("=", 1) for line in (self.root / "output.txt").read_text().splitlines()
        )
        (self.root / "output.txt").unlink()
        outputs["stdout"] = result.stdout
        return outputs

    def test_a_full_run_on_main_plans_both_matrices_and_tracks(self):
        outputs = self.plan("refs/heads/main")
        self.assertEqual("true", outputs["coverage"])
        self.assertEqual("true", outputs["track"])
        self.assertGreater(len(json.loads(outputs["legs"])), 0)
        shards = {item["shard"] for leg in json.loads(outputs["tlc_legs"]) for item in leg["items"]}
        self.assertEqual(set(declared_tlc_shards()), shards)
        # Both matrices pass through runner-overflow.py before they are published.
        for leg in json.loads(outputs["legs"]) + json.loads(outputs["tlc_legs"]):
            self.assertEqual('"ubuntu-latest"', leg["runsOn"])
            self.assertIs(False, leg["pool"])

    def test_a_restricted_branch_run_skips_coverage_and_tracks_only_on_request(self):
        outputs = self.plan("refs/heads/ci/probe", TLC_SHARDS="formal-tlc-smoke")
        self.assertEqual("false", outputs["coverage"])
        self.assertEqual("false", outputs["track"])
        self.assertEqual([], json.loads(outputs["legs"]))
        self.assertEqual(1, len(json.loads(outputs["tlc_legs"])))
        self.assertEqual("true", self.plan("refs/heads/ci/probe", TLC_SHARDS="formal-tlc-smoke",
                                           TRACK_TLC_FAILURES="true")["track"])
        self.assertEqual("false", self.plan("refs/heads/ci/probe")["track"])

    def test_main_ignores_a_restriction_so_a_partial_run_never_stands_in_for_a_night(self):
        outputs = self.plan("refs/heads/main", TLC_SHARDS="formal-tlc-smoke")
        self.assertIn("TLC restriction ignored", outputs["stdout"])
        self.assertEqual("true", outputs["coverage"])
        self.assertEqual(len(declared_tlc_shards()),
                         sum(len(leg["items"]) for leg in json.loads(outputs["tlc_legs"])))

    def test_an_unknown_shard_fails_the_plan(self):
        self.env.update(GITHUB_REF="refs/heads/ci/probe", TLC_SHARDS="formal-tlc-nope")
        result = run_step(self.root, "Plan coverage shards", self.env)
        self.assertNotEqual(0, result.returncode)


class TlcTrackingShellTests(unittest.TestCase):
    TITLE = "Nightly exhaustive TLC failing on main"

    def setUp(self):
        self.root = shell_root(self)
        (self.root / "bin" / "gh").write_text(textwrap.dedent("""\
            #!/bin/bash
            printf '%s\\n' "${*//$'\\n'/ }" >> calls
            if [ "$1" = api ]; then printf '%s' "$OPEN_ISSUES"; fi
        """), encoding="utf-8", newline="\n")
        (self.root / "bin" / "gh").chmod(0o755)
        self.env = dict(os.environ, GITHUB_SERVER_URL="https://github.com", GITHUB_REPOSITORY="o/r",
                        GITHUB_RUN_ID="42", GITHUB_REF_NAME="main", GITHUB_SHA="abc", OPEN_ISSUES="")

    def track(self, legs: str, report: str, open_issues: str = "") -> list[str]:
        self.env.update(TLC_RESULT=legs, REPORT_RESULT=report, OPEN_ISSUES=open_issues)
        (self.root / "calls").unlink(missing_ok=True)
        result = run_step(self.root, "Open, update or close the nightly TLC tracking issue", self.env)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        calls = self.root / "calls"
        return calls.read_text().splitlines() if calls.exists() else []

    def test_a_first_red_night_opens_the_issue_with_the_run_link(self):
        calls = self.track("failure", "success")
        self.assertEqual(2, len(calls))
        self.assertTrue(calls[0].startswith("api --paginate -X GET repos/o/r/issues"), calls[0])
        self.assertIn(f'.title == "{self.TITLE}"', calls[0])
        self.assertIn("-f labels=ci", calls[0])
        self.assertTrue(calls[1].startswith(f"issue create --repo o/r --title {self.TITLE} --label ci"), calls[1])
        self.assertIn("https://github.com/o/r/actions/runs/42", calls[1])

    def test_a_later_red_night_comments_on_the_oldest_open_issue(self):
        calls = self.track("success", "failure", "17\n9\n")
        self.assertEqual(2, len(calls))
        self.assertTrue(calls[1].startswith("issue comment 9 --repo o/r --body"), calls[1])
        self.assertIn("actions/runs/42", calls[1])

    def test_the_first_green_night_closes_the_issue(self):
        calls = self.track("success", "success", "9\n")
        self.assertTrue(calls[1].startswith("issue close 9 --repo o/r --comment"), calls[1])

    def test_a_green_night_with_no_issue_writes_nothing(self):
        self.assertEqual(1, len(self.track("success", "success")))

    def test_no_verdict_touches_nothing(self):
        for legs, report in (("cancelled", "success"), ("skipped", "success"), ("success", "cancelled")):
            with self.subTest(legs=legs, report=report):
                self.assertEqual([], self.track(legs, report, "9\n"))


if __name__ == "__main__":
    unittest.main()
