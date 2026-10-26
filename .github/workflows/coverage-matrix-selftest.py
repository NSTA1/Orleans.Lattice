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
            'shift 3\nexec timeout --kill-after=10s 120s "$@"\n',
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


if __name__ == "__main__":
    unittest.main()
