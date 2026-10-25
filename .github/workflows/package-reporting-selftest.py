#!/usr/bin/env python3
"""Run CI's actual discovery/reporting shell against small selector universes."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest

WORKFLOWS = Path(__file__).resolve().parent
BASH = os.environ.get("BASH", "bash")
if os.name == "nt" and "BASH" not in os.environ:
    BASH = r"C:\Program Files\Git\bin\bash.exe"


def workflow_script():
    yaml = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
    step = re.search(
        r"^      - name: Discover changed packages\n(?P<block>.*?)(?=^      - )",
        yaml, re.MULTILINE | re.DOTALL,
    )
    if step is None:
        raise AssertionError("Missing Discover changed packages workflow step")
    run = re.search(
        r"^        run: \|\n(?P<body>(?:^          .*\n|^\n)+)",
        step["block"], re.MULTILINE,
    )
    if run is None:
        raise AssertionError("Missing discovery shell body")
    return textwrap.dedent(run["body"])


class PackageReportingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        workflows = self.root / ".github" / "workflows"
        workflows.mkdir(parents=True)
        self.selector = workflows / "select-test-packages.sh"
        shutil.copyfile(WORKFLOWS / self.selector.name, self.selector)
        self.selector.chmod(0o755)
        self.sources = ("alpha", "beta", "gamma", "delta")
        for package in self.sources:
            project = self.root / "src" / package / f"{package}.csproj"
            project.parent.mkdir(parents=True)
            project.write_text("<Project />\n", encoding="utf-8")
        integration = self.root / "test" / "lattice.integration"
        integration.mkdir(parents=True)
        (integration / "Integration.Tests.csproj").write_text(
            '<Project><ProjectReference Include="../../src/alpha/alpha.csproj" /></Project>\n',
            encoding="utf-8",
        )
        (self.root / "scratch").mkdir()
        self.summary = self.root / "summary"
        self.output = self.root / "output"

    def run_workflow(self, changed):
        (self.root / "scratch" / "changed-files.txt").write_text(
            "\n".join(changed) + "\n", encoding="utf-8",
        )
        # Only relocate runner-local scratch files; execute the body unchanged.
        body = workflow_script().replace("/tmp/", "scratch/")
        return subprocess.run(
            [BASH, "-c", body], cwd=self.root, text=True, capture_output=True,
            env={**os.environ, "GITHUB_OUTPUT": str(self.output),
                 "GITHUB_STEP_SUMMARY": str(self.summary)},
        )

    def assert_report(self, changed, expected, build_all):
        proc = self.run_workflow(changed)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        outputs = dict(line.split("=", 1) for line in self.output.read_text().splitlines())
        self.assertEqual(sorted(expected), outputs["to_test"].split())
        self.assertEqual(build_all, outputs["build_all"])
        self.assertIn(f"::notice::Testing {len(expected)} of 5 packages.", proc.stdout)
        self.assertIn(
            f"### Test selection: {len(expected)} of 5 packages",
            self.summary.read_text(),
        )

    def test_full_selection_includes_source_and_test_only_packages(self):
        self.assert_report([], [*self.sources, "lattice.integration"], "true")

    def test_narrow_test_only_selection_retains_universe_denominator(self):
        self.assert_report(["test/lattice.integration/Test.cs"], ["lattice.integration"], "false")

    def test_below_half_uses_test_only_package_in_build_density(self):
        self.assert_report(
            ["src/beta/Code.cs", "test/lattice.integration/Test.cs"],
            ["beta", "lattice.integration"], "false",
        )

    def test_at_half_rounded_up_builds_whole_solution(self):
        self.assert_report(
            ["src/beta/Code.cs", "src/gamma/Code.cs", "test/lattice.integration/Test.cs"],
            ["beta", "gamma", "lattice.integration"], "true",
        )

    def test_invalid_test_only_universe_fails_without_success_notice(self):
        (self.root / "test" / "lattice.integration" / "Integration.Tests.csproj").unlink()
        proc = self.run_workflow([])
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("test-only package", proc.stderr)
        self.assertNotIn("::notice::Testing", proc.stdout)
        self.assertFalse(self.output.exists())

    def test_empty_selection_fails_without_success_notice(self):
        self.selector.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        (self.root / "scratch" / "selection.md").touch()
        proc = self.run_workflow([])
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("::error::The package selector produced an empty selection", proc.stdout)
        self.assertNotIn("::notice::Testing", proc.stdout)
        self.assertFalse(self.output.exists())

    def test_empty_universe_fails_without_success_notice(self):
        self.selector.write_text(
            '#!/usr/bin/env bash\nif [ "${1-}" != "--list-packages" ]; then\n'
            '  echo alpha\nfi\n', encoding="utf-8",
        )
        (self.root / "scratch" / "selection.md").touch()
        proc = self.run_workflow([])
        self.assertNotEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertIn("::error::", proc.stdout + proc.stderr)
        self.assertNotIn("::notice::Testing", proc.stdout)
        self.assertFalse(self.output.exists())

    def test_list_packages_is_sorted_validated_and_does_not_read_stdin(self):
        proc = subprocess.run(
            [BASH, str(self.selector), "--list-packages"], cwd=self.root,
            input="ignored\n", text=True, capture_output=True,
        )
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual(sorted([*self.sources, "lattice.integration"]), proc.stdout.splitlines())
        self.assertEqual("", proc.stderr)
        (self.root / "test" / "lattice.integration" / "Integration.Tests.csproj").unlink()
        invalid = subprocess.run(
            [BASH, str(self.selector), "--list-packages"], cwd=self.root,
            text=True, capture_output=True,
        )
        self.assertNotEqual(0, invalid.returncode)
        self.assertIn("test-only package", invalid.stderr)
        self.assertEqual("", invalid.stdout)

    def test_test_only_universe_does_not_invent_a_source_package(self):
        shutil.rmtree(self.root / "src")
        (self.root / "src").mkdir()
        proc = subprocess.run(
            [BASH, str(self.selector), "--list-packages"], cwd=self.root,
            text=True, capture_output=True,
        )
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual(["lattice.integration"], proc.stdout.splitlines())


if __name__ == "__main__":
    unittest.main(verbosity=2)
