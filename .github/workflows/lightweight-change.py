#!/usr/bin/env python3
"""Classify PRs with no .NET or formal source changes for lightweight CI."""

import argparse
from pathlib import Path
import re
import subprocess
import unittest


def is_lightweight(paths: list[str]) -> bool:
    return bool(paths) and all(
        path.startswith(("videos/", ".github/workflows/"))
        or path == ".github/copilot-instructions.md"
        for path in paths
    )


class ClassificationTests(unittest.TestCase):
    def test_video_and_workflow_changes(self):
        self.assertTrue(is_lightweight([
            "videos/package-lock.json", ".github/workflows/ci.yml",
            ".github/workflows/trailer-guard-selftest.py",
            ".github/copilot-instructions.md",
        ]))

    def test_source_and_formal_changes_require_full_ci(self):
        for path in ("src/lattice/Tree.cs", "test/lattice/TreeTests.cs",
                     "spec/Atomic.tla", "docs/lattice/README.md",
                     "Directory.Build.props", ".github/workflows-other/file"):
            with self.subTest(path=path):
                self.assertFalse(is_lightweight(["videos/package-lock.json", path]))

    def test_empty_diff_is_not_lightweight(self):
        self.assertFalse(is_lightweight([]))

    def test_workflow_wires_both_classifiers_and_gates_expensive_steps(self):
        workflow = Path(__file__).with_name("ci.yml").read_text(encoding="utf-8")
        self.assertIn("lightweight: ${{ steps.lightweight.outputs.only }}", workflow)
        for job in ("plan", "content-gates"):
            block = re.search(rf"^  {job}:\n(.*?)(?=^  [\w-]+:|\Z)",
                              workflow, re.MULTILINE | re.DOTALL).group(1)
            self.assertIn("id: lightweight", block)
            self.assertIn("if: github.event_name == 'pull_request'", block)
            self.assertIn('--base "$BASE_SHA" --head "$HEAD_SHA"', block)
        content = re.search(r"^  content-gates:\n(.*?)(?=^  [\w-]+:|\Z)",
                            workflow, re.MULTILINE | re.DOTALL).group(1)
        for name in ("Setup .NET", "Cache NuGet packages", "Restore", "Build",
                     "Run content gates"):
            self.assertIn(
                f"- name: {name}\n        if: steps.lightweight.outputs.only != 'true'",
                content,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base")
    parser.add_argument("--head")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        unittest.main(argv=["lightweight-change"], exit=True)
    if not args.base or not args.head:
        parser.error("--base and --head are required")
    diff = subprocess.check_output(
        ["git", "diff", "--name-only", "--no-renames", "-z",
         f"{args.base}...{args.head}"],
    )
    paths = diff.decode("utf-8").rstrip("\0").split("\0") if diff else []
    print(str(is_lightweight(paths)).lower())
