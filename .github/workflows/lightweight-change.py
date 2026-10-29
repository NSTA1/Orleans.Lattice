#!/usr/bin/env python3
"""Classify PRs with no .NET or formal source changes for lightweight CI."""

import argparse
from pathlib import Path
import re
import subprocess
import unittest

# Workflow files whose change is pinned by .NET tests (TlcCiShardTests,
# CiMemberPullRequestTieringTests, TlcInputClassificationTests): they route TLC
# cases or classify TLC verdict inputs, so the lightweight lane, which runs no
# .NET test, cannot validate them (#5149).
TLC_ROUTING_FILES = frozenset(
    ".github/workflows/" + name
    for name in (
        "plan-test-matrix.py",
        "run-test-leg.py",
        "test-shards.json",
        "tier-scope.py",
        "tlc-inputs.json",
        "tlc-fingerprint.py",
        "tlc-receipts.py",
    )
)


def is_lightweight(paths: list[str]) -> bool:
    return bool(paths) and all(
        (path.startswith(("videos/", ".github/workflows/"))
         or path == ".github/copilot-instructions.md")
        and path not in TLC_ROUTING_FILES
        for path in paths
    )


class ClassificationTests(unittest.TestCase):
    def test_video_and_workflow_changes(self):
        self.assertTrue(is_lightweight([
            "videos/package-lock.json", ".github/workflows/ci.yml",
            ".github/workflows/trailer-guard-selftest.py",
            ".github/copilot-instructions.md",
        ]))

    def test_tlc_routing_files_require_full_ci(self):
        for path in sorted(TLC_ROUTING_FILES):
            with self.subTest(path=path):
                self.assertTrue(Path(__file__).with_name(Path(path).name).is_file(), path)
                self.assertFalse(is_lightweight([path]))
                self.assertFalse(is_lightweight(["videos/package-lock.json", path]))

    def test_source_and_formal_changes_require_full_ci(self):
        for path in ("src/lattice/Tree.cs", "test/lattice/TreeTests.cs",
                     "spec/Atomic.tla", "docs/lattice/README.md",
                     "Directory.Build.props", ".github/workflows-other/file"):
            with self.subTest(path=path):
                self.assertFalse(is_lightweight(["videos/package-lock.json", path]))

    def test_empty_diff_is_not_lightweight(self):
        self.assertFalse(is_lightweight([]))

    def test_tla_toolchain_changes_require_full_ci(self):
        # The toolchain's pins and its provisioning action live outside
        # .github/workflows/ (#5151), so a toolchain change runs TLC.
        root = Path(__file__).resolve().parents[2]
        for path in (".github/actions/tla-toolchain/action.yml",
                     "tools/tlc/toolchain.json", "tools/tlc/toolchain.py"):
            with self.subTest(path=path):
                self.assertTrue((root / path).is_file(), path)
                self.assertFalse(is_lightweight([path]))
                self.assertFalse(is_lightweight([".github/workflows/ci.yml", path]))

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
