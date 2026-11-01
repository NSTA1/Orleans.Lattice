#!/usr/bin/env python3
"""Self-test for journey-screenshot-provenance.py (#5363).

Drives the check against an in-memory fake of the GitHub reads it makes, so
every failure rule is exercised without a network: a valid section passes, and
each rule's violation fails with that rule's message. Run by the lane's
`planner` job.
"""

import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

WORKFLOWS = Path(__file__).resolve().parent
SCRIPT = WORKFLOWS / "journey-screenshot-provenance.py"

spec = importlib.util.spec_from_file_location("journey_screenshot_provenance", SCRIPT)
provenance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provenance)

HEAD = "a" * 40
OTHER = "b" * 40
RUN = 41208589851


def manifest_bytes(jid="a3", commit=HEAD, count=3, failed=(), run=RUN):
    entries = [{"index": index + 1, "step": "step", "substep": f"s{index + 1}", "title": f"Shot {index + 1}",
                "persona": "olivia", "file": f"{index + 1:03}-step-s{index + 1}.png", "sha256": "0" * 64,
                "passed": index + 1 not in failed} for index in range(count)]
    return json.dumps({"journey": jid, "commit": commit, "runId": str(run), "runAttempt": "1",
                       "viewport": "desktop", "width": 1440, "height": 900, "entries": entries},
                      indent=2).encode("utf-8")


class FakeGitHub:
    """Runs, their artifacts and the files inside each artifact."""

    def __init__(self, root):
        self.root = Path(root)
        self.runs = {}
        self.files = {}
        self.downloads = []

    def add_run(self, run_id, head=HEAD, path=provenance.WORKFLOW_PATH, artifacts=None):
        self.runs[run_id] = {"head_sha": head, "path": path}
        for name, (files, expired) in (artifacts or {}).items():
            self.files[(run_id, name)] = (files, expired)

    def run(self, run_id):
        if run_id not in self.runs:
            raise LookupError("HTTP 404: Not Found")
        return self.runs[run_id]

    def artifacts(self, run_id):
        return [(name, expired) for (rid, name), (_, expired) in self.files.items() if rid == run_id]

    def download(self, run_id, name):
        self.downloads.append(name)
        target = self.root / f"{run_id}-{name}"
        for relative, data in self.files[(run_id, name)][0].items():
            path = target / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return target


def marker(jid="a3", sha=HEAD, run=RUN, data=None, count=3):
    digest = hashlib.sha256(data if data is not None else manifest_bytes(jid=jid, count=count, run=run)).hexdigest()
    return f"<!-- journey-screenshots: journey={jid} sha={sha} run={run} manifest={digest} count={count} -->"


def body(markers, images=3, extra=""):
    shots = "\n\n".join(f"**{index + 1}.** caption\n\n![Shot {index + 1}](https://github.com/user-attachments/assets/{index})"
                        for index in range(images))
    return (f"## Summary\n\nRefs #5383\n\n## Journey screenshots\n\n{markers}\n\n{shots}\n\n{extra}"
            "## Changes\n\n![not a journey screenshot](https://example.invalid/x.png)\n")


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.github = FakeGitHub(self._temporary.name)
        self.github.add_run(RUN, artifacts={"journey-screenshots-a3": ({"manifest.json": manifest_bytes()}, False)})

    def check(self, text, journeys=("a3",), head=HEAD):
        return provenance.check(text, head, list(journeys), self.github)

    def assertOneProblem(self, problems, fragment):
        self.assertEqual(1, len(problems), problems)
        self.assertIn(fragment, problems[0])

    def test_a_section_matching_its_run_passes(self):
        self.assertEqual([], self.check(body(marker())))

    def test_crlf_bodies_parse_like_lf_ones(self):
        self.assertEqual([], self.check(body(marker()).replace("\n", "\r\n")))

    def test_the_head_sha_comparison_ignores_case(self):
        self.assertEqual([], self.check(body(marker()), head=HEAD.upper()))

    def test_html_images_count(self):
        text = body(marker(), images=0, extra='<img src="1"> <img alt="2" src="2">\n<IMG src="3">\n\n')
        self.assertEqual([], self.check(text))

    def test_a_missing_marker_fails(self):
        self.assertOneProblem(self.check(body("")), "no screenshot marker")

    def test_a_marker_in_code_is_documentation_not_a_claim(self):
        for wrapped in (f"`{marker()}`", f"```text\n{marker()}\n```", f"~~~\n{marker()}\n~~~"):
            with self.subTest(wrapped=wrapped[:4]):
                self.assertOneProblem(self.check(body(wrapped)), "no screenshot marker")

    def test_a_stale_sha_fails(self):
        self.assertOneProblem(self.check(body(marker(sha=OTHER))), "is not the head commit")

    def test_a_short_sha_is_refused(self):
        problems = self.check(body(marker(sha=HEAD[:12])))
        self.assertTrue(any("full 40-character" in problem for problem in problems), problems)

    def test_a_digest_that_is_not_the_manifest_fails(self):
        self.assertOneProblem(self.check(body(marker(data=b"tampered"))), "is not the sha256")

    def test_fewer_images_than_the_count_fails(self):
        self.assertOneProblem(self.check(body(marker(), images=2)), "shows 2 images")

    def test_images_outside_the_section_do_not_count(self):
        self.assertOneProblem(self.check(body(marker(), images=2)), "markers count 3")

    def test_a_missing_section_fails(self):
        self.assertOneProblem(self.check(marker() + "\n\n![a](1) ![b](2) ![c](3)"), "no '## Journey screenshots' section")

    def test_an_unknown_run_fails(self):
        self.assertOneProblem(self.check(body(marker(run=1))), "404")

    def test_a_run_on_another_commit_fails(self):
        self.github.add_run(7, head=OTHER, artifacts={"journey-screenshots-a3": ({"manifest.json": manifest_bytes()}, False)})
        self.assertOneProblem(self.check(body(marker(run=7))), "ran on")

    def test_a_run_of_another_workflow_fails(self):
        self.github.add_run(8, path=".github/workflows/ci.yml",
                            artifacts={"journey-screenshots-a3": ({"manifest.json": manifest_bytes()}, False)})
        self.assertOneProblem(self.check(body(marker(run=8))), "is a run of")

    def test_a_run_without_the_artifact_fails(self):
        self.github.add_run(9, artifacts={"explorer-journeys-harness": ({"x.log": b"log"}, False)})
        self.assertOneProblem(self.check(body(marker(run=9))), "no journey-screenshots-a3 artifact")

    def test_an_expired_artifact_fails(self):
        self.github.add_run(10, artifacts={"journey-screenshots-a3": ({"manifest.json": manifest_bytes()}, True)})
        self.assertOneProblem(self.check(body(marker(run=10))), "expired")

    def test_a_count_other_than_the_manifest_fails(self):
        data = manifest_bytes(count=4, run=11)
        self.github.add_run(11, artifacts={"journey-screenshots-a3": ({"manifest.json": data}, False)})
        problems = self.check(body(marker(run=11, data=data, count=3), images=4))
        self.assertOneProblem(problems, "manifest holds 4 screenshots")

    def test_a_manifest_of_another_journey_or_commit_fails(self):
        data = manifest_bytes(jid="a4", commit=OTHER, run=12)
        self.github.add_run(12, artifacts={"journey-screenshots-a3": ({"manifest.json": data}, False)})
        problems = self.check(body(marker(run=12, data=data)))
        self.assertEqual(2, len(problems), problems)
        self.assertIn("names journey 'a4'", problems[0])
        self.assertIn("records commit", problems[1])

    def test_a_manifest_of_another_run_fails(self):
        data = manifest_bytes(run=99)
        self.github.add_run(13, artifacts={"journey-screenshots-a3": ({"manifest.json": data}, False)})
        self.assertOneProblem(self.check(body(marker(run=13, data=data))), "records run '99'")

    def test_a_failed_checkpoint_fails(self):
        data = manifest_bytes(failed=(2,), run=14)
        self.github.add_run(14, artifacts={"journey-screenshots-a3": ({"manifest.json": data}, False)})
        self.assertOneProblem(self.check(body(marker(run=14, data=data))), "failed checkpoints: step/s2")

    def test_a_leg_artifact_is_the_fallback_and_must_be_unambiguous(self):
        self.github.add_run(15, artifacts={
            "journey-screenshots-leg-harness": ({"f2-personas/manifest.json": manifest_bytes(jid="f2-personas", run=15)}, False),
            "journey-screenshots-leg-story-contoso": ({"a3/manifest.json": manifest_bytes(run=15)}, False),
        })
        self.assertEqual([], self.check(body(marker(run=15))))
        self.github.add_run(16, artifacts={
            "journey-screenshots-leg-harness": ({"a3/manifest.json": manifest_bytes(run=16)}, False),
            "journey-screenshots-leg-story-contoso": ({"a3/manifest.json": manifest_bytes(run=16)}, False),
        })
        self.assertOneProblem(self.check(body(marker(run=16))), "holds 2 manifests")

    def test_the_isolated_artifact_wins_over_a_leg_artifact(self):
        self.github.add_run(17, artifacts={
            "journey-screenshots-a3": ({"manifest.json": manifest_bytes(run=17)}, False),
            "journey-screenshots-leg-story-contoso": ({"a3/manifest.json": b"different"}, False),
        })
        self.assertEqual([], self.check(body(marker(run=17))))
        self.assertNotIn("journey-screenshots-leg-story-contoso", self.github.downloads)

    def test_an_isolated_artifact_must_hold_its_manifest_at_the_root(self):
        self.github.add_run(18, artifacts={"journey-screenshots-a3": ({"a3/manifest.json": manifest_bytes()}, False)})
        self.assertOneProblem(self.check(body(marker(run=18))), "no manifest.json at its root")

    def test_identical_markers_are_one_claim_and_different_ones_conflict(self):
        self.assertEqual([], self.check(body(marker() + "\n" + marker())))
        problems = self.check(body(marker() + "\n" + marker(run=RUN + 1)))
        self.assertTrue(any("two different markers" in problem for problem in problems), problems)

    def test_a_marker_lacking_a_field_fails(self):
        incomplete = f"<!-- journey-screenshots: sha={HEAD} run={RUN} manifest={'0' * 64} count=3 -->"
        problems = self.check(body(incomplete))
        self.assertTrue(any("lacks journey" in problem for problem in problems), problems)
        self.assertTrue(any("no screenshot marker" in problem for problem in problems), problems)

    def test_a_zero_count_is_refused(self):
        problems = self.check(body(marker(count=0)))
        self.assertTrue(any("positive integers" in problem for problem in problems), problems)

    def test_two_journeys_need_both_markers_and_the_sum_of_their_images(self):
        self.github.add_run(19, artifacts={
            "journey-screenshots-a3": ({"manifest.json": manifest_bytes(run=19)}, False),
            "journey-screenshots-t2": ({"manifest.json": manifest_bytes(jid="t2", count=2, run=19)}, False),
        })
        both = marker(run=19) + "\n" + marker(jid="t2", run=19, count=2)
        self.assertEqual([], self.check(body(both, images=5), journeys=("a3", "t2")))
        self.assertOneProblem(self.check(body(both, images=4), journeys=("a3", "t2")), "markers count 5")
        self.assertOneProblem(self.check(body(marker(run=19), images=5), journeys=("a3", "t2")), "t2: no screenshot marker")

    def test_no_journey_means_nothing_to_check(self):
        self.assertEqual([], self.check("no section at all", journeys=()))

    def test_the_marker_line_round_trips(self):
        markers, problems = provenance.parse_markers(marker())
        self.assertEqual([], problems)
        self.assertEqual(marker(), markers["a3"].line())


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() and result.testsRun > 0 else 1)
