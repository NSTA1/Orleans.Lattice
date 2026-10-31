#!/usr/bin/env python3
"""Check a member pull request's journey screenshot section against its run (#5363, epic D10).

A member pull request into the Explorer journeys bucket that touches
`test/lattice.explorer.e2e/Journeys/<id>/` carries a `## Journey screenshots`
section with one marker per journey:

    <!-- journey-screenshots: journey=<jid> sha=<head> run=<id> manifest=<sha256> count=<n> -->

`tools/Get-JourneyScreenshots.ps1` prints that line from a run's artifact. For
every journey the plan names, the check fails when:

  - the body carries no marker for it (markers in code spans and fenced blocks
    are documentation, not claims, and are ignored), or carries two that differ;
  - `sha` is not the pull request's head commit;
  - `run` is not a run of explorer-journeys.yml on that commit;
  - that run has no `journey-screenshots-<jid>` artifact (or, failing that,
    exactly one leg artifact holding `<jid>/manifest.json`), or it expired;
  - `manifest` is not the sha256 of that manifest.json's bytes;
  - the manifest names another journey or commit, holds a number of entries
    other than `count`, or records a checkpoint that failed;
  - the `## Journey screenshots` section is missing or has fewer images than
    the markers' counts add up to.
"""

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

WORKFLOW_PATH = ".github/workflows/explorer-journeys.yml"
REQUIRED_FIELDS = ("journey", "sha", "run", "manifest", "count")
MARKER = re.compile(r"<!--\s*journey-screenshots:(?P<fields>.*?)-->", re.DOTALL)
FENCE = re.compile(r"^[ \t]{0,3}(?P<fence>`{3,}|~{3,}).*?^[ \t]{0,3}(?P=fence)[ \t]*$", re.MULTILINE | re.DOTALL)
INLINE_CODE = re.compile(r"(`+)(?!`)(?:(?!\n[ \t]*\n).)+?(?<!`)\1(?!`)", re.DOTALL)
SECTION = re.compile(r"^##[ \t]+Journey screenshots[ \t]*$", re.MULTILINE | re.IGNORECASE)
NEXT_SECTION = re.compile(r"^##[ \t]+\S", re.MULTILINE)
MARKDOWN_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]+\)")
HTML_IMAGE = re.compile(r"<img\b[^>]*>", re.IGNORECASE)
HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class Marker:
    journey: str
    sha: str
    run: int
    manifest: str
    count: int

    def line(self):
        return (f"<!-- journey-screenshots: journey={self.journey} sha={self.sha} run={self.run} "
                f"manifest={self.manifest} count={self.count} -->")


def strip_code(body):
    """The body, with normalised line endings, without fenced blocks and inline code spans."""
    body = body.replace("\r\n", "\n").replace("\r", "\n")
    return INLINE_CODE.sub("", FENCE.sub("", body))


def parse_markers(body):
    """(markers by journey, problems) from a pull request body."""
    markers = {}
    problems = []
    for match in MARKER.finditer(strip_code(body)):
        fields = {}
        for token in match.group("fields").split():
            key, separator, value = token.partition("=")
            if not separator:
                problems.append(f"marker token '{token}' is not key=value: {match.group(0)}")
                continue
            if key in fields:
                problems.append(f"marker repeats '{key}': {match.group(0)}")
            fields[key] = value
        missing = [key for key in REQUIRED_FIELDS if key not in fields]
        if missing:
            problems.append(f"marker lacks {', '.join(missing)}: {match.group(0)}")
            continue
        sha = fields["sha"].lower()
        digest = fields["manifest"].lower()
        if not HEX40.match(sha):
            problems.append(f"marker sha '{fields['sha']}' is not a full 40-character commit: {match.group(0)}")
            continue
        if not HEX64.match(digest):
            problems.append(f"marker manifest '{fields['manifest']}' is not a sha256: {match.group(0)}")
            continue
        if not fields["run"].isdigit() or not fields["count"].isdigit() or int(fields["count"]) < 1:
            problems.append(f"marker run and count must be positive integers: {match.group(0)}")
            continue
        marker = Marker(fields["journey"], sha, int(fields["run"]), digest, int(fields["count"]))
        prior = markers.get(marker.journey)
        if prior is not None and prior != marker:
            problems.append(f"two different markers for journey '{marker.journey}'")
            continue
        markers[marker.journey] = marker
    return markers, problems


def screenshot_section(body):
    """The text of the '## Journey screenshots' section, or None."""
    text = strip_code(body)
    match = SECTION.search(text)
    if match is None:
        return None
    rest = text[match.end():]
    following = NEXT_SECTION.search(rest)
    return rest[:following.start()] if following else rest


def count_images(text):
    return len(MARKDOWN_IMAGE.findall(text)) + len(HTML_IMAGE.findall(text))


def find_manifest(github, run_id, jid):
    """(manifest bytes, artifact name) for jid in a run, or raise LookupError.

    The isolated leg's `journey-screenshots-<jid>` artifact is authoritative.
    Only when the run has none is the journey looked for in the run's leg
    artifacts (`journey-screenshots-leg-*`, then the pre-#5363 single
    `explorer-journeys` artifact), and there it must be found exactly once.
    """
    artifacts = dict(github.artifacts(run_id))
    exact = "journey-screenshots-" + jid
    if exact in artifacts:
        if artifacts[exact]:
            raise LookupError(f"artifact '{exact}' of run {run_id} has expired")
        directory = Path(github.download(run_id, exact))
        manifest = directory / "manifest.json"
        if not manifest.is_file():
            raise LookupError(f"artifact '{exact}' of run {run_id} has no manifest.json at its root")
        return manifest.read_bytes(), exact

    tiers = [sorted(name for name in artifacts if name.startswith("journey-screenshots-leg-"))]
    if "explorer-journeys" in artifacts:
        tiers.append(["explorer-journeys"])
    for tier in tiers:
        found = []
        for name in tier:
            if artifacts[name]:
                continue
            directory = Path(github.download(run_id, name))
            found += [(path.read_bytes(), name) for path in sorted(directory.rglob("manifest.json"))
                      if path.parent.name == jid]
        if len(found) > 1:
            raise LookupError(
                f"run {run_id} holds {len(found)} manifests for '{jid}' ({', '.join(name for _, name in found)})")
        if found:
            return found[0]
    raise LookupError(
        f"run {run_id} has no journey-screenshots-{jid} artifact and no unexpired leg artifact holding {jid}/manifest.json")


def check(body, head_sha, journeys, github, workflow_path=WORKFLOW_PATH):
    """Every problem with the body's screenshot provenance for the given journeys."""
    markers, problems = parse_markers(body)
    head = (head_sha or "").lower()
    section = screenshot_section(body)
    required = 0

    for jid in journeys:
        marker = markers.get(jid)
        if marker is None:
            problems.append(
                f"{jid}: no screenshot marker. Run tools/Get-JourneyScreenshots.ps1 -Run <run> -Journey {jid}, "
                "upload the images and paste its section into the body")
            continue
        required += marker.count
        if marker.sha != head:
            problems.append(f"{jid}: marker sha {marker.sha[:12]} is not the head commit {head[:12]}; "
                            "re-capture the screenshots from a run on the head commit")
            continue
        try:
            run = github.run(marker.run)
        except LookupError as error:
            problems.append(f"{jid}: run {marker.run}: {error}")
            continue
        if (run.get("head_sha") or "").lower() != marker.sha:
            problems.append(f"{jid}: run {marker.run} ran on {str(run.get('head_sha'))[:12]}, not {marker.sha[:12]}")
            continue
        if run.get("path", "").split("@", 1)[0] != workflow_path:
            problems.append(f"{jid}: run {marker.run} is a run of '{run.get('path')}', not {workflow_path}")
            continue
        try:
            data, artifact = find_manifest(github, marker.run, jid)
        except LookupError as error:
            problems.append(f"{jid}: {error}")
            continue
        digest = hashlib.sha256(data).hexdigest()
        if digest != marker.manifest:
            problems.append(f"{jid}: marker manifest {marker.manifest[:12]} is not the sha256 {digest[:12]} of "
                            f"{artifact}'s manifest.json in run {marker.run}")
            continue
        try:
            manifest = json.loads(data.decode("utf-8-sig"))
        except ValueError as error:
            problems.append(f"{jid}: {artifact}'s manifest.json is not JSON: {error}")
            continue
        entries = manifest.get("entries") or []
        if manifest.get("journey") != jid:
            problems.append(f"{jid}: the manifest names journey '{manifest.get('journey')}'")
        if str(manifest.get("commit", "")).lower() != marker.sha:
            problems.append(f"{jid}: the manifest records commit '{manifest.get('commit')}', not {marker.sha[:12]}")
        if manifest.get("runId") not in (None, "", str(marker.run), marker.run):
            problems.append(f"{jid}: the manifest records run '{manifest.get('runId')}', not {marker.run}")
        if len(entries) != marker.count:
            problems.append(f"{jid}: marker count {marker.count} but the manifest holds {len(entries)} screenshots")
        failed = [f"{entry.get('step')}/{entry.get('substep')}" for entry in entries if not entry.get("passed")]
        if failed:
            problems.append(f"{jid}: the manifest records failed checkpoints: {', '.join(failed)}")

    if journeys:
        if section is None:
            problems.append("the body has no '## Journey screenshots' section")
        elif count_images(section) < required:
            problems.append(f"the '## Journey screenshots' section shows {count_images(section)} images; "
                            f"the markers count {required}")
    return problems


class GitHubCli:
    """The GitHub reads the check needs, through the gh CLI (GH_TOKEN in the environment)."""

    def __init__(self, repository):
        self.repository = repository
        self.scratch = Path(tempfile.mkdtemp(prefix="journey-provenance-"))

    def _api(self, path, paginate=False, jq=None):
        command = ["gh", "api", "-H", "Accept: application/vnd.github+json"]
        if paginate:
            command.append("--paginate")
        if jq:
            command += ["--jq", jq]
        result = subprocess.run(command + [path], capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            raise LookupError(result.stderr.strip() or f"gh api {path} exited {result.returncode}")
        return result.stdout

    def body(self, number):
        return json.loads(self._api(f"repos/{self.repository}/pulls/{number}")).get("body") or ""

    def run(self, run_id):
        return json.loads(self._api(f"repos/{self.repository}/actions/runs/{run_id}"))

    def artifacts(self, run_id):
        output = self._api(f"repos/{self.repository}/actions/runs/{run_id}/artifacts?per_page=100", paginate=True,
                           jq=".artifacts[] | [.name, (.expired|tostring)] | @tsv")
        return [(name, expired == "true") for name, _, expired in
                (line.partition("\t") for line in output.splitlines() if line.strip())]

    def download(self, run_id, name):
        target = self.scratch / f"{run_id}-{name}"
        if not target.exists():
            result = subprocess.run(
                ["gh", "run", "download", str(run_id), "--repo", self.repository, "--name", name, "--dir", str(target)],
                capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                raise LookupError(f"could not download artifact '{name}' of run {run_id}: {result.stderr.strip()}")
        return target


def main():
    repository = os.environ["REPOSITORY"]
    number = os.environ["PR_NUMBER"]
    head = os.environ["HEAD_SHA"]
    journeys = json.loads(os.environ.get("JOURNEYS") or "[]")
    github = GitHubCli(repository)
    body = github.body(number)
    problems = check(body, head, journeys, github)
    lines = ["## Journey screenshot provenance", ""]
    if problems:
        lines += [f"- **{problem}**" for problem in problems]
    else:
        lines.append("Every marker matches its run's manifest on head `" + head[:12] + "`: "
                     + ", ".join(f"`{jid}`" for jid in journeys))
    text = "\n".join(lines) + "\n"
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(text)
    for problem in problems:
        print(f"::error::{problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
