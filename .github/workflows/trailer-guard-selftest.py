#!/usr/bin/env python3
"""Self-test for the `Guard - branch name and commit trailers` step in ci.yml.

WHY THIS EXISTS
---------------
The guard is an inline bash block inside a workflow step, so there is no
importable script to unit-test. This file EXTRACTS that block from `ci.yml`
verbatim and runs it. It never edits `ci.yml` and never keeps a second copy:
the moment a fixture rewrites the thing under test to make it testable, it
stops testing what CI runs, and every later green is a statement about the
fixture's copy rather than about the workflow.

THE DEFECT THAT PROMPTED THE SECOND ARM
---------------------------------------
The guard's original arm scans the pull request's own commit MESSAGES for a
banned trailer, and it is correct at that. It is also structurally blind to the
way a banned trailer has actually reached `main` here: GitHub composes the
squash commit message itself when no explicit body is supplied, and appends a
`Co-authored-by:` line for every distinct commit-author identity on the pull
request beyond the one it makes the author. Every individual commit message is
clean, the guard passes honestly, and the trailer is generated afterwards, at
the instant of the merge, where no pre-merge check can read it.

The artefact is unreachable; the CAUSE is not. A pull request whose commits all
carry one author identity gives GitHub nothing to attribute. So the second arm
gates the identity COUNT, which is fully decidable while the pull request is
open, and these cases prove that it discriminates.

THE MUTATION SUITE IS THE POINT
-------------------------------
Cases A1-A7 establish that the guard reports what it should on inputs we
control. They cannot, on their own, establish that any individual mechanism
inside it is load-bearing - a guard can pass all seven while one of its parts
has quietly stopped contributing. So M1-M4 each reintroduce a specific defect
and assert that a NAMED case changes its verdict. Each mutation is asserted to
apply exactly once before it is used, because an unapplied mutation and a
robust guard produce the same green.

Run:
  python3 .github/workflows/trailer-guard-selftest.py
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

STEP_NAME = "Guard - branch name and commit trailers"
WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"

# The guard uses bash-only constructs, so it must be run by bash and not by
# whatever /bin/sh happens to be. `bash` is correct on the CI runner. It is not
# reliably correct when running this locally on Windows, where CreateProcess
# searches the system directory before PATH and so resolves `bash` to the WSL
# relay rather than to a real bash; point SELFTEST_BASH at one there.
BASH = os.environ.get("SELFTEST_BASH", "bash")

FAILURES: list[str] = []
CHECKS = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}{(': ' + detail) if detail else ''}")
        FAILURES.append(name)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_guard(text: str) -> str:
    """The guard's bash exactly as the runner receives it.

    YAML strips a block scalar's common indentation before bash ever sees the
    script, so the text reproduced here is the `run: |` body dedented by its own
    minimum indentation. Deriving that rather than assuming a fixed indent keeps
    the extraction correct if the step is ever renested.
    """
    lines = text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == f"- name: {STEP_NAME}":
            start = index
            break
    if start is None:
        raise SystemExit(
            f"FATAL: no step named {STEP_NAME!r} in {WORKFLOW}. This self-test "
            "cannot silently pass over a step it did not find."
        )

    opener = None
    for index in range(start, len(lines)):
        match = re.match(r"^(\s*)run: \|\s*$", lines[index])
        if match:
            opener = (index, len(match.group(1)))
            break
        if index > start and re.match(r"^\s*- name: ", lines[index]):
            break
    if opener is None:
        raise SystemExit(
            f"FATAL: step {STEP_NAME!r} no longer contains a `run: |` block, so "
            "this self-test has lost its subject."
        )

    index, run_indent = opener
    body: list[str] = []
    for line in lines[index + 1:]:
        if line.strip() and (len(line) - len(line.lstrip())) <= run_indent:
            break
        body.append(line)

    widths = [len(line) - len(line.lstrip()) for line in body if line.strip()]
    if not widths:
        raise SystemExit("FATAL: the extracted guard body is empty.")
    dedent = min(widths)
    return "\n".join(line[dedent:] if line.strip() else "" for line in body) + "\n"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def git(repo: Path, *args: str, author: tuple[str, str] | None = None
        ) -> subprocess.CompletedProcess:
    name, email = author or ("default contributor", "default@invalid")
    done = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True,
        env={**os.environ,
             "GIT_AUTHOR_NAME": name, "GIT_AUTHOR_EMAIL": email,
             "GIT_COMMITTER_NAME": "selftest", "GIT_COMMITTER_EMAIL": "selftest@invalid",
             "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull},
    )
    if done.returncode != 0:
        raise SystemExit(f"FATAL: git {' '.join(args)} failed in {repo}:\n{done.stderr}")
    return done


def build_repo(root: Path, commits: list[tuple[str, str, str]]) -> tuple[str, str]:
    """A repo with one base commit plus one commit per (message, name, email).

    Returns (base_sha, head_sha) - the shas the guard would receive from the
    pull_request event, so the range it scans is the planted commits only.
    """
    root.mkdir(parents=True, exist_ok=True)
    git(root, "init", "--quiet", "--initial-branch=main")
    (root / "seed.txt").write_text("seed\n", encoding="utf-8")
    git(root, "add", "seed.txt")
    git(root, "commit", "--quiet", "-m", "base: nothing to see here")
    base_sha = git(root, "rev-parse", "HEAD").stdout.strip()

    for ordinal, (message, name, email) in enumerate(commits):
        target = root / f"file{ordinal}.txt"
        target.write_text(f"content {ordinal}\n", encoding="utf-8")
        git(root, "add", target.name)
        path = root / ".git" / "SELFTEST_MSG"
        path.write_text(message, encoding="utf-8")
        git(root, "commit", "--quiet", "-F", str(path), author=(name, email))
        path.unlink()

    head_sha = git(root, "rev-parse", "HEAD").stdout.strip()
    return base_sha, head_sha


def run_guard(script: str, workdir: Path, *, base_sha: str, head_sha: str,
              branch: str = "feat/a-described-change",
              author: str = "octocat") -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items()
           if k not in {"BRANCH", "AUTHOR", "BASE_SHA", "HEAD_SHA"}}
    env["BRANCH"] = branch
    env["AUTHOR"] = author
    env["BASE_SHA"] = base_sha
    env["HEAD_SHA"] = head_sha
    return subprocess.run([BASH, "-c", script], cwd=workdir,
                          capture_output=True, text=True, env=env)


def mutate(script: str, needle: str, replacement: str, label: str) -> str:
    """Apply a named mutation, asserting it lands exactly once.

    Without the count assertion a mutation that matched nothing would produce a
    green run indistinguishable from a guard that genuinely resisted it, and the
    conclusion drawn would be the opposite of the truth.
    """
    occurrences = script.count(needle)
    if occurrences != 1:
        raise SystemExit(
            f"FATAL: mutation {label} expected exactly 1 occurrence of its anchor, "
            f"found {occurrences}. The guard has been reworded and this mutation no "
            "longer reintroduces the defect it names, so its result would be "
            "meaningless rather than reassuring."
        )
    return script.replace(needle, replacement)


# ---------------------------------------------------------------------------
# Planted commit populations
# ---------------------------------------------------------------------------

ONE = ("first contributor", "first@invalid")
TWO = ("second contributor", "second@invalid")

CLEAN = [("feat: add a thing\n\nA body with no trailer.\n", *ONE),
         ("chore: tidy up\n\nAnother clean body.\n", *ONE)]

# The same person committing under two accounts. This is the population that
# makes GitHub manufacture a trailer at squash time, and every message in it is
# clean - which is why the message arm alone reports it as compliant.
TWO_IDENTITIES = [("feat: add a thing\n\nA body with no trailer.\n", *ONE),
                  ("docs: describe the thing\n\nAlso clean.\n", *TWO)]

# Two accounts sharing one display name. Deduping on the name rather than the
# email would collapse these to one identity and pass.
SAME_NAME = [("feat: add a thing\n", "one person", "account-a@invalid"),
             ("docs: describe it\n", "one person", "account-b@invalid")]

WRITTEN_TRAILER = [("feat: add a thing\n\nCo-authored-by: someone <someone@invalid>\n", *ONE)]


def main() -> int:
    if not WORKFLOW.is_file():
        raise SystemExit(f"FATAL: {WORKFLOW} not found.")
    script = extract_guard(WORKFLOW.read_text(encoding="utf-8"))
    for anchor in ("prefixes=", "trailers=", "identity_count="):
        if anchor not in script:
            raise SystemExit(
                f"FATAL: the extracted guard is missing {anchor!r}, so the extraction "
                "is wrong and every case below would test nothing."
            )
    print(f"Extracted {len(script.splitlines())} lines of guard from {WORKFLOW.name}\n")

    workroot = Path(tempfile.mkdtemp(prefix="trailer-guard-"))
    try:
        # -- Arm 1: trailers written into the commit messages ---------------
        print("Written-trailer arm")

        repo = workroot / "clean"
        base, head = build_repo(repo, CLEAN)
        done = run_guard(script, repo, base_sha=base, head_sha=head)
        check("A1 a clean single-identity pull request passes",
              done.returncode == 0 and "OK:" in done.stdout, done.stdout + done.stderr)

        repo = workroot / "written"
        base, head = build_repo(repo, WRITTEN_TRAILER)
        done = run_guard(script, repo, base_sha=base, head_sha=head)
        check("A2 a trailer written into a commit message fails",
              done.returncode == 1 and "banned trailer" in done.stdout, done.stdout)

        # -- Arm 2: trailers GitHub will manufacture at squash time ---------
        print("\nManufactured-trailer arm (the squash-time half)")

        repo = workroot / "two-identities"
        base, head = build_repo(repo, TWO_IDENTITIES)
        done = run_guard(script, repo, base_sha=base, head_sha=head)
        check("A3 two author identities fail even though every message is clean",
              done.returncode == 1 and "distinct author identities" in done.stdout,
              done.stdout)
        check("A3 the failure pages both identities",
              "first@invalid" in done.stdout and "second@invalid" in done.stdout,
              done.stdout)

        repo = workroot / "same-name"
        base, head = build_repo(repo, SAME_NAME)
        done = run_guard(script, repo, base_sha=base, head_sha=head)
        check("A4 two accounts sharing one display name still fail",
              done.returncode == 1 and "distinct author identities" in done.stdout,
              done.stdout)

        # -- Branch naming, asserted unchanged ------------------------------
        print("\nBranch-name arm (must be unchanged)")

        repo = workroot / "branch"
        base, head = build_repo(repo, CLEAN)
        done = run_guard(script, repo, base_sha=base, head_sha=head,
                         branch="just-a-description")
        check("A5 an unprefixed branch name fails",
              done.returncode == 1 and "naming convention" in done.stdout, done.stdout)

        done = run_guard(script, repo, base_sha=base, head_sha=head,
                         branch="feat/octocat-fixes-it", author="octocat")
        check("A6 a branch carrying the author's login fails",
              done.returncode == 1 and "username" in done.stdout, done.stdout)

        # -- Fail-closed behaviour ------------------------------------------
        print("\nFail-closed behaviour (an unread population is not a clean one)")

        repo = workroot / "empty-range"
        base, head = build_repo(repo, CLEAN)
        done = run_guard(script, repo, base_sha=head, head_sha=head)
        check("A7 a zero-commit range FAILS rather than passing",
              done.returncode == 1 and "zero commits" in done.stdout, done.stdout)

        # -- Mutations: each names a defect and a case that must redden ------
        print("\nMutation suite (each reintroduces one defect)")

        # M1: the identity arm stops firing. A3 must flip to a pass, which is
        # precisely the state this guard was in when a manufactured trailer
        # reached the base branch.
        m1 = mutate(script, 'elif [ "${identity_count}" -gt 1 ]; then',
                    'elif false; then', "M1 identity arm neutered")
        base, head = build_repo(workroot / "m1", TWO_IDENTITIES)
        done = run_guard(m1, workroot / "m1", base_sha=base, head_sha=head)
        check("M1 without the identity arm, two identities report CLEAN",
              done.returncode == 0, done.stdout)

        # M2: dedupe on the display name instead of the email. A4 must flip to
        # a pass - the name is not the identity GitHub attributes on.
        m2 = mutate(script, "git log --format='%ae' \"${BASE_SHA}..${HEAD_SHA}\"",
                    "git log --format='%an' \"${BASE_SHA}..${HEAD_SHA}\"",
                    "M2 dedupe on display name")
        base, head = build_repo(workroot / "m2", SAME_NAME)
        done = run_guard(m2, workroot / "m2", base_sha=base, head_sha=head)
        check("M2 deduping on %an misses two accounts sharing a display name",
              done.returncode == 0, done.stdout)

        # M3: the zero-commit arm is removed. A7 must flip to a pass, which is
        # the "scanned nothing, reported clean" failure both arms share.
        m3 = mutate(script, 'if [ "${identity_count}" -eq 0 ]; then',
                    'if false; then', "M3 zero-commit arm removed")
        base, head = build_repo(workroot / "m3", CLEAN)
        done = run_guard(m3, workroot / "m3", base_sha=head, head_sha=head)
        check("M3 without the zero-commit arm, an empty scan reports CLEAN",
              done.returncode == 0, done.stdout)

        # M4: the written-trailer arm stops reporting. A2 must flip to a pass.
        m4 = mutate(script, 'if [ -n "${offenders}" ]; then', 'if false; then',
                    "M4 written-trailer arm neutered")
        base, head = build_repo(workroot / "m4", WRITTEN_TRAILER)
        done = run_guard(m4, workroot / "m4", base_sha=base, head_sha=head)
        check("M4 without the written-trailer arm, a written trailer reports CLEAN",
              done.returncode == 0, done.stdout)

    finally:
        shutil.rmtree(workroot, ignore_errors=True)

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed.")
    if FAILURES:
        print("FAILED: " + "; ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
