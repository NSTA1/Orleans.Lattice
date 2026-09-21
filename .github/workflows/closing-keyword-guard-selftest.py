#!/usr/bin/env python3
"""Self-test for the `Guard - inert closing keywords` step in ci.yml.

WHAT IT TESTS AND WHY IT IS SHAPED THIS WAY
-------------------------------------------
The guard is an inline Python heredoc inside a workflow step, so there is no
importable module to unit-test. This file therefore EXTRACTS that heredoc from
`ci.yml` verbatim and runs it. It never edits `ci.yml`, and it never keeps a
second copy of the guard: the moment a fixture rewrites the thing under test to
make it testable, it stops testing what CI runs, and every later green is a
statement about the fixture's copy rather than about the workflow.

The guard has two arms - the pull request BODY and the pull request's own COMMIT
MESSAGES (issue #3024) - and one predicate shared between them. The commit arm
is exercised against real temporary git repositories with planted messages,
because the population it reads is `git rev-list BASE_SHA..HEAD_SHA` and a fake
would only prove that the fake was read.

THE MUTATION SUITE IS THE POINT
-------------------------------
Cases 1-11 are the ordinary positive and negative pairs. They establish that the
guard reports what it should on inputs we control. They cannot, on their own,
establish that any individual mechanism inside the guard is load-bearing: a
guard can pass all eleven while one of its parts has quietly stopped
contributing, and that is exactly the failure this epic exists to catch.

So cases M1-M4 each reintroduce a specific defect into the extracted text and
assert that a NAMED case changes its verdict. Each mutation is asserted to
apply exactly once before it is used, so a mutation that silently matched
nothing cannot be mistaken for a guard that survived it - an unapplied mutation
and a robust guard produce the same green.

Run:
  python3 .github/workflows/closing-keyword-guard-selftest.py
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

STEP_NAME = "Guard - inert closing keywords"
WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"

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
    """The guard's Python exactly as bash receives it.

    YAML strips the block scalar's common indentation before bash ever sees the
    script, and an unquoted-delimiter heredoc strips nothing further, so the
    text the interpreter runs is the heredoc body dedented by the indentation of
    its own `python3 <<'PY'` line. Reproducing that here rather than guessing a
    fixed indent keeps the extraction correct if the step is ever renested.
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
        match = re.match(r"^(\s*)python3 <<'PY'\s*$", lines[index])
        if match:
            opener = (index, match.group(1))
            break
        if index > start and re.match(r"^\s*- name: ", lines[index]):
            break
    if opener is None:
        raise SystemExit(
            f"FATAL: step {STEP_NAME!r} no longer contains a `python3 <<'PY'` "
            "heredoc, so this self-test has lost its subject."
        )

    index, indent = opener
    body: list[str] = []
    for line in lines[index + 1:]:
        if line.strip() == "PY" and line.startswith(indent):
            return "\n".join(body) + "\n"
        body.append(line[len(indent):] if line.startswith(indent) else line)
    raise SystemExit("FATAL: unterminated heredoc in the closing-keyword guard.")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    done = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True,
        env={**os.environ,
             "GIT_AUTHOR_NAME": "selftest", "GIT_AUTHOR_EMAIL": "selftest@invalid",
             "GIT_COMMITTER_NAME": "selftest", "GIT_COMMITTER_EMAIL": "selftest@invalid",
             "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull},
    )
    if done.returncode != 0:
        raise SystemExit(f"FATAL: git {' '.join(args)} failed in {repo}:\n{done.stderr}")
    return done


def build_repo(root: Path, messages: list[str]) -> tuple[str, str]:
    """A repo with one base commit plus one commit per planted message.

    Returns (base_sha, head_sha) - the shas the guard would receive from the
    pull_request event, so the range it scans is the planted messages only.
    """
    root.mkdir(parents=True, exist_ok=True)
    git(root, "init", "--quiet", "--initial-branch=main")
    (root / "seed.txt").write_text("seed\n", encoding="utf-8")
    git(root, "add", "seed.txt")
    git(root, "commit", "--quiet", "-m", "base: nothing to see here")
    base_sha = git(root, "rev-parse", "HEAD").stdout.strip()

    for ordinal, message in enumerate(messages):
        target = root / f"file{ordinal}.txt"
        target.write_text(f"content {ordinal}\n", encoding="utf-8")
        git(root, "add", target.name)
        path = root / ".git" / "SELFTEST_MSG"
        path.write_text(message, encoding="utf-8")
        git(root, "commit", "--quiet", "-F", str(path))
        path.unlink()

    head_sha = git(root, "rev-parse", "HEAD").stdout.strip()
    return base_sha, head_sha


def run_guard(script: str, workdir: Path, *, body: str = "",
              base_sha: str | None = None, head_sha: str | None = None,
              base_ref: str = "fix/epic/some-bucket",
              default_branch: str = "main") -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items()
           if k not in {"PR_BODY", "BASE_REF", "DEFAULT_BRANCH", "BASE_SHA", "HEAD_SHA"}}
    env["PR_BODY"] = body
    env["BASE_REF"] = base_ref
    env["DEFAULT_BRANCH"] = default_branch
    if base_sha is not None:
        env["BASE_SHA"] = base_sha
    if head_sha is not None:
        env["HEAD_SHA"] = head_sha
    return subprocess.run([sys.executable, "-c", script], cwd=workdir,
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
# Cases
# ---------------------------------------------------------------------------

CLEAN = ["feat: add a thing\n\nRefs #1\n"]
HONOURED_BODY = ["feat: add a thing\n\nCloses #1\n"]


def main() -> int:
    if not WORKFLOW.is_file():
        raise SystemExit(f"FATAL: {WORKFLOW} not found.")
    script = extract_guard(WORKFLOW.read_text(encoding="utf-8"))
    if "def honoured(" not in script or "def offenders(" not in script:
        raise SystemExit(
            "FATAL: the extracted guard is missing honoured()/offenders(), so the "
            "extraction is wrong and every case below would test nothing."
        )
    print(f"Extracted {len(script.splitlines())} lines of guard from {WORKFLOW.name}\n")

    workroot = Path(tempfile.mkdtemp(prefix="closing-keyword-guard-"))
    try:
        # -- Arm 1: the commit-message arm (new in #3024) -------------------
        print("Commit-message arm")

        repo = workroot / "clean"
        base, head = build_repo(repo, CLEAN + ["chore: another\n\nnothing here\n"])
        done = run_guard(script, repo, base_sha=base, head_sha=head)
        check("1 clean body and clean commits pass", done.returncode == 0,
              done.stdout + done.stderr)
        check("1 the pass states how many commits it read",
              "2 commit message(s) scanned" in done.stdout, done.stdout)

        repo = workroot / "commit-body"
        base, head = build_repo(repo, HONOURED_BODY)
        done = run_guard(script, repo, base_sha=base, head_sha=head)
        check("2 a keyword in a commit BODY fails", done.returncode == 1, done.stdout)
        check("2 the failure names the offending commit",
              "Commit " in done.stdout and head[:9] in done.stdout, done.stdout)

        repo = workroot / "commit-subject"
        base, head = build_repo(repo, ["fix: Fixes #7 in the subject line\n"])
        done = run_guard(script, repo, base_sha=base, head_sha=head)
        check("3 a keyword in a commit SUBJECT fails", done.returncode == 1, done.stdout)

        for ordinal, (label, message) in enumerate([
            ("code span", "chore: discuss\n\nDo not write `Closes #1` here.\n"),
            ("fenced block", "chore: discuss\n\n```\nCloses #1\n```\n"),
            ("blockquote", "chore: discuss\n\n> Closes #1\n"),
        ]):
            repo = workroot / f"inert-{ordinal}"
            base, head = build_repo(repo, [message])
            done = run_guard(script, repo, base_sha=base, head_sha=head)
            check(f"4.{ordinal + 1} a keyword inside a {label} passes",
                  done.returncode == 0, done.stdout)

        # -- Arm 2: the body arm, asserted UNCHANGED ------------------------
        print("\nBody arm (must be unchanged by #3024)")

        repo = workroot / "body-hit"
        base, head = build_repo(repo, CLEAN)
        done = run_guard(script, repo, body="Closes #1", base_sha=base, head_sha=head)
        check("5 a keyword in the PR body still fails", done.returncode == 1, done.stdout)
        check("5 the failure identifies the body as the source",
              "BODY" in done.stdout, done.stdout)

        done = run_guard(script, repo, body="Discussed as `Closes #1` only.",
                         base_sha=base, head_sha=head)
        check("6 a backticked keyword in the PR body still passes",
              done.returncode == 0, done.stdout)

        repo = workroot / "both"
        base, head = build_repo(repo, HONOURED_BODY)
        done = run_guard(script, repo, body="Closes #2", base_sha=base, head_sha=head)
        check("7 both arms report together", done.returncode == 1, done.stdout)
        check("7 both the body hit and the commit hit are named",
              "BODY" in done.stdout and "Commit " in done.stdout, done.stdout)

        # -- Fail-closed behaviour -----------------------------------------
        print("\nFail-closed behaviour (an unread population is not a clean one)")

        repo = workroot / "empty-range"
        base, head = build_repo(repo, CLEAN)
        done = run_guard(script, repo, base_sha=head, head_sha=head)
        check("8 a zero-commit range FAILS rather than passing",
              done.returncode == 1, done.stdout)
        check("8 the failure says the range resolved to zero commits",
              "zero commits" in done.stdout, done.stdout)

        repo = workroot / "bad-range"
        base, head = build_repo(repo, CLEAN)
        done = run_guard(script, repo, base_sha="0" * 40, head_sha=head)
        check("9 an unresolvable range FAILS rather than passing",
              done.returncode == 1, done.stdout)
        check("9 the failure says it could not enumerate",
              "could not enumerate" in done.stdout, done.stdout)

        repo = workroot / "no-env"
        base, head = build_repo(repo, CLEAN)
        done = run_guard(script, repo)
        check("10 absent BASE_SHA/HEAD_SHA FAILS rather than passing",
              done.returncode == 1, done.stdout)

        repo = workroot / "not-a-repo"
        repo.mkdir(parents=True)
        done = run_guard(script, repo, base_sha="0" * 40, head_sha="1" * 40)
        check("11 running outside a git repository FAILS rather than passing",
              done.returncode == 1, done.stdout)

        # -- Mutations: each names a defect and a case that must redden ------
        print("\nMutation suite (each reintroduces one defect)")

        # M1: the commit scanner stops reporting, while honoured() stays healthy.
        # The guard's own in-CI control must catch this on a CLEAN input - which
        # is the whole reason that control exists, since no clean pull request
        # would otherwise exercise the scanner at all.
        m1 = mutate(script,
                    "return [(sha, honoured(text)) for sha, text in messages if honoured(text)]",
                    "return []", "M1 neutered commit scanner")
        base, head = build_repo(workroot / "m1", CLEAN)
        done = run_guard(m1, workroot / "m1", base_sha=base, head_sha=head)
        check("M1 a blind commit scanner is caught by the in-guard control, on clean input",
              done.returncode == 1 and "scanner is broken" in done.stdout, done.stdout)

        # M2: the zero-commit guard is removed. Case 8 must flip to a pass,
        # which is precisely the "scanned nothing, reported clean" failure.
        m2 = mutate(script, "\nif not messages:\n", "\nif False:\n",
                    "M2 zero-commit guard removed")
        base, head = build_repo(workroot / "m2", CLEAN)
        done = run_guard(m2, workroot / "m2", base_sha=head, head_sha=head)
        check("M2 without the zero-commit guard, an empty scan reports CLEAN",
              done.returncode == 0, done.stdout)

        # M3: read the subject only. Case 2 must flip to a pass - every closing
        # keyword found in this repository's own history was in a commit body.
        m3 = mutate(script, '"--format=%B"', '"--format=%s"', "M3 subject-only read")
        base, head = build_repo(workroot / "m3", HONOURED_BODY)
        done = run_guard(m3, workroot / "m3", base_sha=base, head_sha=head)
        check("M3 reading %s instead of %B misses a keyword in a commit body",
              done.returncode == 0, done.stdout)

        # M4: the predicate stops excluding inline code spans. The guard's
        # existing planted probes must catch this before any input is read.
        m4 = mutate(script, r'text = re.sub(r"`[^`\n]*`", "", "\n".join(out))',
                    r'text = "\n".join(out)',
                    "M4 code-span stripping removed")
        base, head = build_repo(workroot / "m4", CLEAN)
        done = run_guard(m4, workroot / "m4", base_sha=base, head_sha=head)
        check("M4 a broadened predicate is caught by the existing planted probes",
              done.returncode == 1 and "self-test" in done.stdout, done.stdout)

    finally:
        shutil.rmtree(workroot, ignore_errors=True)

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed.")
    if FAILURES:
        print("FAILED: " + "; ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
