#!/usr/bin/env python3
"""Self-test for `Guard - bucket closing list` (issue #3320).

WHY A GUARD NEEDS A GUARD
-------------------------
A gate whose healthy outcome is silence is indistinguishable, in its output,
from a gate that has stopped discriminating. #3320 is itself an instance of that
class - a closing list that stopped being extended rendered identically to a
maintained one - so shipping an unproven guard for it would repeat the defect
one layer up. This fixture therefore proves three things on every CI run:

  * the guard FAILS on a bucket that is missing a required closing reference
    (the fail-first evidence; a guard never seen to fail is not a guard),
  * it PASSES on a complete one,
  * and it fails LOUDLY rather than clean when its own discovery step finds
    nothing - no merged members, or members that claim nothing.

It then MUTATES the guard and requires each mutation to be caught. Cases prove
the guard works today; mutations prove the cases are load-bearing. Without them
a case can silently stop testing anything, which is the same failure again.

The guard is never copied here. It is read from the file that CI runs, so this
fixture cannot drift into testing a stale duplicate.

HOW THE NETWORK IS FAKED, AND WHY THAT IS NOT A BACKDOOR
--------------------------------------------------------
The guard addresses GitHub through `GITHUB_API_URL` and `GITHUB_GRAPHQL_URL`.
Those are standard, documented GitHub Actions environment variables that exist
so a workflow can run against GitHub Enterprise Server, and the runner sets them
itself. Pointing them at a local `http.server` is ordinary configuration of a
documented seam, not a test-only branch in production code: the guard has no
idea it is being tested, and there is no code path here that exists only for
tests. That matters, because a guard with a test-only bypass is a guard that can
be bypassed.

Run it directly:
  python3 .github/workflows/bucket-closing-list-guard-selftest.py
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import subprocess
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = pathlib.Path(__file__).resolve().parent
GUARD = HERE / "bucket-closing-list-guard.py"
CI = HERE / "ci.yml"
REPO = "o/r"
BUCKET = "fix/epic/bucket"

FAILURES: list[str] = []
CHECKS = 0


def check(condition: bool, label: str) -> bool:
    global CHECKS
    CHECKS += 1
    if condition:
        print(f"  ok   {label}")
        return True
    print(f"  FAIL {label}")
    FAILURES.append(label)
    return False


# ---------------------------------------------------------------------------
# A fake GitHub
# ---------------------------------------------------------------------------

class Scenario:
    """The three artefacts the guard reads, served over HTTP."""

    def __init__(self, members: list[dict], closing: list[int],
                 issues: dict[int, str]):
        self.members = members
        self.closing = closing
        self.issues = issues
        self.member_pages = 0
        self.closing_pages = 0


def member(number: int, body: str, merged: bool = True) -> dict:
    return {"number": number, "body": body,
            "merged_at": "2026-09-20T00:00:00Z" if merged else None}


def _handler(scenario: Scenario):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def _send(self, code: int, payload: object) -> None:
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler's contract
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            if parsed.path == f"/repos/{REPO}/pulls":
                if query.get("base", [""])[0] != BUCKET:
                    return self._send(200, [])
                scenario.member_pages += 1
                page = int(query.get("page", ["1"])[0])
                start = (page - 1) * 100
                return self._send(200, scenario.members[start:start + 100])
            issue = re.match(rf"^/repos/{re.escape(REPO)}/issues/(\d+)$", parsed.path)
            if issue:
                number = int(issue.group(1))
                state = scenario.issues.get(number)
                if state is None:
                    return self._send(404, {"message": "Not Found"})
                if state == "PR":
                    return self._send(200, {"number": number, "state": "open",
                                            "pull_request": {"url": "x"}})
                return self._send(200, {"number": number, "state": state.lower()})
            return self._send(404, {"message": "Not Found"})

        def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's contract
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or "{}")
            if urllib.parse.urlparse(self.path).path != "/graphql":
                return self._send(404, {"message": "Not Found"})
            scenario.closing_pages += 1
            cursor = int((payload.get("variables") or {}).get("cursor") or 0)
            page = scenario.closing[cursor:cursor + 100]
            has_next = cursor + 100 < len(scenario.closing)
            return self._send(200, {"data": {"repository": {"pullRequest": {
                "closingIssuesReferences": {
                    "pageInfo": {"hasNextPage": has_next,
                                 "endCursor": str(cursor + 100)},
                    "nodes": [{"number": n} for n in page]}}}}})

    return Handler


def run(scenario: Scenario, *, body: str = "", head: str = BUCKET,
        base: str = "main", number: int = 9000,
        source: str | None = None) -> tuple[int, str]:
    """Run the guard against the scenario and return (exit code, output)."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(scenario))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        root = f"http://127.0.0.1:{server.server_address[1]}"
        env = dict(os.environ)
        env.update({"GITHUB_API_URL": root,
                    "GITHUB_GRAPHQL_URL": root + "/graphql",
                    "GITHUB_TOKEN": "selftest",
                    "GITHUB_REPOSITORY": REPO,
                    "PR_NUMBER": str(number),
                    "PR_BODY": body,
                    "HEAD_REF": head,
                    "BASE_REF": base,
                    "DEFAULT_BRANCH": "main"})
        completed = subprocess.run(
            [sys.executable, "-c", source if source is not None else GUARD.read_text(encoding="utf-8")],
            env=env, capture_output=True, text=True, timeout=180)
    finally:
        server.shutdown()
        server.server_close()
    return completed.returncode, completed.stdout + completed.stderr


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def complete() -> Scenario:
    return Scenario([member(1, "Refs #10"), member(2, "Refs #11")],
                    [10, 11], {10: "OPEN", 11: "OPEN"})


def incomplete() -> Scenario:
    """The shape of the live defect: a member merged, its issue never listed."""
    return Scenario([member(1, "Refs #10"), member(2, "Refs #11")],
                    [10], {10: "OPEN", 11: "OPEN"})


def paged() -> Scenario:
    """101 members and 150 closing references - both populations page."""
    members = [member(i, f"Refs #{1000 + i}") for i in range(1, 102)]
    closing = [1000 + i for i in range(1, 102)] + list(range(9100, 9149))
    return Scenario(members, closing, {1000 + i: "OPEN" for i in range(1, 102)})


def paged_gap() -> Scenario:
    """As `paged()`, but the member on PAGE TWO claims an unlisted issue.

    `paged()` cannot tell a paginating member-discovery from a one-page one,
    because every member it drops claimed an issue that is in the closing set
    anyway - so the verdict is clean either way. The gap has to be carried by a
    member that only a second page reaches, which is the whole point: member
    #101 lands on page two, and #1101 is absent from the closing set.
    """
    scenario = paged()
    scenario.closing = [n for n in scenario.closing if n != 1101]
    return scenario


def verbatim(out: str) -> None:
    """Print a guard run's own output, so the evidence is self-producing.

    The fail-first and non-vacuity cases are the two a reviewer has to be able
    to see actually failing, so this fixture prints what the guard said rather
    than asserting silently and leaving the reader to take it on trust.
    """
    print("  --- guard output, verbatim ---")
    for line in out.strip().splitlines():
        print("  | " + line)
    print("  --- end ---")


def main() -> int:
    if not GUARD.is_file():
        print(f"FAIL: {GUARD} is missing - the guard this fixture proves does not exist.")
        return 1
    source = GUARD.read_text(encoding="utf-8")

    print("Guard - bucket closing list: self-test")
    print()
    print("A. FAIL-FIRST - a bucket missing a required closing reference")
    code, out = run(incomplete())
    check(code != 0, "an unlisted open issue claimed by a merged member fails the guard")
    check("#11" in out, "the missing issue is named")
    check("#2" in out, "the member that claimed it is named, so the report is a work list")
    check("#10" not in out.split("INCOMPLETE")[-1],
          "the issue that IS listed is not reported as a gap")
    print("  --- guard output, verbatim ---")
    for line in out.strip().splitlines():
        print("  | " + line)
    print("  --- end ---")

    print()
    print("B. PASS - a complete bucket")
    code, out = run(complete())
    check(code == 0, "a complete closing list passes")
    check("OK:" in out, "and says so")
    verbatim(out)

    print()
    print("C. NON-VACUITY - the discovery step must fail loudly when it finds nothing")
    code, out = run(Scenario([], [10], {10: "OPEN"}))
    check(code != 0, "zero merged member pull requests FAILS rather than passes")
    check("NON-VACUITY" in out, "and says why")
    verbatim(out)
    code, out = run(Scenario([member(1, "no references at all here")], [10], {10: "OPEN"}))
    check(code != 0, "members that claim nothing FAILS rather than passes")
    check("NON-VACUITY" in out, "and says why")
    verbatim(out)
    code, out = run(Scenario([member(1, "Refs #10", merged=False)], [10], {10: "OPEN"}))
    check(code != 0, "an UNMERGED member is not counted as a population")

    print()
    print("D. SHAPE - the guard must not fire on a member or an ordinary pull request")
    code, out = run(incomplete(), base=BUCKET, head="fix/epic/bucket-3320")
    check(code == 0, "a MEMBER pull request (base is the bucket) does not fire")
    check("not applicable" in out, "and says why rather than passing silently")
    code, out = run(incomplete(), head="feat/ordinary-thing")
    check(code == 0, "an ORDINARY pull request into main does not fire")
    check("not applicable" in out, "and says why rather than passing silently")

    print()
    print("E. PAGINATION - neither population may be read one page deep")
    scenario = paged()
    code, out = run(scenario)
    check(code == 0, "101 members and 150 closing references reconcile clean")
    check(scenario.member_pages >= 2, "member discovery read more than one page")
    check(scenario.closing_pages >= 2, "the closing set read more than one page")

    print()
    print("F. CLASSIFICATION - a number may name a pull request, or an issue already closed")
    code, out = run(Scenario([member(1, "Refs #10, #12, #13")], [10],
                             {10: "OPEN", 12: "PR", 13: "CLOSED"}))
    check(code == 0, "a reference naming a PR or a closed issue is not demanded")
    check("#12" in out and "#13" in out, "but both are reported as notices")
    code, out = run(Scenario([member(1, "Refs #10, #99")], [10], {10: "OPEN"}))
    check(code == 0, "a reference that does not resolve at all is not demanded")

    print()
    print("G. EXCLUSIONS - a stated decision is honoured, a reasonless one is not")
    held = "## Deliberately held open\n\n- #11 - the primary defect is untouched by this wave\n"
    code, out = run(incomplete(), body=held)
    check(code == 0, "an explicitly held-open issue with a reason passes")
    code, out = run(incomplete(), body="## Deliberately held open\n\n- #11 -\n")
    check(code != 0, "a reasonless exclusion FAILS rather than waiving silently")
    code, out = run(incomplete(), body="## Deliberately held open\n\n- #11 - n/a\n")
    check(code != 0, "a token reason FAILS too")
    code, out = run(incomplete(), body="```\n" + held + "```\n")
    check(code != 0, "an exclusion inside a fenced block does not count")
    code, out = run(complete(), body=held)
    check(code == 0, "a stale exclusion is a notice, not a failure")
    check("Stale exclusion" in out, "and is reported")

    print()
    print("H. FAIL-CLOSED - anything it could not read is a failure, never a pass")
    code, out = run(complete(), number=0)
    check(code != 0, "no pull-request number fails rather than passing")
    scenario = complete()
    server_down = Scenario([], [], {})
    code, out = run(server_down, head=BUCKET)
    check(code != 0, "an empty API response fails rather than passing")

    print()
    print("I. WIRING - the guard and this fixture must actually run in CI")
    ci = CI.read_text(encoding="utf-8") if CI.is_file() else ""
    check("bucket-closing-list-guard.py" in ci, "ci.yml runs the guard")
    check("bucket-closing-list-guard-selftest.py" in ci, "ci.yml runs this fixture")
    step = ci.split("bucket-closing-list-guard.py")[0][-1400:]
    check("base.ref" in step and "default_branch" in step,
          "the guard step is conditioned on the base being the default branch")
    check("/epic/" in step, "and on the head being an epic/bucket branch")
    for variable in ("PR_NUMBER", "PR_BODY", "HEAD_REF", "BASE_REF",
                     "DEFAULT_BRANCH", "GITHUB_TOKEN"):
        check(variable in step, f"the guard step passes {variable}")
    check("pull-requests: read" in ci and "issues: read" in ci,
          "the job grants the read scopes the guard needs")

    print()
    print("J. MUTATIONS - each case above must be load-bearing")

    def mutate(anchor: str, replacement: str) -> str:
        if source.count(anchor) != 1:
            raise SystemExit(f"mutation anchor is not unique: {anchor!r} occurs "
                             f"{source.count(anchor)} times")
        return source.replace(anchor, replacement)

    # Each mutation reintroduces one named defect. The expectation below is a
    # predicate over (exit code, output) that MUST hold against unmutated guard
    # source and MUST NOT hold against the mutated source. Both halves are
    # asserted, and asserting the first is what stops this suite going vacuous:
    # a scenario that had stopped exercising its case would otherwise let its
    # mutation "pass" for the wrong reason.
    #
    # Two-sided is also the only formulation that works here, because the
    # mutations do not all push the verdict the same way. Neutering a
    # non-vacuity check makes the guard PASS where it used to fail, while
    # truncating a page makes it FAIL where it used to pass. A one-sided
    # "the mutant must fail" assertion silently certifies the first class - the
    # very failure mode #3320 is about.
    #
    # Several expectations pin a message, not just the code. M1 is the reason:
    # zero merged members implies zero claims, so BOTH non-vacuity checks fire
    # on that scenario and an exit-code-only assertion would be satisfied by
    # the wrong one. What distinguishes them is what the report tells a human -
    # discovery found nothing, versus members recorded nothing - so that is
    # what gets asserted.
    mutations = [
        ("M1 zero-member non-vacuity neutered",
         mutate("if not members:", "if False and not members:"),
         dict(scenario=lambda: Scenario([], [10], {10: "OPEN"})),
         lambda c, o: c != 0 and "no MERGED pull request" in o),
        ("M2 claimed-nothing non-vacuity neutered",
         mutate("if not claims:", "if False and not claims:"),
         dict(scenario=lambda: Scenario([member(1, "nothing here")], [10], {10: "OPEN"})),
         lambda c, o: c != 0 and "NOT ONE carries" in o),
        ("M3 closing set read one page deep",
         mutate('if not info.get("hasNextPage"):', "if True:"),
         dict(scenario=paged),
         lambda c, o: c == 0),
        ("M4 member discovery read one page deep",
         mutate("if len(batch) < 100:", "if True:"),
         dict(scenario=paged_gap),
         lambda c, o: c != 0 and "#1101" in o),
        ("M5 reconciler stops reporting gaps",
         mutate("return sorted(n for n in claims if n not in closing and n not in excluded)",
                "return []"),
         dict(scenario=incomplete),
         lambda c, o: c != 0 and "#11" in o),
        # M6-M8 disable one arm of visible_text() each. They are caught by the
        # guard's own planted probes rather than by a scenario, and each is
        # mutated separately on purpose: with any single arm disabled the other
        # probes still pass unanimously, so one combined mutation would let two
        # thirds of the stripper rot undetected.
        ("M6 inline code spans no longer stripped",
         mutate('return re.sub(r"`[^`\\n]*`", "", "\\n".join(out))',
                'return "\\n".join(out)'),
         dict(scenario=complete),
         lambda c, o: c == 0),
        ("M7 blockquotes no longer stripped",
         mutate('if stripped.startswith(">"):', "if False:"),
         dict(scenario=complete),
         lambda c, o: c == 0),
        ("M8 fenced blocks no longer stripped",
         mutate("fence = opener.group(1)[:3]", "fence = None"),
         dict(scenario=complete),
         lambda c, o: c == 0),
        ("M9 exclusion reason requirement removed",
         mutate('len(re.sub(r"\\s", "", reason)) >= MIN_REASON_CHARS', "True"),
         dict(scenario=incomplete, body="## Deliberately held open\n\n- #11 -\n"),
         lambda c, o: c != 0 and "without stating a reason" in o),
        ("M10 every issue classified as already closed",
         mutate('return "OPEN" if item.get("state") == "open" else "CLOSED"',
                'return "CLOSED"'),
         dict(scenario=incomplete),
         lambda c, o: c != 0 and "#11" in o),
        ("M11 the shape check stops excluding non-buckets",
         mutate('or "/epic/" not in head_ref', ""),
         dict(scenario=incomplete, head="feat/ordinary-thing"),
         lambda c, o: c == 0 and "not applicable" in o),
    ]
    for label, mutated, kwargs, expect in mutations:
        factory = kwargs.pop("scenario")
        base_code, base_out = run(factory(), **kwargs)
        check(expect(base_code, base_out),
              f"{label}: the case it protects really holds unmutated")
        mutant_code, mutant_out = run(factory(), source=mutated, **kwargs)
        check(not expect(mutant_code, mutant_out), f"{label} is caught")

    print()
    print(f"{CHECKS} checks, {len(FAILURES)} failed")
    if FAILURES:
        for failure in FAILURES:
            print("  FAILED: " + failure)
        return 1
    if CHECKS < 40:
        print("FAIL: this fixture ran implausibly few checks - it has itself gone vacuous.")
        return 1
    print("Guard - bucket closing list: self-test PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
