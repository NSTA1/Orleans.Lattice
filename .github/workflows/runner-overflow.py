"""Route a planned test matrix between GitHub-hosted runners and the self-hosted pool.

The repository's hosted runners share one account-wide concurrency cap (20 jobs
on the Free plan). Every matrix lane (ci.yml legs, the nightly Coverage legs
and TLC legs, formal-receipts.yml TLC legs) plans its legs, then calls this
script before publishing them; ci.yml's Content gates job is routed as a
single-leg matrix by its own small job. It counts the hosted jobs already running or
queued in this repository - queued counts, because a queued hosted job is one
waiting behind the cap, and a new hosted leg would wait behind it - and keeps
legs on `ubuntu-latest` while that count stays under the budget (HOSTED_RUNNER_BUDGET, default 15 - five short of the
cap, so concurrent planners overlapping by a little, and the jobs that always
run hosted - the planners and routers themselves - do not tip a lane into a
queue). The legs beyond the budget go to the elastic self-hosted pool in
tools/ci-runners, longest first, because a long leg holds a hosted slot longest.

Each leg gains three keys, read by the workflows:
  runsOn  a JSON runs-on value: "ubuntu-latest" or the pool's label array;
  pool    true on a pool leg (the job prefixes its name with "[pool] " and
          its Azurite step runs the npm build rather than the image);
  runner  "hosted" or "pool", for the summary.

FAILS TOWARD HOSTED. Without RUNNER_POOL every leg stays hosted, so the pool is
opt-in by a single repository variable. If the GitHub API cannot be read, every
leg stays hosted too: the cost of a wrong guess is queueing, as before.

The count is a snapshot. Two lanes planning at the same moment can both see
the same headroom; the budget's margin under the cap absorbs that. It counts
this repository only: the cap is account-wide, so other repositories' jobs are
invisible here.

Usage: runner-overflow.py --matrix FILE [--matrix FILE ...]   (rewritten in place)
       runner-overflow.py --self-test
Environment: RUNNER_POOL (JSON label array), HOSTED_RUNNER_BUDGET,
             GITHUB_REPOSITORY, GH_TOKEN or GITHUB_TOKEN, GITHUB_API_URL,
             GITHUB_STEP_SUMMARY.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

HOSTED = "ubuntu-latest"
DEFAULT_BUDGET = 15
ACTIVE = ("queued", "in_progress")


def parse_pool(raw: str | None) -> list[str] | None:
    """The pool's runs-on labels from RUNNER_POOL, or None when routing is off."""
    if not raw or not raw.strip():
        return None
    labels = json.loads(raw)
    if not isinstance(labels, list) or not labels or not all(isinstance(l, str) and l for l in labels):
        raise ValueError(f"RUNNER_POOL must be a non-empty JSON array of labels, got {raw!r}")
    if "self-hosted" not in labels:
        raise ValueError("RUNNER_POOL must include the 'self-hosted' label")
    return labels


def parse_budget(raw: str | None) -> int:
    if not raw or not raw.strip():
        return DEFAULT_BUDGET
    value = int(raw)
    if value < 0:
        raise ValueError("HOSTED_RUNNER_BUDGET must not be negative")
    return value


def route(legs_by_file: list[list[dict]], pool: list[str] | None, busy: int | None, budget: int) -> tuple[int, int]:
    """Annotate every leg in place. Returns (hosted, pooled)."""
    everything = [leg for legs in legs_by_file for leg in legs]
    for leg in everything:
        leg["runsOn"] = json.dumps(HOSTED)
        leg["pool"] = False
        leg["runner"] = "hosted"
    if pool is None or busy is None:
        return len(everything), 0
    slots = max(0, budget - busy)
    overflow = max(0, len(everything) - slots)
    longest_first = sorted(everything, key=lambda leg: float(leg.get("estimate") or 0), reverse=True)
    for leg in longest_first[:overflow]:
        leg["runsOn"] = json.dumps(pool)
        leg["pool"] = True
        leg["runner"] = "pool"
    return len(everything) - overflow, overflow


def _get(url: str, token: str) -> dict:
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "lattice-runner-overflow",
    })
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def count_busy_hosted(api: str, repo: str, token: str) -> int:
    """Hosted jobs queued or running in this repository right now."""
    busy = 0
    seen: set[int] = set()
    for status in ACTIVE:
        for page in range(1, 6):
            runs = _get(f"{api}/repos/{repo}/actions/runs?status={status}&per_page=100&page={page}", token)
            batch = runs.get("workflow_runs", [])
            for run in batch:
                if run["id"] in seen:
                    continue
                seen.add(run["id"])
                for jpage in range(1, 6):
                    jobs = _get(f"{api}/repos/{repo}/actions/runs/{run['id']}/jobs?filter=latest&per_page=100&page={jpage}", token)
                    jbatch = jobs.get("jobs", [])
                    busy += sum(1 for job in jbatch
                                if job.get("status") in ACTIVE and "self-hosted" not in (job.get("labels") or []))
                    if len(jbatch) < 100:
                        break
            if len(batch) < 100:
                break
    return busy


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", action="append", default=[])
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    if not args.matrix:
        ap.error("--matrix is required")

    legs_by_file = []
    for path in args.matrix:
        with open(path, encoding="utf-8") as f:
            legs = json.load(f)
        if not isinstance(legs, list):
            raise SystemExit(f"{path}: expected a JSON array of legs")
        legs_by_file.append(legs)

    pool = parse_pool(os.environ.get("RUNNER_POOL"))
    budget = parse_budget(os.environ.get("HOSTED_RUNNER_BUDGET"))
    busy: int | None = None
    note = ""
    if pool is None:
        note = "RUNNER_POOL is not set, so every leg runs on GitHub-hosted runners."
    else:
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
        try:
            busy = count_busy_hosted(os.environ.get("GITHUB_API_URL", "https://api.github.com"),
                                     os.environ["GITHUB_REPOSITORY"], token)
        except (urllib.error.URLError, KeyError, ValueError, TimeoutError) as e:
            print(f"::warning::runner-overflow: could not count hosted jobs ({e}); every leg stays on hosted runners.")
            note = "The hosted job count could not be read, so every leg stays on hosted runners."

    hosted, pooled = route(legs_by_file, pool, busy, budget)
    for path, legs in zip(args.matrix, legs_by_file):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(legs, f)

    line = (f"Runner routing: {hosted} leg(s) on hosted runners, {pooled} on the self-hosted pool "
            f"(busy hosted jobs {busy if busy is not None else 'unknown'}, budget {budget}).")
    print(line + (f" {note}" if note else ""))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"\n{line}{(' ' + note) if note else ''}\n")
    return 0


def self_test() -> int:
    failures = []

    def legs(*estimates):
        return [{"id": f"leg-{i + 1}", "name": f"n{i + 1}", "estimate": e, "items": []} for i, e in enumerate(estimates)]

    def check(cond, msg):
        if not cond:
            failures.append(msg)

    pool = ["self-hosted", "lattice-pool"]

    a = legs(10, 30, 20)
    check(route([a], None, 0, 18) == (3, 0), "no pool: everything hosted")
    check(all(json.loads(l["runsOn"]) == HOSTED and l["pool"] is False for l in a), "no pool: runsOn hosted")

    b = legs(10, 30, 20)
    check(route([b], pool, None, 18) == (3, 0), "unreadable count: everything hosted")

    c = legs(10, 30, 20)
    check(route([c], pool, 5, 18) == (3, 0), "headroom: everything hosted")

    d = legs(10, 30, 20, 5)
    check(route([d], pool, 16, 18) == (2, 2), "two slots left: two hosted, two pooled")
    pooled = {l["id"] for l in d if l["pool"]}
    check(pooled == {"leg-2", "leg-3"}, f"the longest legs overflow first, got {pooled}")
    check(all(json.loads(l["runsOn"]) == pool for l in d if l["pool"]), "pooled legs carry the pool labels")

    e = legs(10, 30)
    check(route([e], pool, 25, 18) == (0, 2), "over budget: everything pooled")

    f1, f2 = legs(5, 50), legs(40)
    check(route([f1, f2], pool, 17, 18) == (1, 2), "several matrices share one budget")
    check(f1[0]["pool"] is False and f1[1]["pool"] and f2[0]["pool"], "shared budget keeps the shortest leg hosted")

    for raw, ok in (("", None), ('["self-hosted","lattice-pool"]', pool)):
        check(parse_pool(raw) == ok, f"parse_pool({raw!r})")
    for raw in ('["lattice-pool"]', '"self-hosted"', "[]"):
        try:
            parse_pool(raw)
            failures.append(f"parse_pool({raw!r}) should be refused")
        except ValueError:
            pass
    check(parse_budget("") == DEFAULT_BUDGET and parse_budget("20") == 20, "parse_budget")

    if failures:
        for msg in failures:
            print(f"runner-overflow self-test FAILED: {msg}")
        return 1
    print("runner-overflow self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
