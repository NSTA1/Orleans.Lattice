#!/usr/bin/env python3
"""Decide which test tiers a ci.yml run executes, and say why.

THE RULE
--------
A pull request whose BASE is an integration branch (`<type>/epic/<slug>`, an
epic or a bucket) is a MEMBER pull request. It runs the deterministic tier and
skips the two tiers whose cost is dominated by exploration rather than by the
behaviour under test:

- `coyote` - systematic schedule exploration of the extracted concurrency
  cores (`[Category("Coyote")]`). Its cost is the number of schedules
  explored, not the code changed.
- `chaos` - fault-injection and stress runs (`[Category("Chaos")]`), which
  spin up multi-silo clusters, kill and restart them, and are the slowest and
  most flake-prone tier per test.

Every other run keeps all test tiers. Exhaustive TLC is separately scoped:

- a pull request into `main` or an integration branch skips exhaustive TLC only
  when its diff contains no TLC verdict input; its base-model smoke shard still
  runs;
- an integration-branch push always plans exhaustive TLC, with receipts on. It
  never skips by its own diff: that would assume the previous push's run passed
  exhaustive TLC, which nothing checks (it may have failed, or been cancelled by
  this push's concurrency group). Receipts give the saving safely instead,
  because only a fingerprint that actually passed has one;
- a pull request into a `release/**` line, a release-line push and nightly
  coverage keep exhaustive TLC;
- any event or base this script does not recognise. The failure direction is
  deliberately "run more", never "run less".

When exhaustive TLC does run, per-module verdict receipts (tlc-receipts.py,
contract C5, #5164) decide which modules actually run: a module whose
fingerprint has a trusted receipt is verified by that receipt instead. Release
lines never use receipts (`tlc_receipts=off`), and neither does a
`workflow_dispatch` run whose `tlc_receipts` input is `off`.

WHY THE SKIPPED TIERS STILL RUN SOMEWHERE
-----------------------------------------
Every commit a member pull request brings reaches `main` only through its
integration branch, and the integration branch is evaluated with every tier
twice over: on its own push lane after each member merge, and by its pull
request into `main`, which is the required check. A Coyote or chaos regression
introduced by a member therefore still blocks the merge to `main`; what it loses
is per-member attribution, because it surfaces on the bucket with every other
member's changes beside it. That is the accepted cost of this rule.

EXHAUSTIVE TLC RUNS WHEN ITS INPUTS CHANGE
-------------------------------
The `Tlc` category is also exploration-dominated - TLC model-checks every
specification module and a mutant of each definition, and a full exhaustive run
costs about 460-550 runner-minutes - and it is deterministic: the same
specification, harness and toolchain yield the same verdict every run. A pull
request into `main` or an integration branch runs the exhaustive TLC shards
(test-shards.json `"tlc": true`) exactly
when its diff touches a TLC verdict input, and skips them otherwise. Every file
that can affect a TLC run is classified in one file, `tlc-inputs.json` beside
this script, into three classes (`TlcInputClassificationTests` pins it):

- verdict inputs can change a TLC verdict or the set of cases that exist: the
  `.tla`, `.cfg`, manifest and mutation files under spec/, the Formal harness
  that discovers modules, builds cases and cfgs, launches TLC and judges its
  output, the shared hygiene code it compiles against, the TLA+ toolchain
  (including the workflows that still pin it), and the build files;
- routing files decide which job runs which existing case, or provision and
  launch legs (test-shards.json, the planner, this script, `TlcCiShard.cs`);
  a routing-only change runs the smoke shard, which proves provisioning end to
  end, and the partition check in `CiMemberPullRequestTieringTests` proves without a toolchain that the routing
  still selects every TLC case exactly once;
- neutral files never affect a TLC run (refinement notes and READMEs TLC never
  reads, the Formal and workflow files nothing on the TLC path references).

A change that touches no verdict input cannot alter any TLC verdict, so the
skip loses nothing - not even attribution, because a specification change is
precisely a TLC input and still runs TLC on the pull request that wrote it. The
separate `"tlcSmoke": true` base-model shard still runs on every routine CI pull
request. Release-line pull requests and pushes, nightly coverage, and any
unknown event keep the exhaustive shards, and so does every integration-branch
push (with receipts). A missing or
empty changed-file list runs TLC (fail towards "run more").

LEG CAP
-------
A member run is also planned onto at most `MEMBER_MAX_LEGS` legs rather than
the full run's `FULL_MAX_LEGS`. Every leg pays a fixed overhead (checkout, SDK,
restore, a whole-solution build, the TLA+ toolchain) of a few minutes, and a
concurrent-job cap is spent per job, so a bucket with many open members queues
on job count rather than on test time. Fewer legs per member trades a few
minutes of one member's makespan for runner slots the other members are
waiting on.

Usage:
  tier-scope.py --event EVENT [--base-ref REF] [--ref REF] [--changed-files FILE]
                [--tlc-receipts on|off]
                [--github-output FILE] [--summary-file FILE]
  tier-scope.py --self-test

Prints `key=value` lines (scope, skipped_tiers, max_legs, reason,
skip_tlc_reason, tlc_receipts, tlc_receipts_reason) to stdout,
and appends them to --github-output when given.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import sys
import tempfile
import unittest
from pathlib import Path

# The tiers a member pull request does not run, in plan-test-matrix.py's TIERS
# order. Changing this list changes what every member pull request verifies.
MEMBER_SKIPPED_TIERS: list[str] = ["coyote", "chaos"]

# The TLC verdict-input classification (contract C1 of #5148). A pull request
# that touches no verdict input skips the exhaustive "tlc" shards; see EXHAUSTIVE
# TLC RUNS WHEN ITS INPUTS CHANGE. Patterns are fnmatch globs over
# repository-relative paths, where `*` also crosses `/`.
TLC_INPUTS_FILE = Path(__file__).with_name("tlc-inputs.json")


def load_tlc_inputs(path: Path = TLC_INPUTS_FILE) -> tuple[list[str], list[str]]:
    """The verdict-input globs and the exclusion globs (`routing` then `neutral`).
    A routing-only change runs the smoke shard, not the exhaustive shards.
    A missing or malformed file raises:
    a classification nobody can read must fail the run, not skip TLC."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema") != 1:
        raise ValueError(f"{path}: unsupported schema {document.get('schema')!r}")
    lists = []
    for key in ("verdictInputs", "routing", "neutral"):
        globs = document.get(key)
        if not isinstance(globs, list) or not all(isinstance(glob, str) and glob for glob in globs):
            raise ValueError(f"{path}: '{key}' must be a list of non-empty glob strings")
        lists.append(globs)
    if not lists[0]:
        raise ValueError(f"{path}: 'verdictInputs' is empty, so every pull request would skip TLC")
    return lists[0], lists[1] + lists[2]


TLC_INPUTS, TLC_EXCLUSIONS = load_tlc_inputs()

FULL_MAX_LEGS = 10
MEMBER_MAX_LEGS = 6


def is_integration_branch(ref: str) -> bool:
    """`*/epic/**` in Actions' branch-filter sense: `*` stops at `/`."""
    parts = ref.split("/")
    return len(parts) >= 3 and parts[0] != "" and parts[1] == "epic" and all(parts[2:])


def is_tlc_input(path: str) -> bool:
    """A verdict input: matches `verdictInputs` and no `routing` or `neutral` glob."""
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in TLC_INPUTS) and not any(
        fnmatch.fnmatchcase(path, pattern) for pattern in TLC_EXCLUSIONS
    )


def touches_tlc_input(changed: list[str] | None) -> bool:
    """True unless a non-empty changed-file list touches no TLC input."""
    if not changed:
        return True
    return any(is_tlc_input(path) for path in changed)


def decide(
    event: str,
    base_ref: str,
    changed: list[str] | None = None,
    ref: str = "",
    receipts_input: str = "on",
) -> dict[str, str]:
    """The tier scope for one run, plus whether it reads TLC verdict receipts."""
    decision = decide_tiers(event, base_ref, changed)

    if decision["skip_tlc_reason"]:
        receipts, why = "off", "exhaustive TLC does not run, so there is nothing to look up"
    elif (event == "pull_request" and fnmatch.fnmatchcase(base_ref, "release/*")) or (
            event == "push" and fnmatch.fnmatchcase(ref, "release/*")):
        receipts, why = "off", "release lines never use receipts: every module runs"
    elif receipts_input.strip().lower() == "off":
        receipts, why = "off", "the run was dispatched with tlc_receipts=off: every module runs"
    else:
        receipts, why = "on", (
            "a module whose fingerprint has a trusted receipt (own scope, base branch or main) "
            "is verified by it and does not run"
        )
    decision["tlc_receipts"] = receipts
    decision["tlc_receipts_reason"] = why
    return decision


def decide_tiers(event: str, base_ref: str, changed: list[str] | None = None) -> dict[str, str]:
    """The tier scope for one run. Anything unrecognised is fully gated."""
    full = {"scope": "full", "skipped_tiers": "", "max_legs": str(FULL_MAX_LEGS), "skip_tlc_reason": ""}

    if event != "pull_request":
        full["reason"] = (
            f"a '{event or 'unknown'}' run is not a member pull request, so every tier runs"
        )
        return full

    if base_ref == "main":
        full["reason"] = (
            "a pull request into 'main' keeps every tier; exhaustive TLC runs only when a TLC input changes"
        )
        if not touches_tlc_input(changed):
            full["skip_tlc_reason"] = (
                "pull request into 'main' touches no TLC verdict input (only routing, neutral or unclassified paths, per tlc-inputs.json), "
                "so no exhaustive TLC verdict can change; the base-model smoke shard still runs"
            )
        return full

    if fnmatch.fnmatchcase(base_ref, "release/*"):
        full["reason"] = (
            f"a pull request into '{base_ref}' is fully gated, including exhaustive TLC"
        )
        return full

    if not is_integration_branch(base_ref):
        full["reason"] = (
            f"the base '{base_ref or 'unknown'}' is not an integration branch, "
            "so nothing is skipped"
        )
        return full

    skip_tlc_reason = "" if touches_tlc_input(changed) else (
        f"member pull request into integration branch '{base_ref}' that touches no TLC "
        "verdict input (only routing, neutral or unclassified paths, per tlc-inputs.json), so no exhaustive TLC "
        "verdict can change; TLC runs on that branch's own push lane and on its pull "
        "request into main; the base-model smoke shard still runs"
    )
    return {
        "scope": "member",
        "skipped_tiers": ",".join(MEMBER_SKIPPED_TIERS),
        "max_legs": str(MEMBER_MAX_LEGS),
        "skip_tlc_reason": skip_tlc_reason,
        "reason": (
            f"member pull request into integration branch '{base_ref}': the "
            f"{' and '.join(MEMBER_SKIPPED_TIERS)} tiers are skipped here and run on that "
            "branch's own push lane and on its pull request into main"
        ),
    }


class TierScopeSelfTest(unittest.TestCase):
    """The #5149 done-when cases, plus both directions of the C1 gate."""

    MEMBER = "ci/epic/formal-pipeline"

    def skips(self, base: str, changed: list[str]) -> bool:
        return bool(decide("pull_request", base, changed)["skip_tlc_reason"])

    def test_a_neutral_refinement_test_file_skips_exhaustive_tlc_and_keeps_the_smoke_shard(self):
        changed = ["test/lattice/Formal/RefinementDetectorResolverTests.cs"]
        for base in ("main", self.MEMBER):
            with self.subTest(base=base):
                reason = decide("pull_request", base, changed)["skip_tlc_reason"]
                self.assertIn("smoke shard still runs", reason)

    def test_coverage_workflow_plus_a_hygiene_test_skips_exhaustive_tlc(self):
        changed = [".github/workflows/coverage.yml", "test/lattice/Hygiene/AnyHygieneTests.cs"]
        self.assertTrue(self.skips("main", changed))
        self.assertTrue(self.skips(self.MEMBER, changed))

    def test_verdict_inputs_run_exhaustive_tlc(self):
        for path in (
            "test/shared/Orleans.Lattice.Testing/Hygiene/HygieneRepository.cs",
            ".github/actions/tla-toolchain/action.yml",
            "tools/tlc/toolchain.json",
            "test/lattice/Formal/RefinementNote.cs",
            "test/lattice/Formal/RefinementPropertyCoverageTests.cs",
            "test/lattice/Formal/TlcModelCheckTests.cs",
            "test/lattice/Formal/SomeNewHarnessFile.cs",
            "spec/wal/WalMove.tla",
            "spec/wal/mutations/AnyMutation.mutation",
            "tools/tlc/patch.json",
            "test/Directory.Build.props",
        ):
            for base in ("main", self.MEMBER):
                with self.subTest(path=path, base=base):
                    self.assertFalse(self.skips(base, ["src/lattice/X.cs", path]))

    def test_routing_only_changes_skip_exhaustive_tlc_and_keep_the_smoke_shard(self):
        for path in (
            ".github/workflows/test-shards.json",
            ".github/workflows/plan-test-matrix.py",
            ".github/workflows/run-test-leg.py",
            ".github/workflows/tier-scope.py",
            ".github/workflows/tlc-inputs.json",
            ".github/workflows/lightweight-change.py",
            ".github/workflows/select-test-packages.sh",
            ".github/workflows/ci.yml",
            ".github/workflows/publish.yml",
            ".github/workflows/ci-serial-old.yml",
            "test/lattice/Formal/TlcCiShard.cs",
        ):
            for base in ("main", self.MEMBER):
                with self.subTest(path=path, base=base):
                    reason = decide("pull_request", base, [path, "test/lattice/Formal/TlcCiShardTests.cs"])["skip_tlc_reason"]
                    self.assertIn("smoke shard still runs", reason)

    def test_a_routing_change_beside_a_verdict_input_runs_exhaustive_tlc(self):
        self.assertFalse(self.skips("main", [".github/workflows/test-shards.json", "spec/wal/WalMove.cfg"]))
    def test_neutral_paths_inside_verdict_globs_skip(self):
        for path in (
            "spec/wal/Refinement.md",
            "spec/README.md",
            "spec/atomic-commit/perturbations/Any.perturbation",
            "tools/tlc/README.md",
        ):
            with self.subTest(path=path):
                self.assertTrue(self.skips("main", [path]))

    def test_no_or_empty_changed_list_runs_exhaustive_tlc(self):
        self.assertFalse(self.skips("main", []))
        self.assertFalse(bool(decide("pull_request", "main", None)["skip_tlc_reason"]))

    def test_an_integration_push_always_plans_exhaustive_tlc_with_receipts_on(self):
        # Never skipped by the push's own diff: the previous push's run may have
        # failed or been cancelled, so its verdict cannot be assumed (#5187 review).
        decision = decide("push", "", [], self.MEMBER)
        self.assertEqual(decision["skip_tlc_reason"], "")
        self.assertEqual(decision["skipped_tiers"], "", "a push lane skips no test tier")
        self.assertEqual(decision["tlc_receipts"], "on")

    def test_a_release_line_push_never_skips_and_never_uses_receipts(self):
        decision = decide("push", "", [], "release/1.4")
        self.assertEqual(decision["skip_tlc_reason"], "")
        self.assertEqual(decision["tlc_receipts"], "off")
        self.assertEqual(decide("pull_request", "release/1.4", ["spec/a.tla"])["tlc_receipts"], "off")

    def test_receipts_are_on_wherever_exhaustive_tlc_runs_unless_switched_off(self):
        self.assertEqual(decide("pull_request", "main", ["spec/a.tla"])["tlc_receipts"], "on")
        self.assertEqual(decide("pull_request", self.MEMBER, ["spec/a.tla"])["tlc_receipts"], "on")
        self.assertEqual(decide("workflow_dispatch", "", None, "feat/x")["tlc_receipts"], "on")
        self.assertEqual(decide("workflow_dispatch", "", None, "feat/x", "off")["tlc_receipts"], "off")
        self.assertEqual(decide("pull_request", "main", ["src/a.cs"])["tlc_receipts"], "off")

    def test_the_docstring_states_the_current_full_run_cost(self):
        self.assertNotIn("40 runner-minutes", __doc__)
        self.assertIn("460-550 runner-minutes", " ".join(__doc__.split()))

    def test_a_classification_that_cannot_be_read_fails_rather_than_skips(self):
        for body in ("{", '{"schema": 2, "verdictInputs": ["spec/**"], "routing": [], "neutral": []}',
                     '{"schema": 1, "verdictInputs": [], "routing": [], "neutral": []}',
                     '{"schema": 1, "verdictInputs": ["spec/**"], "neutral": []}',
                     '{"schema": 1, "verdictInputs": ["spec/**"], "routing": []}'):
            with self.subTest(body=body), tempfile.TemporaryDirectory() as work:
                path = Path(work) / "tlc-inputs.json"
                path.write_text(body, encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_tlc_inputs(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--event")
    parser.add_argument("--base-ref", default="")
    parser.add_argument(
        "--changed-files",
        help="File listing the diff's changed paths, one per line. Absent, unreadable "
             "or empty means TLC is not skipped.",
    )
    parser.add_argument("--ref", default="", help="The pushed ref name (github.ref_name).")
    parser.add_argument("--tlc-receipts", default="on",
                        help="'off' makes the run ignore TLC verdict receipts (workflow_dispatch input).")
    parser.add_argument("--github-output")
    parser.add_argument("--summary-file")
    args = parser.parse_args()

    if args.self_test:
        result = unittest.main(argv=["tier-scope"], exit=False).result
        return 0 if result.wasSuccessful() and result.testsRun > 0 else 1
    if args.event is None:
        parser.error("--event is required")

    def read_list(path: str | None) -> list[str] | None:
        if not path:
            return None
        try:
            with open(path, encoding="utf-8") as handle:
                return [line.strip() for line in handle if line.strip()]
        except OSError:
            return None

    decision = decide(args.event.strip(), args.base_ref.strip(), read_list(args.changed_files),
                      args.ref.strip(), args.tlc_receipts)
    lines = [
        f"{key}={decision[key]}"
        for key in ("scope", "skipped_tiers", "max_legs", "reason", "skip_tlc_reason",
                    "tlc_receipts", "tlc_receipts_reason")
    ]

    for line in lines:
        print(line)

    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")

    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            if decision["skipped_tiers"]:
                handle.write(f"### Test tiers skipped: {decision['skipped_tiers'].replace(',', ', ')}\n\n")
                handle.write(f"Reason: {decision['reason']}.\n\n")
                handle.write(
                    "These tiers were NOT executed on this run. A green check here does not "
                    "assert anything about them. They are executed on the integration branch's "
                    "push lane after this pull request merges, and by that branch's pull "
                    "request into `main`, which skips nothing.\n\n"
                )
            else:
                handle.write(f"### Test tiers: all ({decision['reason']})\n\n")
            if decision["skip_tlc_reason"]:
                handle.write("### Exhaustive TLC shards skipped\n\n")
                handle.write(f"Reason: {decision['skip_tlc_reason']}.\n\n")
                handle.write("The base-model smoke shard still runs.\n\n")
            else:
                handle.write(f"### TLC verdict receipts: {decision['tlc_receipts']}\n\n")
                handle.write(f"Reason: {decision['tlc_receipts_reason']}.\n\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
