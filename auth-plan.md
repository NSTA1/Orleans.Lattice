# Auth Epic - Coordination Plan (issue #971)

This is the live coordination log for the identity, authorization & enforcement
epic. It is maintained by the **coordinator session only**. Issue numbers are
used throughout (no tracker ids) so this file does not trip the repo hygiene
gates.

## Mission

Implement the entire epic #971 locally on the `feat/auth` branch: two new
sibling libraries (Membership, Auth), an admin API family (Api.Auth[.Grpc]),
an external data-plane API (Api.Data), Entra membership packages, Explorer
auth integration, a security-hardening review, and docs/sample/e2e - all
opt-in, fail-closed, server-side enforced, and observable.

## Operating rules (agreed with owner)

1. No PRs until the very end. All work lands on `feat/auth`. Push `feat/auth`
   to origin after each feature completes (branch push only, no PR).
2. Only the coordinator runs the full test suite. Sub-agents run focused tests
   only. Full non-chaos suite runs at each **package boundary** and must be
   green before the next package. Full suite + chaos runs once at the end.
3. Only the coordinator edits `CHANGELOG.md` and `docs/lattice/features.md`,
   incrementally as each feature is accepted.
4. Coordinator reviews every completed feature for: test gaps, unnecessary
   allocations, security flaws, scope drift. Return to sub-agent or remediate
   directly (coordinator's call, scope-dependent).
5. Coordinator stays resident while sub-agents run; never ends the session just
   for waiting.
6. Only a critical, blocking design flaw justifies stopping (with a summary).
7. Sub-agents work in dedicated git worktrees on child branches
   (`feat/auth-<issue>`), commit there, never push, never open PRs, never touch
   CHANGELOG/features.md, never run the full suite, never use the az CLI or live
   Entra. Coordinator reviews the child branch, then merges into `feat/auth`.
8. Entra / interactive-login / live-Azure paths are covered by fakes in tests;
   genuinely-live tests are marked with a skip category. Only the coordinator
   may use the authenticated az CLI to validate real-world behaviour.

## Package boundaries (full-suite gates)

- core: `Orleans.Lattice` (#973, #976, #977, core identity primitives from #974)
- membership: `Orleans.Lattice.Membership` (#972, #974)
- auth: `Orleans.Lattice.Auth` (#975, #978, #979, #980, #983)
- api.state: existing `Orleans.Lattice.Api.State` (#981)
- api.data: new `Orleans.Lattice.Api.Data` (#1095)
- replication: existing `Orleans.Lattice.Replication` (#982)
- api.auth: `Orleans.Lattice.Api.Auth` (#984) + `.Grpc` (#985)
- entra: `Orleans.Lattice.Membership.Entra`(.Graph) (#1101)
- explorer: `Orleans.Lattice.Explorer` (#1102)
- hardening: security review (#1103)
- docs: docs/sample/e2e (#986)

## Operational playbook (per-feature procedure)

Run this exact loop for every feature. Steps are mechanical so they survive
context loss.

**0. Pre-flight (once per feature).**
- Ensure `feat/auth` is checked out in the main worktree and clean
  (`git -C C:\dev\lattice status -sb`).
- Read the sub-issue body fresh: `gh issue view <n> --repo NSTA1/Orleans.Lattice
  --json title,body`. Note deliverables, acceptance criteria, dependencies.
- Confirm all dependency issues are merged into `feat/auth`.

**1. Create the worktree + child branch.**
- `git -C C:\dev\lattice worktree add C:\dev\lattice.wt\<n> -b feat/auth-<n> feat/auth`
- The child branch starts at the current `feat/auth` tip so it inherits all
  prior merged features.

**2. Dispatch a Feature Dev sub-agent (background).**
Prompt MUST contain the full contract:
- Work only inside `C:\dev\lattice.wt\<n>`; the branch is `feat/auth-<n>`.
- Full issue body pasted in + "also run `gh issue view <n>` to confirm".
- Implement all deliverables + acceptance criteria; follow repo conventions
  (file-scoped ns, one top-level type/file, `[GenerateSerializer]`+`[Alias]`+
  `[Id]`+`[Immutable]`, `ArgumentNullException.ThrowIfNull`, XML docs, options
  validator pattern, `TryAddSingleton` + AddLattice ordering guard).
- Alias rules: package owns `<Pkg>TypeAliases` + mirror alias test; aliases
  unique and <= 6 chars; prefixes olm./olz./oli./ole.; core identity primitives
  use ol. (`ol.sub`). Reserved: ol./olr./ola.
- Every public type/member gets >= 1 test. Write + run ONLY focused tests
  (`dotnet test test/<pkg> --filter <Name>`), never the full suite.
- Hygiene: no em-dash (U+2014), ASCII only, no tracker ids (F-/G-/R-...) in
  source/docs; describe by name or link the issue.
- Do NOT: touch `CHANGELOG.md` or `docs/lattice/features.md`; push; open a PR;
  use the az CLI or contact live Entra/Azure (use fakes, mark live tests with a
  skip category); edit outside the issue scope.
- Commit to `feat/auth-<n>` with a conventional message, no author attribution.
- Report back: what was built, test commands run + results, any assumptions,
  any design concerns.

**3. Review the returned work (coordinator).** Checklist:
- Scope: only in-scope files changed; no drift; deliverables + acceptance
  criteria all met.
- Tests: every new public type/member covered; negative/adversarial paths for
  security-relevant code; focused tests actually pass; deterministic (no sleeps
  where avoidable).
- Allocations: no per-call allocations on hot paths; no LINQ/closures in the
  enforcement/read hot path; `ValueTask` only where the repo sanctions it.
- Security: fail-closed defaults; no bypass; system/maintenance-origin handling
  correct; no secret logging.
- Conventions + hygiene gates (alias, em-dash, tracker-id, XML docs).
- Diff review: `git -C C:\dev\lattice.wt\<n> diff feat/auth...feat/auth-<n>`.
- Decision: accept, remediate directly (small/localized), or return to the
  sub-agent (larger/scope issues).

**4. Merge + integrate (coordinator).**
- From main worktree on `feat/auth`:
  `git -C C:\dev\lattice merge --no-ff feat/auth-<n>` (resolve conflicts).
- Add the incremental `CHANGELOG.md` entry and `docs/lattice/features.md` index
  row for this issue (coordinator-only files).
- Remove the worktree: `git -C C:\dev\lattice worktree remove C:\dev\lattice.wt\<n>`
  and delete the child branch once merged.

**5. Gate.**
- If this feature closes a **package boundary**, run the full non-chaos suite
  (`dotnet test --filter "TestCategory!=Chaos" --blame-hang --blame-hang-timeout
  3m`) and require green before starting the next package. Otherwise run the
  affected package's focused suite for a fast integration check.

**6. Record + push.**
- Update the task ledger status + append a Progress-log entry (what landed,
  key decisions, any follow-ups) in this file; commit it.
- Push: `git -C C:\dev\lattice push -u origin feat/auth` (branch only, no PR).
- Update the session task DB row to `done`.

**End of epic:** all 19 merged -> full suite + chaos green (coordinator) ->
reconcile CHANGELOG + features.md -> raise the PR with `Closes #971` and the
sub-issue closes, applying the correct release label.

## Task ledger (ordered; status tracked in the session task DB)

| # | Issue | Feature | Status |
|---|-------|---------|--------|
| 1 | #972 | Membership: project & package scaffolding | done (merged) |
| 2 | #973 | Core: caller-credential propagation seam | pending |
| 3 | #974 | Membership: subject model, directory & resolution | pending |
| 4 | #975 | Auth: project & package scaffolding | pending |
| 5 | #976 | Core: access-gate enforcement point | pending |
| 6 | #977 | Core: range-scan key-filter | pending |
| 7 | #978 | Auth: authorization rule model & policy store | pending |
| 8 | #979 | Auth: compiled snapshot & decision engine | pending |
| 9 | #980 | Auth: enforcement wiring at LatticeGrain | pending |
| 10 | #981 | State API: honour read-access visibility | pending |
| 11 | #1095 | Api.Data: external read-write data-plane API | pending |
| 12 | #982 | Replication: replicate auth/membership trees | pending |
| 13 | #983 | Auth: observability & audit | pending |
| 14 | #984 | Api.Auth: facade & model | pending |
| 15 | #985 | Api.Auth.Grpc: gRPC binding, client, meta-auth | pending |
| 16 | #1101 | Membership.Entra: Entra ID authenticator | pending |
| 17 | #1102 | Explorer: connect to auth-enabled State API | pending |
| 18 | #1103 | Security hardening: full security & design review | pending |
| 19 | #986 | Docs, sample & end-to-end tests | pending |

Out of scope: #1104 (admin UI follow-up).

## Decision log

- 2026-07-03: Coordination started. Baseline build green (0 warnings/errors) on
  `main`; `feat/auth` branched from `main`.

## Progress log

- 2026-07-03 #972 (Membership scaffolding) MERGED. Empty `Orleans.Lattice.Membership`
  package + test project, slnx + docs skeleton, `LatticeMembershipMarker` + trivial
  green test. Version 7.7.1 (family lockstep with core/api.state). Post-merge focused
  test green (1/1). DECISION: no CHANGELOG/features entry yet - the package is inert
  with nothing user-callable; its entry lands with the first real behaviour (#974).
  Placeholder marker to be deleted when real types arrive (per #974).
