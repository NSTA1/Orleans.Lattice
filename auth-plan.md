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
  IMPORTANT: run `$env:GH_TOKEN=""` first - the shell default `GH_TOKEN`
  resolves to the wrong account (staudtnathan_microsoft) and git push 403s;
  clearing it makes gh's git-credential helper use the NSTA1 keyring account.
  The same `$env:GH_TOKEN=""` prefix is required for every `gh` write.
- Update the session task DB row to `done`.

**End of epic:** all 19 merged -> full suite + chaos green (coordinator) ->
reconcile CHANGELOG + features.md -> raise the PR with `Closes #971` and the
sub-issue closes, applying the correct release label.

## Task ledger (ordered; status tracked in the session task DB)

| # | Issue | Feature | Status |
|---|-------|---------|--------|
| 1 | #972 | Membership: project & package scaffolding | done (merged) |
| 2 | #973 | Core: caller-credential propagation seam | done (merged) |
| 3 | #974 | Membership: subject model, directory & resolution | done (merged) |
| 4 | #975 | Auth: project & package scaffolding | done (merged) |
| 5 | #976 | Core: access-gate enforcement point | done (merged; gate batched with #977) |
| 6 | #977 | Core: range-scan key-filter | done (merged; batched gate) |
| 7 | #978 | Auth: authorization rule model & policy store | done (merged; gate batched with #977) |
| 8 | #979 | Auth: compiled snapshot & decision engine | done (merged; 116 focused tests) |
| 9 | #980 | Auth: enforcement wiring at LatticeGrain | pending |
| 10 | #981 | State API: honour read-access visibility | pending |
| 11 | #1095 | Api.Data: external read-write data-plane API | pending |
| 12 | #982 | Replication: replicate auth/membership trees | pending |
| 13 | #983 | Auth: observability & audit | pending |
| 14 | #984 | Api.Auth: facade & model | pending |
| 15 | #985 | Api.Auth.Grpc: gRPC binding, client, meta-auth | pending |
| 16 | #1101 | Membership.Entra: Entra ID authenticator | done (merged; 53 focused tests) |
| 17 | #1102 | Explorer: connect to auth-enabled State API | pending |
| 18 | #1103 | Security hardening: full security & design review | pending |
| 19 | #986 | Docs, sample & end-to-end tests | pending |

Out of scope: #1104 (admin UI follow-up).

## Open concerns (MUST-CLOSE before final PR)

- **OC-2 (security, for #980): the gate's own policy-store reads must run under
  system-origin.** The #977 re-entrancy fix wraps *subject resolution* in a
  system-origin scope so the membership directory's dogfooded-tree reads bypass
  the gate. When #980 wires a REAL decision engine, the gate/decision path will
  read the `sys-auth-policy` tree (via `ILatticeAuthorizationPolicyStore`, public
  scan surface) - those reads MUST likewise be system-origin or they recurse into
  the gate. Same applies to any membership/auth infra read on the enforced path.
  ACTION: #980 sub-agent contract MUST require system-origin wrapping for all
  gate/decision-engine internal tree reads, with a real-gate + membership-directory
  regression test proving no recursion. Backstop: #1103.

- **OC-1 (security): durable-cursor read path bypasses the key-filter.** #977
  wired the read-path key-filter into the `KeysAsync`/`EntriesAsync`/`GetMany`/
  `Count` surfaces at the `LatticeGrain` merge yield point, but the durable
  paged-cursor surface (`ILatticeCursorGrain`: `OpenKeyCursorAsync`/
  `NextKeysAsync` and the entry/snapshot variants) runs in a SEPARATE grain
  activation and does NOT funnel through `KeysAsyncCore`, so it is currently an
  unfiltered read surface. A `Func<string,bool>` cannot be serialized across the
  `OpenAsync` boundary, so closing this requires the cursor grain to resolve its
  OWN gate + subject (subject flows via `RequestContext`) and apply the filter at
  its page-emit point. ACTION: fold explicitly into #980's enforcement scope (the
  #980 sub-agent contract MUST name this path); #1103 security review is the
  backstop. Do NOT let the epic reach the final PR with this open.

## Decision log

- 2026-07-03: Coordination started. Baseline build green (0 warnings/errors) on
  `main`; `feat/auth` branched from `main`.

- OC-3 (security, for #1103 review): `EntraCredentialAuthenticator.AuthenticateAsync`
  propagates `IEntraGroupResolver` exceptions on the groups-overage path (only the
  NO-resolver case is the silent token-only fallback). A Microsoft Graph outage
  therefore fails authentication outright. Degrading to token-only groups on a
  resolver fault is fail-closed for access (fewer groups = less access) and better
  for availability. Left strict/propagate as the #1101 sub-agent implemented;
  finalize the degrade-vs-fail decision in the #1103 security review.

## Progress log

- 2026-07-03 #979 (F-155, compiled policy snapshot & decision engine) MERGED into
  `feat/auth`. Adds an INERT, in-process decision surface: `ILatticeDecisionEngine`
  (`CurrentEpoch` + synchronous allocation-light `Evaluate`), a `PolicyEvaluator`
  (tiered precedence exact-key > prefix > tree; Deny-over-Allow; optional
  User-over-Group at equal scope; range reads -> `Filtered` predicate whose inner
  per-key resolve is allocation-free), and a `CompiledPolicySnapshotMaintainer`
  (per-silo singleton; observes the core change-feed via `IMutationObserver`,
  coalesced background rebuilds off the write path, atomic snapshot swap, monotonic
  `PolicyEpoch` via Interlocked). NO `ILatticeAccessGate` registered - core gate
  stays `NullLatticeAccessGate` (integration test asserts it); enforcement is #980.
  All new types in-process (no aliases). Reviewed: evaluator precedence correct, DI
  inert, epoch monotonic. COORDINATOR REMEDIATION: the sub-agent had worked around
  a concurrent-scan `EnumerationAbortedException` (maintainer rescan overlaps a
  caller's list scan) with a hand-rolled buffer-and-retry using a fragile
  `ex.GetType().Name` string match. Replaced with core's blessed resilient
  `ScanEntriesAsync<T>` (transparent reconnect, no duplicates/gaps) in the policy
  store - deletes the bespoke retry + string-name predicate. OC-4 (was flag #1)
  thereby CLOSED. Focused Auth suite 116/116, stable across 3 runs. #979 flag #4
  (EnsureWarmAsync is on the concrete maintainer, not the interface) + flag #6
  (`PolicyEpoch` feeds #982's strict fence) carried into #980/#982 scope.

- 2026-07-03 #1101 (Membership.Entra, Entra ID authenticator) MERGED into
  `feat/auth` (parallel independent-package track). Two new packages:
  `Orleans.Lattice.Membership.Entra` (specializes the F-150 `JwtCredentialAuthenticator`
  - OIDC/JWKS discovery + signing-key rotation via `ConfigurationManager<OpenIdConnectConfiguration>`,
  Entra v2.0 claim mapping `oid`/`tid`/`groups`/`roles`, tenant allow-list issuer
  validation, groups-overage detection with pluggable `IEntraGroupResolver` +
  token-only default) and `Orleans.Lattice.Membership.Entra.Graph` (MS Graph-backed
  resolver behind faked seams; MSAL confidential-client app-token acquired/cached/
  transparently refreshed with single-flight `SemaphoreSlim` guard). Reviewed:
  design clean, registration ordering-guarded after `AddLatticeMembership`, no
  aliases needed (all in-process types), 0 warnings, hygiene clean. Focused suites
  green: Entra 32/32, Graph 21/21 (53 total). Pkg pins IdentityModel 8.3.0 (matches
  membership), Graph 5.105.0, MSAL 4.66.1, Kiota 1.22.2 (advisory GHSA-7j59 override).
  Raised OC-3 (resolver-failure = fail vs degrade) -> deferred to #1103. Docs/
  CHANGELOG/features refresh deferred to the final docs pass (#986) + epic PR.

- 2026-07-03 GATE FIX (#977 re-entrancy, coordinator remediation on `feat/auth`).
  The first batched full non-chaos gate (976+977+978) surfaced 4 Membership
  failures (`EnumerationAbortedException`) + 1 flaky Replication failure. ROOT
  CAUSE: `LatticeGrain.AuthorizeAsync` resolved the caller subject on every public
  scan; with Membership registered, the directory reads its `sys-membership-*`
  trees through the public scan surface, so subject resolution re-entered the
  grain mid-enumeration and aborted the enumerator. FIX (core-only, both in
  `AuthorizeAsync`): (A) when only the default null gate is registered, return a
  cached allow-all WITHOUT resolving the subject/allocating a request (restores
  byte-identical pre-gate default path; membership suite 51/51 and its runtime
  dropped ~79s->~4s); (B) when a real gate is registered, resolve the subject
  under a `LatticeAccessGateContext.EnterSystemOrigin()` scope so the directory's
  own dogfooded-tree reads bypass the gate instead of recursing. Re-verified:
  Membership 51/51, Replication 2508/2508 (the 1 failure was a flake), core
  access-gate key-filter 8/8. Raised OC-2 (gate's own policy reads must be
  system-origin in #980). Re-running the full batched gate for clean single-run
  certification before dispatching #979.

- 2026-07-03 #977 (Core: read-path access-gate key-filter, F-153) MERGED into
  `feat/auth`. Delivered the read-path key-filter wiring in `LatticeGrain`
  (`LatticeGrain.AccessGate.cs`): activation-cached lazy `ILatticeAccessGate` +
  `ILatticeMembershipContext` resolution, a general `AuthorizeAsync` helper
  (system-origin bypass, subject resolve, returns the FULL `LatticeAccessDecision`
  so #980 can reuse it for allow/deny), and the two read wrappers. The returned
  `KeyFilter` is applied server-side at the k-way-merge YIELD point in
  `KeysAsyncCore`/`EntriesAsyncCore` - AFTER the merge frontier (`lastYieldedKey`)
  and dedup set advance, so it is a pure caller-visibility prune that cannot break
  reconciliation ordering/dedup; `GetMany` prunes its input list up front (values
  never read for unauthorized keys); `Count` computes via the key stream under the
  already-resolved filter (no double gate consult, values never cross the wire).
  `ISystemLattice` scan callers pass `enforceAccessGate:false`. REVIEW: only 4
  read-path source files touched (no write/delete/CRDT/atomic/RangeDelete/cursor/
  lifecycle path); zero per-key cost on the null path (null filter short-circuits;
  both awaits complete synchronously on the null gate/context so the async
  ValueTask never suspends/boxes; `LatticeAccessRequest` is a struct); hygiene
  clean. Focused suite 8 new + 30 scan-regression + 49 GetMany + 5 hygiene GREEN.
  RAISED OC-1 (durable-cursor bypass) - see Open concerns; folded into #980 scope.
  CHANGELOG folded into the #980 enforcement entry.

- 2026-07-03 #978 (Auth: authorization rule model & policy store) MERGED into
  `feat/auth`. Delivered in `Orleans.Lattice.Auth`: the durable authorization
  policy model (`LatticeAuthorizationRule` with `LatticeSubjectSelector`,
  `LatticeScope` (tree/key/prefix), `LatticeOperation` mask, `LatticeEffect`,
  optional opaque `Condition`; aliases `olz.ar`/`olz.ss`/`olz.sc`, `olz.` mirror
  test) and `ILatticeAuthorizationPolicyStore` (runtime CRUD + prefix/full scans)
  backed by the reserved, dogfooded `sys-auth-policy` tree with an auto-enabled
  durable per-key history view, mirroring the membership template. Registered via
  `AddLatticeAuth()` (ordering-guarded after `AddLattice`). RECONCILIATION: the
  sub-agent branched before #976 and used a local placeholder `LatticeOperation`;
  as coordinator I merged `feat/auth` (with core `LatticeOperation`) into the
  branch, deleted the placeholder, retargeted the rule model + tests at the core
  enum (`Enumerate`->`RangeRead`, `Administer`->`Admin`), and added an Auth-level
  `LatticeAuthOperations.All` grant-mask convenience so core stays pure
  per-request vocabulary. Also refreshed the stale csproj package Description
  (NUGET/docs READMEs deliberately left on the scaffolding text, matching the
  membership package's deferred pattern -> refreshed in the #986 docs pass).
  REVIEW: surgical, allocation-disciplined (`string.Create` rule key, per-scan
  not per-entry prefix bounds, `Volatile.Read` init fast-path), reserved-namespace
  write guard prevents a rule from being scoped over the policy tree itself,
  history auto-on. Auth focused suite: 84/84 green. Full non-chaos gate BATCHED
  with #977 (isolated new package, zero core-file changes, cannot regress the rest
  of the suite). CHANGELOG entry DEFERRED into the consolidated Auth-enforcement
  entry landing with #980 (store alone stores policy that nothing consumes yet;
  mirrors how membership consolidated #972-#975 into one F-150 entry).

- 2026-07-03 #976 (Core: access-gate seam, F-152) MERGED into `feat/auth` and
  pushed. Delivered the inert, allocation-free `ILatticeAccessGate` seam:
  `NullLatticeAccessGate` (cached `Allow()` `ValueTask`, registered by default),
  `LatticeAccessRequest`/`LatticeAccessDecision` (Allow/Deny/Filtered), the
  `LatticeOperation` `[Flags]` vocabulary, the system-origin marker
  (`LatticeAccessGateContext`, key `ol.sysorig`), and the null-tolerant subject
  resolver. No grain enforcement yet (that is #977 read-path + #980 write-path).
  Surgical/additive (only 2 core files touched); no serialization attributes on the
  in-process vocabulary. Focused suite 40 + hygiene 10 green. Full gate batched
  with #977. CHANGELOG folded into the #980 enforcement entry.

- 2026-07-03 #974 (Membership: subject model, directory & resolution) MERGED, and
  the FIRST FULL NON-CHAOS GATE (Membership boundary) PASSED: core 5653, Replication
  2508, Api.State 248, Api.State.Grpc 123, Storage.AzureTable 233, Membership 51,
  plus the rest; 0 failures (Azurite emulator suite skipped as expected). This gate
  also validated #973 + the core identity primitives. Delivered: core `LatticeSubject`
  (`ol.sub`) + `ILatticeMembershipContext` seam + `NullLatticeMembershipContext`
  (Anonymous default, zero-cost when Membership unregistered); `Orleans.Lattice.Membership`
  resolution pipeline (directory over reserved `sys-membership-*` users/groups/edges
  trees, per-silo resolution cache with change-feed invalidation + token-expiry bound,
  default subject mapper, extensible `JwtCredentialAuthenticator` base + anonymous
  fallback, lazy `MembershipInitializer` enabling durable per-key history). Deleted the
  `LatticeMembershipMarker` placeholder from #972.
  REVIEW (3 sub-agent-flagged concerns adjudicated):
  1. Cross-silo cache invalidation: `MembershipResolutionCache` flushes only on
     locally-observed `sys-membership-*` mutations, so a peer silo serves a stale
     subject until the resolution-cache TTL (default 5m) after a membership change on
     another silo. ACCEPTED for now as consistent with the epic's ratified eventual
     posture and bounded by the TTL + token-expiry; LOGGED for the security review
     (#1103) to formally address cluster-wide invalidation (candidate: ride the #982
     replication of the membership trees / a cluster broadcast) and to reconsider the
     default TTL. Not a #974 blocker.
  2. History-view idempotency on silo restart: `MembershipInitializer` re-invokes
     `ILatticeViewFactory.Create` per silo boot. VERIFIED idempotent - `Create` does
     catalog re-register (dictionary set) + durable runtime-registration upsert +
     idempotent `EnsureActiveAsync`; safe on restart. RESOLVED, no change.
  3. Token-asserted groups not transitively expanded: the mapper unioned token-asserted
     / claim-projected seed groups FLAT while only the subject's directory groups were
     expanded, so a nested policy on an ancestor group would NOT apply to a federated
     (Entra, #1101) identity carrying only a child group in its token - an authorization
     correctness gap that also contradicted the mapper's own documented contract.
     REMEDIATED by the coordinator (scope was small/localized): added
     `ILatticeMembershipDirectory.ExpandGroupsAsync(seeds)` (shared BFS with cycle
     detection) and re-expand the merged group set through the directory closure for
     non-`TokenOnly` merge modes, guarded so the pure-directory path keeps a single
     directory round-trip (no perf regression). Added a regression integration test
     (`Token_asserted_group_is_transitively_expanded_through_the_directory`). Membership
     focused suite 51/51 green.
  CHANGELOG: incremental Added entry under Unreleased (F-150, folding the F-148/F-149/
  F-151 scaffold + credential-seam context). features.md index rows move Planned->Shipped
  at the final epic PR (rule 7).
- 2026-07-03 #975 (Auth scaffolding) MERGED (out of numeric order; independent of
  #974). Empty `Orleans.Lattice.Auth` package referencing core + Membership, slnx +
  docs skeleton, marker + trivial green test. Deferred CHANGELOG/features entry (inert
  package), same as #972. Focused test green post-merge.
- 2026-07-03 #973 (core credential seam) MERGED. `LatticeCredential` readonly
  record struct (alias `ol.cdl`, unique/<=6) + `LatticeCredentialContext` ambient
  marker (Current/IsActive/With/Use/Suppress), RequestContext key `ol.cred` placed
  alongside existing keys, docs snippet in api.md. Edge->silo propagation proven via
  IMutationObserver probe; zero-cost when absent. 19 focused + 283 hygiene green;
  full solution build 0/0 post-merge.
  GATING DECISION: #973 is additive-only (nothing else references it yet). Core
  changes accumulate across #973/#974(core primitives)/#976/#977; rather than a full
  suite after each, the first full non-chaos gate runs at the MEMBERSHIP boundary
  (after #974), which also validates #973 + the core identity primitives. Core-seam
  additions (#976/#977) get their own full gate when they land.
- 2026-07-03 #972 (Membership scaffolding) MERGED. Empty `Orleans.Lattice.Membership`
  package + test project, slnx + docs skeleton, `LatticeMembershipMarker` + trivial
  green test. Version 7.7.1 (family lockstep with core/api.state). Post-merge focused
  test green (1/1). DECISION: no CHANGELOG/features entry yet - the package is inert
  with nothing user-callable; its entry lands with the first real behaviour (#974).
  Placeholder marker to be deleted when real types arrive (per #974).
