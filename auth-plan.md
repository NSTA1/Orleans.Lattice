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
| 9 | #980 | Auth: enforcement wiring at LatticeGrain | done (merged; core boundary; closes OC-1/OC-2; sec-review remediated) |
| 10 | #981 | State API: honour read-access visibility | done (merged; identity bridge + catalog scoping + change-feed gating; F-157) |
| 11 | #1095 | Api.Data: external read-write data-plane API | done (merged; new package pair, all ops gated via public ILattice, coarse DenyAll transport gate, deny->PermissionDenied; api.data 35, grpc 55; F-166) |
| 12 | #982 | Replication: replicate auth/membership trees | done (merged; sys-* enrolment LWW/OR-Set + system-origin apply bypass + opt-in epoch fence + drift guards; F-158) |
| 13 | #983 | Auth: observability & audit | done (merged; orleans.lattice.auth meter + ILatticeAuthAuditSink + opt-in TTL sys-auth-audit trail; decision path byte-for-byte unchanged; zero-cost off; 214 tests; F-159) |
| 14 | #984 | Api.Auth: facade & model | done (merged; ILatticeAuthAdmin combined admin API, every op requires Admin verdict on sys-auth-policy, Explain gate-parity, oli.* aliases; 36 tests; F-160) |
| 15 | #985 | Api.Auth.Grpc: gRPC binding, client, meta-auth | done (merged; LatticeAuthApiGrpcClient + DenyAll meta-authorizer + facade self-auth two-layer, deny->PermissionDenied, oli. wire aliases; grpc 89 + api.auth 36; F-161) |
| 16 | #1101 | Membership.Entra: Entra ID authenticator | done (merged; 53 focused tests) |
| 17 | #1102 | Explorer: connect to auth-enabled State API | done (reviewed + merged; provider seam + Entra login + transparent single-flight token refresh + retry-once + tokens-never-persisted + unauthenticated GetAuthScheme advertisement; explorer 380 / entra 17 / api.state.grpc 151 green; F-164) |
| 18 | #1103 | Security hardening: full security & design review | in_progress (security-review agent enumerating findings; then Feature Dev implements adversarial suite + IIncomingGrainCallFilter + closes OC-3/5/7; F-165) |
| 19 | #986 | Docs, sample & end-to-end tests | pending |

Out of scope: #1104 (admin UI follow-up).

## Open concerns (MUST-CLOSE before final PR)

- **DOC-DEBT (MUST-CLOSE in #986, before final PR).** Package READMEs under docs/ are
  stale from the pre-auth scaffolding era - `docs/lattice.auth/README.md` still says the
  package is "scaffolding only ... empty and inert", which is now false (the full auth
  surface F-150..F-162 has shipped). Several Unreleased CHANGELOG entries link these
  READMEs (lattice.auth, api.state, api.data, api.auth, api.auth.grpc). #986 (docs/
  sample/e2e) MUST rewrite every referenced README to reflect the shipped surface so
  every CHANGELOG doc-link resolves to ACCURATE content before the epic PR. Also add an
  auth observability/audit doc section (meter names, sinks, verbosity, durable trail,
  zero-cost-off) and consider an auth `metrics.md` sibling to the core one. Also decide
  whether to wire the subject-resolution cache hit/miss counters (F-159 shipped the
  instruments + public Record* methods as a seam only; the cache lives below auth and
  needs a core->auth callback to feed them - deferred as a documented seam for v1).


- **OC-2 (security) - CLOSED by #980.** Enforcement resolves the subject and all
  gate/decision-engine internal tree reads under `EnterSystemOrigin()` (see
  `LatticeAccessGateEnforcement.ResolveSubjectAsync` and the policy store's
  system-origin scopes); `PolicyAccessGate` does NO storage I/O on the request
  path (in-memory compiled snapshot only). Real-gate + membership-directory
  integration tests prove no recursion. Backstop remains #1103.

- **OC-1 (security) - CLOSED by #980.** The durable cursor grain now resolves its
  own gate + subject (subject flows via `RequestContext`) and applies the
  key-filter at its page-emit point (`LatticeCursorGrain.AccessGate.cs`); snapshot
  cursors re-apply the filter and delete-range cursors hard-deny per step. Tests:
  prefix-scoped resume sees only allowed prefix, no-rule sees nothing, anonymous
  resume fails closed empty.

- **OC-5 (security, for #1103 review): a materialised view read bypasses the
  data-plane gate.** `LatticeAccessGateContext.IsGateBypassed` is true under
  `ViewReadContext`, which the user-facing `ILatticeView` read handle opens around
  every view read. A view materialised over a restricted source data tree can
  therefore be read ungated by a subject denied the source tree. View-level authz
  never existed and is out of #980 scope, but it is a real confidentiality gap.
  These scopes are `internal` (not client-settable), confirmed by the #980
  security review. NOTE (expanded during #981 review): this surface is ALSO
  reachable through the read-only State API - `LatticeStateQuery` admits `view-*`
  trees (`IsTreeReadHiddenAsync` returns not-hidden for them) and opens
  `ViewReadContext` via `OpenViewReadScopeIfNeeded`, so `GetEntry` / `ScanEntries`
  / structure / metrics on a `view-*` tree read the view ungated exactly as the
  core view handle does. #981 deliberately leaves view trees observable (mirroring
  the core reads) rather than silently diverging; the fix belongs with the core
  view-level decision. ACTION: close in #1103 (a view-level Read decision keyed by
  view id or underlying source tree, applied uniformly at the core view handle so
  both the direct `ILatticeView` path and the State-API view path inherit it) -
  the epic MUST NOT reach the final PR with a materialised view over restricted
  data readable ungated by either surface.

- **OC-6 (correctness) - RESOLVED by the Bug Hunter fix (merged into `feat/auth`,
  ex-`hunt/oc6-scan-concurrency` commit 067d802f).** Root cause CONFIRMED: the
  resilient strongly-consistent scan wrapper (`LatticeExtensions.ScanEntriesAsyncCore`
  / `ScanKeysAsyncCore`) emulates one logical scan as a sequence of physical
  `EntriesAsync` segments; a caller-established `EnterSystemOrigin()` scope is reset
  by Orleans in the iterator's own execution flow after the first segment completes,
  so a segment that reopened after a transient `EnumerationAbortedException` (raised
  when a concurrent scan over the same activation evicts the enumerator) resolved to
  an anonymous subject, the fail-closed gate returned a reject-all key-filter, and
  the segment completed normally with zero rows - the wrapper trusted the clean
  completion and silently truncated the scan at the resume floor. Fix: capture the
  caller's system-origin intent once at method entry and re-assert it around every
  physical segment (zero-cost when not system-origin). Coordinator-verified: core
  resilient/strongly-consistent scan tests 38/38, auth policy-store + new
  `ScanReopenPreservesSystemOriginTests` 46/46. The two admin `ListRules*` tests
  keep their `ScanUntilAsync` convergence helper as harmless defense-in-depth; the
  new dedicated racing regression test is the authoritative guard. No longer a
  backstop for #1103.

- **OC-7 (security posture, for #1102/#1103 review) - RAISED by #984 review.** The
  Api.Auth admin facade authorizes every operation by requiring an `Admin` verdict on
  the reserved `sys-auth-policy` tree via the shared enforcement primitive. Under the
  RECOMMENDED `DefaultEffect=Deny` posture this is correctly bootstrap-admin-only and
  fail-closed. BUT under `DefaultEffect=Allow` (an explicitly open cluster) a non-admin
  would PASS the admin check, because no rule can target the reserved `sys-auth-*`
  namespace to deny it and the open default grants it - so the admin control plane
  inherits the cluster's open posture. Not a new hole (everything is open under Allow),
  and not blocking (Allow is a non-recommended operator opt-out of enforcement), but an
  admin control plane arguably warrants a stricter stance (require an explicit admin
  grant rather than inheriting the open default). The sub-agent deliberately did NOT add
  a bespoke stricter path (per the no-new-gated-path constraint). DECISION for security
  review: decide whether admin mutations should require an explicit allow even under
  DefaultEffect=Allow; if so, add a defense-in-depth check + regression test.

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

- 2026-07-03 REVIEWED + MERGED #1102 (F-164, Explorer auth). Verified myself: (a)
  ExplorerAccessTokenSource single-flight (SemaphoreSlim + generation counter, racing
  caller adopts the winner), proactive refresh via injected TimeProvider margin, revoke
  latch (dead token never looped); (b) LatticeStateConnection.ExecuteAsync mid-session
  auth failure does exactly one authRetried-gated silent RefreshAsync-then-retry, else
  faults RequiresAuthentication=true to re-challenge; (c) IsUnauthenticatedMethod exempts
  ONLY GetAuthScheme (exact ordinal match) - every data/catalog RPC stays enforced; (d)
  ExplorerAuthSession.LoginWithMethodAsync: only Basic persists to the store, token schemes
  ClearAsync + _credential=null (tokens in memory only), challenge runs before any state
  mutation (clean rollback on cancel). Coordinator-verified explorer 380 / entra 17 /
  api.state.grpc 151. CHANGELOG F-164 landed. 4 agent deviations FEED INTO #1103 review:
  (1) UnsafeUseInsecureChannelCallCredentials enabled ONLY on the h2c/allow-unencrypted
  branch (TLS uses the secure path) - audit; (2) Entra scope heuristic maps audience ->
  <audience>/.default unless already suffixed - confirm vs #1101 audience convention;
  (3) GrpcExplorerAuthSchemeProbe happy-path only guard-tested (FakeSchemeProbe indirect) -
  needs a live-server harness; (4) explorer docs use plain ```csharp fences (harness lacks
  explorer/Entra refs). NEXT: #1103 (security review; folds #1095, closes OC-3/OC-5/OC-7),
  then #986 (docs/e2e + F-162 benchmark).

- 2026-07-03 REVIEWED + MERGED #985 (F-161, Api.Auth.Grpc). Verified the two-layer
  authorization: (1) transport meta-authorizer ILatticeAuthApiAuthorizer default DenyAll
  + RequireAuthorization=true, prefix-scoped interceptor -> PermissionDenied; (2) facade
  self-auth (Admin on sys-auth-policy) still runs after the credential bridge stamps the
  caller, so opting the transport gate out never opens policy admin to anonymous/non-admin.
  Deny->PermissionDenied trailers carry no value. 15 new oli.+2 (6-char) wire aliases in
  the parent ApiAuthTypeAliases per the issue's explicit reuse-parent directive (siblings
  actually use own tables; accepted as issue-directed, two-assembly alias scan added to
  keep the parent test green). Coordinator-verified grpc 89/89 + api.auth 36/36. CHANGELOG
  F-161 landed. Api.Auth package pair (#984+#985) + observability (#983) all COMPLETE.
  NEXT: #1102 (explorer auth), #1103 (security review), #986 (docs/e2e + F-162 benchmark).

- 2026-07-03 REVIEWED + MERGED #983 (F-159, auth observability & audit). Verified the
  SAFETY-CRITICAL property that the decision path is byte-for-byte unchanged: the
  PolicyEvaluator's original no-match Evaluate now delegates to the new out-match
  overload with `out _`, so audit-on and audit-off run identical evaluation; the gate's
  EvaluateAndObserve calls the original Evaluate(in request) on the audit-off path;
  order (bootstrap-admin -> epoch-fence -> warm/cold eval) preserved; Observe() emitted
  strictly after the decision at every return; audit dispatch fire-and-forget with sink
  exceptions swallowed. Verified zero-cost-off fast-exit (LatticeAuthDecisionObserver
  lines 88-92: three bool reads, no alloc/timestamp/taglist). Event carries no stored
  value. Coordinator-verified 214/214. Accepted the unwired subject-cache counters as a
  documented v1 seam (cache is below auth) -> logged under DOC-DEBT for #986. CHANGELOG
  F-159 landed. RAISED DOC-DEBT concern: stale package READMEs must be rewritten in #986
  before the final PR so CHANGELOG doc-links are accurate. #985 (Api.Auth.Grpc) still
  running in parallel.

- 2026-07-03 REVIEWED + MERGED #984 (F-160, Api.Auth control facade). Verified the
  critical admin-authorization seam: all 19 facade methods (read AND write) call
  AuthorizeAdminAsync first (caller identity, before EnterSystemOrigin), delegating to
  the shared LatticeAccessGateEnforcement.EnforceWholeTreeAsync requiring an Admin
  verdict on sys-auth-policy - no new gated path. ExplainAsync reuses the real
  ILatticeAccessGate decision (parity test). oli.* aliases <=6 chars with mirror test.
  Coordinator touches minimal (slnx +2, core csproj +2 IVT mirroring Api.State src-IVT,
  auth csproj +1 IVT). Coordinator-verified 36/36. RAISED OC-7 (admin facade inherits
  open posture under DefaultEffect=Allow -> #1102/#1103 decision; not blocking).
  CHANGELOG F-160 landed. DISPATCHED #985 (F-161, Api.Auth.Grpc binding) - depends on
  the now-merged #984 facade. #983 (auth observability) still running in parallel.

- 2026-07-03 REVIEWED + MERGED #982 (F-158) and #1095 (F-166). #982: verified the
  system-origin apply bypass in both ReplicationApplier apply paths, the zero-cost
  opt-in PolicyEpoch fence, hardened the reserved-name mirror with drift guards
  (auth + membership test projects). #1095: verified every op routes through the
  gated public ILattice surface (facade + gRPC), fail-closed anonymous, deny->
  PermissionDenied (no value leak), coarse DenyAll transport gate, minimal
  coordinator-owned touches (IVT + docs-harness refs mirroring State API). Focused
  suites green: replication 2521, auth 167, membership 52; api.data 35, api.data.grpc
  55. CHANGELOG F-158 + F-166 landed. DISPATCHED #983 (F-159, auth observability &
  audit) next in dependency order.

- 2026-07-03 DISPATCHED #1095 (F-166, Api.Data external read-write data-plane) and
  #982 (F-158, replicate auth/membership sys-* trees) as PARALLEL Feature Dev
  sub-agents in worktrees 1095 and 982 (disjoint packages: api.data vs replication;
  both depend only on merged auth backbone, not on each other). #1095 brief: new
  package pair mirroring the read-only State API, v1 scope = point set/remove/get +
  single-tree atomic batch + cross-tree atomic + bounded single-page range read
  (streaming deferred); ALL ops route through the gated `ILattice` surface so #980
  enforcement fires automatically (no new authz path); reuse the #981 identity-bridge
  pattern; opt-in/absent by default; map deny -> PermissionDenied. #982 brief: gated
  enrolment of sys-membership-*/sys-auth-policy (+optional sys-auth-audit) into
  replication (LWW policy/membership, OR/append audit); system-origin apply bypass on
  the receiver (security-critical - replicated writes must not be user-authorized);
  opt-in per-tree strict PolicyEpoch fence (off by default = eventual, zero-cost);
  guardrail validation if replication absent. Both: focused tests only, commit in
  worktree (hooks off, no attribution), no merge/push - coordinator reviews & merges.

- 2026-07-03 #981 (F-157, State API read visibility) MERGED into `feat/auth`, and
  the OC-6 core scan fix MERGED alongside it. REVIEW of the sub-agent's work: built
  clean; api.state 277/277, grpc 140/140. Fixed a tracker-id hygiene violation
  (`F-117` in a grpc fixture comment). REMEDIATED a genuine in-scope read-around the
  sub-agent missed: the live change feed (`LatticeStateObserver.ObserveAsync` / gRPC
  `ObserveChanges`) tails the WAL DIRECTLY, not through the gated `ILattice` surface,
  so it hid only system trees - any caller could subscribe to any non-system data
  tree's change feed (keys/kinds/HLC/values). Added a `LatticeStateVisibilityFilter`
  change-feed access resolver + observer gating: anonymous/unauthorized subscriber
  refused (not-found), whole-tree grant streams all, partial (prefix) grant applies
  the gate's per-key filter and NEVER emits a range delete (cannot be narrowed);
  zero-cost when disabled. Added 5 change-feed visibility tests (18 auth-visibility
  total green). OC-5 expanded to name the State-API view-read path (also reachable,
  same deferral to #1103). OC-6: the Bug Hunter CONFIRMED a real core defect (the
  resilient scan wrapper lost the caller's system-origin scope on segment reopen ->
  anonymous subject -> fail-closed gate silently truncated the scan) and fixed it by
  re-asserting system-origin per segment; coordinator-verified (core scan 38/38, auth
  46/46) and MERGED - OC-6 now RESOLVED (convergence helper kept as harmless
  defense-in-depth). CHANGELOG: F-157 Added entry landed.

- 2026-07-03 DISPATCHED #981 (F-157, State API read visibility) Feature Dev
  sub-agent in worktree 981. Key design steer: State-API reads go through the
  PUBLIC `ILattice` surface which #980 already enforces, so the crux is the
  IDENTITY BRIDGE (gRPC ServerCallContext/credential -> LatticeCredentialContext
  so existing enforcement fires) + explicit catalog/structure scoping
  (ListTrees/ListViews/GetTreeStructure omit unreadable trees) + fail-closed on
  unresolved identity + zero-cost when Auth absent. Non-recursion: infra reads
  (registry, policy/membership trees) under system-origin.
- 2026-07-03 DISPATCHED OC-6 Bug Hunter in worktree oc6 (branch
  hunt/oc6-scan-concurrency). Root-cause whether a strongly-consistent core scan
  transiently omits a durably-written key under a concurrent same-activation scan
  (amplified by #980's active maintainer rescan of sys-auth-policy). Resilient
  resume already ruled out; suspect per-activation shared enumeration state /
  ambient snapshot scope trample. Fix in core if genuine; do not weaken tests.

- 2026-07-03 #980 (F-156, enforcement wiring at LatticeGrain) MERGED into
  `feat/auth`. The security-critical core boundary: `ILatticeAccessGate` now
  enforces every user-originated mutation/read at `LatticeGrain`, the durable
  cursor grain, and the cross-tree atomic-write coordinator. New public
  `LatticeAuthorizationDeniedException` (alias `ol.azd`, carries tree/op/subject/
  reason only). `PolicyAccessGate` (Auth) does the real allow/deny over the
  in-memory compiled snapshot with NO request-path storage I/O; registered via
  `services.Replace`. Bootstrap administrators are the root-of-trust bypass.
  Fail-closed throughout: writes/deletes/CRDT/bulk-load/admin throw on deny;
  point reads report absent; range/multi reads prune; range-delete hard-denied
  all-or-nothing; atomic + cross-tree batches authorize all legs before any
  apply. Zero-cost default preserved (null-gate/system-origin short-circuit before
  subject resolution). Closes OC-1 (durable cursor) and OC-2 (no recursion).
  REVIEW: I committed the sub-agent's work (it left it uncommitted), built the
  full solution clean, ran a `security-review` sub-agent, and ran the full
  non-chaos gate in the worktree - all packages green (core 5715, Auth 147).
  REMEDIATED two MEDIUM fail-open reads the sub-agent missed, flagged by the
  security review: `CountPerShardAsync` (per-shard count leak) and the two
  leaf-projection digest reads (content oracle) were ungated - added
  `EnforceUniformRangeReadAsync` (hard-deny: a denied OR partially-authorized
  caller is refused, since these structural reads cannot be narrowed per key) and
  6 regression tests. Also converged the two admin `ListRules*` tests (OC-6).
  RAISED OC-5 (view-read ungated, -> #1103) and OC-6 (core scan under concurrent
  same-activation scan, -> Bug Hunter + #1103). Security review's RequestContext
  capability-key note (`ol.sysorig`/`ol.vw`/`ol.vr` not stripped from client
  calls; deferred deliverable-8 `IIncomingGrainCallFilter`) recorded for #1103.
  CHANGELOG: consolidated F-152..F-156 Added entry landed. Explorer/Membership/
  core focused suites all green post-merge.

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
