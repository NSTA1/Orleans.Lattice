# Backup & Restore epic - implementation coordination plan

Living tracking file for the implementation of the Orleans.Lattice backup & restore epic
(GitHub epic #1119). Owned and maintained by the coordinator session. Pushed to origin as
progress is made. This file is scaffolding for the implementation effort and is expected to be
removed (or superseded by the shipped docs) before the epic's final merge.

## Roles and rules

- **Coordinator (this session)** owns the `feat/backup` base branch exclusively. Nobody else
  commits to it. Reviews each sub-agent's work against spec, then lands it as one squashed
  commit per feature.
- **Sub-agents** are `feature-dev` agents, each in its **own worktree**, implementing exactly
  one sub-issue. Hard constraints imposed on every sub-agent:
  - MUST NOT open a pull request, push to a shared branch, or merge anything.
  - MUST commit its work to its own worktree/session branch only.
  - MUST run only targeted tests (the fixtures relevant to its change), never the full suite.
  - MUST NOT modify `CHANGELOG.md`, `docs/lattice/features.md`, or any other `features.md`,
    and MUST NOT add or edit documentation (`docs/**`, README rows, NUGET_README). All docs
    are the coordinator's responsibility at epic close (issue #1130).
  - MUST follow repo conventions (naming, serialization aliases, hygiene gates, ASCII-only,
    no em-dash, no tracker-id leakage outside CHANGELOG/features.md link-text).
- **Coordinator stays resident** for the whole epic; it does not end its turn merely because it
  is waiting on a sub-agent. It monitors background/worktree sessions and reviews on completion.

## Integration mechanism

1. Each sub-agent branches its worktree off the **current tip of `feat/backup`**.
2. On completion the sub-agent commits to its session branch and notifies the coordinator.
3. Coordinator reviews: spec conformance, minimal-invasiveness, efficiency, test coverage.
4. When satisfied: `git merge --squash <session-branch>` into `feat/backup` -> one commit per
   sub-issue, authored by the coordinator (no author-attribution trailer).
5. `feat/backup` is pushed to origin after each landed feature.

## Full-suite cadence

Sub-agents run targeted tests only. The coordinator runs the full non-chaos suite
(`dotnet test --filter "TestCategory!=Chaos" --blame-hang --blame-hang-timeout 3m`) at the end
of the waves that add cross-cutting seams (end of W2, W3, and W5/W6), remediating any failures
before proceeding.

## Endgame

When the whole epic is green and the coordinator has written all docs / CHANGELOG /
features.md (per issue #1130), raise the final `feat/backup` -> `main` PR. Do NOT set
automerge. Apply release-category + per-package labels.

## Scope

In scope: #1120-#1130 and #1133. Out of scope (deferred follow-on): #1131 (management UI).

## Dependency waves

- **W0:** #1120 scaffolding (base for all).
- **W1 (parallel):** #1121 permission model; #1122 sink abstraction + manifest + catalog hiding.
- **W2:** #1123 full capture (needs #1121 + #1122); #1127 AzureBlob sink (needs #1122) in parallel.
- **W3 (parallel, all need #1123):** #1133 cross-tree fence; #1124 incremental; #1125 scheduling
  + chain retention; #1126 restore (also needs sink + permissions).
- **W4:** #1128 control facade (needs capture/incremental/schedule/restore).
- **W5:** #1129 gRPC binding + client (needs facade).
- **W6:** #1130 observability + samples + e2e tests (closes epic). Sub-agent does code/tests/
  metrics; coordinator writes all docs, CHANGELOG, features.md.

## Design-review decisions carried into implementation

- Incremental rides the core all-origin WAL commit stream (not the local-origin change feed);
  pins the WAL via the cursor registry; falls back to a full backup on fall-off-log (#1124).
- Prefix/key capture opens a scoped snapshot (range bounds + optional predicate push-down),
  not a whole-tree baseline then filter (#1123).
- Capture fails fast on the aggregated shard-root size digest; records structure/topology and
  the compression dictionary; mode-faithful per key (LWW vs CRDT, sourced from the durable log
  record, covering mixed local-only trees) (#1123 / #1122).
- Manifest is self-describing (digest, topology, per-key shape/mode, per-origin provenance,
  dictionary ref); content-descriptor granularity follows the backup definition (#1122).
- Restore: in-place merge mode + atomic shadow-cutover-to-a-fresh-tree mode (revertible);
  validates the artifact before applying; mode-faithful apply; per-origin re-sync via the
  existing anti-entropy digest / re-replay path (#1126).
- Cross-tree consistency via an opt-in shared HLC causal fence reusing the cross-tree
  transaction registry + WAL blocked-floor / causal-stable frontier (#1133).
- Backup read is high-privilege (bypasses the per-key read key-filter); Restore subsumes
  target-scope write; both fail-closed (#1121).
- No artifact encryption (deployer responsibility); restore is the trust/validity boundary.

## Progress log

| Wave | Issue | Feature | Status | Landed commit |
|------|-------|---------|--------|---------------|
| W0 | #1120 | Project & package scaffolding | LANDED | 1323b29f |
| W1 | #1121 | Permission model | LANDED | 356072c4 |
| W1 | #1122 | Sink + manifest + catalog hiding | LANDED | 3c010829 |
| W2 | #1123 | Full capture | LANDED | 72c49323 |
| W2 | #1127 | AzureBlob sink | LANDED | de70a56b |
| W3 | #1133 | Cross-tree fence | DISPATCHED | - |
| W3 | #1124 | Incremental | DISPATCHED | - |
| W3 | #1125 | Scheduling + retention | DISPATCHED | - |
| W3 | #1126 | Restore | DISPATCHED | - |
| W4 | #1128 | Control facade | not started | - |
| W5 | #1129 | gRPC binding + client | not started | - |
| W6 | #1130 | Observability + samples + e2e + docs | not started | - |

Last updated: 2026-07-05 (W2 LANDED: #1123 full capture engine [public ILatticeBackupCaptureService.CaptureAsync; causal cut via public snapshot entry cursor + additive internal raw-entry seam emitting full LWW envelope; whole-tree/prefix/key scope-to-range mapping; content-addressed streaming into the sink; fail-fast size gate; manifest consistency cut + topology + per-key descriptors] committed 72c49323, and #1127 AzureBlob sink de70a56b. Coordinator refinement folded into #1123: the per-key merge-mode LABEL is now resolved from the declared per-tree merge mode via the producer-side merge-mode resolver (CRDT-declared tree -> Crdt; LWW/non-replicated -> LastWriterWins) instead of hard-coding LWW; 3 added mode-labelling tests. Ratified deviation: a local-only tree that mixes LWW and CRDT shapes cannot recover per-key mode from durable snapshot state alone (needs a durable-state schema change) - filed as a follow-up. Full non-chaos suite GREEN at W2 boundary: core lattice 5770, replication 2521, api.state 293, backup 67, explorer 384, all Failed 0. Next: W3 dispatch (#1133, #1124, #1125, #1126 - all need #1123).).

## Review notes / ratified deviations

- **#1120 (landed 1323b29f):** Sub-agent scaffold reviewed against spec and the auth trio.
  - Ratified: NO core change for the reserved system-tree prefix. Verified against source that
    core `LatticeConstants.SystemDataTreePrefix = "sys-"` trees are hidden from the default
    State API catalog (LatticeConstants.cs:84-96), so `sys-backup-` inherits hiding for free,
    exactly as `sys-auth-` / `sys-membership-` do. More minimal than the brief's "likely core"
    hint; matches precedent.
  - Coordinator fix-up at integration: removed the redundant `obg.` gRPC alias registry and
    made `ApiBackupTypeAliases` public so the gRPC binding reuses the parent `oib.` registry
    (matches the auth gRPC precedent; resolves the sub-agent's own doc contradiction).
  - Verified: csproj is a faithful auth mirror (0.1.0, InternalsVisibleTo scoped to the backup
    trio + DynamicProxyGenAssembly2, core reference only); slnx placement consistent with auth
    ordering; targeted build/test/pack all green (7/1/1 tests). backup-plan.md base-drift from
    the sub-agent branch was excluded at integration (coordinator owns this file).

- **#1121 (landed 356072c4; grpc repair 35c53427):** Backup/restore permission model reviewed
  against source (LatticeOperation.cs, LatticeAccessGateEnforcement.cs, PolicyAccessGate.cs).
  - Ratified deviation 1 (prefix-as-point): prefix/key scope routes through the point-check branch
    (EnforcePointAsync with key=prefix), whole-tree through EnforceWholeTreeAsync. Verified
    fail-closed: a narrower grant cannot escalate to a broader backup; whole-tree refuses a
    filtered/partial allow; deny-overrides + prefix-specificity honoured at the scope root. The
    intentional coarseness (a deeper sub-scope deny does not narrow a broader Backup grant) matches
    the resolved design decision that Backup is high-privilege and bypasses per-key read filtering.
    NOTE for #1123: within-prefix denies are intentionally NOT narrowed by the capture authorizer.
  - Ratified deviation 2 (Restore in FenceWriteMask, Backup excluded): correct - Restore bulk-loads
    like BulkLoad; Backup is a read.
  - Ratified deviations 3-4: no AddLatticeBackup DI entrypoint yet (deferred to #1123 consumer);
    BackupScope/Kind/Authorizer kept internal, only LatticeOperation.Backup/Restore new public.
  - Deviation 5 (full-solution build break) was a GENUINE catch: my #1120 grpc alias fix-up was
    applied to the working tree but never committed, so committed feat/backup referenced the
    removed ApiBackupGrpcTypeAliases. Repaired as a distinct commit (35c53427) before landing #1121.
  - Targeted tests green: core LatticeOperation 5/5, auth 236/236 (behaviour unchanged), backup
    authorizer 21/21, grpc scaffolding 1/1. Hygiene scan (em-dash/non-ASCII/tracker-id) clean.
    backup-plan.md base-drift excluded at integration.

- **#1122 (landed 3c010829):** Sink abstraction + manifest/catalog model + catalog hiding reviewed
  against source and the auth/membership dogfood pattern.
  - Verified: ILatticeBackupSink is async-streaming (chunked artifact write/read, never buffered
    whole) + manifest CRUD, content-addressed via BackupContentHash (idempotent). Catalog store
    dogfoods the reserved sys-backup-catalog tree with a durable per-key history view; both
    sys-backup- trees inherit the core sys- catalog-hiding filter (no core change), proven by a new
    api.state catalog test (hidden by default, shown with IncludeSystemTrees). AddLatticeBackup has a
    fail-fast ordering guard after AddLattice, is idempotent, and folds in the view infrastructure.
    All implementation types internal; only wire-contract records, enums, the two seams, options and
    registration are public. Manifest carries topology snapshot + shard-root digests, per-key
    shape/merge-mode, per-origin provenance, compression-dictionary ref; no artifact encryption.
  - Ratified deviations: (1) a second reserved tree sys-backup-store for the in-cluster sink bytes
    alongside sys-backup-catalog; (2) BackupContentHash public static SHA-256 helper; (3) the hiding
    proof added to the api.state test project (test-only, no prod/csproj change); (4) AddLatticeBackup
    folds in AddLatticeViews (idempotent) and defaults history to MetadataOnly + durable view on.
  - COORDINATOR RECONCILIATION with #1121: both features independently introduced a scope descriptor
    and a BackupScopeKind enum (hard duplicate-type collision). Unified on the public serializable
    BackupScopeSelector + public BackupScopeKind (WholeTree/Prefix/Key); deleted #1121's redundant
    internal BackupScope struct and repointed the authorization seam + its tests. The package now
    carries a single scope type so #1123 consumes one model. All 55 backup tests + 64 api.state
    catalog tests green; full-solution Release build green (0/0) at the W1 boundary.

## Coordinator-owned carve-outs (NOT delegated to sub-agents)

- GitHub package labels (`lattice.backup`, `lattice.api.backup`, `lattice.api.backup.grpc`):
  created up front by the coordinator.
- All `docs/**` prose, per-package README/api/configuration/architecture, package `features.md`,
  the root README documentation-table rows, `CHANGELOG.md`, and `docs/lattice/features.md`:
  written by the coordinator at epic close (issue #1130), consistent with the auth epic
  (per-package docs + top-level `backup.md` feature entry point linked from root README +
  a sample added to the `samples.md` gallery, with one sample chosen as the README entry point).
- Sub-agent #1120 therefore delivers code/projects/tests/slnx/alias-reservations only; the
  doc skeletons and README table row from that issue are produced by the coordinator.
