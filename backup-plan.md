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
| W1 | #1121 | Permission model | not started | - |
| W1 | #1122 | Sink + manifest + catalog hiding | not started | - |
| W2 | #1123 | Full capture | not started | - |
| W2 | #1127 | AzureBlob sink | not started | - |
| W3 | #1133 | Cross-tree fence | not started | - |
| W3 | #1124 | Incremental | not started | - |
| W3 | #1125 | Scheduling + retention | not started | - |
| W3 | #1126 | Restore | not started | - |
| W4 | #1128 | Control facade | not started | - |
| W5 | #1129 | gRPC binding + client | not started | - |
| W6 | #1130 | Observability + samples + e2e + docs | not started | - |

Last updated: 2026-07-05 (W0 #1120 LANDED as 1323b29f; W1 unblocked).

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
