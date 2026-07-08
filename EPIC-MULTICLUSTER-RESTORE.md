# Epic coordination: coordinated multi-cluster restore via a cross-cluster saga

Tracking issue: **#1170** (epic). This file coordinates the implementation and is the
single source of truth for progress. It is transient: it lives on the integration branch
`feat/replication-backup-restore-coordination` and is **removed before the final epic PR to
main**.

> Note: this file deliberately refers to sub-issues by GitHub number and descriptive name
> only (never by roadmap tracker ids) and stays plain ASCII, so it does not trip the
> repo-root hygiene gates (tracker-id, em-dash, mojibake).

## Roles

- **Coordinator (resident):** owns this file, the integration branch, `CHANGELOG.md`,
  `docs/**`, and `features.md`. Reviews every sub-issue for correctness, test gaps, and
  unnecessary memory allocations before it lands on the integration branch. Runs the
  non-chaos suite at the integration points below. Stays resident for the whole epic; opens
  the single epic PR to main at the end.
- **Sub-agents (feature-dev):** implement one sub-issue each, strictly to its GitHub issue
  body, plus targeted tests.

## Hard rules for sub-agents

1. Work in a **dedicated git worktree** on a **child branch off the integration branch tip**
   (the coordinator creates the worktree and branch and hands over the absolute path). Never
   touch the coordinator's checkout.
2. Implement only the assigned issue. **Targeted tests only** (Tier 1/2): they may build and
   run their own narrow tests, but **must not** run the non-chaos suite, the full suite, or
   chaos tests.
3. **Must not** modify `CHANGELOG.md`, `features.md`, or anything under `docs/**`. **Must
   not** create planning/notes markdown.
4. **Must not** open PRs, merge, push to main, or edit this file.
5. Commit on the child branch with ASCII-only messages, no author-attribution trailers.
6. Honour repo conventions: serialization discipline (`[GenerateSerializer]`, stable
   `[Alias]`, sequential `[Id]`, `[Immutable]` where apt), `ArgumentNullException.ThrowIfNull`
   on public params, XML docs on public surface, file-scoped namespaces, one top-level type
   per file, no em-dash / non-ASCII.

## Coordinator review checklist (per sub-issue, before merge to integration branch)

- Correctness against the issue's design and acceptance criteria.
- Test coverage: every new public type/member has a test; the issue's acceptance scenarios
  are exercised; targeted tests actually run green.
- Unnecessary memory allocations on hot paths (avoidable LINQ/closures/boxing, needless
  copies, `ValueTask` fast-path opportunities per repo guidance).
- Serialization/versioning discipline and naming conventions.
- No forbidden edits (docs/changelog/features.md untouched by the sub-agent).

## Worktree workflow (coordinator)

```
# create child branch + worktree off the integration branch tip
git worktree add -b feat/<child> ../lattice-wt/<child> feat/replication-backup-restore-coordination
# ... launch feature-dev with cwd = absolute worktree path ...
# review child branch, then integrate:
git switch feat/replication-backup-restore-coordination
git merge --no-ff feat/<child>
git worktree remove ../lattice-wt/<child>
git branch -d feat/<child>
```

## Sub-issue plan (dependency order)

| Order | Issue | Title (short) | Depends on | Status |
|---|---|---|---|---|
| 1 | #1171 | Cross-cluster saga control channel (gRPC) | - | DONE (merged, 38 tests green) |
| 2 | #1172 | Durable saga coordinator + internal participant model | #1171 | DONE (merged, 34 tests green) |
| 3 | #1173 | Per-tree write fence + shipping pause | #1172 | DONE (merged, 779 tests green) |
| 4 | #1174 | Shared external sink, capturing-cluster stamp, chain affinity | - | DONE (merged, 39 tests green) |
| 5 | #1175 | Coordinated restore as first internal participant | #1172, #1173, #1174 | pending |
| 6 | #1176 | Public user-defined saga participant SPI | #1175 | pending |
| 7 | #1177 | Observability, docs, sample wiring, chaos coverage | all | pending |

## Non-chaos integration points (coordinator runs these)

- After #1171 lands: build + targeted replication / replication.grpc suites.
- After #1173 lands: non-chaos on the replication + core slices (coordinator + fence + control channel together).
- After #1175 lands: **full non-chaos suite** (this is the correctness milestone that fixes #1169).
- After #1177 lands / before the epic PR: **mandatory full non-chaos run with blame-hang**
  (`dotnet test --filter "TestCategory!=Chaos" --blame-hang --blame-hang-timeout 3m`), plus
  the docs-snippet gate since docs land here.

## Coordinator-owned artifacts (added as sub-issues land, by the coordinator only)

- `CHANGELOG.md` entries.
- `features.md` moves (Planned -> Shipped) once each capability is on main (done at/after the epic PR).
- `docs/**` and sample wiring (with #1177).

## Coordinator-owned integration follow-ups (deferred wiring, tracked here)

- Replication-side `IReplicatedTreeMembership`: #1174 added the backup-local seam with a default no-op (so single-cluster is a no-op and the guard cannot fire spuriously). The replication package must register a real implementation (projecting `LatticeReplicationOptions.ReplicatedTrees`) so the fail-fast guard actually fires in a replicated deployment. Wire this during #1175/#1177 integration, keeping backup's no-dependency-on-replication layering intact (replication references core+backup direction is acceptable; backup must not reference replication).
- Origin-trust hardening on the saga control channel (from #1171): require the authenticated origin header (or require it to match `CoordinatorClusterId`) rather than falling back to the caller-supplied body field. Fold into #1172 review / #1175.

## Progress log

- Setup: integration branch `feat/replication-backup-restore-coordination` created off main; coordination file added.
- #1171 (saga control channel): implemented in worktree, reviewed, merged (`8c4be88f`). Adds the `orleans.lattice.replication.LatticeSaga` sibling gRPC service (Prepare/Commit/Abort/GetStatus), request/response DTOs (aliases `olr.sq`/`olr.sv`), client channel + server handler/authorizer seams, and a peer-authorization gate. Integration checkpoint: 38 targeted + hygiene tests green on the epic branch. Follow-up for #1172: tighten origin trust (require the authenticated origin header, or require it to match `CoordinatorClusterId`) rather than falling back to the caller-supplied body field.
- #1174 (backup shared-sink guard, capturing-cluster stamp, chain affinity): implemented in parallel worktree, reviewed, merged. Adds `BackupManifest.CapturingClusterId` (additive, null-default, wire-compatible), full-path cluster-id stamping + incremental base-stamp inheritance, chain-affinity enforcement via the full-fallback path, and a dependency-free shared-external-sink startup guard behind a backup-local `IReplicatedTreeMembership` seam (default no-op). Checkpoint: 39 stamp/guard/affinity + hygiene tests green.
- #1172 (durable saga coordinator + internal participant model): implemented in worktree, reviewed, merged (`24d3f156`). Adds the reminder-driven `CrossClusterSagaCoordinatorGrain` (resumable Preparing/Committed/Aborted/Completed phase machine, memoized outcome, SHA-256 participant-set fingerprint for re-submit stability, 1h prepare-progress deadline) and `CrossClusterSagaParticipantGrain` (durable prepared record, reminder-anchored 5-min cutover fence with auto-compensation on coordinator loss, idempotent commit/abort), plus the real `LatticeSagaControlHandler` that wins over the gRPC `NoParticipant` default via `TryAddSingleton` ordering. 8 new `olr.z*` aliases. Integration checkpoint: 34 saga + hygiene tests green on the epic branch. Origin-trust hardening (from #1171) is a transport-authorizer concern; deferred to #1175/#1177 gRPC wiring.
- #1173 (per-tree write fence + shipping pause): implemented in worktree, reviewed (one correctness fix required, see below), merged (`85343aa5` + fix `b60fd8d7`). Adds the durable self-lifting write-fence gate on the shard root (`ShardRootGrain.WriteFence.cs`; single null-check hot path; retryable `LatticeWriteFencedException`), the group-atomic `SagaWriteFenceGrain` orchestrator enforcing the two-release-point rule (local write unblock per-cluster on flip vs. shipping/receive resume gated on observed global completion via `ISagaCompletionSource`/`CoordinatorSagaCompletionSource`), the durable shipper admin-pause (distinct from transient `PauseForMs`), and the inbound `TreeReceiveFenceGrain`/`ReplicationReceiveGate`. New aliases `ol.wfx` (core) + `olr.f*` family. REVIEW FIX: the receive-fence defer originally returned `ApplyResult{Applied=false,HWM=Zero}` which every receive path turned into an `Accepted=true` ack, so the shipper advanced its cursor past the deferred entries and never re-shipped them (silent data loss during the pause window). Fixed by an explicit `ApplyResult.Deferred [Id(2)]` flag mapped to a not-accepted, cursor-preserving retryable ack on every receive path (gRPC `Push` + in-process transports), with a real ship->ack->cursor round-trip regression test proving the cursor stays put while fenced and the same entries re-ship and apply after lift. Integration checkpoint: 448 replication + 331 core (incl. write-fence integration fixture) + hygiene tests green on the epic branch.
