# TLA+ specifications of backup and restore against in-flight sagas

This directory holds seven TLA+ modules that check the backup and restore
protocols where they meet concurrent atomic sagas, replication, captures and
failures. Four are the deliverable of issue #4440, part of the formal-coverage
epic #4430; `BackupIncremental` was added by its fix for #4589,
`BackupRetention` by issue #5350 of epic #5348, and `BackupIdClaim` by that
epic's fix for #5627.
Each module has its own TLC configuration, manifest, mutation directory and
refinement note. The layout every module follows, how to run TLC, and why TLC
runs in CI are described in [`spec/README.md`](../README.md); this README covers
only what is particular to the backup area.

## The seven modules, and why seven

The area splits along the seam where the state each part needs stops
overlapping, which also keeps every module's TLC run small:

| Module | What it checks | Instance |
|--------|----------------|----------|
| [`BackupCapture`](BackupCapture.tla) | A capture racing in-flight sagas: the per-tree capture under the #4485 decision gate, and a cross-tree set's fence, drain gate, gated re-check and validation, with a lease lapse or a fault at any step. | Two trees, three shards, a single-tree and a cross-tree saga, a standalone or a set capture, two attempts. |
| [`BackupIncremental`](BackupIncremental.tla) | An incremental backup racing a saga: the forward WAL drain from the base's frontier, the saga's prepares and terminals in the window resolved against the decision gate, the frontier held back for an unsettled saga, the undecided sagas a link hands on, and the fall back to a full backup (#4589). | One saga over two keys on two WAL partitions, its prepares and terminals on different partitions, a full capture and up to two increments. |
| [`BackupProvenance`](BackupProvenance.tla) | What a chain records: per-origin provenance, the #2621 empty-origin rule, and the chain's HLC frontier (#3758). | A local and a replicated origin, three writes, a chain of three links. |
| [`BackupRestore`](BackupRestore.tla) | A coordinated restore across two regions, its per-record admission, and the replication that resumes after it, with delivery split into a cached-gate admission and a later landing, a causal-apply buffer, and the restored copy's receive fence (#4593). | Two clusters, two copies each, a backup with one admitted and one foreign record, two application writes, one saga (receive-fence epochs 0 and 1). |
| [`BackupCutover`](BackupCutover.tla) | A local shadow-cutover restore and its revert: the alias and shard map that move together, the redirects that heal stale routing, the alias reservation, and a crash with a retry. | One tree, two copies, one stale routing cache, one crash. |
| [`BackupRetention`](BackupRetention.tla) | Retention pruning racing full and incremental captures of the same scope: the keep set and its base closure, the pass's row-first delete and dependent scan, the increment's base re-check, tip-first deletion, one pass at a time with its listing read under the guard, a content-addressed id republished under a per-id hold with a refreshed ranking, a set member listed only once it is stamped, and a crash at any step (#5357, #5412, #5418, #5449). | Three backup ids, two capture slots, one retention pass (two in the `TwoPasses` variant), a clock of two ticks, one crash, keep-last of at most one and max-age of one tick. |
| [`BackupIdClaim`](BackupIdClaim.tla) | Which scope owns a full backup's bare content-addressed id: the claim grain's persisted per-capture claims, each scope's per-id hold, a retention pass of the id by its owning scope, overlapping operator deletes of it, each with a lease of its own renewed while it deletes artifacts and a fenced, scope-checked manifest delete, and a catalog rebuild that re-lists only a whole backup, so no publish replaces another scope's backup under it, no delete removes a backup a publish listed or another scope owns, and no rebuild lists a backup a delete half removed (#5620, #5627, #5631, #5632, #5637). | Two scopes, three captures of identical content (two in one scope), one retention pass, two operator deletes and one catalog rebuild in the owning scope. |

An increment needs the WAL's partitions and offsets, which no other capture
question does, so it is a module of its own rather than a third capture kind in
`BackupCapture`, whose state space is already the largest here. Capture and
restore share no state the other needs: a restore installs an image
as one value, and whether that image is whole is the capture module's question.
Coordinated restore and the local cutover split for the same reason: the first
is about clusters and replication, the second about one cluster's routing.
Retention needs none of the saga, WAL or routing state, only manifests, their
base links, the sink and the catalog, so it is a module of its own too. Who owns a
bare content-addressed id is a question across scopes, while retention is one
scope's; modelled together, a second scope would multiply retention's already
large state space, so `BackupIdClaim` checks it alone, over one id.

### Retention pruning

`BackupRetention` reuses the chain abstraction of `BackupIncremental` at a
coarser grain: a backup is a manifest with an artifact and, for an increment, a
base, and what an increment holds is not modelled. A backup is restorable while
its manifest resolves (catalog row first, then the sink manifest, as the restore
engine resolves it), its artifact is in the sink, and its base is restorable.
Every capture path that can run beside a pass for the same scope is one capture
slot: the scheduler's own, the backup API's (which may name any base), and
another silo's; a full capture may also be a member of a multi-tree backup set,
whose catalog row waits for the set's membership stamp. The model found #5357,
and its reviews found #5412, #5418, #5449, #5529, #5531 and #5532 (and #5530,
fixed in the anti-entropy digest); it checks every fix it models. See
[`RefinementRetention.md`](RefinementRetention.md).

## Properties checked

| Module | Property | Kind | Meaning |
|--------|----------|------|---------|
| `BackupCapture` | `BackupSagaConsistent` | Invariant | An accepted capture never holds part of a saga within one tree. |
| | `SetSagaConsistent` | Invariant | An accepted cross-tree set never holds part of a saga across its members. |
| | `SetComplete` | Invariant | An accepted capture holds every member shard. |
| | `CaptureStrictIsolation` | Invariant | A capture never holds an uncommitted saga's writes. |
| | `SetCaptureCompletes` | Liveness | Every capture is accepted or fails explicitly, by the protocol's own steps. |
| `BackupIncremental` | `BackupSagaConsistent` | Invariant | No restore of a chain holds part of a saga. |
| | `CaptureStrictIsolation` | Invariant | No restore of a chain holds a write of a saga that did not commit. |
| | `ChainCoversCommitted` | Invariant | A link whose decision snapshot holds a saga committed restores it whole. |
| | `SagaFallbackOnlyAcrossFull` | Invariant | An increment falls back to a full backup only for a saga straddling the full capture's frontier. |
| `BackupProvenance` | `ProvenanceNoEmptyOrigin` | Invariant | No link names the empty origin (#2621). |
| | `ProvenanceCoversCaptured` | Invariant | No captured write from a real origin goes unattributed. |
| | `FrontierCoversCaptured` | Invariant | Every link's cut HLC covers every write it captured (#3758). |
| | `ChainFrontierMonotonic` | Invariant | A chain's frontier never regresses. |
| `BackupRestore` | `RestoreAllOrNothing` | Invariant | No cluster serves its restored copy unless every cluster voted commit and none compensated. |
| | `RestoredCutNotReAdvanced` | Invariant | No pre-cutover write reaches a restored copy. |
| | `RestoreAdmitsOnlyNamespace` | Invariant | A restore never installs a record outside the tenant's namespace. |
| | `AckedWritesServed` | Invariant | A post-cutover write stays served by its author. |
| | `RestoreConverges` | Liveness | A restore followed by resumed replication converges. |
| `BackupRetention` | `RetainedRestorable` | Invariant | Every backup the policy keeps, with its whole base chain, is restorable. |
| | `ListedRestorable` | Invariant | Every backup the catalog lists, retained or not, is restorable. |
| | `ListedInSink` | Invariant | Every backup the catalog lists has its manifest in the sink. |
| | `SinkChainIntact` | Invariant | A published increment still in the sink has a restorable base. |
| | `InFlightBaseSurvives` | Action | An increment is published only while its base is restorable. |
| | `KeepPolicyHonoured` | Action | Nothing the policy keeps is pruned. |
| | `ExpiredEventuallyPruned` | Liveness | A listed backup outside the policy that nothing builds on is eventually pruned. |
| `BackupIdClaim` | `ListedInSink` | Invariant | A retire or a delete never deletes the manifest of a row a publish registered meanwhile. |
| | `ListedRestorable` | Invariant | Every listed backup's manifest is in the sink with every artifact it references: a rebuild never lists a backup a delete half removed. |
| | `ScopeAgreement` | Invariant | The bare id's catalog row and sink manifest name one scope. |
| | `DeleteFence` | Invariant | While a delete of the id is in progress and its lease is live, no publish of the id in its scope runs. |
| | `NoForeignOverwrite` | Action | No publish replaces another scope's backup under the bare id. |
| | `NoForeignDelete` | Action | No delete in one scope removes another scope's backup under the bare id. |
| | `LeaseEventuallyEnds` | Liveness | A delete whose caller is lost does not keep the id reserved for good. |
| `BackupCutover` | `RestoreNeverTorn` | Invariant | A reader never pairs one copy with another copy's shard map. |
| | `CutoverServesRestored` | Invariant | Once a restore returns, every reader is served the restored copy. |
| | `RevertNeverServesRestored` | Invariant | Once a revert returns, no reader is served the restored copy. |
| | `DeleteNeverMidCutover` | Invariant | A tree is never deleted while its copies are in motion. |
| | `RestoreReturns` | Liveness | A restore whose shadow is built eventually returns. |

Every module also checks `TypeOK`. Each liveness property fails on a protocol
defect under the fairness its module asserts, demonstrated by a mutation that
leaves that fairness intact; see each mutation directory.

## Defects the modules found

The models specify the INTENDED design. Where production differed, a mutation
reproduces production before the fix, and the issue stays cited in the
refinement note's rows:

- **#4485** (`BackupCapture`, fixed): a snapshot capture, and so every full
  backup and a cross-tree set, could hold an atomic batch torn, because each
  shard was captured at its own moment and a still-pending bucket was served
  pre-saga. Confirmed by execution. The module checks the fix that shipped, a
  lease-fenced decision gate; `BackupSagaConsistentPendingReadsPre` and
  `BackupSagaConsistentShardsCapturedApart` reproduce production before it, and
  their code analogues turn the fix's regression tests red.
- **#4490** (`BackupRestore`, fixed by #4498): a shipper whose alias-change push
  was lost resumed from the retired copy's log and re-advanced the peer's
  restored cut. The shipper half was confirmed by execution. The module checks
  a resume that rebinds first, which the fix implements;
  `RestoredCutNotReAdvancedResumeShipsRetiredLog` reproduces production before
  it, and its code analogue turns the fix's regression test red.

- **#4593** (`BackupRestore`, fixed): a write a stale cached receive gate
  admitted before a coordinated restore paused receiving reached the tree after
  the alias swap, or after the lift, and landed on the restored copy; so did an
  entry parked in the causal-apply buffer before the pause. The module checks
  the fix: the restored copy is born closed with the pause's epoch as its
  floor, the seam refuses a closed copy or a stale admission, a park re-reads
  the fence, and the drain discards an entry parked before the pause.
  `RestoredCutNotReAdvancedCopyFenceDropped` reproduces production before it,
  and its code analogue turns the fix's regression tests red.
- **#4589** (`BackupIncremental`, fixed): an incremental backup copied a saga's
  prepared writes as data and dropped its terminals, so a restore could hold an
  aborted or undecided batch's writes, or a batch partially. Confirmed by
  execution. The module checks the fix; `BackupSagaConsistentIncrementCopiesPrepares`
  reproduces production before it. Checking the fix found one more case the
  window cannot see - a saga undecided at a full base and committed before any of
  its terminals reaches the WAL - which the fix closes by recording each capture's
  undecided sagas in its cut.
- **#4686** (`BackupIncremental`, fixed by #4690): an increment on a legacy base
  captured before the #4589 fix recorded its undecided sagas could restore a
  batch committed in its decision snapshot as absent, because the base carried
  no undecided set the increment could look the saga up in. The module checks
  the fix: `LatticeBackupCaptureService.PredatesUndecidedSagaRecording`
  recognises such a base and falls back to a full backup rather than layering an
  increment on it. `LatticeBackupIncrementalSagaConsistencyTests.An_increment_on_a_legacy_base_that_recorded_no_undecided_sagas_falls_back_to_a_full_backup_holding_the_batch_whole`
  reproduces production before the fix, and its code analogue turns the fix's
  regression test red.

- **#5357** (`BackupRetention`, fixed): a retention pass could delete the base
  of an incremental backup still being captured, because it planned from a
  catalog listing that cannot hold the in-flight increment, and the increment
  read its base only before draining; the increment was then published with no
  base. Two passes could also run at once on one scheduler activation and undo
  each other's protection. The module checks the fix: a pass removes each
  doomed row first, scans the sink for what an increment outside the pass builds
  on and keeps it, and deletes tip first; an increment re-reads its base's
  catalog row after writing its sink manifest and falls back to a full backup if
  it is gone; one pass runs at a time. `InFlightBaseSurvivesNoBaseRecheck` and
  `RetainedRestorableNoDependentScan` reproduce production before it, and their
  code analogues turn the fix's regression tests red.
- **#5412** (`BackupRetention`, fixed): the #5357 fix as first written read a
  pass's catalog listing before it checked the one-pass guard, so a pass could
  wait out another pass and then plan from a listing that still held rows that
  pass had pruned; its scan could find an increment built on one of them and
  restore the row of a backup whose manifest and artifact were gone, and the
  increment's re-check then passed against it. Found by the module's
  independent review (#5352) against a model that splits the listing from the
  guard as production ordered them. The module checks the fix, a listing read
  under the guard; `ListedRestorableListingBeforeGuard` reproduces production
  before it, and its code analogue turns the fix's regression test red.

- **#5418** (`BackupRetention`, fixed): a backup id is a content address, as
  10.2.0 documents, so a full capture of an unchanged tree republished an
  existing backup's id. A pass that had listed and doomed that backup removed its
  row and deleted its sink manifest, and the republishing capture then
  registered the row again over a sink manifest that was gone; with the row
  removed later, the capture's backup was lost outright and its artifact leaked.
  A republish also kept the backup's first capture time, all a pass ranked by,
  so the next pass could prune the backup a capture had just returned. Found by
  the module's independent review (#5352). The fix keeps 10.2.0's ids and
  serialises per id on the scope's scheduler grain: a pass reserves every id it
  dooms before it removes a row and never dooms one a capture holds, a publish
  of a retiring id waits, and a publish records the latest capture time
  (`BackupManifest.LastCapturedAtUtc`), which retention ranks by. The module
  checks the republish under that hold; `ListedInSinkUngatedRepublish` and
  `KeepPolicyHonouredStaleRank` reproduce production before it, and their code
  analogues turn the fix's regression tests red. (A first fix, #5434, made every
  id unique per capture instead; that broke 10.2.0's documented idempotent ids
  and was reverted for this one.)
- **#5449** (`BackupRetention`, fixed): a multi-tree backup set registered each
  member's catalog row as soon as the member was captured, and its membership
  stamp re-wrote the member's sink manifest and registered the row again once
  every member was captured. In between, the member was an ordinary listed
  backup, so a retention pass could prune it, and the stamp then listed it
  again over its deleted artifact. Found by the module's confirmation review
  (#5352). The fix defers a member's row to the stamp, its only registration;
  a member nothing will stamp is registered unstamped. The module models the
  stamp (`Stamp`); `ListedRestorableStampReregisters` reproduces production
  before the fix, and its code analogue turns the fix's regression test red.
- **#5531** (`BackupRetention` gap, fixed): once schedule ticks were
  coalesced, a full-backup tick that fired while an incremental cycle ran was
  dropped rather than coalesced, so a scope with both schedules could skip its
  full backup. Found by the module's confirmation review (#5352). Coalescing is
  now per kind: such a full tick runs its cycle once the incremental one ends.
  Tick cadence is outside the model (its gap register says so); the fix is
  pinned by regression tests shown red in CI before it.
- **#5529** (`BackupProvenance`, fixed): an incremental capture kept the local
  cluster id as the origin of the tree's own writes, while a full capture and
  the live tree store an own row's origin as null, so a restored chain gave own
  rows two origins. Found by the module's confirmation review (#5352). The
  incremental collector now goes through the same origin seam
  (`LatticeRowOrigin.FromRecord`); the module already specifies an own row as
  unstamped, and a capture-level regression test pins the call site.
- **#5532** (fixed, pinned): on a replicated tree a replayed own saga prepare
  took the cross-cluster preserve-HLC commit branch instead of re-stamping like
  the foreground saga, because its rows carried the local cluster id as their
  origin. The origin seam above removed the cause; found by the module's
  confirmation review (#5352), and now pinned by a regression test shown red in
  CI against the earlier behaviour. It lies outside the backup modules.
- **#5627** (`BackupIdClaim`, fixed): the claim grain admitted the scope that
  owned a bare id without recording a claim. A capture of that scope resolved the
  id and waited on its hold while the scope's own retention pass or delete
  removed it; another scope's capture of identical content took the ownerless id,
  and the waiting capture then published over it, so the other scope's caller
  held an id that listed and restored as the first scope's. Found by the
  confirmation review (#5352). The fix records one claim per admitted capture,
  the owner's included, released one at a time; `NoForeignOverwriteOwnerUnclaimed`
  and `NoForeignOverwriteReleaseClearsAll` reproduce production before it.
- **#5628** (`BackupRetention` gap, fixed): an operator delete ran its catalog
  scan and its chunked artifact deletes inside one scheduler grain call, so a
  large delete timed out and skipped its health cleanup. The grain now removes the
  row and reserves the id by a lease, and the artifacts and then the sink
  manifest are deleted outside it; the manifest goes last, so a retry finishes an
  interrupted delete (which also fixes #5334). Pinned by regression tests shown
  red in CI before the fix.
- **#5629** (`BackupRetention`, fixed): a set member's pending-stamp hold was
  never released if its capture died, exempting the backup it republished from
  retention for as long as the scheduler activation lived. The hold is now a
  lease, as the model's crash already releases it. Pinned by a regression test
  shown red in CI before the fix.
- **#5631** (`BackupIdClaim`, fixed): overlapping operator deletes of one id - a
  retry after a timeout, a double submission - shared one lease, which the first
  to end removed, and each deleted the sink manifest outside the grain,
  unconditionally. A capture of identical content republished the id as soon as
  the first delete ended, and the second then deleted the republished manifest,
  leaving a listed backup with nothing in the sink; a single delete whose lease
  ran out reached the same state. Found by the confirmation review (#5352). Each
  delete now holds a lease of its own and deletes the sink manifest in the grain,
  under the hold, only while no row lists the id and the manifest is still its
  scope's; a delete that reaches an id another scope has since taken leaves it
  alone. `ListedInSinkJoinedDeleteSharedLease` and `DeleteFenceEndClearsEveryLease`
  reproduce production before it.
- **#5632** (`BackupIdClaim`, fixed): the claim grain kept its claims in
  activation memory only, so losing its activation while an owning scope's
  capture waited on its hold reopened #5627. The claims are now persisted. Pinned
  by a regression test, shown red in CI before the fix, that deactivates the claim
  grain mid-publish. The last claim released now clears the stored record, so
  no bare id leaves one behind for good.
- **#5637** (`BackupIdClaim`, fixed): a catalog rebuild re-listed a backup an
  operator delete had half removed. Since #5628 a delete removes the row, then
  the artifacts, then the sink manifest, so a retry resumes it; a rebuild
  registered any sink manifest, and a delete's lease was not renewed while it
  deleted artifacts. A delete whose caller was lost part-way, or whose artifact
  deletes outlasted its lease, let a rebuild list a backup that could not be
  restored. Found by the confirmation review (#5352). The delete now renews its
  lease in the background and deletes no further artifact once a renewal is
  refused, and a rebuild registers a manifest only when every artifact it
  references is in the sink. `ListedRestorableDeleteBeforeFix` reproduces
  production before the fix.

All eighteen fixes have landed, so the reproducing mutations now stand as
regression checks: each must keep firing, and each has a code analogue that turns
the fix's regression tests red.
## Saga abstraction

`BackupCapture` restates the saga steps of
[`spec/atomic-commit/AtomicCommit.tla`](../atomic-commit/AtomicCommit.tla)
instead of instancing that module. A module in a sibling directory is not
copied into TLC's scratch directory, so the gates could not check an
`INSTANCE` across directories, and the capture needs per-tree local decisions
and cross-tree delegation rows that the atomic-commit instance does not carry.
The mapping from each restated action to its atomic-commit counterpart is a
table in [`RefinementCapture.md`](RefinementCapture.md).

## Pure cores and Coyote models

| Core | Routes | Coverage |
|------|--------|----------|
| `CrossTreeFenceWindow` (backup) | The drain gate and post-capture re-observation of `LatticeBackupCaptureService` | `CrossTreeFenceWindowTests`; Coyote `CrossTreeFenceCaptureCoyoteTests`, which models the fence, its lapse window, the gate, the gated re-check and the post-capture re-observation over a two-tree saga, with a fixed-design arm, a quiet-set arm, an anti-vacuity witness that an accepted set holds the committed saga, four single-defence arms (each of the drain, the re-check, the re-observed epoch and the re-observed in-flight count alone keeps the set whole against the race it covers), and two guards that find a torn set once the defences for a race are removed. |
| `IncrementalSagaStaging` (backup) | How an increment resolves the sagas in its window against the decision gate, in `IncrementalDeltaCollector` | `IncrementalSagaStagingTests` (core unit suite; the staging is a fold over the drained entries against a fixed decision snapshot, so it is not schedule-sensitive). |
| `BackupChainFrontier` (backup) | Origin normalisation, per-origin high-water, and both consistency cuts, in both collectors and the capture service | `BackupChainFrontierTests` (core unit suite; the rules are not schedule-sensitive). |
| `BackupRetentionPlanner` (backup) | The keep set, base closure, retained artifacts, dependent scan and tip-first order of `BackupSchedulerGrain`'s retention pass, and the publish check of `LatticeBackupCaptureService.CaptureIncrementalAsync` | `BackupRetentionPlannerTests`; Coyote `BackupRetentionCoyoteTests`, which races a pass against an increment and a newer full capture through the production planner, with a fixed-design arm, an anti-vacuity witness that the increment falls back, and one guard per side of the fence. |
| `CrossClusterSagaDecisionCore` (replication) | The coordinated restore's single global decision in `CrossClusterSagaCoordinatorGrain` | `CrossClusterSagaDecisionCoreTests`; Coyote `CoordinatedRestoreDecisionCoyoteTests`, with a fixed-design arm and a guard. |

## How to run TLC

From this directory, with the toolchain described in
[how to run TLC](../README.md#how-to-run-tlc), for example:

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -config BackupCapture.cfg BackupCapture.tla
```

## Counts

The modules' current totals. `SpecModuleDiscoveryTests` checks this table
against each module's manifest, and the other Formal gates check the manifests
against the specifications, the cfgs, the mutation catalogues, the refinement
notes and TLC's own state counts.

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `BackupCapture` | 5 | 1 | 19 | 21 | 23 | 256,606 |
| `BackupIncremental` | 5 | 0 | 8 | 13 | 11 | 1,709 |
| `BackupProvenance` | 5 | 0 | 4 | 5 | 8 | 2,199 |
| `BackupRestore` | 5 | 1 | 16 | 20 | 20 | 22,700 |
| `BackupCutover` | 5 | 2 | 10 | 12 | 16 | 39 |
| `BackupRetention` | 5 | 3 | 18 | 28 | 24 | 2,680,648 |
| `BackupIdClaim` | 5 | 3 | 13 | 22 | 19 | 6,824 |
