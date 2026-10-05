# TLA+ specifications of backup and restore against in-flight sagas

This directory holds five TLA+ modules that check the backup and restore
protocols where they meet concurrent atomic sagas, replication, and failures.
Four are the deliverable of issue #4440, part of the formal-coverage epic #4430;
`BackupIncremental` was added by its fix for #4589.
Each module has its own TLC configuration, manifest, mutation directory and
refinement note. The layout every module follows, how to run TLC, and why TLC
runs in CI are described in [`spec/README.md`](../README.md); this README covers
only what is particular to the backup area.

## The five modules, and why five

The area splits along the seam where the state each part needs stops
overlapping, which also keeps every module's TLC run far inside the five-minute
CI timeout:

| Module | What it checks | Instance |
|--------|----------------|----------|
| [`BackupCapture`](BackupCapture.tla) | A capture racing in-flight sagas: the per-tree capture under the #4485 decision gate, and a cross-tree set's fence, drain gate, gated re-check and validation, with a lease lapse or a fault at any step. | Two trees, three shards, a single-tree and a cross-tree saga, a standalone or a set capture, two attempts. |
| [`BackupIncremental`](BackupIncremental.tla) | An incremental backup racing a saga: the forward WAL drain from the base's frontier, the saga's prepares and terminals in the window resolved against the decision gate, the frontier held back for an unsettled saga, the undecided sagas a link hands on, and the fall back to a full backup (#4589). | One saga over two keys on two WAL partitions, its prepares and terminals on different partitions, a full capture and up to two increments. |
| [`BackupProvenance`](BackupProvenance.tla) | What a chain records: per-origin provenance, the #2621 empty-origin rule, and the chain's HLC frontier (#3758). | A local and a replicated origin, three writes, a chain of three links. |
| [`BackupRestore`](BackupRestore.tla) | A coordinated restore across two regions, its per-record admission, and the replication that resumes after it, with delivery split into a cached-gate admission and a later landing, a causal-apply buffer, and the restored copy's receive fence (#4593). | Two clusters, two copies each, a backup with one admitted and one foreign record, two application writes, one saga (receive-fence epochs 0 and 1). |
| [`BackupCutover`](BackupCutover.tla) | A local shadow-cutover restore and its revert: the alias and shard map that move together, the redirects that heal stale routing, the alias reservation, and a crash with a retry. | One tree, two copies, one stale routing cache, one crash. |

An increment needs the WAL's partitions and offsets, which no other capture
question does, so it is a module of its own rather than a third capture kind in
`BackupCapture`, whose state space is already the largest here. Capture and
restore share no state the other needs: a restore installs an image
as one value, and whether that image is whole is the capture module's question.
Coordinated restore and the local cutover split for the same reason: the first
is about clusters and replication, the second about one cluster's routing.

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

All five fixes have landed, so the reproducing mutations now stand as
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
