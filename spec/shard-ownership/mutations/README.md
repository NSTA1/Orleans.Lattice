# Shard-ownership mutations

Each `.mutation` file here, and in [`../mutations-retention/`](../mutations-retention/),
[`../mutations-crdt/`](../mutations-crdt/) and [`../mutations-cutover/`](../mutations-cutover/),
is one controlled experiment: a deliberate defect in one module that must make
one named property fire, run as two arms (the property holds on the unmutated
module, and fails on the mutant). The file format, and why a mutation is
generated from the base rather than checked in as a copy, are described in
[`spec/atomic-commit/mutations/README.md`](../../atomic-commit/mutations/README.md#file-format);
these catalogues use it unchanged, including `PERTURBS:`.

This directory holds the `ShardOwnership` catalogue and
`../mutations-retention/` the `ShardOwnershipRetention` one, and
`../mutations-crdt/` the `ShardOwnershipCrdt` one, and `../mutations-cutover/`
the `ShardOwnershipCutover` one. Mutation names are
unique across every module, because they name test cases; where both modules
pair an action with the same kind of defect, the retention module's file has its
own name.

## Kinds of mutation

- **Reproductions of production.** Where a module models the intended design
  because production has an open defect, a mutation restores production's
  behaviour, and the header says `Reproduces current production` and names the
  issue. These are the area's standing checks on those defects; when the fix
  lands, the mutation stays, as the regression check for the behaviour it
  replaced. The refinement notes list them against their issues.
- **Regression checks for fixed defects.** The #4357, #4358, #4362 and #4369
  torn-batch family, #4453, #4452, #4454, #4455, #4473, #4474, #4475, #4503, #4522, #4545, #4564, #4611, #4613, #4618, #4619 and #4689, are reproduced as standing checks.
- **Checks against a naive fix.** Where the obvious fix for an open defect
  would break a different property, a mutation stands against it:
  `AtomicOnOwnerDiscardedCopyTerminalRedirects` is #4474's broadcast following
  the discarded copy's refusal to the old copy. `ReadableOnceCompleteWitnessNotCarried` and
  `ReadableOnceCompleteWitnessNotDurable` are #4545's second fix without the
  witness's carriage or durability, and `NoLiveBucketAfterForgetForwardRecreatesRow`
  and `AtomicOnOwnerParticipantRowBestEffort` are #4619's fix without its
  join-only registration or with a best-effort participant row.
- **Pairings.** Every other mutation pairs an action with a property, so that
  every action in each module's `Next` is perturbed by at least one mutation
  and every checked property fires under at least one.

## No mutation adds an action

Every mutation here perturbs an action the module already has. Some also edit
`Quiescent`, which is not an action: a mutation that removes the split's abandon
(`SplitAbandon`) lets a split an undo stranded count as finished; and the
purged-copy and discarded-copy
stalls let the stalled saga count as quiescent. Those edits only keep the mutant free of deadlock, so
that the named property is the only thing TLC can report, and each header says
so.

The #4503 reproductions that serve a read from the purged copy edit
`RoutedRefused`, the operator every routed read and write shares, rather than an
action, and so declare no `PERTURBS`; the write half is reproduced separately,
inside `LaterWrite`. The #4522 reproductions edit `TermRow`, the operator the
terminal broadcast applies, or the stamp operators `BVal`, `KnowsP` and
`WVal` that the read gate, the drain and the later write share, and likewise
declare no `PERTURBS`; `NoKeyLostLaterWriteBelowP` breaks property H the same way. Every mutant in both catalogues was checked free of deadlock with only
`TypeOK` checked, apart from the two whose target is `TypeOK` itself.

## Inventory: ShardOwnership

| Mutation | Target | Class | Perturbs | What it breaks |
|----------|--------|-------|----------|----------------|
| [`AtomicOnOwnerDecideBeforeDispatch`](AtomicOnOwnerDecideBeforeDispatch.mutation) | `AtomicOnOwner` | Invariant | `SagaDecide` | the decision is recorded before every entry is dispatched, so the batch reads torn |
| [`AtomicOnOwnerDiscardedCopyTerminalRedirects`](AtomicOnOwnerDiscardedCopyTerminalRedirects.mutation) | `AtomicOnOwner` | Invariant | `SagaTerminal` | terminals of a saga bound to an undone resized copy are re-sent to the old copy, landing part of the batch there |
| [`AtomicOnOwnerRouterIgnoresBinding`](AtomicOnOwnerRouterIgnoresBinding.mutation) | `AtomicOnOwner` | Invariant | `SagaPrepare` | the routing tier ignores the binding, so part of a bound batch lands only on a copy an undo discards |
| [`AtomicOnOwnerStartSkipsEntry`](AtomicOnOwnerStartSkipsEntry.mutation) | `AtomicOnOwner` | Invariant | `SagaStart` | the saga records an entry as dispatched before dispatching it, so the batch commits torn |
| [`NoKeyLostAckBeforeDecision`](NoKeyLostAckBeforeDecision.mutation) | `NoKeyLost` | Invariant | `SagaComplete` | the caller is acknowledged before the decision is recorded |
| [`NoKeyLostCommitSkipsFinalDrain`](NoKeyLostCommitSkipsFinalDrain.mutation) | `NoKeyLost` | Invariant | `SplitCommit` | the split moves the map without the final drain, so the new owner does not hold the key |
| [`NoKeyLostFreshStampBackstop`](NoKeyLostFreshStampBackstop.mutation) | `NoKeyLost` | Invariant | none (edits `TermRow`) | the terminal's backstop installs over a later write with a dominating stamp |
| [`NoKeyLostFreshStampDrainOverMigratedRow`](NoKeyLostFreshStampDrainOverMigratedRow.mutation) | `NoKeyLost` | Invariant | none (edits `TermRow`) | the drain skips only a newer non-migrated row and otherwise installs at a fresh stamp, overwriting a migrated later write |
| [`NoKeyLostLaterWriteBelowP`](NoKeyLostLaterWriteBelowP.mutation) | `NoKeyLost` | Invariant | none (edits `WVal`) | a later write stamped below the saga's prepare loses to the drain or the backstop at P |
| [`NoKeyLostLaterWriteNotMirrored`](NoKeyLostLaterWriteNotMirrored.mutation) | `NoKeyLost` | Invariant | `LaterWrite` | a write during a resize is not mirrored, so the flip loses it |
| [`NoKeyLostMigrationImportDropped`](NoKeyLostMigrationImportDropped.mutation) | `NoKeyLost` | Invariant | `SplitCommit`, `LaterWrite` | a migration import over a non-migrated destination row is dropped, losing a later write across the split |
| [`NoKeyLostPurgeClearsLiveCopy`](NoKeyLostPurgeClearsLiveCopy.mutation) | `NoKeyLost` | Invariant | `ResizePurge` | the purge clears the live copy's rows instead of the retired copy's |
| [`NoKeyLostPurgedCopyAcceptsWrites`](NoKeyLostPurgedCopyAcceptsWrites.mutation) | `NoKeyLost` | Invariant | `LaterWrite` | a routed write on the purged old copy is accepted and acknowledged but lands where nothing reads it |
| [`NoKeyLostResizeDuringSplit`](NoKeyLostResizeDuringSplit.mutation) | `NoKeyLost` | Invariant | `ResizeBegin` | a resize starts with a split in flight, so writes on the split target are lost at the flip |
| [`NoKeyLostResizeMirrorUnmarkedPrepare`](NoKeyLostResizeMirrorUnmarkedPrepare.mutation) | `NoKeyLost` | Invariant | none (edits `BVal`, `KnowsP`) | the resize mirror re-mints a prepare at R's clock, so a later write on R is stamped below P and the backstop overwrites it |
| [`NoKeyLostSnapshotResolvesAtFreshStamp`](NoKeyLostSnapshotResolvesAtFreshStamp.mutation) | `NoKeyLost` | Invariant | `SnapCopy` | the snapshot resolves a decided saga's bucket on R at a fresh stamp, overwriting a later write already copied |
| [`NoKeyLostSnapshotSkipsRows`](NoKeyLostSnapshotSkipsRows.mutation) | `NoKeyLost` | Invariant | `SnapCopy` | the snapshot copies no committed rows, so the resized copy loses every pre-resize key at the flip |
| [`NoKeyLostTerminalDropsBucket`](NoKeyLostTerminalDropsBucket.mutation) | `NoKeyLost` | Invariant | `SagaTerminal` | the terminal discards the bucket instead of draining it |
| [`NoKeyLostUndoRestoresWrongMap`](NoKeyLostUndoRestoresWrongMap.mutation) | `NoKeyLost` | Invariant | `UndoSwap` | the undo pairs the old copy with a map that does not describe it |
| [`NoResurrectionFlipBeforeFence`](NoResurrectionFlipBeforeFence.mutation) | `NoResurrection` | Invariant | `ResizeFlip` | the alias flips before the old copy is fenced, so a stale router reads an older value from it |
| [`NoResurrectionPurgedCopyServesEmpty`](NoResurrectionPurgedCopyServesEmpty.mutation) | `NoResurrection` | Invariant | none (edits `RoutedRefused`) | a routed read on the purged old copy answers empty, below an acknowledged value |
| [`OwnerMonotonicAbortOverturnsCommit`](OwnerMonotonicAbortOverturnsCommit.mutation) | `OwnerMonotonic` | Invariant | `SagaAbort` | an abort overwrites a recorded commit, so a key already read committed reverts |
| [`OwnerMonotonicFreezeBeforeSweep`](OwnerMonotonicFreezeBeforeSweep.mutation) | `OwnerMonotonic` | Invariant | `SplitFreeze` | the split freezes before its retroactive sweep, so a visible commit reverts when the map moves |
| [`OwnerMonotonicSnapshotSkipsBuckets`](OwnerMonotonicSnapshotSkipsBuckets.mutation) | `OwnerMonotonic` | Invariant | `SnapCopy` | the snapshot does not carry prepared buckets, so a commit decided before the flip reverts on the resized copy |
| [`OwnerMonotonicSweepSkipsReplay`](OwnerMonotonicSweepSkipsReplay.mutation) | `OwnerMonotonic` | Invariant | `SplitSweep` | the retroactive sweep drops an undecided prepare, so a visible commit reverts when the map moves |
| [`ReshardCompletesForgetsCompletion`](ReshardCompletesForgetsCompletion.mutation) | `ReshardCompletes` | Temporal | `ReshardFinish` | the reshard coordinator goes idle at its target instead of completing |
| [`ResizeCompletesFenceNoOp`](ResizeCompletesFenceNoOp.mutation) | `ResizeCompletes` | Temporal | `ResizeFence` | fencing a shard has no effect, so the alias flip never becomes enabled |
| [`RoutingConvergesUndoBeforeFlipKeepsFence`](RoutingConvergesUndoBeforeFlipKeepsFence.mutation) | `RoutingConverges` | Temporal | `UndoBeforeFlip` | an undo before the flip leaves the split-allocated shard fenced, so it refuses forever |
| [`RoutingConvergesUndoKeepsFence`](RoutingConvergesUndoKeepsFence.mutation) | `RoutingConverges` | Temporal | `UndoClear` | the undo leaves the split-allocated shard fenced, so the restored tree refuses it forever |
| [`SagaBatchOnOneCopyPreDecisionRebindIgnoresMirror`](SagaBatchOnOneCopyPreDecisionRebindIgnoresMirror.mutation) | `SagaBatchOnOneCopy` | Invariant | `SagaRebindBeforeDecision` | the pre-decision check re-binds although the bound copy mirrors, stranding the batch on the old copy |
| [`SagaBatchOnOneCopyRebindIgnoresMirror`](SagaBatchOnOneCopyRebindIgnoresMirror.mutation) | `SagaBatchOnOneCopy` | Invariant | `SagaRebindOnRefusal` | a mid-dispatch re-bind ignores the mirror check and strands partial prepares on the old copy |
| [`SagaBatchOnOneCopyRouterIgnoresBinding`](SagaBatchOnOneCopyRouterIgnoresBinding.mutation) | `SagaBatchOnOneCopy` | Invariant | `SagaPrepare`, `ResizeFlip` | stale routers place a bound saga's prepares on the old copy, which nothing fences |
| [`SagaCompletesDiscardedCopyRefusesTerminal`](SagaCompletesDiscardedCopyRefusesTerminal.mutation) | `SagaCompletes` | Temporal | `SagaTerminal` | a saga bound to a copy an undo discarded can never deliver its remaining terminals |
| [`SagaCompletesPurgedCopyRefusesTerminal`](SagaCompletesPurgedCopyRefusesTerminal.mutation) | `SagaCompletes` | Temporal | `SagaTerminal` | a saga bound to a purged old copy can never deliver its terminals |
| [`SagaCompletesTerminalNotRecorded`](SagaCompletesTerminalNotRecorded.mutation) | `SagaCompletes` | Temporal | `SagaTerminal` | the terminal broadcast does not checkpoint progress, so the saga never completes |
| [`SplitCompletesSweepStalls`](SplitCompletesSweepStalls.mutation) | `SplitCompletes` | Temporal | `SplitSweep` | the sweep never advances the split's phase, so the split never completes |
| [`SplitCompletesUndoWithoutAbandon`](SplitCompletesUndoWithoutAbandon.mutation) | `SplitCompletes` | Temporal | `SplitAbandon` | a split of the resized copy an undo retargets is never abandoned, so it never completes |
| [`TypeOkRefusalRunaway`](TypeOkRefusalRunaway.mutation) | `TypeOK` | Invariant | `ResizeFlipRefused` | a refused flip grows the refusal budget past its declared domain |
| [`UniqueOwnerLiftAfterLandedFlip`](UniqueOwnerLiftAfterLandedFlip.mutation) | `UniqueOwner` | Invariant | `ResizeFlipRefused` | the fence is lifted after a flip that landed, so the old copy serves beside the new one |
| [`UniqueOwnerReshardDuringResize`](UniqueOwnerReshardDuringResize.mutation) | `UniqueOwner` | Invariant | `ReshardStart` | a reshard starts during a resize, so its split leaves a stale router serving the split target after the flip |
| [`UniqueOwnerRetireLiftsFence`](UniqueOwnerRetireLiftsFence.mutation) | `UniqueOwner` | Invariant | `ResizeRetire` | retiring the old copy lifts its fence, so a stale router is served by it again |
| [`UniqueOwnerSplitDuringResize`](UniqueOwnerSplitDuringResize.mutation) | `UniqueOwner` | Invariant | `SplitBegin` | an adaptive split opens during a resize, so a stale router keeps serving the split target after the flip |
| [`UniqueOwnerUndoArmsNothing`](UniqueOwnerUndoArmsNothing.mutation) | `UniqueOwner` | Invariant | `UndoArm` | the undo arms no redirect on the resized copy, so a stale router keeps using it |
| [`UniqueOwnerUndoClearsBeforeSwap`](UniqueOwnerUndoClearsBeforeSwap.mutation) | `UniqueOwner` | Invariant | `UndoArm`, `UndoClear` | the undo lifts the old copy's fence before the swap and arms the new copy after it, so both serve |

## Inventory: ShardOwnershipRetention

| Mutation | Target | Class | Perturbs | What it breaks |
|----------|--------|-------|----------|----------------|
| [`AtomicOnOwnerParticipantRowBestEffort`](../mutations-retention/AtomicOnOwnerParticipantRowBestEffort.mutation) | `AtomicOnOwner` | Invariant | none (edits `PRow`) | with a best-effort participant row the guard refuses a live saga's forward, tearing the batch |
| [`AtomicOnOwnerPrepareNotMirrored`](../mutations-retention/AtomicOnOwnerPrepareNotMirrored.mutation) | `AtomicOnOwner` | Invariant | `SagaPrepare` | a prepare during a resize is not mirrored, so the resized copy reads the batch torn |
| [`AtomicOnOwnerRetainedDecidesEarly`](../mutations-retention/AtomicOnOwnerRetainedDecidesEarly.mutation) | `AtomicOnOwner` | Invariant | `SagaDecide` | the decision is recorded once any entry is dispatched, so readers see the batch torn |
| [`AtomicOnOwnerRetainedStartCheckpointsEntry`](../mutations-retention/AtomicOnOwnerRetainedStartCheckpointsEntry.mutation) | `AtomicOnOwner` | Invariant | `SagaStart` | the saga records an entry as dispatched before dispatching it, so the batch commits torn |
| [`NoKeyLostPurgeWipesResizedCopy`](../mutations-retention/NoKeyLostPurgeWipesResizedCopy.mutation) | `NoKeyLost` | Invariant | `ResizePurge` | the purge clears the resized copy instead of the old one |
| [`NoKeyLostResizeCapturesPinnedShardsOnly`](../mutations-retention/NoKeyLostResizeCapturesPinnedShardsOnly.mutation) | `NoKeyLost` | Invariant | `ResizeBegin` | the resize captures only the pinned shard, so a split-allocated shard is neither copied nor forwarded |
| [`NoKeyLostRetainedCommitSkipsDrain`](../mutations-retention/NoKeyLostRetainedCommitSkipsDrain.mutation) | `NoKeyLost` | Invariant | `SplitCommit` | the split moves the map without its final drain, so the destination misses an acknowledged write |
| [`NoKeyLostRetainedFreshStampBackstop`](../mutations-retention/NoKeyLostRetainedFreshStampBackstop.mutation) | `NoKeyLost` | Invariant | none (edits `TermRow`) | the terminal's backstop installs over a later write with a dominating stamp |
| [`NoKeyLostRetainedLaterWriteNotMirrored`](../mutations-retention/NoKeyLostRetainedLaterWriteNotMirrored.mutation) | `NoKeyLost` | Invariant | `LaterWrite` | a write during a resize is not mirrored, so the flip loses it |
| [`NoKeyLostRetainedSplitDuringResize`](../mutations-retention/NoKeyLostRetainedSplitDuringResize.mutation) | `NoKeyLost` | Invariant | `SplitBegin` | a split opens while a resize is in flight, so the resize neither captures nor fences its target |
| [`NoKeyLostSplitInSoftDeleteWindow`](../mutations-retention/NoKeyLostSplitInSoftDeleteWindow.mutation) | `NoKeyLost` | Invariant | none (edits `TermClosure`) | a split on the resized copy in the soft-delete window strands the bound saga's bucket, because the mirrored terminal does not follow it |
| [`NoKeyLostUndoRestoresForeignMap`](../mutations-retention/NoKeyLostUndoRestoresForeignMap.mutation) | `NoKeyLost` | Invariant | `UndoSwap` | the undo moves the alias back with a map that routes a key to a shard the old copy never held it on |
| [`NoLiveBucketAfterForgetForwardRecreatesRow`](../mutations-retention/NoLiveBucketAfterForgetForwardRecreatesRow.mutation) | `NoLiveBucketAfterForget` | Invariant | none (edits `PRow`) | the late forward's registration recreates the forgotten saga's row, so the guard never fires |
| [`NoLiveBucketAfterForgetLateForwardBucketed`](../mutations-retention/NoLiveBucketAfterForgetLateForwardBucketed.mutation) | `NoLiveBucketAfterForget` | Invariant | `DeliverLate` | a late forward of a forgotten saga is bucketed on a leaf that lost its memory of the terminal, stranded for good |
| [`NoResurrectionLatePrepareActivationMemory`](../mutations-retention/NoResurrectionLatePrepareActivationMemory.mutation) | `NoResurrection` | Invariant | `DeliverLate` | the late-prepare refusal reads per-activation memory, so a late orphan serves a stale value |
| [`NoResurrectionRetainedFlipUnfenced`](../mutations-retention/NoResurrectionRetainedFlipUnfenced.mutation) | `NoResurrection` | Invariant | `ResizeFlip` | the alias flips before the old copy is fenced, so a reader still on it is served a value the new copy has superseded |
| [`NoResurrectionRetainedPurgedCopyServesEmpty`](../mutations-retention/NoResurrectionRetainedPurgedCopyServesEmpty.mutation) | `NoResurrection` | Invariant | none (edits `RoutedRefused`) | a routed read on the purged old copy answers empty, below an acknowledged value |
| [`NoResurrectionRetireDropsFence`](../mutations-retention/NoResurrectionRetireDropsFence.mutation) | `NoResurrection` | Invariant | `ResizeRetire` | retiring the old copy lifts its fence, so a stale reader is served the old copy again |
| [`NoResurrectionUndoLeavesResizedCopyServing`](../mutations-retention/NoResurrectionUndoLeavesResizedCopyServing.mutation) | `NoResurrection` | Invariant | `UndoArm` | the undo arms nothing on the resized copy, so a reader still on it is served a value the old copy has superseded |
| [`NoStrandedBucketTerminalNotMirrored`](../mutations-retention/NoStrandedBucketTerminalNotMirrored.mutation) | `NoStrandedBucket` | Temporal | `SagaTerminal` | a terminal to the old copy is not mirrored, so the resized copy keeps the batch's buckets forever |
| [`OwnerMonotonicForgetBeforeFanOut`](../mutations-retention/OwnerMonotonicForgetBeforeFanOut.mutation) | `OwnerMonotonic` | Invariant | `RegistryForget` | the registry forgets a decided saga before its terminals land, so a committed key reverts |
| [`OwnerMonotonicMaskReportsAbsent`](../mutations-retention/OwnerMonotonicMaskReportsAbsent.mutation) | `OwnerMonotonic` | Invariant | `RegistryMask` | a masked registry row reads as absent, so an undrained committed key reverts |
| [`OwnerMonotonicReactivationDropsBuckets`](../mutations-retention/OwnerMonotonicReactivationDropsBuckets.mutation) | `OwnerMonotonic` | Invariant | `Reactivate` | a reactivation loses the leaf's prepared buckets, so a visible commit reverts |
| [`OwnerMonotonicRetainedAbortOverturns`](../mutations-retention/OwnerMonotonicRetainedAbortOverturns.mutation) | `OwnerMonotonic` | Invariant | `SagaAbort` | an abort overwrites a recorded commit, so a key already read committed reverts |
| [`OwnerMonotonicRetainedSnapshotDropsBuckets`](../mutations-retention/OwnerMonotonicRetainedSnapshotDropsBuckets.mutation) | `OwnerMonotonic` | Invariant | `SnapCopy` | the snapshot copies committed entries only, so a commit decided before the flip reverts on the resized copy |
| [`OwnerMonotonicSweepIndeterminateLeavesMarker`](../mutations-retention/OwnerMonotonicSweepIndeterminateLeavesMarker.mutation) | `OwnerMonotonic` | Invariant | `SplitSweep` | the sweep replays an Indeterminate prepare that the destination refuses, leaving only an activation-scoped marker |
| [`ReadableOnceCompleteDeadMarkerTransferred`](../mutations-retention/ReadableOnceCompleteDeadMarkerTransferred.mutation) | `ReadableOnceComplete` | Invariant | `DeliverLate`, `LeafSplit` | a marker installed after its terminal is copied to a fresh sibling leaf, which gates the key after the saga completed |
| [`ReadableOnceCompleteMarkerWithoutSelfCheck`](../mutations-retention/ReadableOnceCompleteMarkerWithoutSelfCheck.mutation) | `ReadableOnceComplete` | Invariant | `DeliverLate` | an unstamped marker on a leaf with no durable witness of the terminal gates the key after the saga completed |
| [`ReadableOnceCompleteWitnessNotCarried`](../mutations-retention/ReadableOnceCompleteWitnessNotCarried.mutation) | `ReadableOnceComplete` | Invariant | `LeafSplit` | a leaf split drops the witness, so an unstamped marker on the sibling gates the key after the saga completed |
| [`ReadableOnceCompleteWitnessNotDurable`](../mutations-retention/ReadableOnceCompleteWitnessNotDurable.mutation) | `ReadableOnceComplete` | Invariant | `Reactivate` | a reactivation loses the witness, so a later unstamped marker gates the key after the saga completed |
| [`ResizeCompletesFenceNeverLands`](../mutations-retention/ResizeCompletesFenceNeverLands.mutation) | `ResizeCompletes` | Temporal | `ResizeFence` | the fence step records nothing, so the flip never becomes enabled |
| [`ResizeCompletesUndoNeverClears`](../mutations-retention/ResizeCompletesUndoNeverClears.mutation) | `ResizeCompletes` | Temporal | `UndoClear` | the undo's last step never records that the undo finished |
| [`SagaCompletesCompletionNeverRecorded`](../mutations-retention/SagaCompletesCompletionNeverRecorded.mutation) | `SagaCompletes` | Temporal | `SagaComplete` | the saga never records its completion once the broadcast has visited every target |
| [`SplitCompletesFreezeNeverLands`](../mutations-retention/SplitCompletesFreezeNeverLands.mutation) | `SplitCompletes` | Temporal | `SplitFreeze` | the freeze step leaves the source accepting, so the split never reaches its commit |
| [`SplitCompletesRetainedUndoWithoutAbandon`](../mutations-retention/SplitCompletesRetainedUndoWithoutAbandon.mutation) | `SplitCompletes` | Temporal | `SplitAbandon` | a split of the resized copy an undo retargets is never abandoned, so it never completes |
| [`TypeOkLateForwardOutsideDomain`](../mutations-retention/TypeOkLateForwardOutsideDomain.mutation) | `TypeOK` | Invariant | `DeliverLate` | a delivered late forward records a state outside its domain |

## Running one by hand

Apply the mutation's edits to a copy of the module, rename the copy's
`MODULE` header to the mutation's `MODULE` name, and check it with a cfg that
names `TypeOK` and the mutation's `TARGET` (under `INVARIANTS` for an
invariant, `PROPERTIES` for a temporal property). `TlcModelCheckTests`
generates exactly that cfg.

## Inventory: ShardOwnershipCrdt

| Mutation | Target | Class | Perturbs | What it breaks |
|----------|--------|-------|----------|----------------|
| [`NoLostContributionBackstopInstallsLastWriterWins`](../mutations-crdt/NoLostContributionBackstopInstallsLastWriterWins.mutation) | `NoLostContribution` | Invariant | `Terminal` | the terminal's backstop installs the staged state last-writer-wins, dropping a write acknowledged after the stage |
| [`NoLostContributionBeginSkipsDrain`](../mutations-crdt/NoLostContributionBeginSkipsDrain.mutation) | `NoLostContribution` | Invariant | `Begin` | the window opens without the drain, so a resize swaps to a copy missing an earlier write |
| [`NoLostContributionCompleteSkipsDestination`](../mutations-crdt/NoLostContributionCompleteSkipsDestination.mutation) | `NoLostContribution` | Invariant | `Complete` | the saga completes before its terminal reaches the migration's destination |
| [`NoLostContributionDecisionAcknowledgesCaller`](../mutations-crdt/NoLostContributionDecisionAcknowledgesCaller.mutation) | `NoLostContribution` | Invariant | `Decide` | the saga acknowledges its caller at the decision, before any terminal |
| [`NoLostContributionLeafSplitLeavesRowBehind`](../mutations-crdt/NoLostContributionLeafSplitLeavesRowBehind.mutation) | `NoLostContribution` | Invariant | `LeafSplit` | a leaf split does not move the key's row to the sibling that now declares it |
| [`NoLostContributionResizeDrainLastWriterWins`](../mutations-crdt/NoLostContributionResizeDrainLastWriterWins.mutation) | `NoLostContribution` | Invariant | `Copy` | the resize's snapshot drain merges last-writer-wins, losing a contribution only the source held |
| [`NoLostContributionResizeWriteNotMirrored`](../mutations-crdt/NoLostContributionResizeWriteNotMirrored.mutation) | `NoLostContribution` | Invariant | `WriteB` | a CRDT write after the resize's drain passed its key is not mirrored and is lost at the swap |
| [`NoLostContributionSplitImportDropsOverOwnRow`](../mutations-crdt/NoLostContributionSplitImportDropsOverOwnRow.mutation) | `NoLostContribution` | Invariant | `WriteB`, `Copy`, `Commit` | a split's import drops the source's row over the destination's own fold, losing a contribution only the source held |
| [`NoLostContributionStageOmitsOwnContribution`](../mutations-crdt/NoLostContributionStageOmitsOwnContribution.mutation) | `NoLostContribution` | Invariant | `Stage` | the staged state omits the saga's own contribution, which a backstop then never applies |
| [`TypeOkMigrationPhaseOutsideDomain`](../mutations-crdt/TypeOkMigrationPhaseOutsideDomain.mutation) | `TypeOK` | Invariant | `Begin` | an opened migration window records a phase outside its domain |

## Inventory: ShardOwnershipCutover

| Mutation | Target | Class | Perturbs | What it breaks |
|----------|--------|-------|----------|----------------|
| [`AtomicAcrossCutoverAbortAfterCommit`](../mutations-cutover/AtomicAcrossCutoverAbortAfterCommit.mutation) | `AtomicAcrossCutover` | Invariant | `Abort` | a compensation overturns a recorded commit part way through the terminal broadcast |
| [`AtomicAcrossCutoverCheckBeforeDispatched`](../mutations-cutover/AtomicAcrossCutoverCheckBeforeDispatched.mutation) | `AtomicAcrossCutover` | Invariant | `Check` | the pre-decision check passes before the whole batch is prepared, so one key surfaces while the other waits for the backstop |
| [`AtomicAcrossCutoverDecideSkipsDiscard`](../mutations-cutover/AtomicAcrossCutoverDecideSkipsDiscard.mutation) | `AtomicAcrossCutover` | Invariant | `Decide` | **reproduces production before #4689 was fixed**: the decision does not wait for the discards, so the previous copy keeps an orphan bucket a revert serves torn |
| [`AtomicAcrossCutoverDiscardForgetsTerminal`](../mutations-cutover/AtomicAcrossCutoverDiscardForgetsTerminal.mutation) | `AtomicAcrossCutover` | Invariant | `Discard` | the leaf does not remember the discard, so a straggling prepare recreates the orphan after the decision |
| [`AtomicAcrossCutoverRebindForgetsLeftCopy`](../mutations-cutover/AtomicAcrossCutoverRebindForgetsLeftCopy.mutation) | `AtomicAcrossCutover` | Invariant | `RebindOnRefusal` | **production's mid-dispatch re-bind before #4689 was fixed**: the copy left behind is not recorded, so nothing discards it |
| [`AtomicAcrossCutoverRouterIgnoresBinding`](../mutations-cutover/AtomicAcrossCutoverRouterIgnoresBinding.mutation) | `AtomicAcrossCutover` | Invariant | `Prepare` | the routing tier ignores the binding (#4358), placing a prepare on a copy the saga never discards |
| [`AtomicAcrossCutoverTerminalRoutedThroughAlias`](../mutations-cutover/AtomicAcrossCutoverTerminalRoutedThroughAlias.mutation) | `AtomicAcrossCutover` | Invariant | `Terminal` | a routed terminal is refused by the retained redirect and follows the alias, installing the batch on the shadow one key at a time |
| [`CommittedBatchOnBoundCopyRebindBeforeDecisionForgets`](../mutations-cutover/CommittedBatchOnBoundCopyRebindBeforeDecisionForgets.mutation) | `CommittedBatchOnBoundCopy` | Invariant | `RebindBeforeDecision` | **production's pre-decision re-bind before #4689 was fixed**: the whole batch stays prepared on the previous copy after the commit |
| [`CommittedBatchOnBoundCopyStragglerLands`](../mutations-cutover/CommittedBatchOnBoundCopyStragglerLands.mutation) | `CommittedBatchOnBoundCopy` | Invariant | `Land` | a straggling prepare lands on a leaf that discarded the saga |
| [`SagaSettlesArmsAliasCopy`](../mutations-cutover/SagaSettlesArmsAliasCopy.mutation) | `SagaSettles` | Temporal | `Arm` | the cutover arms the copy the alias moved onto, which then refuses its own bound saga |
| [`SagaSettlesDiscardRouted`](../mutations-cutover/SagaSettlesDiscardRouted.mutation) | `SagaSettles` | Temporal | `Discard` | the discard is routed, so the retained redirect refuses it and the saga can never decide (`DEADLOCK: off`) |
| [`SagaSettlesRevertKeepsRedirect`](../mutations-cutover/SagaSettlesRevertKeepsRedirect.mutation) | `SagaSettles` | Temporal | `RevertArm` | the revert leaves the previous copy redirecting, so the copy the alias is back on refuses its bound saga |
| [`SagaSettlesRevertSwapLeavesAlias`](../mutations-cutover/SagaSettlesRevertSwapLeavesAlias.mutation) | `SagaSettles` | Temporal | `RevertSwap` | the revert's swap leaves the alias on the shadow, which its fix-up then arms |
| [`SagaSettlesSwapLeavesAlias`](../mutations-cutover/SagaSettlesSwapLeavesAlias.mutation) | `SagaSettles` | Temporal | `Swap` | the cutover's swap leaves the alias on the previous copy, which the restore then arms |
| [`TypeOkCutoverDiscardOutsideCopies`](../mutations-cutover/TypeOkCutoverDiscardOutsideCopies.mutation) | `TypeOK` | Invariant | none (edits `Discard`) | the discard bookkeeping records a value outside the copies |
