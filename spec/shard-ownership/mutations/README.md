# Shard-ownership mutations

Each `.mutation` file here, and in [`../mutations-retention/`](../mutations-retention/),
is one controlled experiment: a deliberate defect in one module that must make
one named property fire, run as two arms (the property holds on the unmutated
module, and fails on the mutant). The file format, and why a mutation is
generated from the base rather than checked in as a copy, are described in
[`spec/atomic-commit/mutations/README.md`](../../atomic-commit/mutations/README.md#file-format);
these catalogues use it unchanged, including `PERTURBS:`.

This directory holds the `ShardOwnership` catalogue and
`../mutations-retention/` the `ShardOwnershipRetention` one. Mutation names are
unique across every module, because they name test cases; where both modules
pair an action with the same kind of defect, the retention module's file has its
own name.

## Three kinds of mutation

- **Reproductions of production.** Where a module models the intended design
  because production has an open defect, a mutation restores production's
  behaviour, and the header says `Reproduces current production` and names the
  issue. These are the area's standing checks on those defects; when the fix
  lands, the mutation stays, as the regression check for the behaviour it
  replaced. The refinement notes list them against their issues.
- **Regression checks for fixed defects.** The #4357, #4358, #4362 and #4369
  torn-batch family, and #4453, are reproduced as standing checks.
- **Pairings.** Every other mutation pairs an action with a property, so that
  every action in each module's `Next` is perturbed by at least one mutation
  and every checked property fires under at least one.

## No mutation adds an action

Every mutation here perturbs an action the module already has. Some also edit
`Quiescent`, which is not an action: a mutation that breaks the split/resize
interlock lets a split the alias left count as finished, standing for
production's `AbandonRetargetedSplitAsync`, which neither module has because
its interlock makes it unreachable; and the purged-copy stall lets the stalled
saga count as quiescent. Those edits only keep the mutant free of deadlock, so
that the named property is the only thing TLC can report, and each header says
so. Every mutant in both catalogues was checked free of deadlock with only
`TypeOK` checked, apart from the two whose target is `TypeOK` itself.

## Inventory: ShardOwnership

| Mutation | Target | Class | Perturbs | What it breaks |
|----------|--------|-------|----------|----------------|
| [`AtomicOnOwnerDecideBeforeDispatch`](AtomicOnOwnerDecideBeforeDispatch.mutation) | `AtomicOnOwner` | Invariant | `SagaDecide` | the decision is recorded before every entry is dispatched, so the batch reads torn |
| [`AtomicOnOwnerDiscardedCopyTerminalRedirects`](AtomicOnOwnerDiscardedCopyTerminalRedirects.mutation) | `AtomicOnOwner` | Invariant | `SagaTerminal` | terminals of a saga bound to an undone resized copy are re-sent to the old copy, landing part of the batch there |
| [`AtomicOnOwnerStartSkipsEntry`](AtomicOnOwnerStartSkipsEntry.mutation) | `AtomicOnOwner` | Invariant | `SagaStart` | the saga records an entry as dispatched before dispatching it, so the batch commits torn |
| [`NoKeyLostAckBeforeDecision`](NoKeyLostAckBeforeDecision.mutation) | `NoKeyLost` | Invariant | `SagaComplete` | the caller is acknowledged before the decision is recorded |
| [`NoKeyLostCommitSkipsFinalDrain`](NoKeyLostCommitSkipsFinalDrain.mutation) | `NoKeyLost` | Invariant | `SplitCommit` | the split moves the map without the final drain, so the new owner does not hold the key |
| [`NoKeyLostLaterWriteNotMirrored`](NoKeyLostLaterWriteNotMirrored.mutation) | `NoKeyLost` | Invariant | `LaterWrite` | a write during a resize is not mirrored, so the flip loses it |
| [`NoKeyLostPurgeClearsLiveCopy`](NoKeyLostPurgeClearsLiveCopy.mutation) | `NoKeyLost` | Invariant | `ResizePurge` | the purge clears the live copy's rows instead of the retired copy's |
| [`NoKeyLostResizeDuringSplit`](NoKeyLostResizeDuringSplit.mutation) | `NoKeyLost` | Invariant | `ResizeBegin` | a resize starts with a split in flight, so writes on the split target are lost at the flip |
| [`NoKeyLostSnapshotSkipsRows`](NoKeyLostSnapshotSkipsRows.mutation) | `NoKeyLost` | Invariant | `SnapCopy` | the snapshot copies no committed rows, so the resized copy loses every pre-resize key at the flip |
| [`NoKeyLostTerminalDropsBucket`](NoKeyLostTerminalDropsBucket.mutation) | `NoKeyLost` | Invariant | `SagaTerminal` | the terminal discards the bucket instead of draining it |
| [`NoKeyLostUndoRestoresWrongMap`](NoKeyLostUndoRestoresWrongMap.mutation) | `NoKeyLost` | Invariant | `UndoSwap` | the undo pairs the old copy with a map that does not describe it |
| [`NoResurrectionFlipBeforeFence`](NoResurrectionFlipBeforeFence.mutation) | `NoResurrection` | Invariant | `ResizeFlip` | the alias flips before the old copy is fenced, so a stale router reads an older value from it |
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
| [`SagaCompletesPurgedCopyRefusesTerminal`](SagaCompletesPurgedCopyRefusesTerminal.mutation) | `SagaCompletes` | Temporal | `SagaTerminal` | a saga bound to a purged old copy can never deliver its terminals |
| [`SagaCompletesTerminalNotRecorded`](SagaCompletesTerminalNotRecorded.mutation) | `SagaCompletes` | Temporal | `SagaTerminal` | the terminal broadcast does not checkpoint progress, so the saga never completes |
| [`SplitCompletesSweepStalls`](SplitCompletesSweepStalls.mutation) | `SplitCompletes` | Temporal | `SplitSweep` | the sweep never advances the split's phase, so the split never completes |
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
| [`AtomicOnOwnerPrepareNotMirrored`](AtomicOnOwnerPrepareNotMirrored.mutation) | `AtomicOnOwner` | Invariant | `SagaPrepare` | a prepare during a resize is not mirrored, so the resized copy reads the batch torn |
| [`AtomicOnOwnerRetainedDecidesEarly`](AtomicOnOwnerRetainedDecidesEarly.mutation) | `AtomicOnOwner` | Invariant | `SagaDecide` | the decision is recorded once any entry is dispatched, so readers see the batch torn |
| [`AtomicOnOwnerRetainedStartCheckpointsEntry`](AtomicOnOwnerRetainedStartCheckpointsEntry.mutation) | `AtomicOnOwner` | Invariant | `SagaStart` | the saga records an entry as dispatched before dispatching it, so the batch commits torn |
| [`NoKeyLostPurgeWipesResizedCopy`](NoKeyLostPurgeWipesResizedCopy.mutation) | `NoKeyLost` | Invariant | `ResizePurge` | the purge clears the resized copy instead of the old one |
| [`NoKeyLostResizeCapturesPinnedShardsOnly`](NoKeyLostResizeCapturesPinnedShardsOnly.mutation) | `NoKeyLost` | Invariant | `ResizeBegin` | the resize captures only the pinned shard, so a split-allocated shard is neither copied nor forwarded |
| [`NoKeyLostRetainedCommitSkipsDrain`](NoKeyLostRetainedCommitSkipsDrain.mutation) | `NoKeyLost` | Invariant | `SplitCommit` | the split moves the map without its final drain, so the destination misses an acknowledged write |
| [`NoKeyLostRetainedLaterWriteNotMirrored`](NoKeyLostRetainedLaterWriteNotMirrored.mutation) | `NoKeyLost` | Invariant | `LaterWrite` | a write during a resize is not mirrored, so the flip loses it |
| [`NoKeyLostSplitInSoftDeleteWindow`](NoKeyLostSplitInSoftDeleteWindow.mutation) | `NoKeyLost` | Invariant | `SplitBegin` | a split on the resized copy during the soft-delete window strands the bound saga's bucket on the split target |
| [`NoKeyLostUndoRestoresForeignMap`](NoKeyLostUndoRestoresForeignMap.mutation) | `NoKeyLost` | Invariant | `UndoSwap` | the undo moves the alias back with a map that routes a key to a shard the old copy never held it on |
| [`NoResurrectionLatePrepareActivationMemory`](NoResurrectionLatePrepareActivationMemory.mutation) | `NoResurrection` | Invariant | `DeliverLate` | the late-prepare refusal reads per-activation memory, so a late orphan serves a stale value |
| [`NoResurrectionRetainedFlipUnfenced`](NoResurrectionRetainedFlipUnfenced.mutation) | `NoResurrection` | Invariant | `ResizeFlip` | the alias flips before the old copy is fenced, so a reader still on it is served a value the new copy has superseded |
| [`NoResurrectionRetireDropsFence`](NoResurrectionRetireDropsFence.mutation) | `NoResurrection` | Invariant | `ResizeRetire` | retiring the old copy lifts its fence, so a stale reader is served the old copy again |
| [`NoResurrectionUndoLeavesResizedCopyServing`](NoResurrectionUndoLeavesResizedCopyServing.mutation) | `NoResurrection` | Invariant | `UndoArm` | the undo arms nothing on the resized copy, so a reader still on it is served a value the old copy has superseded |
| [`NoStrandedBucketTerminalNotMirrored`](NoStrandedBucketTerminalNotMirrored.mutation) | `NoStrandedBucket` | Temporal | `SagaTerminal` | a terminal to the old copy is not mirrored, so the resized copy keeps the batch's buckets forever |
| [`OwnerMonotonicForgetBeforeFanOut`](OwnerMonotonicForgetBeforeFanOut.mutation) | `OwnerMonotonic` | Invariant | `RegistryForget` | the registry forgets a decided saga before its terminals land, so a committed key reverts |
| [`OwnerMonotonicMaskReportsAbsent`](OwnerMonotonicMaskReportsAbsent.mutation) | `OwnerMonotonic` | Invariant | `RegistryMask` | a masked registry row reads as absent, so an undrained committed key reverts |
| [`OwnerMonotonicReactivationDropsBuckets`](OwnerMonotonicReactivationDropsBuckets.mutation) | `OwnerMonotonic` | Invariant | `Reactivate` | a reactivation loses the leaf's prepared buckets, so a visible commit reverts |
| [`OwnerMonotonicRetainedAbortOverturns`](OwnerMonotonicRetainedAbortOverturns.mutation) | `OwnerMonotonic` | Invariant | `SagaAbort` | an abort overwrites a recorded commit, so a key already read committed reverts |
| [`OwnerMonotonicRetainedSnapshotDropsBuckets`](OwnerMonotonicRetainedSnapshotDropsBuckets.mutation) | `OwnerMonotonic` | Invariant | `SnapCopy` | the snapshot copies committed entries only, so a commit decided before the flip reverts on the resized copy |
| [`OwnerMonotonicSweepIndeterminateLeavesMarker`](OwnerMonotonicSweepIndeterminateLeavesMarker.mutation) | `OwnerMonotonic` | Invariant | `SplitSweep` | the sweep replays an Indeterminate prepare that the destination refuses, leaving only an activation-scoped marker |
| [`ResizeCompletesFenceNeverLands`](ResizeCompletesFenceNeverLands.mutation) | `ResizeCompletes` | Temporal | `ResizeFence` | the fence step records nothing, so the flip never becomes enabled |
| [`ResizeCompletesUndoNeverClears`](ResizeCompletesUndoNeverClears.mutation) | `ResizeCompletes` | Temporal | `UndoClear` | the undo's last step never records that the undo finished |
| [`SagaCompletesCompletionNeverRecorded`](SagaCompletesCompletionNeverRecorded.mutation) | `SagaCompletes` | Temporal | `SagaComplete` | the saga never records its completion once the broadcast has visited every target |
| [`SplitCompletesFreezeNeverLands`](SplitCompletesFreezeNeverLands.mutation) | `SplitCompletes` | Temporal | `SplitFreeze` | the freeze step leaves the source accepting, so the split never reaches its commit |
| [`TypeOkLateForwardOutsideDomain`](TypeOkLateForwardOutsideDomain.mutation) | `TypeOK` | Invariant | `DeliverLate` | a delivered late forward records a state outside its domain |

## Running one by hand

Apply the mutation's edits to a copy of the module, rename the copy's
`MODULE` header to the mutation's `MODULE` name, and check it with a cfg that
names `TypeOK` and the mutation's `TARGET` (under `INVARIANTS` for an
invariant, `PROPERTIES` for a temporal property). `TlcModelCheckTests`
generates exactly that cfg.