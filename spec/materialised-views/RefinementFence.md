# Replay-fence lifetime refinement

This is a documented abstraction, not a machine-checked refinement proof.
The fixed initial prefix is the two-two-key analogue of the production
reactivation regression: a peer-delayed batch remains staged, another batch
flushes, and older ordinary HLCs occur at later physical WAL offsets.
The unread-suffix rows have a production mutation/control pair described below.
Other Pending rows retain their distinct acceptance obligations and deliberately
fail the unqualified-Yes acceptance gate until their evidence is integrated.
No detector executes this entire finite state machine inside one fixture.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `wal`, `fenceWal`, `rebound` | Physical WAL identity and scope invalidation | `ViewCheckpointState.ReplayFenceWalTreeId`, `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`. |
| `value` | The durable backing view values | `ViewMaintainerGrain.ApplyAsync`. |
| `ckpt`, `pos`, `livePos` | Durable hold-back and in-activation read progress | `ViewCheckpointState.AppliedOffsets`, `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewMaintainerGrain.RecordDrainReadOffsets`. |
| `fence`, `armed`, `fenceStaged` | Persisted and armed replay fence | `ViewCheckpointState.ReplayFenceOffsets`, `ViewCheckpointState.ReplayFenceStagedTransactions`, `ViewMaintainerGrain.ArmReplayFence`, `ViewMaintainerGrain.UpdateReplayFence`. |
| `prepared`, `terminal`, `staged`, `resolved` | Atomic staging and resolved transaction memory | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`, `ViewMaintainerGrain.MarkResolved`. |
| `supersession` | Restored source-HLC winners, independent of WAL offset | `ViewCheckpointState.ReplayFenceSupersession`, `ViewMaintainerGrain.IsSupersededByOrdinary`. |
| `peer` | Peer view has registered its slice | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. |
| `established`, `phase`, `crashes` | Prefix, pass phases and finite environment budget | Modelling devices; no persisted production counterpart. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Establish` | Full first pass flushes the newer batch but holds the peer-delayed batch | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.UpdateReplayFence`. | Yes: `ViewMaintainerReplayFlushTests.A_replay_fence_left_unpassed_when_staging_empties_still_fences_the_rest_of_the_old_pass`. |
| `Crash` | Reactivation loses staging but retains backing values, checkpoint and fence | `ViewMaintainerGrain.ArmReplayFence`. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_reactivation_that_replays_an_already_flushed_batch_keeps_the_batch_value_over_the_older_ordinary_write`. |
| `PeerReady` | The peer's registration allows the delayed joint flip | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Pending: existing regression detector `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_two_non_empty_slices_issue_cross_tree_commit`. |
| `Begin` | Resume below the old head with the persisted replay fence armed | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewMaintainerGrain.ArmReplayFence`. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_reactivation_that_replays_an_already_flushed_batch_keeps_the_batch_value_over_the_older_ordinary_write`. |
| `Read` | Read a bounded window; skip reflected ordinary writes and resolved batches | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.IsReplayFenced`. | Yes: `ViewMaintainerReplayFlushTests.A_replay_fence_left_unpassed_when_staging_empties_still_fences_the_rest_of_the_old_pass`. |
| `Flush` | Source HLC supersession prevents the delayed batch from replacing newer keys | `ViewMaintainerGrain.IsSupersededByOrdinary`. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_reactivation_that_replays_an_already_flushed_batch_keeps_the_batch_value_over_the_older_ordinary_write`. |
| `Persist` | Keep an unpassed fence even after every re-staged batch resolves | `ViewMaintainerGrain.UpdateReplayFence`, `ViewMaintainerGrain.ClearReplayFence`. | Yes: `ViewMaintainerReplayFlushTests.A_replay_fence_left_unpassed_when_staging_empties_still_fences_the_rest_of_the_old_pass`. |
| `Rebind` | Rebuild on another physical copy and invalidate retired-log replay state | `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`. | Pending: existing regression detector `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap`. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `QuiescentExact` | A completed replay retains the source LWW winner, not the later-offset older write | Yes: `ViewMaintainerReplayFlushTests.A_replay_fence_left_unpassed_when_staging_empties_still_fences_the_rest_of_the_old_pass`. |
| `FenceCoversUnread` | Empty staging does not clear a fence whose recorded head has not been consumed | Yes: `ViewMaintainerReplayFlushTests.A_replay_fence_left_unpassed_when_staging_empties_still_fences_the_rest_of_the_old_pass`. |
| `ScopeExact` | A new physical binding never consumes retired-log replay state | Pending: existing regression detector `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap`. |
| `NoFencedRegression` | Replayed ordinary entries and delayed prepares cannot overwrite the reflected newer batch | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_reactivation_that_replays_an_already_flushed_batch_keeps_the_batch_value_over_the_older_ordinary_write`. |
| `Converges` | The peer resolves, replay reaches the old head, and the fence clears | Yes: `ViewMaintainerReplayFlushTests.A_replay_fence_left_unpassed_when_staging_empties_still_fences_the_rest_of_the_old_pass`, `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_two_non_empty_slices_issue_cross_tree_commit`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite model-domain constraint, not a production behaviour. |

## Evidence and limits
The bounded unread-suffix detector now asserts the durable fence immediately
when the delayed peer resolves and staging empties, before the unread suffix
is consumed. Changing the actual `UpdateReplayFence` retirement predicate from
`staged.Count == 0 && !armedUnpassed` to `staged.Count == 0` fails that assertion
(expected head4, observed null). The same detector passes after restoration and
then verifies replay resumes below the retained head, keeps the newer value,
advances the checkpoint and finally retires the fence. This is one production
mutation/control pair, not six independent mutants or proof of the fresh joint
repair protocol. Other Pending rows retain their separate acceptance obligations.

`Establish` compresses a reachable first-pass prefix, not every interleaving of
initial transaction creation. Both batches contain both keys. Source HLCs
5/6 and 20/21 differ from offsets 1/2 and 4/5, and the ordinary HLC 10 is
logged at offsets 7/8. The variant changes it to 30 and permits another
crash. The deterministic idempotent flush of the already-reflected second
batch never writes again; that is precisely why losing the ordinary-write
fence creates a permanent, not transient, regression.

Peer readiness is eventual here. Coordinator slice durability, degrades,
timeouts and retry identities belong to the staging and joint-visibility
models. The physical rebind is an atomic invalidation macro onto an empty
new source, not an exploration of alias reservation or source copy phases.
The tail and lifecycle models cover those phases. This model has one physical
WAL partition; the horizon companion checks non-aligned multi-partition
prepares and terminals. Production detector coverage is at named seams,
not a proof of every crash boundary or of every two-key projection shape.
