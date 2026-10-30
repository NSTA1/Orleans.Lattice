# Refinement note: ShardOwnershipFold to production

This note maps the TLA+ specification in
[`ShardOwnershipFold.tla`](ShardOwnershipFold.tla) to Orleans.Lattice as it
exists in code. The module is the fifth of this area's modules, beside
`ShardOwnership` ([`Refinement.md`](Refinement.md)),
`ShardOwnershipRetention` ([`RefinementRetention.md`](RefinementRetention.md)),
`ShardOwnershipCrdt` ([`RefinementCrdt.md`](RefinementCrdt.md)) and
`ShardOwnershipCutover` ([`RefinementCutover.md`](RefinementCutover.md)).

It is a **documented mapping, not a machine-checked refinement proof**. The
Detector column names the production test that goes red if the behaviour a row
abstracts regresses.

## What this module is for

The other modules move a slot onto a shard that has never held it: a split, and
in `ShardOwnership` a shard fold (`TreeShardConsolidationGrain`) onto a fresh
survivor (#5125). When a fold moves a slot back onto the shard an earlier split
took it from, the survivor is not a fresh destination:

- it still carries the slot in its permanent moved-away seal, so its write gate
  refuses the slot until the fold reclaims it (`ShardRootGrain.ReclaimSlotsAsync`);
- it still holds every row it had for the slot when the split moved it away,
  which the split leaves behind the seal by design;
- it can hold buckets of the same saga as the donor, so applying the saga's
  terminal there must drain them all.

This module models that round trip: a split of `k2`'s slot from `s1` to `s2`, a
fold back onto `s1` with its retroactive sweep, freeze, final drain, reclaim
and the donor's retirement, a cancel of the fold, an atomic-write saga over
`k1` and `k2`, stale routers, one later write or delete of `k2`, and tombstone
compaction.

## The base module models the fixed design

Every defect this module covers was fixed before or with it, and its
reproduction stays as a standing regression check:

- **The fold's retroactive sweep** (#5125, fixed by #5126). The fold sweeps the
  donor's prepares onto the survivor before it drains, as a split does: the
  base's `MigSweep`. `AtomicOnOwnerFoldBeginSkipsSweep` reproduces production
  before the fix.
- **Writes made since the split** (#5130, fixed by #5131). A migration import
  onto a row the survivor still holds behind its seal is resolved by
  last-writer-wins, and a live sealed row whose key the frozen donor no longer
  holds, because compaction reaped the delete that superseded it, is
  tombstoned just above its own stamp before the survivor reclaims the slot:
  the base's `MigCommit`. `NoKeyLostFoldImportDroppedOverSealedRow` and
  `NoKeyLostFoldSkipsReapReconciliation` reproduce production before the fix,
  one half each.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `rmap` | Where `k2` routes | The registry's shard map for `k2`'s virtual slot; `k1`'s slot never moves. |
| `published` | Every map the registry has published | The shard maps any router may still have cached; a stale router retries on `StaleShardRoutingException`. |
| `row[s][k]` | The committed projection value on a shard | The leaf projection row for the key on shard `s`, with its `LwwValue.Timestamp`. `Reaped` is a tombstone `BPlusLeafGrain.CompactTombstonesAsync` removed; a value in `Recons` is the fold's reconciliation tombstone at `TreeShardConsolidationGrain.JustAbove` the row it covers. |
| `pend[s][k]` | The saga's prepared bucket on a shard | The leaf's pending bucket (`BPlusLeafGrain.PendingTx`). |
| `term[s]` | The leaf activation remembers applying the saga's terminal | The orphan guard `MigrationTerminalCore.DecideBucketAction` consults. |
| `sealed[s]` | `k2`'s slot is in a shard's permanent moved-away seal | The leaf's moved-away slots, set by `ShardRootGrain.MarkLeavesMovedAwayAsync` and lifted by `BPlusLeafGrain.UnmarkSlotsMovedAwayAsync`, checked by `BPlusLeafGrain.IsKeyMovedAway` and the shard's `ShardRootGrain.ThrowIfRejectedForKey`. |
| `retired[s]` | A shard a committed fold retired | `ShardRootGrain.RetireAsync`, driven by `TreeShardConsolidationGrain.RetireDonorAsync`. |
| `mp` | The migration's phase | `TreeShardSplitGrain`'s or `TreeShardConsolidationGrain`'s phase: shadow write, swept, frozen (Reject), flipped. |
| `mk` | Which migration | A split (`TreeShardSplitGrain`) or a fold (`TreeShardConsolidationGrain`). |
| `src`, `dst` | The migration's source and destination | A split's source and target shard; a fold's donor and survivor. |
| `mdone` | The migrations that have committed | The coordinators' completed state. |
| `cancels` | The environment's budget for cancelling a fold | Calls to `TreeShardConsolidationGrain.CancelAsync`. |
| `sg`, `prepped`, `told`, `dec` | The saga's phase, dispatched keys, visited shards and decision | `AtomicWriteState.Phase`, its dispatched entries, the delivered-shard bookkeeping of `AtomicWriteGrain.BroadcastTerminalsAsync`, and the transaction registry's decision. |
| `opDone` | The later write or delete of `k2` has happened | A routed `SetAsync` or `DeleteAsync` of the key. |
| `young` | The delete landed while the saga was deciding and the saga has not completed | Not a production variable: the model's statement that the grace period outlasts a saga's broadcast (see the deliberate gaps). |
| `ackOn`, `vis` | Ghosts: the newest version acknowledged, and the highest a fresh reader was served | Not production state. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `SplitBegin` | An adaptive split opens its shadow-write window | `TreeShardSplitGrain.SplitAsync` into `TreeShardSplitGrain.InitiateSplitStateAsync` and `ShardRootGrain.BeginSplitAsync`. **Environment action:** a split may begin whenever no migration is in flight. | Yes: `TreeShardSplitGrainTests.ProcessNextPhase_drives_the_shadow_write_phase_through_the_full_split_pass`. |
| `FoldBegin` | A fold of the split's destination back onto its source opens the donor's window | `TreeShardConsolidationGrain.StartAsync` into `TreeShardConsolidationGrain.InitiateConsolidationStateAsync`, which opens the donor's shadow-write window. **Environment action:** the fold may begin once the split has committed. | Yes: `TreeShardConsolidationGrainTests.StartAsync_persists_intent_and_opens_the_donor_shadow_write_window` and `ShardConsolidationIntegrationTests.A_shard_split_away_and_then_folded_back_is_fully_readable_again`. |
| `MigSweep` | The retroactive sweep of the source's prepares that predate the window | `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync` for a split and `TreeShardConsolidationGrain.RetroactiveSweepPreparedMutationsAsync` for a fold (#5125): an undecided prepare is replayed through the destination's write gate, so a sealed survivor refuses it and the sweep is re-run on the next pass; a decided one has its terminal applied at the destination through `ShardRootGrain.AppendTxTerminalAsync`, which drains every bucket the destination holds for the saga. | Yes: `TreeShardConsolidationGrainTests.Initiate_replays_a_prepare_already_on_the_donor_onto_the_survivor_before_the_drain`, `TreeShardConsolidationGrainTests.Initiate_stays_in_BeginShadowWrite_when_the_prepared_sweep_fails`, `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` and `ShardConsolidationIntegrationTests.A_saga_prepared_on_the_donor_before_a_fold_opens_stays_atomically_visible_after_the_swap`. |
| `MigFreeze` | The source refuses the moved slot | Both `TreeShardSplitGrain.SwapAsync` and `TreeShardConsolidationGrain.SwapAsync` call `ShardRootGrain.SealMovedSlotsAndEnterRejectAsync`: it awaits `ShardRootGrain.MarkLeavesMovedAwayAsync` before `ShardRootGrain.EnterRejectPhaseAsync`, with the same leaf seals and Reject write. The final drain still precedes map publication; only the inter-call scheduling gap is removed. | Yes: `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map` and `TreeShardConsolidationGrainTests.Swap_freezes_the_donor_before_the_routing_map_flips`. |
| `MigCommit` | The final drain, then for a fold the reconciliation and the reclaim, then the map moves | `TreeShardConsolidationGrain.SwapAsync`: `TreeShardConsolidationGrain.DrainToEndAtomicallyAsync`, a migration import that `BPlusLeafGrain.MergeIntoStateAsync` resolves by last-writer-wins even over a row in a slot the survivor still holds sealed (#5130); then `TreeShardConsolidationGrain.TombstoneRowsTheDonorReapedAsync`, which tombstones each live sealed survivor row whose key the donor holds neither as a row nor as a prepared bucket, at `TreeShardConsolidationGrain.JustAbove` its stamp (#5130); then `ShardRootGrain.ReclaimSlotsAsync`; then the registry's fenced slot diff. A split's is `TreeShardSplitGrain.SwapAsync`'s final drain and slot reassignment. | Yes: `TreeShardConsolidationGrainTests.Swap_drains_after_the_freeze_so_the_survivor_copy_is_authoritative`, `TreeShardConsolidationGrainTests.Swap_reclaims_on_the_survivor_after_the_final_drain_and_before_the_flip`, `TreeShardConsolidationGrainTests.Swap_tombstones_a_sealed_survivor_row_the_donor_no_longer_holds_before_reclaiming`, `TreeShardConsolidationGrainTests.Swap_leaves_a_sealed_survivor_row_alone_when_the_donor_holds_a_prepared_bucket_for_it`, `BPlusLeafGrainTests.MergeMany_migration_import_supersedes_an_older_row_in_a_sealed_slot`, `BPlusLeafGrainTests.MergeMany_migration_import_does_not_regress_a_sealed_row_with_a_newer_stamp`, `ShardConsolidationIntegrationTests.A_key_updated_after_a_split_keeps_its_new_value_when_the_slot_folds_back` and `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip`. |
| `MigFinish` | The source records the slot in its permanent seal; a fold retires its donor | For a split, `TreeShardSplitGrain.FinaliseAsync` promoting the moved slots into the source's seal; for a fold, `TreeShardConsolidationGrain.FinaliseAsync`, which drains once more and retires the donor through `TreeShardConsolidationGrain.RetireDonorAsync`, keeping its storage while anything still needs it. | Yes: `ShardRootGrainSplitTests.CompleteSplitAsync_promotes_moved_slots_into_MovedAwaySlots`, `TreeShardConsolidationGrainTests.Finalise_retires_the_donor_and_clears_the_coordinator` and `ShardConsolidationIntegrationTests.A_fold_releases_the_donor_storage_only_after_the_survivor_has_absorbed_it`. |
| `FoldCancel` | A fold is cancelled before its freeze | `TreeShardConsolidationGrain.CancelAsync`, which abandons a fold that has not reached its swap and restores the donor, and leaves one past it to finish. **Environment action:** an operator's call, so neither fair nor bounded by the fold. | Yes: `TreeShardConsolidationGrainTests.Cancel_during_drain_abandons_the_fold_and_restores_the_donor` and `TreeShardConsolidationGrainTests.Cancel_past_the_swap_still_lets_the_fold_run_to_completion`. |
| `SagaStart` | The saga starts | `AtomicWriteGrain` entering its execute phase. | Yes: `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on`. |
| `SagaPrepare(k, m)` | One key's prepared write through a router holding map `m` | A routed prepare refused at a shard that froze or sealed the slot (`ShardRootGrain.ThrowIfRejectedForKey`), retried by the router; in a window, forwarded to the destination through its write gate, so a sealed survivor refuses it too. **Over-approximation:** one key per step. | Yes: `ShardRootGrainSplitTests.SetAsync_throws_StaleShardRouting_for_key_in_MovedAwaySlots_after_complete` and `LatticeGrainTests.SetAsync_absorbs_repeated_StaleShardRoutingException_throws`. |
| `SagaDecide` | The commit decision is recorded | `AtomicWriteGrain` recording the decision in the transaction registry once every entry is prepared. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaAbort` | The abort decision is recorded | `AtomicWriteGrain.BroadcastTerminalsAsync` with an abort after a failed execute phase. **Environment action:** unguarded through the execute phase and not fair. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |
| `SagaTerminal(s)` | One shard of the terminal broadcast | `AtomicWriteGrain.MarkOneShardAsync` into `ShardRootGrain.AppendTxTerminalAsync`; the leaf applies it through `MigrationTerminalCore.DecideBucketAction`, and its committed-values backstop installs a key the owner holds no bucket for. The targets include an open migration's destination. | Yes: `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_to_the_fenced_copy_directly_is_applied_and_forwarded` and `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `SagaComplete` | The broadcast finished; the caller is acknowledged | `AtomicWriteGrain.CompleteSagaAsync` after the broadcast reached every target. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `LaterOp(m, v)` | A later write or delete of `k2` through a router holding map `m` | A routed `SetAsync` or `DeleteAsync`, refused at a frozen or sealed shard. In a window a write is forwarded to the destination as a migration import, which no gate refuses; a plain delete is not forwarded (`ShardRootGrain.ForwardLocalDeleteToShadowIfNeededAsync` forwards only a saga's prepared delete), and the final drain carries its tombstone. **Environment action:** any published map may carry it. | Yes: `ShardRootGrainSplitTests.SetAsync_throws_StaleShardRouting_for_key_in_MovedAwaySlots_after_complete`, `ShardConsolidationIntegrationTests.A_concurrent_writer_loses_no_write_across_a_fold` and `ShardConsolidationIntegrationTests.A_key_deleted_after_a_split_stays_deleted_when_the_slot_folds_back`. |
| `Compact(s)` | Tombstone compaction reaps a tombstone past its grace period | `TombstoneCompactionGrain` driving `BPlusLeafGrain.CompactTombstonesAsync` on every leaf of every shard. **Environment action:** compaction may run on any shard between migrations; see the deliberate gaps for its guards. | Yes: `TombstoneCompactionGrainTests.ProcessNextShard_passes_configured_grace_period_to_leaves` and `ShardConsolidationIntegrationTests.A_key_deleted_and_reaped_after_a_split_stays_deleted_when_the_slot_folds_back`. |
| `Stutter` | Quiescence | Not a protocol step: a stuttering successor once nothing is in flight. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `UniqueOwner` | Every map a router may hold is refused for a key or reaches that key's one owner, across a split and a fold back. | Yes: `TreeShardConsolidationGrainTests.Swap_never_leaves_a_virtual_slot_unrouted`, `TreeShardConsolidationGrainTests.Swap_repoints_every_donor_slot_onto_the_survivor` and `ShardRootGrainSplitTests.SetAsync_throws_StaleShardRouting_for_key_in_MovedAwaySlots_after_complete`. |
| `NoKeyLost` | The owner holds every write and delete acknowledged since the split, after the slot folds back. | Yes: `ShardConsolidationIntegrationTests.A_key_updated_after_a_split_keeps_its_new_value_when_the_slot_folds_back`, `ShardConsolidationIntegrationTests.A_key_deleted_and_reaped_after_a_split_stays_deleted_when_the_slot_folds_back` and `ShardConsolidationIntegrationTests.A_concurrent_writer_loses_no_write_across_a_fold`. |
| `NoResurrection` | No read through any map returns a version older than one acknowledged, including a deleted key. | Yes: `ShardConsolidationIntegrationTests.A_key_deleted_after_a_split_stays_deleted_when_the_slot_folds_back` and `ShardConsolidationIntegrationTests.Tombstones_and_expiring_entries_survive_a_fold`. |
| `AtomicOnOwner` | A fresh reader sees a saga's batch on both keys or on neither across a fold. | Yes: `ShardConsolidationIntegrationTests.A_saga_prepared_on_the_donor_before_a_fold_opens_stays_atomically_visible_after_the_swap` and `ShardConsolidationIntegrationTests.A_saga_prepared_on_the_donor_after_a_fold_opens_stays_atomically_visible_after_the_swap`. |
| `OwnerMonotonic` | A fresh reader's value for a key never moves backwards across a fold. | Yes: `ShardConsolidationIntegrationTests.A_concurrent_reader_observes_no_missing_key_at_any_instant_of_a_fold`. |
| `MigrationCompletes` | A split or fold that opened its window finishes. | Yes: `TreeShardConsolidationGrainTests.RunConsolidationPass_drives_a_fold_from_start_to_completion` and `ShardConsolidationIntegrationTests.Repeated_consolidation_folds_an_over_split_tree_back_down`. |
| `SagaCompletes` | A saga that started completes. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `RoutingConverges` | Eventually the registry's own map serves every key: the survivor reclaims a slot it once gave away. | Yes: `ShardConsolidationIntegrationTests.A_shard_split_away_and_then_folded_back_is_fully_readable_again` and `TreeShardConsolidationGrainTests.Swap_reclaims_on_the_survivor_after_the_final_drain_and_before_the_flip`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Over-approximation arguments

Each environment action is argued in its row. These deliberate
under-approximations are stated so that they are not mistaken for coverage:

- **One physical copy.** The resize, its undo and the registry's retention are
  the other modules'; a fold refuses while a resize holds migrations.
- **Two shards, one slot moving, one round trip.** A fold onto a shard that
  never held the slot is a split in reverse, which `ShardOwnership` covers; a
  second round trip adds no new way for the survivor's old rows to meet the
  donor's.
- **The drain imports the whole row.** Production's drains page through slots;
  for one key a page is the row.
- **Stamps follow real time.** Every version's stamp is ordered as its write
  was, so the module cannot show a defect that needs an HLC inversion;
  `ShardOwnership` separates the two for the split's imports (#4564).

## Deliberate abstraction gaps

- **Compaction and prepared buckets.** Compaction does not check for a pending
  bucket. The module reaps a tombstone only when no undrained bucket for the
  key is on that shard and the saga that was deciding when it was written has
  completed (`young`): a bucket or a broadcast that outlives the grace period
  (a day by default) is a stranded saga, which `ShardOwnershipRetention`'s
  `NoStrandedBucket` rules out. `NoKeyLostCompactReapsYoungTombstone` shows the
  assumption is load-bearing.
  Production symbols: `BPlusLeafGrain.CompactTombstonesAsync`.
- **Compaction runs between migrations.** Reaping a tombstone that had aged out
  before a window opened is the same as reaping it just before; one that ages
  out inside a window needs a migration open for most of the grace period.
  Production symbols: `TombstoneCompactionGrain`.
- **A sealed survivor refuses a prepare in the window.** A saga's prepare of a
  folding slot, forwarded to a survivor that still holds the slot sealed, or
  replayed there by the sweep, is refused until the survivor reclaims the slot,
  and the saga or the sweep retries. That is a loss of availability, not of
  safety, and `MigrationCompletes` and `SagaCompletes` hold under it.
  Production symbols: `ShardRootGrain.ThrowIfRejectedForKey`.
- **Time.** No timers or deadlines beyond the compaction guards above.
  No production symbol: the gap is the model's absence of time, not a production
  mechanism.

## Territory owned by other open issues

No open issue currently owns a claim here. The section stays, empty of owners,
so a later census can see the question was asked; re-populate it when an open
issue next takes ownership of a claim made here.
