# Refinement note: ShardOwnershipCutover to production

This note maps the TLA+ specification in
[`ShardOwnershipCutover.tla`](ShardOwnershipCutover.tla) to Orleans.Lattice as
it exists in code. It is the fourth module in this area, alongside
`ShardOwnership` ([`Refinement.md`](Refinement.md)), `ShardOwnershipRetention`
([`RefinementRetention.md`](RefinementRetention.md)) and `ShardOwnershipCrdt`
([`RefinementCrdt.md`](RefinementCrdt.md)).

It is a **documented mapping, not a machine-checked refinement proof**. The
Detector column names the production test that goes red if the behaviour a row
abstracts regresses. Every detector added with the module was shown red against
a perturbation of the seam it pins; the pull request that added the module holds
the log.

## What this module is for

`ShardOwnership` checks an atomic saga bound to one physical copy across an
online resize. In a resize the copy the saga leaves is the resize source, which
mirrors everything it takes into the destination, so the saga stays bound
(#4369). A local shadow-cutover restore also moves the alias, but the copy it
retains mirrors nowhere. A saga part way through its dispatch therefore re-binds
to the restored copy and re-dispatches its whole batch there. The prepares it
already took on the previous copy are left behind as buckets of a saga that goes
on to commit. A stale reader is served the previous copy until the restore arms
its redirect, and every reader is served it after a revert. Either reader can be
served the batch torn.

`BackupCutover` (spec/backup/) checks the restore's own protocol: the alias and
the map move together, stale routing heals, the alias reservation holds, and a
crash is retried. It has no writers. This module takes the restore's four steps
as its environment, adds the saga, and checks that no reader is served the
saga's batch torn across the cutover or its revert.

## The base module models the intended design where production has an open defect

**#4689 (open).** When the saga re-binds away from a copy, production
(`AtomicWriteGrain.RebindToAsync`, from `AtomicWriteGrain.TryRebindToResolvedCopyAsync`
and `AtomicWriteGrain.RebindAcrossAliasSwapAsync`) records nothing about the
copy it leaves and never discards the prepares it took there. A revert then
serves `(new, old)` while the decision's tombstone still reads committed, and
`k1` reads as absent once the tombstone's retention lapses. This was reproduced
by running it, and the reproduction is recorded on the issue.

The base models the fix the issue specifies:

- the re-bind records the copy it leaves (`left`);
- after the pre-decision check, the saga discards its prepares on every such
  copy, through a call addressed to the physical copy, which a retained redirect
  admits (`Discard`);
- the decision waits until every discard is acknowledged (`Decide`);
- the leaf remembers the discard as it remembers a terminal, so a routed prepare
  still on the wire is refused (`Land`).

Three mutations reproduce production. `AtomicAcrossCutoverDecideSkipsDiscard`
removes the wait. `AtomicAcrossCutoverRebindForgetsLeftCopy` and
`CommittedBatchOnBoundCopyRebindBeforeDecisionForgets` are the two re-bind paths
as they stand. The rows below that rest on the fix read `Partial (#4689)`. Each
one names the coverage of the production seam the fix will extend. When the fix
lands, the row is retired and its detector proven red against the reproducing
mutation.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alias` | The copy the logical tree resolves to | The tree registry's alias, moved with the map by `AliasCutoverShardMaps.SwapCutoverAsync` and `AliasCutoverShardMaps.RevertAsync`. |
| `pub` | The pairs a routing activation may hold | Every pair `LatticeGrain.GetRoutingAsync` has published; a StatelessWorker activation can hold any of them. |
| `redir[c]` | The retained redirect on a copy | `ShardRootState.RetainedRedirect`, armed by `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync`, cleared by `LatticeBackupRestoreService.ClearRetainedTreeRedirectAsync`, enforced by `ShardRootGrain.ThrowIfRetainedRedirect`. |
| `rp` | The restore and its revert | `LatticeBackupRestoreService.RestoreAsync` (`LatticeRestoreMode.ShadowCutover`) and `LatticeBackupRestoreService.RevertRestoreAsync`. |
| `row[c][k]` | The committed row | The leaf projection row for the key on that copy. |
| `pend[c][k]` | The saga's prepared bucket | The leaf's pending bucket for the saga's transaction id (`BPlusLeafGrain.AddPreparedMutation`). |
| `term[c][k]` | The leaf remembers the saga's terminal or discard | The leaf's recently-terminal memory, `BPlusLeafGrain.IsLatePrepareForTerminalTransactionAsync`'s first test. The discard's entry is the fix (#4689). |
| `late` | A routed prepare still on the wire | A shard call from `LatticeGrain.SetManyAsync` whose response the saga gave up on, then retried. |
| `sg` | The saga's phase | `AtomicWriteState.Phase` with `AtomicWriteState.NextIndex`. |
| `bound` | The copy the saga is bound to | `AtomicWriteState.BoundPhysicalTreeId`. |
| `prepped` | Keys prepared under the current binding | `AtomicWriteState.NextIndex` over `AtomicWriteState.Entries`, reset by `AtomicWriteGrain.RebindToAsync`. |
| `left`, `disc` | Copies the saga left and the discards acknowledged | Not in production (#4689); the fix records them in the saga's durable state. |
| `dec` | The recorded decision | The decision in the logical tree's registry (`ITxRegistryGrain.GetStatusAsync`). |
| `told` | Shards the terminal broadcast has delivered | `AtomicWriteGrain.MarkOneShardAsync`'s per-shard bookkeeping. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Swap` | The cutover moves the alias and the map onto the shadow | `LatticeBackupRestoreService.RestoreAsync` into `AliasCutoverShardMaps.SwapCutoverAsync`. **Environment action:** not fair, since a restore is a caller's choice; `BackupCutover` checks the step itself. | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_tree_keeps_every_key_readable` and `ShadowCutoverBoundSagaIntegrationTests.A_saga_split_across_a_cutover_rebinds_and_commits_whole_on_the_restored_copy`. |
| `Arm` | The previous copy is armed to redirect routed traffic | `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync` after the swap. **Environment action:** fair, which is `BackupCutover`'s `RestoreReturns`. | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_aliased_tree_arms_every_retained_shard`. |
| `RevertSwap` | The revert moves the alias and the map back | `LatticeBackupRestoreService.RevertRestoreAsync` into `AliasCutoverShardMaps.RevertAsync`. **Environment action:** not fair, since a revert is a caller's choice. | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_of_a_resharded_tree_keeps_every_original_key_readable`. |
| `RevertArm` | The revert clears the previous copy's redirect and arms the shadow | `LatticeBackupRestoreService.ClearRetainedTreeRedirectAsync` then `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync`. **Environment action:** fair, which is `BackupCutover`'s `RevertReturns`. | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_arms_every_shard_of_the_restored_copy_to_redirect_back` and `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |
| `Prepare(k)` | One key's prepare, placed on the bound copy and refused by a retained redirect | `AtomicWriteGrain.ExecutePhaseAsync` into `LatticeGrain.SetManyAsync` under the saga's binding, which refuses a copy other than the bound one (`SagaCopyBinding.DispatchCopy`). The bound copy's shard refuses a routed prepare while a retained redirect is armed (`ShardRootGrain.ThrowIfRetainedRedirect`): unlike a resize fence, the redirect admits no bound saga. A prepare whose first attempt the saga gave up on stays on the wire (`late`). **Over-approximation:** one key per step. | Yes: `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_to_a_retained_copy_is_refused_when_routed_through_the_alias` (red with the redirect gate lifted), `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy` and `LatticeGrainTests.SetManyAsync_under_a_saga_binding_rereads_a_pair_cached_before_a_swap`. |
| `Land(c, k)` | A straggling routed prepare reaches its shard | The shard call arriving late. A retained redirect refuses it (`ShardRootGrain.ThrowIfRetainedRedirect`), and so does a leaf that remembers the saga's terminal (`BPlusLeafGrain.IsLatePrepareForTerminalTransactionAsync`). A leaf that remembers the saga's discard is the fix. **Environment action:** not fair, since a straggler may be lost. | Partial (#4689): the leaf has no discard to remember yet. The redirect's refusal is covered by `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_to_a_retained_copy_is_refused_when_routed_through_the_alias`, and the remembered terminal by `BPlusLeafGrainTests.Forwarded_prepare_remembered_as_terminal_is_refused_without_a_registry_call`. |
| `RebindOnRefusal` | A refused dispatch re-binds to the resolved copy | `AtomicWriteGrain.TryRebindToResolvedCopyAsync`. A retained copy mirrors nowhere (`AtomicWriteGrain.BoundCopyMirrorDestinationAsync` answers null), so `SagaCopyBinding.AfterRefusal` returns `Rebind`, and `AtomicWriteGrain.RebindToAsync` re-dispatches the whole batch. Recording the copy it leaves is the fix. | Partial (#4689): production records nothing about the copy it leaves. The re-bind is covered by `ShadowCutoverBoundSagaIntegrationTests.A_saga_split_across_a_cutover_rebinds_and_commits_whole_on_the_restored_copy` (red with `Rebind` turned into `StayBound`), `AtomicWriteGrainTests.ExecuteAsync_rebinds_and_commits_on_the_new_copy_when_its_bound_copy_moved_during_dispatch` and `SagaCopyBindingTests.AfterRefusal_stays_bound_when_the_bound_copy_mirrors_into_the_new_one_and_rebinds_otherwise`. |
| `RebindBeforeDecision` | The pre-decision check re-binds a fully dispatched batch | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` with `SagaCopyBinding.BeforeDecision` returning `Rebind`. Recording the copy it leaves is the fix. | Partial (#4689): production records nothing about the copy it leaves. The re-bind is covered by `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy` and `SagaCopyBindingTests.BeforeDecision_rebinds_when_the_bound_copy_mirrors_nowhere_or_elsewhere`. |
| `Check` | The pre-decision check finds the tree on the bound copy | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` with `SagaCopyBinding.BeforeDecision` returning `Commit`, reached only once `AtomicWriteState.NextIndex` covers every entry. | Yes: `SagaCopyBindingTests.BeforeDecision_commits_when_the_tree_still_resolves_to_the_bound_copy` and `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on`. |
| `Discard(c)` | The saga discards its prepares on a copy it left | The fix (#4689): a discard addressed to the physical copy directly, with no routed-logical stamp, which a retained redirect admits as it admits a direct terminal (`ShardRootGrain.PrepareForTerminalAsync`), applied by the leaf like an abort terminal. | Partial (#4689): no discard exists. The direct address it relies on is covered by `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_directly_to_a_retained_copy_is_applied` (red with the redirect refusing an unstamped call), and the leaf's discard of a bucket by `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_aborted_outcome_also_discards`. |
| `Decide` | The commit decision, once every discard is acknowledged | `AtomicWriteGrain` recording the decision in the logical tree's registry after `AtomicWriteGrain.RebindAcrossAliasSwapAsync`. Waiting for the discards is the fix. | Partial (#4689): the decision does not wait for any discard. Recording it is covered by `AtomicWriteGrainTests.RunSagaAsync_abort_records_the_decision_before_broadcasting_terminals` and `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `Abort` | The batch fails and the saga compensates | `AtomicWriteGrain`'s compensate path once the retry budget is spent. **Environment action:** not fair. | Yes: `AtomicWriteGrainTests.Aborting_saga_broadcasts_its_single_recorded_abort_verdict_to_every_touched_shard`. |
| `Terminal(k)` | One shard of the terminal broadcast, addressed to the bound copy directly | `AtomicWriteGrain.MarkOneShardAsync` into `ShardRootGrain.AppendTxTerminalAsync`, which carries no routed-logical stamp and so passes a retained redirect (`ShardRootGrain.PrepareForTerminalAsync`); the leaf applies it (`BPlusLeafGrain.ApplyTxTerminalAsync`). A routed terminal is still refused. | Yes: `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_directly_to_a_retained_copy_is_applied` (red with the redirect refusing an unstamped call), `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_routed_through_the_alias_is_refused_by_a_retained_copy` (red with the redirect gate lifted) and `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `Stutter` | Quiescence | Not a protocol step: a stuttering successor once the saga is done and nothing is on the wire. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `AtomicAcrossCutover` | No reader is served a copy holding the saga's batch on one key and not the other: neither a fresh reader nor a stale one, whether before the cutover, between its swap and its redirect, or after a revert. | Partial (#4689): production serves a revert torn, and the red reproduction is recorded on the issue. Atomicity on the restored copy is covered by `ShadowCutoverBoundSagaIntegrationTests.A_saga_split_across_a_cutover_rebinds_and_commits_whole_on_the_restored_copy`, and under load (chaos tier, CI only) by `ShadowCutoverAtomicVisibilityChaosTests.Cutovers_and_a_revert_of_a_resharded_tree_never_tear_an_atomic_batch`, which misses the race. |
| `CommittedBatchOnBoundCopy` | Once committed, the saga holds prepared buckets only on its bound copy. | Partial (#4689): production leaves them on the copy it re-bound away from. The binding is covered by `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy` and `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy`. |
| `SagaSettles` | A saga settles however a restore and its revert interleave with it: the copy the alias names never redirects its own saga, and every call the saga needs on a redirected copy is admitted. | Yes: `ShadowCutoverBoundSagaIntegrationTests.A_saga_split_across_a_cutover_rebinds_and_commits_whole_on_the_restored_copy` (red with `Rebind` turned into `StayBound`), `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_directly_to_a_retained_copy_is_applied` and `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Over-approximation arguments

Each environment action is argued in its row. These deliberate
under-approximations are stated so that they are not mistaken for coverage:

- **One restore and one revert.** A second cutover after a revert is a new
  restore with a new shadow. It adds a copy the saga can leave, which the same
  record and discard cover. Production bounds the re-binds of one dispatch
  (`AtomicWriteGrain.TryRebindToResolvedCopyAsync` at most three times), then
  treats a tree whose alias never settles as an ordinary failure, which `Abort`
  covers.
- **One straggler at a time, one key per shard.** A second straggler is
  refused, or recreates a bucket, by the same rule. The discard is modelled per
  copy: production discards per shard, and a straggler that lands between two
  shard discards lands on a shard that has not been discarded yet.
- **The shadow is whole.** The restored copy holds the backup's contents, here
  the pre-saga value on both keys. A backup that captured part of a batch is
  `BackupCapture`'s `BackupSagaConsistent` (spec/backup/).
- **The registry always answers.** The decision stays readable for as long as
  the model runs. In production its tombstone reads committed for
  `TxDecisionRetention` and then `Indeterminate`, and #4689 records both
  symptoms. A shorter answer only removes behaviours from the window the fix
  closes. The mask and retirement themselves are `ShardOwnershipRetention`'s.

## Deliberate abstraction gaps

- **The restore's own crash and retry, the reservation, and the alias and map
  as two values** are `BackupCutover`'s. Its `RestoreReturns` and
  `RevertReturns` are what make `Arm` and `RevertArm` fair here.
- **A resize or split during the cutover** is `ShardOwnership`'s. The two never
  hold a saga's binding at once: a split an alias move retargets is abandoned
  (`TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map`).
- **Leaf reactivation.** The fix makes the discard durable, as #4545 made the
  applied terminal durable, so a reactivated leaf still refuses a straggler. The
  model's leaves never lose memory. The mutation
  `AtomicAcrossCutoverDiscardForgetsTerminal` shows what a discard the leaf
  forgets costs.
- **Time.** No retention windows, deadlines or timeouts.

## Territory owned by other open issues

| Issue | Claim | Detector |
|-------|-------|----------|
| #4689 | A re-bind records the copy it leaves; the decision waits for an acknowledged, remembered discard on every such copy; a revert therefore never serves a re-bound saga torn. | None yet: the model states the design, the mutations `AtomicAcrossCutoverDecideSkipsDiscard`, `AtomicAcrossCutoverRebindForgetsLeftCopy` and `CommittedBatchOnBoundCopyRebindBeforeDecisionForgets` reproduce production, and the issue carries the red reproduction for the fix to adopt. |
