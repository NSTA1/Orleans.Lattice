# Independent-clock ready-closure refinement

This is a documented bounded abstraction, not a machine-checked refinement
proof. Detector rows map bounded production seams with paired controls. A finite model or final-value check alone does not establish implementation refinement.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `read`, `ckpt` | Bounded subscription progress and unresolved-entry holdback | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`, `ViewCheckpointState.AppliedOffsets`. |
| `prepares`, `terminals` | Per-batch index/shard completeness | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`. |
| `done` | Durable effect/idempotency identity, not a new persisted maintainer field | `ViewMaintainerGrain.MarkResolved`; backing atomic-write idempotency is abstracted with the effect. |
| `value`, `history` | Current projected rows or revision-addressed history | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`, `HistoryLatticeViewProjection`. |
| `horizon` | Source-confirmation read entitlement | `ViewMaintainerGrain.ConfirmStagedBatchAgainstSourceAsync` captures heads after coherent source observation; consumed positions must cover those heads before omission. |
| `snapshots`, `historySnapshots` | One consumer's observable complete read cut | `ILattice.GetManyAsync`; individual reads, not cross-view simultaneity. |
| `accumulative`, `crashes` | Registration branch and bounded failure environment | `ViewRegistration.Accumulative`; finite crash counter is a modelling device. |
| `deferred` | External eligibility hold on a staged replacement despite consumed heads | SourceAdmission rely-guarantee precondition; not an inference from terminal completeness. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Read` | Consume one partition entry and stage prepares/distinct terminals | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_complete_prepares_require_every_distinct_shard_before_atomic_flush` and `ViewMaintainerPhysicalStagingTests.Drain_unresolved_multipart_batch_holds_every_prepared_checkpoint` distinguish complete/distinct terminals from consumed entries. |
| `Capture` | Arm post-source-confirmation head evidence before allowing omission | `ViewMaintainerGrain.ConfirmStagedBatchAgainstSourceAsync`, `SampleHeadOffsetsAsync`. | Yes: `ViewMaintainerReplayFlushTests.Source_confirmation_consumes_the_captured_replacement_horizon_before_omitting_a_key` advances the head inside the logical source read; sampling before observation fails while its fixed-head control passes. |
| `Publish` | Fold exact per-key stamps and emit one closed atomic slice | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`; candidate local closure replaces serial batch flips. | Yes: `ViewMaintainerReplayFlushTests.Crossed_prepare_clock_dependencies_publish_the_complete_local_closure_atomically`, `MaterialisedViewReadyClosureSnapshotTests.Crossed_prepare_clocks_publish_one_complete_winner_cut_in_public_GetMany` and `MaterialisedViewReadyClosureSnapshotTests.Public_snapshot_detector_rejects_split_crossed_clock_closure`. |
| `HistoryPublish` | Atomically retain both revisions without winner filtering | `HistoryLatticeViewProjection`, `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`. | Yes: `MaterialisedViewHistoryReadAtomicTests.Committed_history_batch_never_splits_its_revision_pair_in_one_public_GetMany_snapshot` and its split-publication detector control exercise the actual outgoing atomic history write. `MaterialisedViewHistoryRebuildCompositionTests.Rebuild_captured_floor_keeps_post_floor_revisions_after_lost_response_and_reactivation` retains ordinary and overlapping committed revisions across response loss and reactivation; skipping ordinary revisions in the production drain callback makes this positive fail. |
| `Checkpoint` | Persist only resolved progress or a floor below unresolved entries | `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_unresolved_multipart_batch_holds_every_prepared_checkpoint` and `ViewMaintainerPhysicalStagingTests.Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals` fail under the actual durable-holdback caller bypass. |
| `Crash` | Lose volatile staging and replay; retain completed atomic effects | `ViewMaintainerGrain.ArmReplayFence`, `ViewMaintainerGrain.MarkResolved`. | Yes: `ViewMaintainerReplayFlushTests.A_transitive_ready_closure_revisits_earlier_candidates_and_replays_one_complete_effect` loses the publication response, retains checkpoint -1, reactivates the maintainer, and reattaches one complete operation identity. Prepublication replay is separately covered by `ViewMaintainerPhysicalStagingTests.Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals`. |
| `Observe` | Publicly read one complete local cut, independent of other consumers' lag | `ILattice.GetManyAsync`. | Yes: `ViewMaintainerReplayFlushTests.Crossed_prepare_clock_dependencies_publish_the_complete_local_closure_atomically`, `MaterialisedViewReadyClosureSnapshotTests.Crossed_prepare_clocks_publish_one_complete_winner_cut_in_public_GetMany` and `MaterialisedViewReadyClosureSnapshotTests.Public_snapshot_detector_rejects_split_crossed_clock_closure`. |
| `Release` | Fair external progress makes the held replacement eligible without changing its WAL progress | `ViewMaintainerGrain.GateBatchesOnUnappliedReplacements` keeps external eligibility separate from consumed local heads. | Yes: `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_waits_for_an_unconfirmed_replacement_after_all_heads_are_consumed` consumes both batches completely but holds the replacement while one source row is absent; installing that row without advancing any WAL head releases the complete closure. Disabling the actual non-flushing staged replacement gate publishes j alone and advances its checkpoint; restoration passes. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `CheckpointHeld` | Reading a prepare does not authorize checkpointing past its unresolved identity | Yes: `ViewMaintainerPhysicalStagingTests.Drain_unresolved_multipart_batch_holds_every_prepared_checkpoint` and `ViewMaintainerPhysicalStagingTests.Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals` fail under the actual durable-holdback caller bypass. |
| `SnapshotExact` | Crossed independent clocks publish one correct winner cut, never a scalar-batch or partial-key cut | Yes: `ViewMaintainerReplayFlushTests.Crossed_prepare_clock_dependencies_publish_the_complete_local_closure_atomically`, `MaterialisedViewReadyClosureSnapshotTests.Crossed_prepare_clocks_publish_one_complete_winner_cut_in_public_GetMany` and `MaterialisedViewReadyClosureSnapshotTests.Public_snapshot_detector_rejects_split_crossed_clock_closure`. |
| `HistoryExact` | Accumulative projections retain every committed revision, including low-clock losers | Yes: the two `ViewMaintainerHistoryAdmissionTests.Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay` cases retain low-clock revisions through normal and lost-response replay. `MaterialisedViewHistoryRebuildCompositionTests.Rebuild_captured_floor_keeps_post_floor_revisions_after_lost_response_and_reactivation` checks public post-floor history after rebuild, overlapping commits, a lost atomic-publication response and reactivation; its paired detector control rejects split publication. The boundary is the captured floor: older or trimmed revisions are not promised. |
| `HistoryReadAtomic` | One history batch's two revision rows are not split by a public snapshot | Yes: `MaterialisedViewHistoryReadAtomicTests.Committed_history_batch_never_splits_its_revision_pair_in_one_public_GetMany_snapshot` and `MaterialisedViewHistoryReadAtomicTests.History_snapshot_detector_rejects_split_publication_at_the_captured_atomic_call`. |
| `PublicationEntitled` | Every omitted key's ready replacement dependency closes transitively with head evidence | Yes: `ViewMaintainerReplayFlushTests.A_transitive_ready_closure_revisits_earlier_candidates_and_replays_one_complete_effect` rejects one-pass closure on a deterministic four-batch ring; `ViewMaintainerReplayFlushTests.Source_confirmation_consumes_the_captured_replacement_horizon_before_omitting_a_key` independently rejects unconsumed and pre-observation heads. External eligibility retains its separate Release control. |
| `Converges` | Each fairly scheduled consumer completes independently after finite crashes | Yes: for bounded recovery under the stated fairness/capacity relies: `ViewMaintainerReplayFlushTests.A_transitive_ready_closure_revisits_earlier_candidates_and_replays_one_complete_effect` completes after lost response/reactivation with one effect; `ViewMaintainerReplayFlushTests.Source_confirmation_consumes_the_captured_replacement_horizon_before_omitting_a_key` completes after the actual replacement arrives, and `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_waits_for_an_unconfirmed_replacement_after_all_heads_are_consumed` completes after source readiness changes without new WAL progress. Permanent unavailability is excluded, not proven convergent. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite domain check, not a production behaviour. |

## Evidence and limits
The crossed-clock closure has both actual maintainer call-path and real public
read controls. Replacing `PublishLocalClosureSliceAsync` atomic publication with
sequential production point calls makes the unit snapshot detector fail; its
restored control passes. The integration pair stages two committed batches with
opposed per-key clocks on distinct source shards, holds the actual outgoing
atomic publication, and checks one public `GetManyAsync` result against the
complete winner cut. Its deliberate split publishes one key, holds before the
second, and must fail that same snapshot assertion. The restored pair and the
independent history split-publication pair execute four passing cases, including
two expected-failure controls. HistoryReadAtomic covers indivisible revision-pair
reads only; captured-floor history convergence retains its separate obligation.

The staged-replacement eligibility control distinguishes consumed heads from
source readiness. Both batches and their terminal horizon are already consumed;
the replacement's second source row remains absent. Three drains publish
nothing and retain checkpoint -1. Installing that row alone releases all three
view keys and the final checkpoint. Appending `&& false` to the actual
`WaitsOnNonFlushingBatch` call in `GateBatchesOnUnappliedReplacements` makes
this detector fail on a j-only snapshot and checkpoint 2 (one executed failure,
`parent-staged-replacement-discriminator-red.trx`). Restoring production and
rebuilding passes the new and existing deferred-replacement cases (2/2,
`parent-staged-replacement-discriminator-restored.trx`). The earlier detector
alone stayed green under this mutation because its unread source head supplied
a second gate. This accepts the bounded external eligibility/release seam, not
by itself the entire three-batch transitive closure or crash-convergence claim.

The parent production controls isolate closure iteration and horizon ordering.
`A_transitive_ready_closure_revisits_earlier_candidates_and_replays_one_complete_effect`
uses deterministic transaction IDs and equal maximum prepare clocks so a far
candidate precedes its bridges. Replacing the actual local-publication fixed
point with one iteration fails both normal and lost-response/reactivation
cases (`parent-transitive-ring-ordered-one-pass-red.trx`); restored adjacent
scope passes four (`parent-transitive-ring-restored.trx`). Random candidate
ordering let an earlier one-pass control survive and is not accepted evidence.
The model's three-batch ring and detector's four-batch ring share the transitive
closure seam; neither is a source-wide or cross-tree capture proof.

`Source_confirmation_consumes_the_captured_replacement_horizon_before_omitting_a_key`
observes a newer logical row while its actual replacement event is not consumed.
Bypassing `Covers` exposes a j-only snapshot and checkpoint 2 rather than -1
(`parent-source-confirmation-horizon-bypass-red.trx`). Its advancing-head case
changes the physical head inside the actual source-root read response, so an
actual source mutation sampling heads before that read fails only this case;
the fixed-head case passes (`parent-source-confirmation-early-head-red.trx`,
one failed/one passed). Production is restored byte-for-byte; the complete
ReplayFlush scope passes 51 (`parent-source-confirmation-after-observation-restored.trx`).
All accepted red outcomes are assertion failures with zero timeout, inconclusive
or unexecuted cases. Controls are committed as d9fcad0f3 and 735bd18ee.

The three-batch ring is a reachable committed/installed source prefix. It is
not arbitrary transaction creation or a replacement for source admission.
Each key's prepare/terminal offsets 1..4 are unrelated to source HLCs 10/30.
All current-state batches depend on another ready batch's replacement, so the
whole ring must close before publication. Durable `done` stands for the
backing store's keyed, idempotent effect and history identity, not persistence
of the maintainer's volatile resolved set. One physical identity is fixed; a
reused transaction ID on a different physical log must not inherit this `done`
state. The separate [ResolvedIdentity companion](../view-resolved-identity/README.md)
discharges that bounded scoping obligation (#4895) through actual-tail ownership,
same-log retention and set/eviction-queue discrimination. It does not enlarge
this model or the existing single-transaction Rebind abstraction. Crash after
publication but before checkpoint includes a lost response/replay boundary
without modelling every await.

See the module README's explicit SourceAdmission rely-guarantee contract.
History bypass is a separate registration branch; it does not erase the
current-state admission obligation. The TwoConsumers instance has independent
positions and snapshot cuts; nothing requires equal progress or simultaneous
completion. Coordinator degraded paths and cross-tree atomic-write internals
are outside this local closure model.
