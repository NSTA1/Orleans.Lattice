# Refinement note: materialiser drain scheduling

## Model scope in one paragraph

Five disjoint finite scenarios distinguish online snapshot shard-copy slots,
leaf replay/checkpoint/snapshot/pin progress after admission, feedback-driven
view service, finite-budget WAL-shard shutdown, and rebuild/reconcile over a
split under a stable physical source identity. They deliberately do not turn
three different drain units into one semaphore or one deadline. A scenario
selector is a sum, not concurrent composition of independent components.
The reviewed WalBackPressure ViewBatch operator supplies the consumer policy;
ReplayAdmission represents the completed grant/refusal boundary of ReplayPermits.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `mode`, `phase` | Ghost scenario selector and observable operation stages, not persisted production fields. | `TreeSnapshotGrain.DrainAllShardsOnlineAsync`, `BPlusLeafGrain.ReplayWalSinceCheckpointAsync`, `WalShardGrain.OnDeactivateAsync`, `ViewMaintainerGrain.RebuildAsync`. |
| `queued`, `active` | Online shard-copy work and semaphore owners. | `TreeSnapshotGrain.DrainOnlineSliceAsync`. |
| `remaining`, `work` | Remaining source leaves and leaves visited by one pass. | `TreeSnapshotGrain.CopyShardAsync`, `LatticeOptions.BackgroundDrainLeavesPerPass`. |
| `applied`, `checkpoint` | Highest consumed offset and durably committed replay checkpoint. | `BPlusLeafGrain.ReplayWalSinceCheckpointCoreAsync`, `BPlusLeafGrain.FlushPendingCheckpointAsync`. |
| `covered`, `pin` | Durable snapshot coverage and published trim entitlement. | `BPlusLeafGrain.DurableSnapshotCoverageForPartition`, `BPlusLeafGrain.ResolveDurablePinForPartition`. |
| `checkpointFailed` | At most one injected storage failure, not a retry limit. | `BPlusLeafGrain.RollbackCheckpointCommit`. |
| `lag`, `pressure`, `acute`, `batch` | Unread offsets, sampled regime, independent acute cause and effective service. | `WalSaturationSampler.Classify`, `ViewBackpressure.ScaleBatch`. |
| `outstanding`, `pending` | In-flight flush slots and optional follow-on batch. | `WalShardGrain.DrainInFlightAsync`, `WalShardGrain.OnDeactivateAsync`. |
| `elapsed`, `drainRound`, `faultOwner` | Delivered deadline progress, fresh second budget and attributed expiration. | `WalShardGrain.ForceFaultRemainingSlotsForDrainBudgetExpiry`, `LatticeOptions.WalDrainBudget`. |
| `split`, `route`, `key`, `repairKind` | Live topology, selected root, finite source scan and repair entry point. | `ViewMaintainerGrain.ReadPhysicalSourceRoutingAsync`, `ViewMaintainerGrain.ReadPhysicalSourceValueAsync`, `ViewMaintainerGrain.ReconcileAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Begin` | Enter the selected real operation. | `TreeSnapshotGrain.DrainAllShardsOnlineAsync`, `WalShardGrain.OnDeactivateAsync`. | Yes: `TreeSnapshotGrainTests.DrainAllShardsOnline_respects_MaxConcurrentDrains_cap`, `WalShardGrainTests.OnDeactivateAsync_force_faults_in_flight_slot_when_drain_budget_expires`. |
| `StartDrain` | Acquire an online shard-copy slot before starting work. | `TreeSnapshotGrain.DrainOnlineSliceAsync`. | Yes: `TreeSnapshotGrainTests.DrainAllShardsOnline_respects_MaxConcurrentDrains_cap`. |
| `LeafPass` | Visit at most the configured source leaves in a pass. | `TreeSnapshotGrain.CopyShardAsync`. | Yes: `TreeSnapshotGrainTests.Online_copy_visits_at_most_the_configured_leaves_per_pass`, `TreeSnapshotGrainTests.Online_slice_bounds_each_merge_pass_before_completing_all_source_leaves`. |
| `CompleteDrain` | Release the slot and continue finite queued work. | `TreeSnapshotGrain.DrainShardForSliceAsync`. | Yes: `TreeSnapshotGrainTests.DrainAllShardsOnline_respects_MaxConcurrentDrains_cap`, `TreeSnapshotGrainTests.Online_copy_resumes_from_the_persisted_key_cursor_and_copies_every_entry_once`. |
| `ReplayAdmission` | An admission operation settles as grant or refusal. | `BPlusLeafGrain.ReplayWalSinceCheckpointAsync`. | Yes: `BPlusLeafGrainTests.A_bulk_activation_past_the_bound_is_refused_rather_than_queued`. |
| `ReplayRead` | Read only offsets below the exclusive WAL head. | `BPlusLeafGrain.ReplayWalSinceCheckpointCoreAsync`. | Yes: `BPlusLeafGrainTests.Materialiser_does_not_read_past_newest_entry_below_exclusive_head`. |
| `CheckpointFault` | Failed persist leaves the durable checkpoint unchanged. | `BPlusLeafGrain.RollbackCheckpointCommit`. | Yes: `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint`. |
| `CheckpointPersist` | Persist a consumed replay prefix. | `BPlusLeafGrain.FlushPendingCheckpointAsync`. | Yes: `BPlusLeafGrainTests.Materialiser_advances_checkpoint_after_clean_replay`, `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint`. |
| `SnapshotCapture` | Successful capture banks covering durable state. | `BPlusLeafGrain.RecordDurableSnapshotCoverage`. | Yes: `BPlusLeafGrainTests.OnDeactivateAsync_teardown_tail_publishes_the_post_capture_pin_when_frontier_pin_is_skipped`. |
| `PinPublish` | Publish only min(persisted checkpoint, durable coverage). | `BPlusLeafGrain.FlushDurableMaterialiserFrontierAsync`. | Yes: `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint`, `BPlusLeafGrainTests.OnDeactivateAsync_teardown_tail_publishes_the_post_capture_pin_when_frontier_pin_is_skipped`. |
| `OfferWrites` | Offer at most one row between successful service opportunities, with independent acute pressure. | `ICommitLogReader.GetHeadOffsetAsync`, `WalLogSubscriber.DrainAsync`. | Yes: `WalLogSubscriberTests.Cooperative_view_drains_keep_stable_offered_load_bounded_in_every_pressure_arm`. |
| `Sample` | Lag-only pressure is Throttled; another acute cause may be Saturated. | `WalSaturationSampler.Classify`, `ViewBackpressure.ScaleBatch`. | Yes: `WalSaturationSamplerDrainLagTests.Escalates_to_Throttled_after_consecutive_windows`, `WalSaturationSamplerDrainLagTests.Drain_lag_never_escalates_to_Saturated`, `ViewMaintainerBackpressureIdentityTests.Drain_obeys_saturation_of_the_bound_physical_log`. |
| `ViewPull` | Execute positive per-partition service without gating the consumer on its source. | `WalLogSubscriber.DrainAsync`, `ViewMaintainerGrain.DrainAsync`. | Yes: `WalLogSubscriberTests.Cooperative_view_drains_keep_stable_offered_load_bounded_in_every_pressure_arm`, `ViewMaintainerBackpressureIdentityTests.Drain_obeys_saturation_of_the_bound_physical_log`. |
| `StorageSettle` | Provider completion may settle a slot before expiry; permanent hangs remain possible. | `IWalStorageProvider.AppendBatchAsync`, `WalShardGrain.DrainInFlightAsync`. | Yes: `WalShardGrainTests.OnDeactivateAsync_waits_for_a_live_slot_to_settle_without_a_budget_fault`. |
| `BudgetTick` | Delivered finite-deadline progress, not storage cooperation. | `WalShardGrain.DrainInFlightAsync`. | Yes: `WalShardGrainTests.OnDeactivateAsync_force_faults_in_flight_slot_when_drain_budget_expires`. |
| `DrainFinish` | Natural completion or attributed force-fault; a pending batch gets a fresh budget. | `WalShardGrain.OnDeactivateAsync`, `WalShardGrain.ForceFaultRemainingSlotsForDrainBudgetExpiry`. | Yes: `WalShardGrainTests.OnDeactivateAsync_force_faults_in_flight_slot_when_drain_budget_expires`, `WalShardGrainTests.OnDeactivateAsync_waits_for_a_live_slot_to_settle_without_a_budget_fault`. |
| `SplitTopology` | Environment moves a source key while the physical identity stays fixed. | `TreeRegistryEntry.ShardMap`, `ViewMaintainerGrain.ReadPhysicalSourceRoutingAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Repair_refreshes_the_live_alias_topology_after_a_source_key_moves`. |
| `RefreshSourceRouting` | Read the owning logical row's current topology on each retry. | `ViewMaintainerGrain.ReadPhysicalSourceRoutingAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Repair_refreshes_the_live_alias_topology_after_a_source_key_moves`. |
| `ReadSource` | Retry stale routing, then read the moved key through its current root. | `ViewMaintainerGrain.ReadPhysicalSourceValueAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Repair_refreshes_the_live_alias_topology_after_a_source_key_moves`. |
| `PublishRepair` | Return after a complete rebuild or conservative reconciliation. | `ViewMaintainerGrain.RebuildAsync`, `ViewMaintainerGrain.ReconcileAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Repair_refreshes_the_live_alias_topology_after_a_source_key_moves`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `DrainSlotsRespected` | Online shard-copy parallelism does not exceed MaxConcurrentDrains. | Yes: `TreeSnapshotGrainTests.DrainAllShardsOnline_respects_MaxConcurrentDrains_cap`. |
| `BackgroundPassBounded` | A source-leaf copy pass respects BackgroundDrainLeavesPerPass. | Yes: `TreeSnapshotGrainTests.Online_copy_visits_at_most_the_configured_leaves_per_pass`, `TreeSnapshotGrainTests.Online_slice_bounds_each_merge_pass_before_completing_all_source_leaves`. |
| `NoDrainPastHead` | Replay's upper bound is the newest offset below the exclusive head. | Yes: `BPlusLeafGrainTests.Materialiser_does_not_read_past_newest_entry_below_exclusive_head`. |
| `PinBackedByDurability` | A failed checkpoint cannot grant GC rights above durable state. | Yes: `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint`. |
| `BudgetForceFaultAttributed` | Expiry metrics and failed acknowledgements belong to the affected WAL shard. | Yes: `WalShardGrainTests.OnDeactivateAsync_force_faults_in_flight_slot_when_drain_budget_expires`. |
| `ConsumerBatchesRespond` | Healthy/full, Throttled/scaled and Saturated/drip-feed batches stay positive. | Yes: `ViewMaintainerBackpressureIdentityTests.Drain_obeys_saturation_of_the_bound_physical_log`, `WalLogSubscriberTests.Cooperative_view_drains_keep_stable_offered_load_bounded_in_every_pressure_arm`. |
| `CheckpointAfterApply` | Checkpoint advancement never precedes consumed replay work. | Yes: `BPlusLeafGrainTests.Materialiser_advances_checkpoint_after_clean_replay`, `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint`. |
| `SourceReadUsesLiveTopology` | A successful source read uses the current topology, not a frozen physical registry row. | Yes: `ViewMaintainerRebuildIdentityTests.Repair_refreshes_the_live_alias_topology_after_a_source_key_moves`. |
| `LagBounded` | Offered rows per successful service interval do not exceed minimum service. | Yes: `WalLogSubscriberTests.Cooperative_view_drains_keep_stable_offered_load_bounded_in_every_pressure_arm`. |
| `DrainEventuallyCompletes` | A finite-budget WAL shutdown settles or faults callers even if storage ignores cancellation. | Yes: `WalShardGrainTests.OnDeactivateAsync_force_faults_in_flight_slot_when_drain_budget_expires`. |
| `RebuildProgressAcrossAlias` | Finite split interference cannot strand rebuild/reconcile on stale routing. | Yes: `ViewMaintainerRebuildIdentityTests.Repair_refreshes_the_live_alias_topology_after_a_source_key_moves`. |
| `LeafDrainCompletes` | Settled admission and successful replay/durability completions reach a safe final pin, or admission refuses. | Yes: `BPlusLeafGrainTests.A_bulk_activation_past_the_bound_is_refused_rather_than_queued`, `BPlusLeafGrainTests.OnDeactivateAsync_teardown_tail_publishes_the_post_capture_pin_when_frontier_pin_is_skipped`. |
| `OnlineDrainCompletes` | Finite source leaves and completing copy calls drain the queued shards. | Yes: `TreeSnapshotGrainTests.DrainAllShardsOnline_respects_MaxConcurrentDrains_cap`, `TreeSnapshotGrainTests.Online_copy_resumes_from_the_persisted_key_cursor_and_copies_every_entry_once`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, with its own paired mutations; not production representation. |

<a id="property-classification"></a>

## Property classification (issue #2321)

The six invariants are state safety. ConsumerBatchesRespond,
CheckpointAfterApply and SourceReadUsesLiveTopology are action safety.
The remaining five properties are conditional temporal liveness.
Larger increases consumers, leaves, replay head/batch, budget and source keys.
It retains every state, action and temporal check; the base checks are unchanged.

| Mutation | Classification |
|----------|----------------|
| TypeOKBeginOutsideDomain | Anchored action: Begin. |
| DrainSlotsRespectedCapPlusOne | Anchored action: StartDrain. |
| BackgroundPassBoundedUnboundedPass | Anchored action: LeafPass. |
| OnlineDrainCompletesSlotNeverReleased | Anchored action: CompleteDrain. |
| LeafDrainCompletesAdmissionNeverSettles | Anchored action: ReplayAdmission. |
| NoDrainPastHeadInclusiveHead | Anchored action: ReplayRead. |
| TypeOKCheckpointFaultOutsideDomain | Anchored action: CheckpointFault. |
| CheckpointAfterApplyCheckpointAhead | Anchored action: CheckpointPersist. |
| LeafDrainCompletesSnapshotNeverBanks | Anchored action: SnapshotCapture. |
| PinBackedByDurabilityPinAhead | Anchored action: PinPublish. |
| LagBoundedOfferedRateIgnored | Anchored action: OfferWrites. |
| ConsumerBatchesRespondLagSaturates | Anchored action: Sample. |
| LagBoundedZeroSaturatedBatch | Anchored action: Sample. |
| LagBoundedPullWaitsOnGate | Anchored action: ViewPull. |
| TypeOKStorageSettlementOutsideDomain | Anchored action: StorageSettle. |
| DrainEventuallyCompletesInfiniteDrainBudget | Anchored action: BudgetTick. |
| BudgetForceFaultAttributedWrongOwner | Anchored action: DrainFinish. |
| TypeOKSplitOutsideDomain | Anchored action: SplitTopology. |
| RebuildProgressAcrossAliasFrozenPhysicalMap | Anchored action: RefreshSourceRouting. |
| SourceReadUsesLiveTopologyAcceptStaleRoute | Anchored action: ReadSource. |
| RebuildProgressAcrossAliasCursorReset | Anchored action: PublishRepair. |

## Detectors

The cooperative Coyote case executes the real WalLogSubscriber and
ViewBackpressure cores with completed collaborators. It explores offers and
all pressure arms, checks per-opportunity backlog, then supplies an explicit
finite no-arrival tail. It is choice-space exploration, not rewritten product
thread schedules. Async grain regressions separately cover real permit refusal,
leaf replay, failed persistence, post-capture pins, online copying, WAL expiry,
attributed metrics, and both real rebuild and reconcile entry points.
The topology fixture moves a key on a stale-root response while retaining the
physical WAL identity; it does not replace rebuild with a fake repair machine.

## Executed source-negative controls

Eighteen one-site production perturbations were compiled and executed separately,
then restored byte-for-byte and rebuilt before their clean arm ran. All sixteen
mapped detector methods have an actual failing result, not merely membership in
a filter. The packet retains each assertion/mock-verification failure, source
hash triplet and restored TRX. Compile errors, missing TRXs and zero executed
tests are rejected rather than credited as negative evidence. Parameterized
display names are resolved back to method keys using the assertion stack.

| Production perturbation | Negative failed/executed | Restored passed/executed |
|-------------------------|--------------------------|--------------------------|
| `background-copy-unbounded` | 1/1 | 1/1 |
| `checkpoint-one-ahead` | 1/1 | 1/1 |
| `copy-cap-plus-one` | 1/1 | 1/1 |
| `copy-pass-plus-one` | 1/3 | 3/3 |
| `copy-resume-ignored` | 1/1 | 1/1 |
| `drain-return-before-settlement` | 1/1 | 1/1 |
| `drain-wrong-shard-attribution` | 1/1 | 1/1 |
| `failed-checkpoint-retained` | 1/1 | 1/1 |
| `finite-drain-budget-disabled` | 1/1 | 1/1 |
| `frozen-physical-topology` | 2/2 | 2/2 |
| `inclusive-replay-head` | 1/2 | 2/2 |
| `kept-snapshot-coverage-not-banked` | 1/1 | 1/1 |
| `lag-saturates` | 3/3 | 3/3 |
| `online-slot-not-released` | 1/1 | 1/1 |
| `pin-uses-pending` | 1/1 | 1/1 |
| `post-capture-pin-omitted` | 1/1 | 1/1 |
| `refusal-not-consulted` | 1/1 | 1/1 |
| `zero-saturated-service` | 3/7 | 7/7 |

Issue [#4999](https://github.com/NSTA1/Orleans.Lattice/issues/4999) records two
detector gaps proved here. The old concurrency test released its first wave at
two and stayed green under cap-plus-one; the corrected test parks the whole wave
until it asserts the launch count. The standalone copy-pass tests stayed green
when only the independent online-slice budget was enlarged; the added slice
test observes real merge-pass sizes 2, 2, 1 over five leaves and rejects that
same perturbation. Both histories retain old-green/new-red/restored-green TRXs.

The original TypeOK checkpoint-fault mutation mixed a boolean with an integer
and caused a TLC evaluation error. It now selects a same-typed, out-of-domain
phase, with a genuine single-target invariant-violation trace and a clean control.
The old failure remains in drain-tlc-paired.trx; the repaired trace is in
drain-final-typeok-mutant.log. A later repeated sampler expression also exposed
an ambiguous mutation anchor; widening it to include the Sample-only batch
assignment preserves the invariant and changes only the intended action.
drain-final-tlc-anchor-failure.trx retains that rejected authoring attempt.

## Measured bounds and cost

Both configurations keep all six invariants and eight properties, including
all five temporal checks. Measurements use the pinned TLC 2.19 jar, two visible
CPUs/two workers, one TLC process at a time, ParallelGC and configured Xmx1800m.
Working set is a process high-water mark, not the configured Java heap ceiling.

| Configuration | Distinct/generated states | Process elapsed seconds | Peak working set bytes |
|---------------|---------------------------|-------------------------|------------------------|
| base | 1,076/3,126 | 1.7064307 | 246,595,584 |
| larger | 3,780/11,866 | 2.7617059 | 380,887,040 |

Larger increases consumers, source leaves, replay head/batch, finite budget and
repair keys rather than suppressing a liveness branch. Historical 1,060/3,760
state runs predate the unread-work/age-window correction and are not this
certificate. This module adds no helper module; WalBackPressure remains the
already-certified imported consumer-policy module, unchanged from the explicit
dependency boundary f62ba058f9dab86f587cc9a9dca98d1a1051de4a.

## Defect mutations whose fixes have landed

[#4905](https://github.com/NSTA1/Orleans.Lattice/issues/4905)'s aliased
rebuild/reconcile source reads used stale physical topology after a split.
The live-topology repair landed in
[PR #4909](https://github.com/NSTA1/Orleans.Lattice/pull/4909).
`RebuildProgressAcrossAliasFrozenPhysicalMap` reinstates the stale route in
`RefreshSourceRouting`; `SourceReadUsesLiveTopologyAcceptStaleRoute`
independently accepts the stale read. The real
`ViewMaintainerRebuildIdentityTests.Repair_refreshes_the_live_alias_topology_after_a_source_key_moves`
exercises both rebuild and reconcile: one stale response moves the key, the
retry must use the live root, and the captured physical WAL identity must not
change. The retained `frozen-physical-topology` production bypass fails both cases;
the historical PR's separate scanner bypass stayed green and was not credited
as evidence for this fix.

The offset-based drain-age repair and starvation-drive interleaving are already
baseline. The frozen topology mutation captures the stable-identity/live-map
boundary, not an invented alias cursor epoch. The cursor-reset experiment is
synthetic progress loss and does not claim a historical production defect.
No resolution of the deployment investigation in issue 3481 is asserted.

## Replaced draft assertions

MaxConcurrentDrains limits online shard-copy calls. BackgroundDrainLeavesPerPass
limits source leaves in one pass, not an entire multi-pass slice or grain turn.
WalDrainBudget bounds one WAL-shard in-flight drain; a pending follow-on batch
can receive a second fresh budget. It does not bound all leaf teardown barriers.
InfiniteTimeSpan is an intentional opt-out, outside the finite-budget theorem.
Lag-only feedback is Throttled, never Saturated; acute pressure is independent.
The model's lag is unread work, not the sampler's offset-derived wall-clock age.
Sample abstracts a completed classification into either non-acute arm; it does
not infer an age threshold or consecutive-window verdict from a row count.
The Saturated arm is an independently supplied regime (including retention of
a prior regime), so the service proof covers every actual consumer pressure arm.

Weak fairness means delivered enabled work eventually runs, not bounded latency.
Offered is measured between successful service opportunities, including
configured pauses and storage completion, not per sampler tick. LagWitness
clamps an overflow witness without blocking arrivals. Minimum service is one;
faster offers, permanent storage faults or a never-delivered callback invalidate
the convergence premise. ReplayAdmission assumes admission has settled; cold
replay may legitimately wait until activation-lifetime cancellation, and this
model does not impose a new cold-residency deadline.

## Deliberate abstraction gaps

- **One bounded drain scenario.** No second permit-pool model, cross-scenario
  shared-resource contention, arbitrary crash/restart sequence, byte-level
  snapshot format, saga/CRDT proof, unbounded split interference, or absolute
  time/latency claim. CheckpointFault models one transient failed persist;
  successful subsequent snapshot/pin completions are explicit environment
  premises. Alias cutover to a foreign physical source is covered by the
  physical-source identity regressions, not by this single-split scenario. The
  BP instance reuses the pure consumer operator only; its unrelated closed-loop
  variables are substituted constants, not asserted composition.
  No production symbol: the gap states a bound or premise of the scheduling
  model, not a production mechanism.
