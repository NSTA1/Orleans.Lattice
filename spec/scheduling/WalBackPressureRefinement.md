# Refinement note: WAL back-pressure control loop

## Model scope in one paragraph

The cached per-silo classifier combines partition-local depth with tree-wide
trip sums and sustained flush, drain-lag and durable-pin-latency windows. Samples
publish a worst-state roll-up and a tree-wide recovery floor. A finite callback
pass visits the ordinary writer, saga quiesce, snapshot opener, receiver policy
and view maintainer; relief actors complete independently of the signal.
Aliases distinguish physical writer/saga/view identities from unobserved logical
snapshot/receiver names. Shutdown terminates the pass. No wall-time throughput,
observer-delivery deadline or deterministic fairness from jitter is asserted.

Composition with WalAdmission is at the signal-decision seam: Admission in
this wait-for graph means GateOnSaturationAsync, not a capacity reservation.
Signal release, downstream capacity admission and storage completion remain
distinct; terminal relief bypasses only the first and pacing. The actual
writer controls retain the latter two. WalAdmission's independently variable
acute partition inputs are a conservative admission abstraction; this
classifier instead uses the production tree-wide acute trip sums.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `input`, `sampledCause` | Sample inputs and ordered cause telemetry. | `WalSaturationSampler.SampleOnceAsync` and `AttributeCause`. |
| `verdict`, `treeVerdict` | Cached partition and tree classifications. | `WalSaturationSignal.UpdatePartitionState` and `UpdateState`. |
| `recovery`, `history` | Sample-unit recovery age and bounded falling-edge history. | `WalSaturationSampler.SampleOnceAsync`. |
| `consumer` | Offered-load response and refusal provenance. | `WalCommitLogWriter.GateOnSaturationAsync` and `PaceOnThrottleAsync`. |
| `saga`, `epoch`, `snapshotUsable` | Captured participant scope, resumable outcome and routing refresh. | `AtomicWriteGrain.QuiesceOnSaturatedAsync`. |
| `aliased` | Logical name has no published physical-copy verdict. | `WalSaturationSignal.GetCurrentState`. |
| `shuttingDown` | Terminal host drain. | `WalCommitLogWriter.DrainAsync`. |
| `phase`, `remaining`, `reliefDone`, `waitsOn` | Finite opportunities, finite offered bursts, completed relief and dependency edges. | `WalSaturationSampler.SampleOnceAsync` and `AtomicWriteGrain.RunSagaAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `CauseOn` | Environment introduces classifier input. | `WalSaturationSampler.SampleOnceAsync`. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_returns_Saturated_when_dispatch_timeout_delta_crosses_threshold`. |
| `CauseOff` | Environment stops the input. | `WalSaturationSampler.SampleOnceAsync`. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_fires_recovery_transition_with_previous_state_tag`. |
| `Sample` | Classify, roll up, apply hysteresis and publish. | `WalSaturationSampler.SampleOnceAsync`. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_aggregates_worst_case_across_partitions_of_same_tree`. |
| `GateCheck` | Check a physical writer partition. | `WalCommitLogWriter.GateOnSaturationAsync`. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Healthy_does_not_call_WaitForHealthyAsync`. |
| `Pace` | Delay Throttled point/batch writes, not Healthy writes, without saturation refusal. | `WalCommitLogWriter.PaceOnThrottleAsync`. | Yes: `WalCommitLogWriterThrottledPaceTests.AppendAsync_signal_Throttled_never_throws`, `WalCommitLogWriterThrottledPaceTests.AppendAsync_signal_Throttled_paces_the_local_dispatch`, `WalCommitLogWriterThrottledPaceTests.AppendManyAsync_signal_Throttled_paces_the_batched_dispatch` and `WalCommitLogWriterThrottledPaceTests.AppendAsync_signal_Healthy_does_not_pace`. |
| `ParkOrRefuse` | Remember the verdict that caused the bounded wait. | `WalCommitLogWriter.GateOnSaturationAsync`. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_past_budget_throws_LatticeSaturatedException`. |
| `ReReadAfterExpiry` | Re-read before refusing an expired wait. | `WalCommitLogWriter.GateOnSaturationAsync`. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_but_recovered_at_recheck_suppresses_refusal`. |
| `SagaQuiesce` | Wait only on captured partitions, with tree fallback. | `AtomicWriteGrain.QuiesceOnSaturatedAsync`. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`. |
| `SagaRefuse` | Preserve Execute progress without compensation. | `AtomicWriteGrain.QuiesceOnSaturatedAsync`. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`. |
| `SagaReRoute` | Refresh routing before redispatch. | `AtomicWriteGrain.QuiesceOnSaturatedAsync`. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routing_epoch_change_during_quiesce_refreshes_snapshot_before_dispatch`. |
| `ShedOpen` | Shed only a Saturated logical lookup. | `LatticeGrain.OpenSnapshotCursorAsync`. | Yes: `LatticeGrainTests.OpenSnapshotEntryCursorAsync_sheds_only_the_observed_logical_Saturated_tree_with_its_own_source`. |
| `ReceiverShrinkBatch` | Preserve Healthy offered load, reduce Throttled batches and drip feed Saturated. | `WalSaturationReceiverFlowControlPolicy.EvaluateAsync`. | Yes: `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_returns_none_when_tree_is_healthy`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_throttled_halves_batch_and_requests_short_pause` and `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_saturated_drip_feeds_and_requests_long_pause`. |
| `ViewShrinkBatch` | Preserve Healthy and reduce both Throttled and Saturated batches of the bound physical source. | `ViewMaintainerGrain.DrainAsync`. | Yes: `ViewMaintainerBackpressureIdentityTests.Drain_obeys_saturation_of_the_bound_physical_log`. |
| `Relief` | Complete signal-independent progress. | `WalShardGrain.FlushAsync`, `WalShardGrain.OnDeactivateAsync`, `BPlusLeafGrain.ReplayWalSinceCheckpointCoreAsync`, `LatticeWalGc.RunOnceAsync`, `BPlusLeafGrain.OnCoverageLagTimerTickAsync`, `WalMaterialiserPinGrain.ReportAsync`, `WalCommitLogWriter.AppendAsync`, and `AtomicWriteGrain.RunSagaAsync`. | Yes: `WalShardGrainTests.Flush_and_drain_relief_ignore_signal_pressure`, `WalShardGrainTests.Drain_relief_settles_a_pending_storage_flush_without_a_signal_release`, `BPlusLeafGrainTests.Materialiser_drain_relief_replays_and_persists_without_a_signal_release`, `LatticeWalGcDurablePinFloorTests.Gc_trim_relief_runs_while_saturated_and_retains_the_durable_floor`, `BPlusLeafGrainTests.Checkpoint_relief_persists_and_publishes_its_pin_while_saturated`, `WalMaterialiserPinGrainTests.Durable_pin_relief_persists_while_the_signal_is_not_healthy`, `WalCommitLogWriterAdmissionSaturationTests.Terminal_relief_choices_never_depend_on_a_saturation_or_pace_release`, and `AtomicWriteGrainTests.Terminal_resumption_choices_never_wait_for_pressure`. |
| `ReliefComplete` | Finish a finite set of relief opportunities. | `WalCommitLogWriter.AppendManyAsync`. | Yes: `WalCommitLogWriterAdmissionSaturationTests.Terminal_relief_preserves_partition_capacity_and_storage_dispatch`. |
| `ShutdownDrain` | End gate waiting with shutdown, not another saturation park. | `WalCommitLogWriter.DrainAsync`. | Yes: `WalCommitLogWriterAdmissionSaturationTests.Terminal_relief_still_observes_writer_shutdown`. |
| `NextRound` | Environment offers the next bounded callback pass. | `WalSaturationSampler.SampleOnceAsync`. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_recovery_window_falls_back_to_Healthy_after_window_expires`. |
| `Stutter` | No observable step. | No production step. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
| --- | --- | --- |
| `SaturatedLatencyOneTick` | A sampled acute trigger is not delayed by recovery hysteresis. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_returns_Saturated_when_dispatch_timeout_delta_crosses_threshold`, `WalSaturationSamplerTests.SampleOnceAsync_attributes_each_cause_in_priority_order_and_floor_only_None` and `WalSaturationSamplerTests.SampleOnceAsync_sums_acute_trip_deltas_before_publishing_every_partition`. |
| `TotalOrderAndRollUp` | Tree depth is the worst partition; tree-wide acute trip sums reach every published partition. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_aggregates_worst_case_across_partitions_of_same_tree` and `WalSaturationSamplerTests.SampleOnceAsync_sums_acute_trip_deltas_before_publishing_every_partition`. |
| `PartitionScopedRefusal` | An unrelated busy partition cannot gate a plain writer. | Yes: `WalSaturationCrossPartitionBlockingTests.AppendAsync_to_idle_partition_is_admitted_while_a_different_partition_is_saturated`. |
| `NoSpuriousRefusal` | Writer and saga expiry re-read their current scoped verdict. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_but_recovered_at_recheck_suppresses_refusal` and `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`. |
| `ThrottledNeverRefuses` | Throttled pacing and snapshot admission do not refuse for saturation. | Yes: `WalCommitLogWriterThrottledPaceTests.AppendAsync_signal_Throttled_never_throws` and `LatticeGrainTests.OpenSnapshotEntryCursorAsync_sheds_only_the_observed_logical_Saturated_tree_with_its_own_source`. |
| `SagaRefusalPreservesProgress` | Saturation preserves Execute and NextIndex without compensation. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`. |
| `SagaQuiesceWinsDispatch` | Effective quiesce budget does not exceed dispatch budget. | Yes: `AtomicWriteGrainTests.ExecuteAsync_quiesce_uses_dispatch_budget_before_shutdown_watchdog`. |
| `SagaScopedToCapturedPartitions` | Captured scope or fail-closed tree fallback controls quiesce. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`. |
| `RoutingSnapshotFresh` | Routing epoch changes refresh participants before redispatch. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routing_epoch_change_during_quiesce_refreshes_snapshot_before_dispatch`. |
| `ReliefNeverGated` | Flush, materialiser drain, GC, checkpoint/pin and terminal/resume relief never wait on the signal. | Yes: `WalShardGrainTests.Flush_and_drain_relief_ignore_signal_pressure`, `BPlusLeafGrainTests.Materialiser_drain_relief_replays_and_persists_without_a_signal_release`, `LatticeWalGcDurablePinFloorTests.Gc_trim_relief_runs_while_saturated_and_retains_the_durable_floor`, `BPlusLeafGrainTests.Checkpoint_relief_persists_and_publishes_its_pin_while_saturated`, `WalMaterialiserPinGrainTests.Durable_pin_relief_persists_while_the_signal_is_not_healthy`, `WalCommitLogWriterAdmissionSaturationTests.Terminal_relief_choices_never_depend_on_a_saturation_or_pace_release` and `AtomicWriteGrainTests.Terminal_resumption_choices_never_wait_for_pressure`. |
| `NoBackPressureCycle` | Checkpoint and terminal relief cannot wait on the signal they help relieve. | Yes: `BPlusLeafGrainTests.Checkpoint_relief_persists_and_publishes_its_pin_while_saturated` and `WalCommitLogWriterAdmissionSaturationTests.Terminal_relief_choices_never_depend_on_a_saturation_or_pace_release`. |
| `CauseAttributed` | Every sampled cause follows priority order; a recovery-only floor carries None. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_attributes_each_cause_in_priority_order_and_floor_only_None`. |
| `RefusalSourceAttributed` | Refusal identifies its writer, saga or snapshot-open seam. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_past_budget_throws_LatticeSaturatedException`, `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress` and `LatticeGrainTests.OpenSnapshotEntryCursorAsync_sheds_only_the_observed_logical_Saturated_tree_with_its_own_source`. |
| `ShutdownTerminal` | Writer shutdown remains terminal, including relief. | Yes: `WalCommitLogWriterAdmissionSaturationTests.Terminal_relief_still_observes_writer_shutdown`. |
| `AliasedTreeUnshielded` | Receiver lookup uses its supplied logical name rather than an observed physical copy. | Yes: `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_uses_the_supplied_logical_name_not_an_observed_physical_copy`. |
| `ConsumerBatchesRespond` | Receiver and physical projection/aggregation views apply Healthy, Throttled and Saturated responses. | Yes: `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_returns_none_when_tree_is_healthy`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_throttled_halves_batch_and_requests_short_pause`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_saturated_drip_feeds_and_requests_long_pause` and `ViewMaintainerBackpressureIdentityTests.Drain_obeys_saturation_of_the_bound_physical_log`. |
| `NoFlapping` | Positive recovery holds prevent direct falling edges; zero disables a recovery-only floor. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_recovery_window_upgrades_Healthy_to_Throttled_within_window` and `WalSaturationSamplerTests.SampleOnceAsync_recovery_window_zero_disables_the_upgrade`. |
| `StickyFloor` | Infinite recovery holds a previously saturated tree at least Throttled. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_recovery_window_infinite_holds_Throttled_forever_after_Saturated`. |
| `BackPressureClears` | Finite depth drains when acute inputs stop; Healthy additionally requires no sustained throttling input and finite recovery. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_fires_recovery_transition_with_previous_state_tag`, `WalSaturationSamplerTests.SampleOnceAsync_recovery_window_falls_back_to_Healthy_after_window_expires` and `BPlusLeafGrainTests.Materialiser_drain_relief_replays_and_persists_without_a_signal_release`. |
| `OfferedLoadResponds` | Non-relief producers apply the response on a finite callback opportunity. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_past_budget_throws_LatticeSaturatedException`, `LatticeGrainTests.OpenSnapshotEntryCursorAsync_sheds_only_the_observed_logical_Saturated_tree_with_its_own_source`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_saturated_drip_feeds_and_requests_long_pause` and `ViewMaintainerBackpressureIdentityTests.Drain_obeys_saturation_of_the_bound_physical_log`. |
| `SagaEventuallyCompletesOrRefuses` | A quiesce refusal is resumable after recovery. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Model-domain sanity, not a production representation. Independently targeted by paired mutations. |

## Property classification (issue #2321)

State invariants check domain, quiesce bounds, relief enabling, acyclicity and
finite/Infinite hysteresis. Action properties inspect the exact sample or
consumer boundary, not inputs that the environment changed after publication.
BackPressureClears, OfferedLoadResponds and SagaEventuallyCompletesOrRefuses
are conditional temporal progress. Larger variants retain every safety check;
these temporal checks stay at the base bound to bound TLC's temporal graph cost.

All mutations perturb their declared Next actions. NoFlappingNoHysteresis and
StickyFloorInfiniteSentinelIgnored change Sample's partition and tree
publication, omitting the respective recovery floor. No experiment adds a new
transition. Stutter is the only
non-behavioural action and has its own action row.

## Consumer regime audit

| Consumer | Healthy | Throttled | Saturated |
| --- | --- | --- | --- |
| Ordinary writer | No gate wait or pacing. | Configured point/batch pacing, never a saturation refusal. | Scoped bounded wait and an expiry re-read before refusal. |
| Saga | No quiesce wait. | No initial wait; expiry recovery to Throttled proceeds. | Captured partitions or fail-closed tree fallback; resumable refusal, refreshed routing before redispatch. |
| Snapshot open | Admit. | Admit. | Shed before capture when enabled, with SnapshotCursorOpen source. |
| Receiver | No preference, preserving offered load. | Ratio-scaled batch and short pause hint. | Drip-feed batch and long pause hint. |
| Projection/aggregation view | Full batch. | Ratio-scaled batch. | Configured drip-feed batch. |

The model's 4/2/1 batch values preserve these three regimes, not the concrete
configured production sizes. Pause hints and writer pacing are responses,
not wall-time fairness premises. The quiesce-budget detector enters the
real coordinator with a short dispatch deadline and an independent shutdown
cleanup watchdog; it requires the dispatch timer to win. Bypassing the budget
helper at its production call site fails this detector while all eight pure
finite/zero/Infinite helper cases remain green. The watchdog is a test guard,
not a claimed production latency SLA. These are enabled-signal/gate/batching
configurations; real controls also cover absent signals, disabled pacing,
snapshot opt-out, cancellation and downstream capacity/storage deadlines.
View cases execute both actual drain implementations for all three regimes.
Receiver controls assert both batch and pause hints. Separate point/batch
writer controls assert an incurred delay, not just successful completion.

## Detectors

The saga Coyote model executes the real coordinator with controlled choices of
initial Healthy/Throttled/Saturated captured/sibling pressure, tree fallback and expiry recovery to Healthy,
Throttled or Saturated. It observes real Execute progress, source attribution,
dispatch count and successful resumption, not a duplicate saga state machine.
The terminal Coyote model executes the real writer over commit/abort,
single-entry and batch paths, and Saturated/Throttled inputs. A production
negative control reinstating both terminal signal dependencies makes its
ReliefNeverGated/NoBackPressureCycle assertion fail; restoring the bypass
restores the passing run. A separate Coyote choice model drives the real
keepalive reminder over persisted Execute terminal tails and Compensate tails
under all three pressure states. It asserts actual commit/abort terminal
delivery, the persisted transaction/outcome, completion without new data, and
no signal wait. Reintroducing QuiesceOnSaturatedAsync before terminal
resumption fails both Saturated controls and this Coyote detector while the
four Healthy/Throttled controls pass. No CLR-thread schedule exploration is
claimed.

## Defect mutations whose fixes have landed

The #4986 terminal relief control reinstates the mechanism repaired in this member:
prepared writes can pin progress while their terminal waits on the same signal.
The real writer regression first failed for all four commit/abort point/batch
cases; mixed-data refusal and shutdown controls already passed. The repair
classifies an entire caller batch before fan-out and bypasses only signal
admission/pacing, never capacity, storage acknowledgement, deadlines,
cancellation or shutdown.

#4990 repairs ordered comment lexing in the refinement symbol resolver. XML
line comments containing a path ending in `/*` previously hid a real later
OnDeactivateAsync declaration. Two delimiter controls failed while one passed;
single-pass ordered stripping restores all 17 resolver controls.

#4994 repairs the detector gap-citation gate. Actual resolvable None/Partial
method identifiers in Yes cells produced two false failures. Six full-gate
controls retain genuine uncited-gap failures and cited-gap acceptance; the
fix classifies the leading coverage verdict, never substrings of an identifier.

The source/model audit also repaired this draft's missing Throttled view
reduction. Its new isolated paired mutation checked clean on the old model,
correctly failing certification. Projection and aggregation production
perturbations failed with 256 instead of 128 while Saturated controls passed.
Dedicated receiver-Throttled and point/batch pace-skipping controls likewise
reject those regressions without relying on a Saturated-only assertion.
Tree-trip controls reject max-instead-of-sum aggregation for both dispatch
and provider trips, observing tree and every partition publication.
All temporary production perturbations are removed from the delivered code.

## Corrections to the draft

Fair sampling does not imply a wall-time interval bound. Acute latency means
the next actual sample after a qualifying input, including any configured
sustained-window threshold. The sampler awaits observers and delays between
passes. The Infinite sentinel is encoded as 99, outside finite-window values.
The zero sentinel disables the floor. NoFlapping checks both its bounded
falling-edge census and the stronger positive-age floor.

Only raw Healthy tree classifications receive the tree-wide recovery floor.
A depth-throttled sibling alone never floors an idle partition. Provider and
dispatch counters are tree-wide sums, not provider counters independently
owned by each partition. Sampled cause telemetry is captured before the floor;
floor-only Throttled has None. LatticeSaturatedException has TreeId and
SaturationSource but no Cause API. The model abbreviations map in order to
the actual enum members DispatchTimeouts, ProviderFailures, FlushLatency,
AdmissionDepth, MaterialiserDrainLag and MaterialiserPinLatency. None is the
recovery-floor-only case, not an exception source.

Saga and view consumers bind to physical source identity. Only the supplied
logical-name snapshot/receiver lookup is unshielded in the alias arm. A
Throttled result at saga expiry permits dispatch even though its normal
quiesce wait asks for Healthy. An epoch refresh is a redispatch boundary.
A still-Saturated saga cannot take SagaRefuse before its bounded wait expires.
Partition vertices in the wait-for relation distinguish the captured saga
partition from the ordinary writer's partition. The checked configurations
use two or three partition identities, asserted by the model. Model partitions
are one-based; production partitions are zero-based. Model refused/waiting
are callback outcomes, not new persisted saga phases: production preserves
Execute and its NextIndex so the next invocation can resume.

SagaResume relief means resuming the terminal-delivery tail of a prepared
commit or an abort, not exempting new or replayed data dispatch. Production
reaches that tail through RunSagaAsync; the reminder-driven controls seed
its persisted binding, routing epoch and captured partitions consistently.
Ordinary Execute resumption with remaining data is still a producer subject
to captured-partition quiesce. Its recovered retry is not the detector for
ungated terminal resumption.

Flush drains the bounded outstanding admission cohort; materialiser drain
replays the bounded WAL cohort and advances its checkpoint. The latter is
`BPlusLeafGrain.ReplayWalSinceCheckpointCoreAsync`, not the separate WAL
storage shutdown-drain. Both have real relief controls. GC trim, checkpoint,
durable-pin writes, saga terminals and saga resumption have independent
completion bits, but do not manufacture disappearance of a sampled latency
fault. In particular, a terminal releasing a retention pin does not decrement
the durable-pin write-latency counter.

BackPressureClears assumes all acute inputs eventually stop, including
sustained FlushLatency, not just provider/dispatch faults. Finite new bursts
and actual flush opportunities then clear admission depth, so Saturated
cannot persist. Healthy additionally requires the throttling inputs to stop
and a finite recovery window; persistent slow pin writes or lag may
legitimately retain Throttled. These are the draft's conditional guarantees,
not a storage-throughput or elapsed-time guarantee. A no-op Stutter is
explicit, so the checkpoint-cycle mutation needs no deadlock-check opt-out;
it violates the acyclic relation directly.

## Deliberate abstraction gaps

The finite pipeline represents callback opportunities, not a claimed CLR turn
order among independent callers. Opportunity fairness is an environment
callback/sampler opportunity premise; the gate and batching decisions inside
it have no independent fairness assumptions. Relief completion order is
enumerated, but provider-internal retries, storage service rates and the
protocol safety of WAL/checkpoint/pin records remain in their owning models.
The bounded classifier episodes use one shared sustained-window threshold
and simultaneous onset for causes in an episode. This does not assert that
production counters or their configurable windows share histories. Real
sampler regressions cover cause priority and each production window; the
larger two-cause variant is a bounded overlap check, not exhaustive coverage
of arbitrary independent signal histories.
ReceiverBatch, ViewBatch and Shed are the response operators for later
composition; those independent scheduling modules are not implemented here.
