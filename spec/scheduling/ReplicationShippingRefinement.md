# Refinement note: replication shipping opportunities

## Contract and scope

This is a finite physical-source shipping window, not the cross-cluster saga
atomicity protocol. The queue records launch order; accepted acknowledgements
fold FIFO. A rejected head never advances the cursor over a later success.
The cursor claim is scoped to one physical source lineage: explicit rebind and
reseed can legitimately reset it and are covered by the replication modules.
One abstract record stands for one batch acknowledgement obligation. Prepare,
terminal and ordinary-record failure paths remain distinct.

Receiver classification is an input, not a second sampler. The instance calls
the actual `WalBackPressure.ReceiverShrinkBatch` action. Its logical-name lookup
and Healthy/Throttled/Saturated 4/2/1 regimes are retained, including the
unobserved logical-alias case. Shared carrier `anchor` binds only variables
unused by that consumer action; it is not a production state machine or a new
helper module. Batch and pause hints reduce offered load after observation.
They cannot recall batches already launched. The pump-local stop survives later
healthy acknowledgements, including the helper that releases held terminals;
the latest null hint can restore full load on the next pump.

`ObserveBatchOnlyFeedback` separately offers a valid batch-only acknowledgement
on the first head, without inventing a pause. This is an input opportunity over
the optional receiver fields, not a relabelled WAL Healthy verdict or a change
to the actual shared consumer action. Existing receiver policy options may
omit a positive pause, and the real held-terminal regression already exercises
this shape. The finite first-head opportunity is explicit; it does not claim
arbitrary custom-policy histories. A later real Healthy/null acknowledgement
can clear the offered cap, but must not clear this pump's observed-feedback stop.

`EveryDeferralBounded` is conditional, retry-driven progress, not a wall-time
deadline. This is the parent-autopilot scope decision, not a direct user selection, in
[#4967 comment 6391108683](https://github.com/NSTA1/Orleans.Lattice/issues/4967#issuecomment-6391108683).
For an eligible failed record, expiration is observed on a subsequent retry.
Capacity for poison, quarantine and dead-letter storage is independent. Those
three full states are checked fail-closed, not excluded. A decided prepare
cannot safely be poisoned: it remains unacknowledged until application can
succeed. `RepairDecided` explicitly represents that recovery, not a changed
registry decision. A receiver restart loses volatile first-deferral history
but preserves durable poison/quarantine and acknowledged delivery.

The minimal real-code counterexample to the rejected unconditional contract is
three retries with zero timeout against a Committed registry: every result is
Deferred, with no poison or dead-letter entry. Poisoning that prepare could
discard a key of a saga whose commit is already recorded. A full bounded queue
likewise cannot acknowledge a record it did not retain. No overflow protocol,
forced decision rewrite or durable timeout protocol is introduced.

Capacity is an admission predicate, not an unbounded store. `FreeCapacity`
abstracts sufficient stable admission for the remaining finite obligations;
it does not prove progress against arbitrary competing traffic that refills
a freed slot. Each initially full capacity is still checked fail-closed.

Progress assumes finite transient transport faults, fair callback/retry and
elapsed-time opportunities, eventual free capacity, completed reseeding and a
stable receiver lifetime long enough to observe expiry. The finite restart
budget represents a final stable lifetime, not an assertion that crashes stop
in production. Safety checks include the restart before that lifetime. An
unrepaired decided prepare may safely block indefinitely; `StreamProgresses`
requires its application fault to have cleared. Jitter is not fairness.
The no-reseed-cycle claim assumes its durable retired/quarantine marker is
retained: the finite record universe cannot evict the production retired
history's 4096-entry bound. Arbitrary histories that evict that marker are not
certified as permanent no-reseed memory.

The receiver failure submodel begins at the `MaxApplyRetries` threshold; earlier
failed attempts rethrow without starting this clock. The real detectors use a
one-attempt threshold and exercise actual decorator retries.

`age = Timeout + 1` means no first deferral has been observed. `Defer` starts
age at zero without counting retries as elapsed time. `Elapse` independently
advances a coarse elapsed-time abstraction; retries alone do not expire it.
The finite shared window and callback abstraction does not quantify over
arbitrary classifier histories, unbounded restarts, wall-clock scheduling or
changing source identities. Pause countdown is an environment opportunity
after a window drains, not a measured latency bound.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `cursor`, `sent`, `queue` | Source cursor, read-ahead and ordered launched batches. | `ReplicationShipperGrain.PumpPipelinedOnceAsync`, `ReplicationShipperGrain.AdvanceCursorPipelinedAsync`, `ReplicationShipperState.Cursor`. |
| `applied`, `parked`, `accepted` | Application or retained dead-letter acceptance. | `DeadLetterTrackingReplicationApplier.ApplyAsync`, `DeadLetterTrackingReplicationApplier.OnFailureAsync`. |
| `mode` | Durable fresh/poisoned/reseed-retired/quarantined saga state. | `ReceiverSagaPoisonGrain.PoisonAsync`, `ReceiverSagaPoisonGrain.RetireAsync`, `ReceiverSagaPoisonGrain.QuarantineAsync`. |
| `age`, `decided`, `bad` | Volatile first deferral, authoritative decision and application failure. | `DeadLetterTrackingReplicationApplier.TryPoisonTimedOutSagaRecordAsync`, `ITxRegistryGrain.GetRecordedStatusAsync`. |
| `space` | Independent dead-letter, poison and quarantine admission. | `DeadLetterTrackingReplicationApplier.ParkPoisonedSagaRecordAsync`, `ReceiverSagaPoisonGrain.PoisonAsync`, `ReceiverSagaPoisonGrain.QuarantineAsync`. |
| `failures`, `backoff`, `wait`, `faultsLeft` | Finite transport faults and retry budget. | `ReplicationShipperGrain.ApplyBackoff`, `ReplicationShipperGrain.ProcessNextPhaseAsync`. |
| `phase`, `restartsLeft` | Callback phase and bounded receiver reconstruction. | `ReplicationShipperGrain.ProcessNextPhaseAsync`, `DeadLetterTrackingReplicationApplier.OnFailureAsync`. |
| `receiverState`, `aliased`, `controlPhase`, `control` | Offered logical classification/shared consumer result, or separately captured batch-only acknowledgement. | `WalSaturationReceiverFlowControlPolicy.EvaluateAsync`, `ReplicationShipperGrain.DrainOneInFlightAsync`. |
| `batchCap`, `stop`, `pause` | Captured batch cap, pump-local feedback latch and unexpired pause. | `ReplicationShipperGrain.ComputeMaxPerBatch`, `ReplicationShipperGrain.PumpPipelinedOnceAsync`, `ReplicationShipperGrain.DrainOneInFlightAsync`. |
| `anchor` | Inert composition carrier. | No production representation; only binds unused instance variables. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Send` | Launch within the offered window. | `ReplicationShipperGrain.PumpPipelinedOnceAsync`. | Yes: `ReplicationShipperGrainTests.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion`. |
| `SealWindow` | Drain a bounded window before drawing more work. | `ReplicationShipperGrain.PumpPipelinedOnceAsync`. | Yes: `ReplicationShipperGrainTests.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion`. |
| `Apply` | Apply through the real receiver. | `DeadLetterTrackingReplicationApplier.ApplyAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Decided_prepare_recovers_when_application_succeeds_without_rewriting_the_decision`. |
| `TransientFailure` | Reject a transient send without advancing. | `ReplicationShipperGrain.DrainOneInFlightAsync`. | Yes: `ReplicationShipperGrainTests.Retry_without_jitter_preserves_cursor_then_success_resets_backoff`. |
| `Defer` | Retain failed saga delivery and start first-deferral history. | `DeadLetterTrackingReplicationApplier.OnFailureAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`, `DeadLetterTrackingReplicationApplierTests.Positive_timeout_expires_from_first_deferral_without_resetting_on_retry`. |
| `Escalate` | On retry, poison an eligible expired saga or quarantine a retired one; acknowledge only retained records. | `DeadLetterTrackingReplicationApplier.TryPoisonTimedOutSagaRecordAsync`, `DeadLetterTrackingReplicationApplier.TryQuarantineAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`, `DeadLetterTrackingReplicationApplierTests.Decided_prepare_at_expired_timeout_remains_unacknowledged_without_poisoning_a_decision`. |
| `ObserveReceiver` | Offer Healthy, Throttled or Saturated batch and pause feedback. | `WalSaturationReceiverFlowControlPolicy.EvaluateAsync`. | Yes: `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_returns_none_when_tree_is_healthy`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_throttled_halves_batch_and_requests_short_pause`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_saturated_drip_feeds_and_requests_long_pause`. |
| `ReceiverShrinkBatch` | Execute the shared consumer action at the supplied logical name. | `WalSaturationReceiverFlowControlPolicy.EvaluateAsync`. | Yes: `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_throttled_halves_batch_and_requests_short_pause`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_saturated_drip_feeds_and_requests_long_pause`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_uses_the_supplied_logical_name_not_an_observed_physical_copy`. |
| `ObserveBatchOnlyFeedback` | Capture an optional batch hint with no offered pause, separately from WAL policy classification. | `ReplicationShipperGrain.DrainOneInFlightAsync`. | Yes: `ReplicationShipperGrainTests.Pipeline_stops_launching_after_receiver_feedback`, `ReplicationShipperGrainTests.Released_terminal_waits_for_next_pump_after_mixed_window_feedback`. |
| `Accept` | Fold the captured cursor only after the ordered acknowledgement. | `ReplicationShipperGrain.DrainOneInFlightAsync`, `ReplicationShipperGrain.AdvanceCursorPipelinedAsync`. | Yes: `ReplicationShipperGrainTests.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion`, `ReplicationShipperGrainTests.Released_terminal_waits_for_next_pump_after_mixed_window_feedback`. |
| `Reject` | Retain the unacknowledged head, then back off. | `ReplicationShipperGrain.DrainOneInFlightAsync`, `ReplicationShipperGrain.ApplyBackoff`. | Yes: `ReplicationShipperGrainTests.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion`, `ReplicationShipperGrainTests.Retry_without_jitter_preserves_cursor_then_success_resets_backoff`. |
| `Tick` | An elapsed retry/pause deadline permits another callback. | `ReplicationShipperGrain.ProcessNextPhaseAsync`. | Yes: `ReplicationShipperGrainTests.Retry_without_jitter_preserves_cursor_then_success_resets_backoff`. |
| `NextPump` | Apply the latest cap only on the next pump. | `ReplicationShipperGrain.PumpOnceAsync`, `ReplicationShipperGrain.ComputeMaxPerBatch`. | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_clamps_drain_to_receiver_suggested_batch_size`, `ReplicationShipperGrainTests.Released_terminal_waits_for_next_pump_after_mixed_window_feedback`. |
| `FreeCapacity` | Operator resolution frees bounded storage; retry still performs actual retention. | `ReceiverSagaPoisonGrain.ReleaseQuarantineAsync`, `DeadLetterTrackingReplicationApplier.OnFailureAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Full_dead_letter_queue_preserves_unacknowledged_record_until_capacity_returns`, `DeadLetterTrackingReplicationApplierTests.A_retired_saga_that_fails_again_while_the_quarantine_set_is_full_is_held_and_never_re_seeded`. |
| `Reseed` | Successful reseeding retires durable poison. | `ReceiverSagaPoisonGrain.RetireAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`. |
| `Restart` | Reconstruct the decorator without discarding durable saga safety. | `DeadLetterTrackingReplicationApplier.TryPoisonTimedOutSagaRecordAsync`, `ReceiverSagaPoisonGrain.FilterPoisonedAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Receiver_restart_preserves_poison_and_requires_a_new_expiry_opportunity_for_fresh_deferrals`. |
| `Elapse` | Time, separately from retry count, reaches expiration. | `DeadLetterTrackingReplicationApplier.TryPoisonTimedOutSagaRecordAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`, `DeadLetterTrackingReplicationApplierTests.Positive_timeout_expires_from_first_deferral_without_resetting_on_retry`. |
| `RepairDecided` | Clear the application fault without rewriting a decision or acknowledging unseen data. | `DeadLetterTrackingReplicationApplier.ApplyAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.Decided_prepare_recovers_when_application_succeeds_without_rewriting_the_decision`. |
| `Stutter` | No observable step. | No production step. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
| --- | --- | --- |
| `InFlightRespected` | The real pump never exceeds its configured launch window. | Yes: `ReplicationShipperGrainTests.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion`. |
| `NoSkipWithoutDeadLetter` | A rejected head is retained; a full queue never turns loss into acceptance. | Yes: `ReplicationShipperGrainTests.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion`, `DeadLetterTrackingReplicationApplierTests.Full_dead_letter_queue_preserves_unacknowledged_record_until_capacity_returns`. |
| `DecidedPrepareSafe` | A decided prepare stays unacknowledged rather than poisoning a recorded decision. | Yes: `DeadLetterTrackingReplicationApplierTests.Decided_prepare_at_expired_timeout_remains_unacknowledged_without_poisoning_a_decision`. |
| `CursorMonotone` | Same-source HLC progress never rewinds, including lower-clock records and rejection. | Yes: `ReplicationShipperGrainTests.Retry_without_jitter_preserves_cursor_then_success_resets_backoff`, `ReplicationShipperGrainTests.Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion`. |
| `FlowControlHonoured` | Future sends use the observed cap and respect the active pause. | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_clamps_drain_to_receiver_suggested_batch_size`, `ReplicationShipperGrainTests.Pipeline_stops_launching_after_receiver_feedback`. |
| `FeedbackStopsReplenishment` | Observed shrink survives later null feedback through held-terminal release in this pump. | Yes: `ReplicationShipperGrainTests.Pipeline_stops_launching_after_receiver_feedback`, `ReplicationShipperGrainTests.Released_terminal_waits_for_next_pump_after_mixed_window_feedback`. |
| `BackoffRecovers` | Failure preserves the cursor; subsequent success clears the failure budget without jitter. | Yes: `ReplicationShipperGrainTests.Retry_without_jitter_preserves_cursor_then_success_resets_backoff`. |
| `PausePreserved` | Later successful in-flight acknowledgements cannot erase an active receiver deadline. | Yes: `ReplicationShipperGrainTests.Pipeline_preserves_pause_when_a_later_inflight_ack_is_healthy`. |
| `CapacityFailClosed` | Full dead-letter, poison or quarantine storage never grants unretained delivery. | Yes: `DeadLetterTrackingReplicationApplierTests.Full_dead_letter_queue_preserves_unacknowledged_record_until_capacity_returns`, `DeadLetterTrackingReplicationApplierTests.Full_poison_set_preserves_delivery_until_retirement_frees_capacity`, `DeadLetterTrackingReplicationApplierTests.A_retired_saga_that_fails_again_while_the_quarantine_set_is_full_is_held_and_never_re_seeded`. |
| `RestartPreservesDurable` | Receiver reconstruction loses volatile age, not durable poison or acknowledged safety. | Yes: `DeadLetterTrackingReplicationApplierTests.Receiver_restart_preserves_poison_and_requires_a_new_expiry_opportunity_for_fresh_deferrals`. |
| `NoReseedLoop` | A deterministic failure after retirement quarantines rather than starting another poison cycle. | Yes: `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`, `DeadLetterTrackingReplicationApplierTests.A_retired_saga_that_fails_again_while_the_quarantine_set_is_full_is_held_and_never_re_seeded`. |
| `NoEarlyEscalation` | A newly observed retired-saga failure still waits for its deferral timeout before quarantine. | Yes: `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`. |
| `ReceiverRegimes` | Logical Healthy preserves load; Throttled reduces; Saturated drip-feeds, each with its own pause. A distinct batch-only acknowledgement captures its cap without inventing a pause. | Yes: `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_returns_none_when_tree_is_healthy`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_throttled_halves_batch_and_requests_short_pause`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_saturated_drip_feeds_and_requests_long_pause`, `WalSaturationReceiverFlowControlPolicyTests.EvaluateAsync_uses_the_supplied_logical_name_not_an_observed_physical_copy`, `ReplicationShipperGrainTests.Pipeline_stops_launching_after_receiver_feedback`, `ReplicationShipperGrainTests.Released_terminal_waits_for_next_pump_after_mixed_window_feedback`. |
| `EveryDeferralBounded` | Eligible expired prepare and commit/abort retries progress under the explicit capacity/lifetime premises. | Yes: `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`, `ReplicationShippingCoyoteTests.Shipping_and_deferral_obey_controlled_production_opportunities`, `ReplicationShippingCoyoteTests.Exploration_reaches_terminal_deferral`, `DeadLetterTrackingReplicationApplierTests.Positive_timeout_expires_from_first_deferral_without_resetting_on_retry`. |
| `StreamProgresses` | Conditional retries recover without skipping data, after reseeding or application/capacity recovery. | Yes: `ReplicationShipperGrainTests.Retry_without_jitter_preserves_cursor_then_success_resets_backoff`, `DeadLetterTrackingReplicationApplierTests.Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop`, `DeadLetterTrackingReplicationApplierTests.Decided_prepare_recovers_when_application_succeeds_without_rewriting_the_decision`, `DeadLetterTrackingReplicationApplierTests.Full_dead_letter_queue_preserves_unacknowledged_record_until_capacity_returns`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Model-domain sanity, not a production representation. Independently targeted by paired mutations; no behavioural action is exempted. |

## Property classification and bounds

Both configurations retain all four invariants and twelve properties, including
the conditional temporal `EveryDeferralBounded` and `StreamProgresses` checks.
The larger workload adds a second prepare and increases the in-flight cap,
not an unused constant. Every Next/environment action except Stutter has its
own production mapping and perturbation. The TypeOK mutation corrupts the clock
domain; other mutations retain TypeOK and target one property only.

## Real-code evidence

#4998 retains the three original assertion-red feedback cases and the later
held-terminal/mixed-ack counterexample. The Coyote reachability probe executes
the real terminal timeout/poison path before its deliberate probe assertion.
Suppressing that production expiry arm rejects both the lifecycle tests and
the reachability control. Neither the probe nor the safety exploration is a
second copy of the shipping state machine, and no CLR Task interleaving
exploration is claimed. Controlled choices select prepare/commit/abort,
feedback regime, rejected head and serial/pipelined recovery scenarios.

The lifecycle tests shorten a mutable positive timeout to zero between public
retries without sleeping. A separate three-case detector retains the positive
one-day timeout, ages the existing first-deferral timestamp, and retries the
same receiver. Refreshing that timestamp on every retry leaves the older four
lifecycle/restart cases green but makes all three elapsed-time cases assertion
red; exact source restoration and rebuild restores all three. This distinguishes
elapsed time from retry count, receiver reconstruction and changed options.
Backoff tests first reject an immediate retry under a positive deadline, then
use the existing explicit deadline-reset test seam to represent elapsed time.
The production UTC clock remains authoritative.

A grounding reread stopped the unfinished earlier TLC cohort: the draft had
incorrectly allowed immediate post-retirement quarantine. That stopped cohort
is historical, not certification. The corrected model checks NoEarlyEscalation;
its one-target QuarantineIgnoresNewExpiry experiment rejects the same removed
expiry guard that makes the real positive-timeout post-retirement detector
assertion red in all three prepare/commit/abort cases. Exact restoration and
rebuild restores those three cases without changing production behavior.

The original feedback packet with 135 cases has 133 passing and two failing
new cursor expectations; it is not the clean certificate. The historical
319-case clean packet and reviewed 322-case clean restoration are retained,
not relabeled or overwritten. The held-terminal/mixed-ack bypass is recorded
in #4998 alongside the three original feedback-loss regressions.

The final affected restoration has 327 passing cases and zero other outcomes
in `shipping-final-affected-restoration.trx`: the reviewed 322 cases plus two
full-poison-capacity cases and three unchanged-positive-timeout cases. The
exact regression-detector union is twenty fully qualified method keys.

## Retained assurance-defect controls

The first full 27-case harness retained 25 passing cases and two expected gate
failures exposing authored assurance defects, not final certification. Both
full configurations passed, but SuccessRewindsCursor set every accepted cursor
to zero and never established positive progress to rewind. The corrected
mutation first permits progress, then rewinds an already-positive cursor in
Accept; the oracle and shared helper are unchanged. LaterHealthyAckErasesLatch
escaped because every offered batch shrink also offered a positive pause,
which masked the missing sticky stop. The explicit batch-only opportunity now
separates those fields without weakening any WAL regime or identity assertion.
BatchOnlyFeedbackIgnored independently targets capture of that opportunity.
The original 25-green/2-red TRX and cost remain historical escaped controls.

Dropping the actual SuggestedBatchSize capture produces two assertion failures
(batch-only replenishment and held-terminal release), while the pause-only case
still passes. Exact source restoration and rebuild restores all three cases.
No production or test candidate was changed to repair these model artifacts.

## Revised full measurements with batch-only feedback

The grounding-reviewed revision retains four invariants, twelve properties,
nineteen Next/environment actions (Stutter only nonbehavioural), twenty-six
one-target mutations and thirty-three behaviour rows. Its three targeted pairs
all pass: both previously escaped mutants now violate their unchanged original
oracles, and BatchOnlyFeedbackIgnored violates ReceiverRegimes. Each actual
paired harness case requires a clean control and one named target violation.

Both revised checked-in full configurations then passed, serially at configured
1800 MB heap and two workers, without safety-only omission or larger-heap retry.
The independent preliminary measurements restate the exact manifest counts;
they are not a replacement for the complete final paired harness.

| Configuration | Distinct states | Generated states | Depth | Monotonic elapsed | Final temporal check |
| --- | --- | --- | --- | --- | --- |
| Base: Records=3, Cap=2 | 527689 | 2174007 | 57 | 171.2132167 seconds | 56 seconds, 4 branches |
| FourRecords: Records=4, Cap=3 | 3306850 | 14126738 | 71 | 1809.0807471 seconds | 790 seconds, 5 branches |

The final FourRecords temporal graph had 16534250 states. The JVM reported
1600 MB effective heap and 64 MB off-heap; peak resident memory was not measured.
Raw logs are shipping-batch-only-full-base.log and
shipping-batch-only-full-four.log; the monotonic receipt is
shipping-batch-only-full-cost.json. Model and both full configuration digests
were unchanged across each run. No cross-run control cache or harness bypass
was introduced.

## Accepted complete harness cost and evidence limits

The complete revised harness passed 28/28 cases with zero other outcomes:
its base and full-temporal FourRecords checks plus all twenty-six one-target
paired experiments. Its launcher-artifact receipt records 2745.4195361
monotonic seconds, with configured 1800 MB heap, two workers and one concurrent
TLC process. The actual runner invocation used fresh process-local controls;
no cross-run cache or count-guard bypass was introduced. Earlier standalone
full measurements are separate diagnostics and are not added to this price.
The final harness independently reran its two full cases, rather than reusing
those standalone receipts as a substitute for executing an obligation.

Evidence is retained in shipping-batch-only-final-28.trx, the redirected runner
log and shipping-batch-only-final-28-cost.json. The launcher guards against an
existing final TRX, captures the immediate native LASTEXITCODE and writes the
post-completion Stopwatch receipt. Parent review accepted this exact binding
as launcher-artifact completion, not a recovered CLI terminal event. The
original async-launch and later shell-unknown read events are retained; no
original native terminal exit/output event was recovered. No loaded-DLL hash
or MVID was captured at launch, and no final peak RSS was measured. Source
hashes are not retroactively an at-launch loaded-binary attestation.

The raw TRX SHA256 is
B82A67B1267A2916601301F3597554B57FADD9CB7798068A44CBF152482D7556;
the final cost receipt SHA256 is
2E19CBB0F464098D4FF2913B69F3C6E34CFBC01781FD7B6C09E9386D95E62CA8.
Parent authentication is recorded in shipping-final-28-parent-review.json,
SHA256 428AFBFE9C3DC7493D0DD82D819A7873A120E3D6B8CF01576F09974238FCE205.
The immutable final source/proof review and native-binding addendum preserve
all prior failed controls and the explicit provenance limitations.

The module's twenty mapped method names overlap the prior global detector
union by four policy names, including Healthy. Seventeen own source-red plus
three reused source-red methods is evidence provenance, not the count of new
names: only sixteen names are new to that union. The separate receiver-policy
restoration is 16/16; it is not part of the 327 affected shipping cases.

## Historical bounded cost before the batch-only opportunity

The earlier model revision passed both full configurations with the pinned TLC 2.19 jar, two workers and
`-XX:ActiveProcessorCount=2 -Xmx1800m`. The JVM banner reported 1600 MB effective
heap and 64 MB off-heap storage; no larger-heap retry was used.

| Configuration | Distinct states | Generated states | Depth | Full elapsed | Final temporal check |
| --- | --- | --- | --- | --- | --- |
| Base: Records=3, Cap=2 | 438704 | 1810686 | 57 | 142 seconds | 48 seconds, 4 branches |
| FourRecords: Records=4, Cap=3 | 2856762 | 12219808 | 72 | 1231 seconds | 495 seconds, 5 branches |

The larger configuration retains both progress properties, not just safety.
These historical state counts are superseded by the revised full measurements above.
These are direct full-configuration measurements, not per-mutation control-arm
costs. Logs are retained as `shipping-tlc-guarded-base.log` and
`shipping-tlc-guarded-four-full.log`; peak resident memory was not measured.
