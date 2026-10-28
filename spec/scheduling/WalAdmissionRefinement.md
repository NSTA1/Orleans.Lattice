# Refinement note: WAL signal release and semaphore admission

<a id="model-scope"></a>

## Model scope in one paragraph

Finite callers park on partition signal queues, recover in oldest-first batches,
and separately acquire capacity. A finite acute-fault episode can remain on
forever. The sampler explicitly samples, releases, then visits expiries. Arrivals,
cancellation, fault observation and clearance are environment steps, not fairness
assumptions. Per-call and per-wait budget counters are independent observations.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `inflight` | Independent admission ownership count. | `WalCommitLogWriter.PartitionTracker.AcquireAsync` and `ReleaseAdmission`. |
| `parked` | Oldest-first partition wait queues. | `WalSaturationSignal._waiters`. |
| `state` | Last sampled partition verdict, not instantaneous depth. | `WalSaturationSignal.GetCurrentState`. |
| `acuteCause`, `faultPhase` | Finite environment acute-fault episode. | `WalSaturationSampler.SampleOnceAsync`. |
| `recovery` | Remaining tree-wide recovery-window samples. | `WalSaturationSampler.SampleOnceAsync`. |
| `outcome`, `home` | Caller lifecycle and routing. | `WalCommitLogWriter.GateOnSaturationAsync` and `WalSaturationSignal.WaitForNotSaturatedAsync`. |
| `waitLeft`, `callLeft` | Independent finite deadlines. | `WalCommitLogWriter.ResolveEffectiveGateBudget`. |
| `phase`, `cursor` | Finite sampler/release/expiry opportunities. | `WalSaturationSampler.SampleOnceAsync` and `WalCommitLogWriter.GateOnSaturationAsync`. |
| `refusalState` | Verdict witnessed at refusal. | `WalCommitLogWriter.GateOnSaturationAsync` re-reading the signal after expiry. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Arrive` | A new append enters its gate. | `WalCommitLogWriter.GateOnSaturationAsync`. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Healthy_does_not_call_WaitForHealthyAsync`. |
| `Park` | Link a saturated caller. | `WalSaturationSignal.WaitForNotSaturatedAsync`. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `Admit` | Acquire an independent capacity slot. | `WalCommitLogWriter.PartitionTracker.AcquireAsync`. | Yes: `WalAdmissionCoyoteTests.Released_signal_waiter_still_waits_for_partition_capacity`. |
| `Flush` | Environment returns the capacity slot. | `WalCommitLogWriter.PartitionTracker.ReleaseAdmission`. | Yes: `WalAdmissionCoyoteTests.Released_signal_waiter_still_waits_for_partition_capacity`. |
| `Sample` | Reclassify depth/acute causes and recovery age. | `WalSaturationSampler.SampleOnceAsync`. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_returns_Healthy_when_depth_well_below_throttled_ratio`. |
| `ReleaseTick` | Complete the oldest eligible signal waits, capped per partition. | `WalSaturationSignal.CompleteWaiters`. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `BudgetExpire` | Expiry re-reads the current verdict before refusal. | `WalCommitLogWriter.GateOnSaturationAsync`. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_but_recovered_at_recheck_suppresses_refusal`. |
| `Cancel` | Settle and unlink cancellation. | `WalSaturationSignal.WaitCoreAsync`. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `AcuteOn` | Environment introduces an acute trigger. | `WalSaturationSampler.SampleOnceAsync`. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_returns_Saturated_when_dispatch_timeout_delta_crosses_threshold`. |
| `AcuteOff` | Environment clears the acute trigger. | `WalSaturationSampler.SampleOnceAsync`. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_fires_recovery_transition_with_previous_state_tag`. |
| `Stutter` | No progress. | No production step. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `CapRespected` | Semaphore ownership never exceeds its positive cap. | Yes: `WalAdmissionCoyoteTests.Released_signal_waiter_still_waits_for_partition_capacity`. |
| `NoLostWaiter` | Live queue entries and parked outcomes agree, including cancellation. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `OldestFirst` | A release removes a prefix, not a newest-first suffix. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `PerPartitionIsolation` | A recovering partition cannot release a saturated sibling. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `RefusalOnlyWhenSaturated` | Refusal records the current saturated verdict after expiry. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_but_recovered_at_recheck_suppresses_refusal`. |
| `UpgradeSound` | An idle partition is floored only within a tree-wide recovery window and an otherwise Healthy tree sample. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_returns_Healthy_when_depth_well_below_throttled_ratio`. |
| `PacedRecoveryRelease` | Positive batch bounds signal completions per eligible tick, not admissions. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `BoundedResidueDrain` | Each eligible tick removes the oldest min(batch, residue) waits. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `ProgressWhenCapacityFree` | Even without capacity pressure, an eligible tick releases a nonempty residue. | Yes: `WalAdmissionCoyoteTests.Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue`. |
| `EveryWaiterResolves` | Finite deadlines settle a parked caller even if recovery never arrives. | Yes: `WalCommitLogWriterAdmissionSaturationTests.AppendAsync_signal_Saturated_past_budget_throws_LatticeSaturatedException`. |
| `NoAbsorbingSaturation` | Stable cleared causes and an empty partition eventually stop sampling Saturated. | Yes: `WalSaturationSamplerTests.SampleOnceAsync_fires_recovery_transition_with_previous_state_tag`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain sanity, not the production representation; still paired with its own violating mutation. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, CapRespected, NoLostWaiter and RefusalOnlyWhenSaturated are state safety.
OldestFirst, PerPartitionIsolation, UpgradeSound, PacedRecoveryRelease,
BoundedResidueDrain and ProgressWhenCapacityFree are action safety: they inspect
the transition, not stale audit snapshots. EveryWaiterResolves is temporal under
fair sampler opportunities that age finite deadlines. NoAbsorbingSaturation is
conditional temporal progress under a permanently cleared cause and empty
partition. Flush fairness states the storage progress premise explicitly.
The larger cohort bounds out these two temporal properties, not any safety
check. Its TypeOK check is retained.

### Mutation classification

Every mutation below is an **action perturbation**: an anchored edit of its
`PERTURBS` action in `Next`. None is a transition splice.

- `BoundedResidueDrainRecoveryEdgeOnly`
- `CapRespectedAdmissionIgnoresCap`
- `CapRespectedFlushLeaksCapacity`
- `EveryWaiterResolvesExpiryNeverResolves`
- `NoAbsorbingSaturationSaturationIsSticky`
- `NoLostWaiterCancelKeepsRegistration`
- `NoLostWaiterParkLosesWaiter`
- `OldestFirstReleaseNewestFirst`
- `PacedRecoveryReleaseRecoveryHerd`
- `PerPartitionIsolationReleaseSaturatedSibling`
- `ProgressWhenCapacityFreeRecoveryNeverReleases`
- `RefusalOnlyWhenSaturatedRefuseWithoutRecheck`
- `TypeOKArrivalOutsideDomain`
- `TypeOKFaultClearOutsideDomain`
- `TypeOKFaultObservationOutsideDomain`
- `UpgradeSoundIdleThrottleWithoutWindow`

## Detectors

The Coyote recovery model drives real WalSaturationSignal queues, varying
partition tick and cancellation order. Exact task status checks discriminate
FIFO, bounded release, sibling isolation and level-triggered residue drain.
Released_signal_waiter_still_waits_for_partition_capacity uses the real signal
and real PartitionTracker to distinguish signal completion from admission.
Writer deadline/recheck and sampler classifier fixtures cover the other seams.
These are bounded cooperative choices, not CLR thread schedules.

Production negative controls also invert the expiry recheck (recovered callers
throw and still-saturated callers incorrectly pass), invert the Healthy fast
path (an unnecessary wait is observed), increase the real semaphore's capacity
by one (the released follower completes despite the held slot), and perturb
the classifier's Healthy/acute outcomes (the sampled-state and recovery-tag
assertions fail). Each perturbation is restored before the passing regression
run. These checks exercise the mapped writer and sampler tests as well as the
signal's historical release-all control.

The registration ownership race discovered while implementing this model is
checked independently by
`WalSaturationSignalRegistrationTests.Completion_before_registration_installation_does_not_retain_the_callback`,
`Registration_installed_before_completion_is_disposed`, and
`Cancellation_during_registration_can_complete_without_deadlock`.
The production helper owns registration installation/disposal atomically and
disposes outside locks; the model abstracts that handshake into Park/Cancel.

## Defect mutations whose fixes have landed

PacedRecoveryReleaseRecoveryHerd reinstates the mechanism from
[#3402](https://github.com/NSTA1/Orleans.Lattice/issues/3402), fixed in
[#3404](https://github.com/NSTA1/Orleans.Lattice/pull/3404). Its one-target
PacedRecoveryRelease experiment releases two waits against a positive batch
of one. BoundedResidueDrainRecoveryEdgeOnly separately rejects leaving residue on later eligible
ticks. The production perturbation forces CompleteWaiters' release-all branch;
the mapped production Coyote detector rejects its first oversized batch.
These are mechanism counterexamples, not a fabricated temporal congestion proof.

## Replaced draft assertions

The draft conflated gate release with admission. CapRespected now measures
semaphore ownership, not the current saturation verdict or the number of
completed signal waits. A released caller can legitimately wait for capacity.
The production capacity detector explicitly holds the only slot across release.

The draft's unconditional no-absorbing-saturation/herd progress assertion cannot
hold under unbounded offered load, persistent acute faults or stalled storage.
NoAbsorbingSaturation retains progress with the explicit stable-environment
premise above; historical herd detection remains the separate release-cap
experiment. ProgressWhenCapacityFree does not promise admission from a signal
alone, and BoundedResidueDrain bounds eligible release opportunities for a
finite queue, not append completion time. Independent jitter is not modelled
as deterministic fairness. No permanent Healthy verdict is asserted.

An initial UpgradeSound snapshot invariant failed because another admission
can change depth after a sample while the cached verdict legitimately persists.
The action formulation checks the exact sample boundary instead. Its dedicated
UpgradeSoundIdleThrottleWithoutWindow mutation rejects a genuinely unsupported idle upgrade.
The recovery age is tree-wide, as in production, not a separate timestamp for
each partition. Its floor propagates to idle siblings only when the raw tree
classification is Healthy; another partition's raw depth throttle is not itself
a reason to floor an idle sibling.

## Deliberate abstraction gaps

Finite cohort, fixed positive semaphore/release cap and sample-unit clocks.
No tree roll-up, metric observers, throughput estimate, jitter distribution,
unbounded offered load, dynamic cap changes, zero-batch opt-out, or runtime timer
latency. Resolved callers' deadline counters are canonicalized to zero because
they cannot influence later steps. The sampler/callback phases are finite
opportunities, not a claim that production callbacks run atomically or in one
specific CLR schedule. Cancellation/expiry linearize at their winning step.
Acute inputs are conservatively independently variable per partition; a genuine
tree-wide trigger is the subset in which every partition's acute input is on.
The model does not claim that production provider-failure counters are partition
local. Their internal aggregation is outside this queue/admission abstraction.
