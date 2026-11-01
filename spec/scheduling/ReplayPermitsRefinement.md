# Replay permit ownership, independent cold lifetime and resumable banking

## Model scope in one paragraph

A finite replay-owner population shares physical, held and memory-withheld
capacity. Admission observes class-specific queue and wait bounds; clients have
deadlines distinct from the shared cold lifetime. Drive budget opportunities
return outer ownership, while completed slices bank a finite durable prefix.
The conditional progress premises and intentional hung-cold behavior are
specified below; neither admission nor fairness is a wall-time SLA.

## Guarantee correction and verification status

The original #4967 draft promised bounded residency for every permit, including
hung cold replay. That conflicts with intentional #2871 survival of background
cold replay across request and activation deadlines. The parent coordinator chose under delegated autopilot (not a direct user selection)
"Preserve cold replay behavior; explicitly correct the formal guarantee";
the [public correction](https://github.com/NSTA1/Orleans.Lattice/issues/4967#issuecomment-6389976747)
is part of this refinement, not an exclusion disguised as a proved guarantee.

`DriveResidencyBounded` applies to the drive budget. Cold replay has no
unconditional residency bound. `ColdCancellationReleases` requires actual
lifetime cancellation or retirement and subsequent runtime continuation
opportunities; it is not a wall-time cancellation SLA. `Hang` can park cold as
well as drive owners before cancellation. `ColdDeadlineIsolation` separately
protects the caller/owner lifetime boundary.

The exact current model has completed clean base and both larger-bound checks,
including every temporal branch at the original 1800 MB configured heap.
All 29 one-target pairs that predate the reserved system lane and all 23 final harness controls have passed; the lane's three mutations are certified by CI (see "Reserved system lane" below).
Their original and repaired run artifacts remain distinct, as detailed below.
Obsolete draft graphs do not certify the corrected guarantee.

## Actual owner and cancellation boundaries

`EnsureReplayStarted` starts `RunReplayBarrierAsync` after yielding.
`ExecuteActivationReplayAsync`, not the returning `OnActivateAsync` frame,
acquires the semaphore permit, enrolls the service hold, runs replay and calls
`FinishReplayPermit` in its `finally`. `RetireReplayBarrier` and deactivation's
`finally` cancel the independent `_replayBarrierCts`.

Request and activation deadlines do not own this background lifetime. Preserving
replay across those deadlines is intentional
[#2871](https://github.com/NSTA1/Orleans.Lattice/issues/2871) behavior.
`DriveStarvedCheckpointAsync` separately owns a budgeted outer frame. Its inner
work task may outlive that frame, but the frame returns its own permit.

[#4984](https://github.com/NSTA1/Orleans.Lattice/issues/4984) exposed a different
defect: after genuine lifetime cancellation, uncooperative slice, head and tail
providers kept the cold owner awaiting and retained its permit. The shared
`ReplaySliceReader.AwaitProviderReadAsync` helper cancellation-bounds those
read-only awaits, rejects late results and observes late provider faults.
The replay state machine itself is not detached;
it unwinds through normal prefix banking and permit disposition.

Cancellation-to-unwind requires runtime continuation opportunities and completion
of prefix banking and teardown writes. Those writes are not detached or silently
cancelled: durability cannot be traded for an apparent permit-release bound.
Provider reads are distinct from the replay/apply state machine. An owner held
forever by a non-completing durability write is outside that completion premise,
not evidence of an unconditional cold residency bound.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `holders`, `free` | Physical ownership and available capacity. | `BPlusLeafGrain.AcquireReplayPermitAsync`, `BPlusLeafGrain.TryAcquireStarvationDrivePermitAsync`, `BPlusLeafGrain.FinishReplayPermit`, `BPlusLeafGrain.ReleaseStarvationReplayPermit`. |
| `withheld` | Combined withheld capacity; service-owned baseline is separate. | `BPlusLeafGrain._withheldReplayPermits`, `BPlusLeafGrain._serviceWithheldReplayPermits`, `BPlusLeafGrain.TryWithholdReplayPermitOnPressure`, `BPlusLeafGrain.TryRestoreWithheldReplayPermit`. |
| `queue`, `status`, `observed` | Pending observation, semaphore waiter and winning outcome. | `BPlusLeafGrain.TryAdmitReplayPermitWaiter`, `BPlusLeafGrain.AcquireReplayPermitAsync`, arrival/departure accounting. |
| `role` | Admission policy, not a work-type-specific queue. `System` is a system-tree replay waiting for the reserved lane and `Lane` is one holding it (#5569); `OnLane` and `LaneFree` are derived from it. | `LatticeReplayAdmissionContext.Current`, `SystemTreeNamespace.IsSystemOwned`, `BPlusLeafGrain.AcquireSystemReplayPermitAsync`; drive share admission is separate. |
| `pressure`, `pressurePhase` | Finite external heap-pressure episode. | `ReplayHeapPressure`, memory-fault return disposition. |
| `drain` | Classified fresh mean wait or lack of acquisition progress. | `BPlusLeafGrain.ClassifyReplayPermitQueueDrain`. |
| `frontier`, `banked` | Reached per-partition prefix and durable reread coverage. | `BPlusLeafGrain.TryBankColdReplayProgressAsync`, `BPlusLeafGrain.BankCancelledWarmReplayProgressAsync`, incremental snapshot capture. |
| `checkpoint` | Already-applied durable checkpoint, not cold reread coverage. | `BPlusLeafGrain.GetPersistedCheckpointForPartition`, snapshot replay coverage. |
| `holes`, `ledger` | Deferred terminal and durable unresolved-work record. | `BPlusLeafGrain.TryRecordUnresolvedReplayWork`, `UnresolvedReplayWork`. |
| `hung` | A read provider ceases completing during a cold or drive attempt. | `ReplaySliceReader.ReadSliceAsync`. |
| `holdLeft`, `deadline` | Independent drive budget and client-wait budget. | `BPlusLeafGrain.DriveStarvedCheckpointAsync`, `BPlusLeafGrain.JoinReplayBarrierAsync`. |
| `opportunity`, `dispatchEpoch` | Generic callback/dispatch observations. | Runtime continuation opportunities, not elapsed-time guarantees. |
| `clientActive`, `cancelled` | Request wait versus actual independent owner cancellation. | `BPlusLeafGrain.JoinReplayBarrierAsync`, `BPlusLeafGrain.RetireReplayBarrier`, `BPlusLeafGrain.RunReplayBarrierAsync`. |
| `arrivalsClosed` | The external finite offered-attempt population has stopped growing. | Calls into `BPlusLeafGrain.EnsureReplayStarted`; the owner does not manufacture callers. |

## Action mapping

The environment, caller, provider and owner boundaries are kept distinct.

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Request` | Observe inherited class and current bound. | `BPlusLeafGrain.TryAdmitReplayPermitWaiter`, `LatticeReplayAdmissionContext.Current`. | Yes: `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`. |
| `Enqueue` | Register one admitted waiter. | `BPlusLeafGrain.AcquireReplayPermitAsync`, `BPlusLeafGrain.NoteReplayPermitArrival`. | Yes: `BPlusLeafGrainTests.Cancelled_queued_replay_replacements_leave_no_waiters_or_unowned_permits`. |
| `RefuseWait` | Refuse fresh excessive wait below depth. | `BPlusLeafGrain.AcquireReplayPermitAsync`. | Yes: `BPlusLeafGrainTests.A_wait_exceeded_mean_refuses_below_the_admitted_waiter_bound`. |
| `RefuseDepth` | Refuse stalled over-depth arrival. | `BPlusLeafGrain.AcquireReplayPermitAsync`. | Yes: `BPlusLeafGrainTests.A_no_progress_queue_at_the_circulating_bound_is_refused`. |
| `DriveAdmission` | Try the nonqueueing drive share. | `BPlusLeafGrain.TryAcquireStarvationReplayPermit`. | Yes: `StarvationReplayAdmissionTests.Ordinary_replay_owners_do_not_consume_gc_reservations` and `StarvationReplayAdmissionTests.Concurrent_acquisitions_cannot_exceed_the_gc_share`. |
| `Release` | Return physical ownership once. | `BPlusLeafGrain.FinishReplayPermit`, `BPlusLeafGrain.ReleaseStarvationReplayPermit`. | Yes: `BPlusLeafGrainTests.Activation_releases_the_replay_permit_when_the_post_acquire_observation_throws`. |
| `Hang` | Environment parks a provider read. | `ReplaySliceReader.ReadSliceAsync`. | Yes: `BPlusLeafGrainTests.Cancelling_a_replay_returns_its_permit_before_uncooperative_storage_completes`. |
| `ReplaySlice` | Complete and durably bank a reached prefix. | `BPlusLeafGrain.TryBankColdReplayProgressAsync`, incremental capture. | Yes: `BPlusLeafGrainTests.Cancelled_cold_replay_banks_the_re_read_frontier_as_durable_snapshot_coverage`. |
| `DeferTerminal` | Preserve a skipped terminal durably. | `BPlusLeafGrain.TryRecordUnresolvedReplayWork`. | Yes: `BPlusLeafGrainTests.A_saturated_ledger_admits_the_head_of_window_terminal_and_refuses_the_ones_behind_it`. |
| `Restore` | Restore memory-owned capacity only. | `BPlusLeafGrain.TryRestoreWithheldReplayPermit`. | Yes: `BPlusLeafGrainTests.Clean_activation_memory_restore_cannot_return_a_service_owned_permit`. |
| `MemoryPressureOn` | Environment introduces memory pressure. | `BPlusLeafGrain.TryWithholdReplayPermitOnPressure`. | Yes: `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`. |
| `MemoryPressureOff` | Environment relieves pressure. | `BPlusLeafGrain.TryRestoreWithheldReplayPermit`. | Yes: `BPlusLeafGrainTests.Clean_activation_restores_one_withheld_replay_permit`. |
| `CloseArrivals` | External caller stops offering new work. | Callers stop invoking activation; `BPlusLeafGrain.AcquireReplayPermitAsync` does not create arrivals. | Yes: `BPlusLeafGrainTests.Cancelled_queued_replay_replacements_leave_no_waiters_or_unowned_permits`. |
| `ObserveDrain` | Classify fresh wait and acquisition evidence. | `BPlusLeafGrain.ClassifyReplayPermitQueueDrain`. | Yes: `BPlusLeafGrainTests.A_wait_exceeded_mean_refuses_below_the_admitted_waiter_bound`. |
| `CapacityOpportunity` | A generic runtime callback grants an eligible semaphore waiter if one exists. | `BPlusLeafGrain.AcquireReplayPermitAsync`. | Yes: `BPlusLeafGrainTests.Finite_interactive_and_bulk_cohort_progresses_when_capacity_and_runtime_are_available`. |
| `BudgetOpportunity` | Visit drive budget, completion and actual lifetime-cancellation continuations. | `BPlusLeafGrain.DriveStarvedCheckpointAsync`, `BPlusLeafGrain.RunReplayBarrierAsync`. | Yes: `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_releases_its_replay_permit_while_the_slice_read_is_still_parked` and `BPlusLeafGrainTests.Cancelling_a_replay_returns_its_permit_before_uncooperative_storage_completes`. |
| `ClientReplacement` | A replacement client rejoins the one live owner. | `BPlusLeafGrain.EnsureReplayStarted`, `BPlusLeafGrain.JoinReplayBarrierAsync`. | Yes: `BPlusLeafGrainTests.Caller_deadlines_and_replacements_preserve_the_live_cold_owner`. |
| `ClientDeadline` | Settle the client without cancelling its cold owner. | `BPlusLeafGrain.JoinReplayBarrierAsync`, `BPlusLeafGrain.EnsureReplayStarted`. | Yes: `BPlusLeafGrainTests.Caller_deadlines_and_replacements_preserve_the_live_cold_owner`. |
| `CancelColdLifetime` | Environment actually retires/cancels the independent owner lifetime. | `BPlusLeafGrain.RetireReplayBarrier`, `BPlusLeafGrain.RunReplayBarrierAsync`. | Yes: `BPlusLeafGrainTests.Cancelling_a_replay_returns_its_permit_before_uncooperative_storage_completes`. |
| `SystemAdmission` | A system-tree replay takes a free shared permit, else waits for the reserved lane; it is never judged by the shared arms (#5569). | `BPlusLeafGrain.AcquireSystemReplayPermitAsync`, `SystemTreeNamespace.IsSystemOwned`. | Yes: `BPlusLeafGrainTests.A_system_tree_replay_takes_a_free_shared_permit_before_the_lane`, `BPlusLeafGrainTests.A_system_tree_replay_is_admitted_through_the_reserved_lane_while_the_shared_pool_is_wedged`. |
| `LaneOpportunity` | A waiting system-tree replay acquires the reserved lane, whatever the shared pool holds (#5569). | `BPlusLeafGrain.AcquireSystemReplayPermitAsync`, `BPlusLeafGrain.ReleaseSystemReplayLane`. | Yes: `BPlusLeafGrainTests.A_system_tree_replay_is_admitted_through_the_reserved_lane_while_the_shared_pool_is_wedged`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|---------------------|----------|
| `PermitConservation` | Physical free, held and withheld capacity account for the ceiling. | Yes: `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`. |
| `FloorOne` | Memory pressure never removes the last circulating permit. | Yes: `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`. |
| `QueueOwnership` | Cancelled waiters leave no residue and return no unowned permit. | Yes: `BPlusLeafGrainTests.Cancelled_queued_replay_replacements_leave_no_waiters_or_unowned_permits`. |
| `CoverageSound` | Cold coverage never exceeds its reread frontier. | Yes: `BPlusLeafGrainTests.Cancelled_cold_replay_banks_the_re_read_frontier_as_durable_snapshot_coverage`. |
| `UnresolvedWorkDurable` | Deferred terminals remain in the durable unresolved ledger. | Yes: `BPlusLeafGrainTests.A_saturated_ledger_admits_the_head_of_window_terminal_and_refuses_the_ones_behind_it`. |
| `DriveResidencyBounded` | A drive returns its outer permit when its budget ends, including parked storage. | Yes: `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_releases_its_replay_permit_while_the_slice_read_is_still_parked`. |
| `DeadlineBounded` | The client deadline settles its wait, not the independent cold lifetime. | Yes: `BPlusLeafGrainTests.Caller_deadlines_and_replacements_preserve_the_live_cold_owner`. |
| `PriorityReservation` | Bulk retains its floor and Interactive admission headroom. | Yes: `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`. |
| `QueueBounded` | An observed harmful queue is refused, not hard-capped after the fact. | Yes: `BPlusLeafGrainTests.A_no_progress_queue_at_the_circulating_bound_is_refused`. |
| `SmoothedWaitRefusal` | Fresh excessive mean refuses even below depth. | Yes: `BPlusLeafGrainTests.A_wait_exceeded_mean_refuses_below_the_admitted_waiter_bound`. |
| `BankingMonotone` | Cancellation preserves a durable resumable prefix. | Yes: `BPlusLeafGrainTests.Cancelled_cold_replay_banks_the_re_read_frontier_as_durable_snapshot_coverage`. |
| `MemoryWithholding` | Pressure actually subtracts capacity independently of the floor. | Yes: `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`. |
| `RestoreOwnership` | Memory cannot restore service-owned capacity. | Yes: `BPlusLeafGrainTests.Clean_activation_memory_restore_cannot_return_a_service_owned_permit`. |
| `ClassPriority` | Observed bounds use inherited class; cold owners do not consume GC reservations. | Yes: `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract` and `StarvationReplayAdmissionTests.Ordinary_replay_owners_do_not_consume_gc_reservations`. |
| `ClassNonStarvation` | Both ambient classes progress in a finite stable cohort when existing holders complete and runtime opportunities continue. | Yes: `BPlusLeafGrainTests.Finite_interactive_and_bulk_cohort_progresses_when_capacity_and_runtime_are_available`. |
| `NoSelfSustainingBacklog` | Replacements rejoin the existing owner without generating waiter/permit residue. | Yes: `BPlusLeafGrainTests.Caller_deadlines_and_replacements_preserve_the_live_cold_owner` and `BPlusLeafGrainTests.Cancelled_queued_replay_replacements_leave_no_waiters_or_unowned_permits`. |
| `ReplayConverges` | Durably shorter prefixes converge under recurring live-attempt storage completions. | Yes: `BPlusLeafGrainTests.Successive_interrupted_replays_advance_the_checkpoint_until_convergence` and `BPlusLeafGrainTests.Cancelled_cold_replay_banks_the_re_read_frontier_as_durable_snapshot_coverage`. |
| `ColdCancellationReleases` | Actual owner cancellation unwinds the cold holder or waiter under continuation/banking opportunities. | Yes: `BPlusLeafGrainTests.Cancelling_a_replay_returns_its_permit_before_uncooperative_storage_completes` and `BPlusLeafGrainTests.Cancelled_queued_replay_replacements_leave_no_waiters_or_unowned_permits`. |
| `ColdDeadlineIsolation` | Client deadlines preserve the independent cold owner, physical ownership, queue and cancellation membership. | Yes: `BPlusLeafGrainTests.Caller_deadlines_and_replacements_preserve_the_live_cold_owner`. |
| `LaneExclusive` | Only a system-tree replay ever holds the reserved lane; a user or user-nameable tree is refused by the shared arms rather than taking a free lane (#5569). | Yes: `BPlusLeafGrainTests.A_non_system_replay_is_refused_rather_than_taking_the_free_lane`, `BPlusLeafGrainTests.A_non_system_replay_takes_a_shared_permit_and_never_needs_the_lane`, `SystemTreeNamespaceTests.IsSystemOwned_user_nameable_tree_is_rejected`. |
| `SystemLaneProgress` | A system-tree replay acquires while every shared permit is held by a replay that never returns, provided system replays are bounded (#5569). | Yes: `BPlusLeafGrainTests.A_system_tree_replay_is_admitted_through_the_reserved_lane_while_the_shared_pool_is_wedged`. |

## Reserved system lane (#5569)

A schema migration on the journey estate held every shared permit with replays that read whole, untrimmed WALs from a saturated store. The shared gate's `no_progress` arm then refused the small system trees that authorization reads on every request, so one bulk operation took sign-in down for minutes. The lane gives replays of trees `SystemTreeNamespace.IsSystemOwned` accepts one permit outside the shared pool.

The predicate accepts only the `_lattice_` and `sys-` namespaces, which refuse user-origin content on every path that can write rows: the public surface for `_lattice_`, and for `sys-` the user-origin write guard, bulk-load and snapshot destinations, merge sources, alias targets and tenant addressing. A view's name is caller-chosen, so `view-sys-*` is excluded. No caller outside the library can therefore make a lane replay expensive.

**The assumption.** `SystemLaneProgress` assumes system replays are bounded: its premise `LaneHoldersComplete` requires every lane holder to leave the lane eventually, and nothing else. It names no shared holder, pressure or drain state, so it holds while every shared holder is hung, which is the regime the defect was. A system replay that never ends wedges the lane exactly as it would wedge a shared permit; the lane protects system trees from user-tree replays, not from themselves. The premise also asks for a finite offered population (`<>[]arrivalsClosed`), as `ClassNonStarvation` does, because selection among lane waiters is nondeterministic rather than FIFO.

Lane permits take no part in memory or service-rate withholding, which adapt the shared ceiling, so `MemoryWithholding` excludes a lane holder and `PermitConservation` counts the lane separately. A lane waiter never takes a shared permit freed while it waits; it took a free shared permit on arrival if there was one.

## Excluded properties

This table excludes TypeOK only from production representation mapping, not
from TLC checks or its independent one-target experiments. No checked property
is omitted from a configuration or from the mutation catalogue.

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain sanity, not production representation; paired with independent environment-domain mutants. |

## Admission is not a hard queue-cardinality cap

The production decision observes depth before incrementing arrival accounting.
Concurrent observations, later withholding and intentionally admitted draining
fan-out prevent an invariant of the form `Len(queue) <= depth * circulating`.
`QueueBounded` therefore checks the observed decision transition.

A fresh excessive mean refuses a nonempty queue even below the depth bound.
`NoProgress` needs the additional over-depth observation. An empty queue,
disabled bound, unsized gate and stale wait evidence preserve their recovery
paths. The draft abstracts the classifier's outcomes; the real classifier and
acquisition tests remain necessary to verify freshness and threshold behavior.

Actual ambient classes are Interactive and Bulk. Cold replay, vector writes,
compaction and deferred replay are callers, not six fixed classes or six queues.
They inherit context; cold and vector work are not automatically Bulk.
Drive admission separately attempts a bounded share without queueing.
`SemaphoreSlim` supplies no contractual FIFO guarantee.

The Bulk bound is `max(1, interactive - circulating)`. At depth one its
reservation difference is `min(circulating, interactive - 1)`, not always
`circulating`. Withholding must actually remove capacity while the floor must
independently preserve one circulating permit. Ignoring withholding cannot
violate that floor. Memory restoration cannot return a service-owned permit.

<a id="banking-and-historical-attribution"></a>

## Defect mutations whose fixes have landed

The closed incidents below motivate distinct mechanisms; a closed issue or a
merged omnibus is not itself a reproduced root cause. The production negative
controls in [Detectors](#detectors) establish the mapped seams, not the original
deployment's throughput, heap size or complete ANN recovery loop.

- [#2781](https://github.com/NSTA1/Orleans.Lattice/issues/2781) introduced the
  missing memory dimension, repaired by
  [PR #2804](https://github.com/NSTA1/Orleans.Lattice/pull/2804).
  `MemoryWithholdingIgnoredUnderPressure` removes pressure withholding while
  preserving the floor; `FloorOneClampRemoved` independently removes that floor.
  `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`
  exercises the real withholding/restoration methods and physical accounting;
  the retained memory-withhold/floor controls reject their production bypasses.
  [PR #2866](https://github.com/NSTA1/Orleans.Lattice/pull/2866) subsequently
  added proactive heap observation when slice narrowing swallowed reactive
  faults. Pressure is an input here: the heap-observation/classifier path and
  the reported deployment's reclamation chain are not reproduced by this model.
- [#3284](https://github.com/NSTA1/Orleans.Lattice/issues/3284) was repaired by
  [PR #3285](https://github.com/NSTA1/Orleans.Lattice/pull/3285), with the
  harmful-queue refinement in
  [PR #3295](https://github.com/NSTA1/Orleans.Lattice/pull/3295).
  `QueueBoundedEnqueueIgnoresRefusal`, `QueueBoundedRefuseDepthQueues` and
  `SmoothedWaitRefusalAdmits` restore omitted refusal decisions, not a hard
  queue-cardinality cap. The real admission detectors, including
  `BPlusLeafGrainTests.A_no_progress_queue_at_the_circulating_bound_is_refused`,
  assert refusal on harmful observed depth; the retained excessive-wait bypass
  below independently fails both Interactive and Bulk cases. This matches the gate
  mechanism, not the separate ANN `OpenSliceBudget` accounting and circuit
  breaker; no exact model-mutation/production-detector pair for that full loop
  is claimed here.
- [#3065](https://github.com/NSTA1/Orleans.Lattice/issues/3065)'s uncancellable
  starvation-drive frame was repaired by
  [PR #3078](https://github.com/NSTA1/Orleans.Lattice/pull/3078).
  `DriveResidencyBoundedBudgetNeverAbandons` removes the budget opportunity that
  returns the outer drive's ownership. The real detector
  `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_releases_its_replay_permit_while_the_slice_read_is_still_parked`
  first establishes actual acquisition, then requires permit return while the
  provider is still parked. PR #3078 records a budget-removal perturbation that
  times out rather than returning; the current retained release-control below
  independently fails at free capacity 1 rather than 2 while storage is parked.
  This does not identify the host provider's original blocking cause, cancel
  an independent cold lifetime on a caller deadline, or make eventual banking
  into a wall-time bound.
- [#2951](https://github.com/NSTA1/Orleans.Lattice/issues/2951) is design lineage
  for self-reinforcing cold-replay demand, not a separately certified ANN fix.
  It was closed by the integration
  [PR #2482](https://github.com/NSTA1/Orleans.Lattice/pull/2482); that closure
  does not isolate a root-cause repair. The
  `NoSelfSustainingBacklogReplacementQueuesAgain` experiment duplicates a
  shared owner's queue lineage, and
  `BPlusLeafGrainTests.Caller_deadlines_and_replacements_preserve_the_live_cold_owner`
  rejects replacement-owner duplication in production. Neither executes the
  ANN corpus-open/breaker retry loop or establishes why the historical leaf
  read timed out. That exact end-to-end pairing remains absent.

The banking lineage below is separate from those admission and owner-lifetime
repairs; in particular, its first sweep reorder was not a complete banking fix.

A completed `ReplaySlice` represents a durably banked slice boundary. Only its
reached partition advances. An unreached partition must not acquire coverage
from the larger applied checkpoint. Cancelled warm replay banks the pending
checkpoint and snapshot state; cancelled cold replay banks its reread frontier.
An apply exception must preserve its preceding prefix and still propagate.

These histories are distinct:

- [#2089](https://github.com/NSTA1/Orleans.Lattice/issues/2089) was superseded by
  [#2098](https://github.com/NSTA1/Orleans.Lattice/issues/2098).
  [#2091](https://github.com/NSTA1/Orleans.Lattice/pull/2091) reordered sweeping;
  it was not the complete banking fix.
- [#2165](https://github.com/NSTA1/Orleans.Lattice/issues/2165), fixed by
  [#2178](https://github.com/NSTA1/Orleans.Lattice/pull/2178), records unresolved
  work before checkpoint advancement crosses its hole.
- [#2471](https://github.com/NSTA1/Orleans.Lattice/pull/2471) separately banks
  cold reread coverage below an existing monotone applied checkpoint.
- [#2746](https://github.com/NSTA1/Orleans.Lattice/issues/2746) is closed and its
  interrupted-replay regression is runnable. Its saturated-head membership
  detector checks the bounded cap overshoot and refuses later offers.

The partition vector belongs to one arbitrary focal lineage, `Focal = 1`.
Other owners are silo resource competitors, not writers advancing that same
leaf's coverage. A competitor's completed work may release its permit without
completing the focal leaf. Client replacements preserve their live owner;
a satisfied barrier never creates a fresh replay attempt. A partial-prefix
configuration distinguishes a slice advance from whole-window completion.

<a id="real-production-negative-controls"></a>

## Detectors

Every perturbation below was applied to actual production source, run against
the named detector and restored before the passing control run. Counts are
individual observed experiments, not a final module acceptance census.

| Perturbation | Production detector | Observed result |
|--------------|---------------------|-----------------|
| Cancelled uncooperative read on original source. | `BPlusLeafGrainTests.Cancelling_a_replay_returns_its_permit_before_uncooperative_storage_completes`. | Slice originally failed at gate 1 rather than baseline 2. Extending the reachable boundary detector exposed two additional assertion failures, head and tail; all three fixed controls pass. |
| Remove only the post-await cancellation recheck. | `BPlusLeafGrainTests.A_completed_provider_read_does_not_bypass_replay_cancellation`. | Both filtered/unfiltered completed-provider cases assertion-red (no exception); restoring/rebuilding the guard makes both green in the 12-case owner/read preservation run. |
| Couple caller cancellation to independent owner lifetime. | `BPlusLeafGrainTests.Caller_deadlines_and_replacements_preserve_the_live_cold_owner`. | Queued and parked-read cases both assertion-red; restored controls green. |
| Remove the live-owner singleflight guard. | Same actual-owner replacement detector. | Both cases assertion-red: changed barrier identity and extra queue/permit ownership. |
| Omit queued-departure accounting. | `BPlusLeafGrainTests.Cancelled_queued_replay_replacements_leave_no_waiters_or_unowned_permits`. | Assertion-red at queued residue 1 instead of 0; restored owner/read preservation control green. |
| Park queued acquisition forever despite available capacity. | `BPlusLeafGrainTests.Finite_interactive_and_bulk_cohort_progresses_when_capacity_and_runtime_are_available`. | Both class-order cases assertion-red with free=1 and queued=2; actual owners cancelled and counters cleaned during teardown. |
| Count cold owners against the GC reservation share. | `StarvationReplayAdmissionTests.Ordinary_replay_owners_do_not_consume_gc_reservations`. | Both physical-occupancy arrangements assertion-red; restored controls green. |
| Remove last-permit withholding floor. | `ReplayPermitsCoyoteTests.Memory_returns_and_priority_observations_preserve_the_gate_contract`. | One failing Coyote case. |
| Ignore memory withholding entirely. | Same real-helper Coyote detector. | One failing case, independent of the floor obligation. |
| Remove Interactive/Bulk reservation margin. | Same real-helper Coyote detector. | One failing case. |
| Bank persisted checkpoint instead of cold reread frontier. | `BPlusLeafGrainTests.Cancelled_cold_replay_banks_the_re_read_frontier_as_durable_snapshot_coverage` and `Cancelled_cold_replay_does_not_claim_coverage_for_a_partition_it_never_re_read`. | Both fail. |
| Omit cancelled warm replay banking. | The three `Cancelled_warm_replay` methods. | All three fail. |
| Omit durable deferred-terminal insertion but retain its index. | `BPlusLeafGrainTests.A_saturated_ledger_admits_the_head_of_window_terminal_and_refuses_the_ones_behind_it`. | Exact membership/count assertions fail; interrupted progress alone did not detect this perturbation. |
| Omit actual drive permit release. | `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_releases_its_replay_permit_while_the_slice_read_is_still_parked`. | Fails at free 1 instead of 2 while provider remains parked. |
| Ignore excessive-wait refusal in acquisition. | `BPlusLeafGrainTests.A_wait_exceeded_mean_refuses_below_the_admitted_waiter_bound`. | Interactive and Bulk cases both fail. |
| Let memory restoration steal service capacity. | `BPlusLeafGrainTests.Clean_activation_memory_restore_cannot_return_a_service_owned_permit`. | Combined ledger 0 instead of 1; physical free 3 instead of 2 while service ledger remains 1. |

The last detector originally failed before its ownership seam on a two-CPU
process because it assumed default capacity above two. The three-permit memory
restore detector similarly assumed capacity above three. Both now explicitly
configure the capacity their experiments need; visible processor count remains
two. Both original precondition failures were recorded, then both corrected
tests passed. The precondition-only mutant run is not ownership evidence.

The Coyote test explores cooperative choices over real helpers and a real
semaphore with per-run state. It is not a claim about rewritten CLR schedules.

## Deadline ownership detector correction

The earlier authored `ColdDeadlineIsolation` guard required unchanged owner
status before checking lifetime cancellation. That hid a deadline which directly
retired the shared owner while returning its permit and preserving cancellation
membership. The independent `ColdDeadlineIsolationAbandonsOwner` control exposed
this gap: the old target incorrectly accepted all 4,567,280 mutant states, so the
paired regression failed. The strengthened target identifies `ClientDeadline`
without presupposing its owner-preserving result, and checks unchanged holders,
free/withheld capacity, queue, status, role and cancellation membership. Both the
original cancellation-coupling control and the direct-abandonment control now
fire independently against clean controls. No Next action or bound changed.

## Property classification (issue #2321)

The [mutation catalogue](replay-permits-mutations/README.md#classification)
classifies every anchored action, helper and read-definition perturbation.
There are no added-transition splices. Helper edits are not credited as direct
Next-action coverage. The target's invariant/action/temporal classification
does not convert the explicit environment premises into guarantees.

## Single-target detector corrections

The first final exact-hash sweep executed all 29 Replay pairs and 21 harness
controls: 47 passed and three correctly refused to certify their mutants.
Both `ClassPriority` mutants violated the intended clause, but the separately
boxed conjunction made TLC print a source-span action name instead of
`ClassPriority`. Folding `[][A]_vars /\ [][B]_vars` into
`[][A /\ B]_vars` preserves both action clauses and stuttering semantics while
giving the one-target checker one named action property. Arbitrary-trace
equivalence controls in periodic and final modes also reject a fold that drops
the second clause. Strict target-name recognition is not relaxed.

The lifetime-domain mutant previously added a string to numeric request ids:
TLC rejected a mixed-domain comparison while evaluating TypeOK rather than
reporting a TypeOK violation. It now introduces numeric owner 0, outside the
declared request population, matching the independently reachable hang-domain
control. Evaluation errors remain disallowed counterexamples.

These artifact repairs change the source hash without changing Next, bounds,
fairness or any checked obligation. Clean base, variants and all 29 paired runs
use the repaired hash; earlier-hash results remain historical evidence.

The subsequent 52-case cohort passed every Replay pair and 21 harness controls.
Its two folding probes exposed an authored-probe defect: TLC does not support
the nested temporal action formulas used directly in that equivalence. Both
modes rejected the probe with exit 255, rather than finding a model violation.
That failed artifact is retained, not rewritten as a passing cohort.

The repaired probe observes arbitrary Boolean action truths `a`, `b` and a
shared `stuttering` predicate. A boxed action means its action predicate or
that same stuttering predicate, so its full temporal comparison is
`[](a \/ stuttering) /\ [](b \/ stuttering)` against
`[]((a /\ b) \/ stuttering)`, checking both implications. All eight initial
valuations and arbitrary successors overapproximate every concrete action
trace; this is not one hand-picked trace or merely a pointwise assertion.
Dropping `b` remains the genuine negative control. Both launch modes produce
clean complete-state controls and temporal counterexamples, including
`a = TRUE`, `b = FALSE`, `stuttering = FALSE`. Their native outputs are retained
with the before/after probe source. No target recognition or outcome assertion
was relaxed, and no Replay model, bound, premise or launch policy changed.

<a id="non-circular-temporal-premises-and-limits"></a>

## Deliberate abstraction gaps

- **Class non-starvation assumes finite load and departing holders.**
  `ClassNonStarvation` covers both actual queued ambient classes. The external
  load eventually stops offering attempts or replacements; pressure is absent
  and the classifier remains `Draining` on the stable suffix. Existing holders
  complete. A workload with bounded holder completion satisfies this premise;
  `HoldersComplete` states eventual departure, not a wall-time upper bound. Its
  normalized `[]<>~held` form is exactly equivalent to `held ~> ~held` on all
  Boolean traces: when not held the eventual goal already holds now; when held
  both forms require a future non-held state. Infinite reacquisition remains
  allowed and an immortal holder still violates the premise. The
  arbitrary-holder trace control checks both implications without fairness and
  rejects the opposite recurrence, in periodic and final modes. The finite
  offered-attempt population means another class cannot manufacture an endless
  stream of overtaking acquisitions.
  No production symbol: the gap states an environment premise of the model, not
  a production mechanism.
- **Budget opportunities are not a dispatcher.** The round-robin
  `BudgetOpportunity` counter abstracts continued execution of each existing
  owner's registered budget/cancellation continuation, not a production FIFO or
  class dispatcher. A drive's bound is counted in its budget visits; arbitrary
  interleavings or absent runtime opportunities have no claimed wall-time bound.
  No production symbol: the gap is a modelling abstraction with no single
  production owner.
- **Capacity callbacks are a runtime premise.** `CapacityOpportunity` is an
  unconditionally enabled generic runtime callback: it toggles its epoch even
  without work. Weak fairness assumes callbacks keep running, not that any
  particular class is selected or that a desired service event occurs. Its
  implementation grants some eligible waiter when capacity exists. The
  starving-admission mutation preserves callbacks and these load and holder
  premises while withholding every grant. The real production negative control
  similarly parks queued acquisitions despite available capacity. Neither
  premise assumes FIFO, fair class admission or jitter-derived fairness.
  No production symbol: the gap states an environment premise of the model, not
  a production mechanism.
- **Replacement lineage is a transition property.** `NoSelfSustainingBacklog` is
  a transition property of replacement lineage: reactivating a client on a live
  owner must not change that owner, queue or held permit set. Queued
  actual-lifetime cancellation independently removes arrival accounting before a
  new attempt can replace it.
  No production symbol: the gap states how a property of the model is phrased,
  not a production mechanism.
- **Replay convergence premises.** `ReplayConverges` requires infinitely
  recurring non-hung focal ownership if it has not already completed, and strong
  fairness of each unfinished partition's storage-slice completion inside a live
  attempt. This storage-completion premise is explicit: completion only after
  every abandoned attempt is insufficient. Completed slices durably shorten a
  finite gap; interruption cannot erase them. No fairness of `Hang` or actual
  cold cancellation is assumed. A hung, uncancelled cold owner remains
  representable forever, as the approved guarantee requires. Banking and
  teardown persistence must finish for cancellation unwind; no state-mutating
  continuation is detached to manufacture this guarantee.
  No production symbol: the gap states an environment premise of the model, not
  a production mechanism.

## Harness and fixture corrections encountered here

[#4989](https://github.com/NSTA1/Orleans.Lattice/issues/4989) repairs strict
recognition of TLC's genuine initial-state invariant banner. The normal and
initial-state forms both count as violations of exactly the named invariant;
evaluation errors, wrong targets/classes, prefixed prose and malformed suffixes
do not. Thirteen parser cases and a genuine Synthetic clean/initial-state
counterexample pair exercise this boundary.

[#4991](https://github.com/NSTA1/Orleans.Lattice/issues/4991) gives the two
existing capacity-dependent fixtures explicit test capacities while preserving
two visible CPUs. Their original precondition failures, repaired positives and
an actual service-ledger ownership perturbation are retained separately.

[#4995](https://github.com/NSTA1/Orleans.Lattice/issues/4995) fixes avoidable
positive-leads-to tableau expansion in the holder-completion premise. The two
class-progress branches of the original form each grew a roughly 4.3 GB graph.
The exact temporal normalization above preserves that premise on every trace;
it changes no Next action, bound, fairness or checked guarantee.

An exploratory sequential launch policy passed argument and genuine temporal
controls but still exhausted the same heap on the full old-form graph. It is
not shipped: periodic checking keeps the original default policy, and an
explicit final check still uses `final`, not `seqfinal`. The actual launch-option
controls retain both modes and two state-exploration workers; genuine temporal
positive and negative controls exercise both modes.

The read-only supplied jar is byte-identical to the repository-pinned CI
v1.7.4 artifact: SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The historical strengthened, normalized base with SHA-256
`A15F8BEBCB64877BDEDA75140B26F5DFED005A99493FE58B72F2DF94CED7034B`
completed all nine invariants, eleven properties and five temporal branches at
the same 1800 MB configured heap, two workers and two visible CPUs under the
original periodic policy. It explored 695,064 distinct / 6,628,373 generated
states, depth 27, in 12 minutes 29 seconds, including the final 1 minute 34 second
liveness check. This established the root repair, not the earlier passing
argument tests. It is earlier-hash evidence, superseded for final certification
by the current-hash measurements below. All historical OOM runs remain explicitly
non-certification.

## Retired-attempt class quotient

The model canonicalizes only `role[r]` after an owner returns or a cancelled
queue attempt departs into `idle`. The equivalence relation identifies states
that differ only in those retired-attempt labels. Active, pending and queued
classes, shared-owner identity, holders, queue entries, client state, captured
observations and durable progress remain distinct. Every `Request` captures its
class afresh; no guard or future choice consults an idle attempt's old class.

All checked clauses are preserved by this relation: class-sensitive predicates
inspect an active/pending/queued owner or the freshly captured primed class.
No checked clause relies on a retired class. Every transition and its projected
successor have the same action label and enabledness, including the fairness
subactions. Retirement still changes status, so quotienting its redundant class
write cannot turn a fairness-relevant transition into a stutter. Bounds, Next
actions, fairness, temporal premises and all twenty checked clauses are unchanged.
This is removal of unobservable attempt history, not removal of contention or a
class guarantee. The actual new acquisition reads ambient class afresh.

The larger pre-quotient draft enumerated 741,256 distinct / 7,019,293 generated
states, depth 27, in 16 minutes 13 seconds with two TLC workers and two visible
CPUs. Its final five-branch liveness check exhausted the modest 1800 MB configured
heap. That run is historical graph/cost evidence, NOT final certification.
Its source SHA-256 is
`08385E0D2EEF6F15D11E0FD61E0562419107A2863859EC6859F6023C7AF116CE`.
The current quotient's complete base and larger-bound remeasurements are below;
all 29 independent one-target pairs have also passed against that exact source.

## Configurations and reproducibility

Every base and larger-bound configuration retains all nine invariants and all
eleven properties, including TypeOK. All three completed runs below use exactly
the current source SHA-256
`657769D201A472F5EFFF54C85EC17A2F56D0CB81F6F4844F0B91E49426BB10D0`.
Each finished with no error after checking all five temporal branches, using
the original periodic launch policy, two state-exploration workers, two visible
CPUs and an 1800 MB configured heap (TLC reports 1600 MB usable).

| Configuration | Bounds changed from base | Distinct / generated states | Depth | Total process time | Final liveness |
|---------------|--------------------------|-----------------------------|-------|--------------------|----------------|
| Base | None. | 695,064 / 6,628,373 | 27 | 756.410 s | 1 min 50 s |
| TwoPartitions | `Partitions = {1, 2}`. | 2,762,408 / 26,786,445 | 29 | 4,748.324 s | 23 min 45 s |
| PartialPrefix | `GapLen = 2`, `Slice = 1`. | 1,038,664 / 9,888,749 | 29 | 1,058.110 s | 2 min 57 s |

The table above certifies the model before the #5569 reserved system lane. The
current model was checked in CI run 41274403258, which records only distinct
states: Base 1,058,056, TwoPartitions TWOPART_FMT and PartialPrefix 1,582,264.

`TwoPartitions` exercises independent per-partition progress and durable
coverage. `PartialPrefix` requires multiple completed slices to finish one gap;
a single slice is not whole-window completion. Both are enrolled repository
configurations and manifest variants. The obsolete TwoPartitions count 351,244
does not describe the current model. The complete three-configuration process
cost was 6,562.843 s (about 109 min 23 s), measured serially rather than inferred
from the base. The low-memory warning during TwoPartitions liveness was followed
by successful completion at the same heap, not a heap increase or omitted branch.

The 29 mutation pairs isolate one target each (plus TypeOK for non-TypeOK
targets). The final 23 harness controls include strict banners, genuine
initial-state and temporal positive/negative cases, both launch modes, and
arbitrary-trace holder and action-folding equivalence controls. All have passed.

Retained `replay-named-final-29-pairs-and-harness-controls.trx` records all 29
Replay pairs passing within the original 52-case run (50 passed, two unsupported
authored folding probes failed). Its native test-run duration was 23 min 19 s;
its UTC TRX start/finish interval was 1,413.7458152 s. No separate monotonic
process receipt was recorded for that run, so neither number is presented as
one. The corrected `replay-final-23-harness-controls-fixed.trx` records all 23
harness cases passing, with no skipped cases, and retains both native folding
control/counterexample outputs. Its native test-run duration was 30 s; the
separately measured build-plus-controls process cost was 125.8571349 s.
These are separate real artifacts, not a manufactured merged green TRX.
The unchanged 29 model pairs and three complete configurations were not rerun
to repair a probe-only issue.

Run the scoped repository TLC harness with `FullyQualifiedName~ReplayPermits`,
`TLA_TOOLS_JAR`, `JAVA_HOME`, `LATTICE_TLC_CONCURRENCY=1` and
`DOTNET_PROCESSOR_COUNT=2` configured as in the [root guide](../README.md).
The anchor catalogue is in [replay-permits-mutations](replay-permits-mutations/README.md).
