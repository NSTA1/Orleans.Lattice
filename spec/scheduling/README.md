# Scheduling assurance

These models separate activation-local turn ownership, saturation-signal waits
and independent WAL semaphore admission. They reuse the explicit turn/queued
request/external-completion encoding of SplitLinkTurns and SplitGateCycles.
TurnOccupancy is a fully discovered module, not a helper exemption.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| TurnOccupancy | 4 | 1 | 8 | 7 | 11 | 961 |
| WalAdmission | 4 | 8 | 11 | 16 | 21 | 86496 |
| WalBackPressure | 6 | 16 | 18 | 34 | 38 | 135464 |
| ReplayPermits | 9 | 13 | 22 | 32 | 42 | 1058056 |
| MaterialiserDrain | 6 | 8 | 21 | 21 | 33 | 1076 |
| SagaReadProgress | 3 | 5 | 6 | 11 | 12 | 2451 |
| TagIndexReconcile | 3 | 6 | 12 | 15 | 19 | 2784 |
| ReplicationShipping | 4 | 12 | 19 | 26 | 33 | 527689 |
| WalGcStarvationDrive | 5 | 12 | 11 | 23 | 26 | 7160 |
| WalGcStarvationDriveWitness | 2 | 3 | 13 | 14 | 13 | 7906 |

## Instances and progress premises

TurnOccupancy's base has two grains and one finite timer episode per grain.
ThreeGrains adds an independent activation. Its safety checks remain exhaustive;
EveryCallServed is bounded-out there to avoid multiplying liveness tableaux.
The base retains that temporal check. The local bound counts only admission
and dispatch opportunities on the waiting activation. It does not count
arbitrary stutters, work on another activation, storage completion or time.
Weak runtime fairness supplies eventual opportunities, never a latency bound.
External drive completion is deliberately unfair: it may remain pending forever.

WalAdmission's base has two callers, two partitions, capacity one, positive
release batch one, three sample-unit wait/call budgets, and a two-sample recovery
window. Every caller-to-partition assignment is explored. Thus two callers can
park on one partition (herd, FIFO and residue witnesses), or one on each
(isolation). ThreeCallers adds an asymmetric fixed cohort, two callers on one
partition and one on its sibling. Its action safety checks are exhaustive;
EveryWaiterResolves and NoAbsorbingSaturation are bounded-out there for liveness
budget, with both retained on the base.

The original three-caller draft instance with independent persistent audit
snapshots took over eight minutes. The final base explores all routing choices
for the smallest cohort preserving both required witness shapes. Transition
properties inspect before/after queue state directly instead of retaining
redundant audit history. Unused deadline counters are canonicalized to zero
after signal resolution/admission; those counters cannot affect later steps.
The larger asymmetric cohort retains the third caller without a Cartesian
product of irrelevant routing choices. No property or behavioural action is
exempted for this reduction.

The release-edge negative control exposed an invalid first reduction: one-unit
deadlines removed the residue before the next recovery tick. Both base and
variant therefore retain three-unit deadlines, long enough for the two-waiter
residue to survive into a later qualifying tick. This bound is part of the
clean control too, not a mutant-only override.

The sampler iteration and storage completion are explicit environment steps.
Sampler fairness is needed to age finite budgets. Flush fairness is an explicit
storage-progress premise, not a production guarantee. Acute faults may remain
on forever: deadline refusal still resolves parked callers. Eventual permanent
non-saturation additionally requires permanently cleared acute causes and an
empty partition. No permanent Healthy verdict is promised under offered load.

## WAL back-pressure

[`WalBackPressure`](WalBackPressure.tla), its
[manifest](WalBackPressure.manifest.json) and
[refinement mapping](WalBackPressureRefinement.md) model the cached classifier
and its writer, saga, snapshot-open, receiver and physical view consumers.
Partition depth contributes to the worst-state roll-up; acute dispatch/provider
trip deltas are summed across the tree before publication. Cause priority is
separate from refusal provenance. Recovery floors the whole tree only when its
raw verdict is Healthy: zero disables the floor and Infinite keeps it sticky.

The composition seam with WalAdmission is the saturation decision, not a
capacity reservation. Signal release, downstream semaphore admission and
storage completion remain distinct. WalAdmission's independent acute inputs
are conservative admission cases, not a claim that the actual classifier's
acute trip counters are partition-local.

All seven relief actors - flush, materialiser drain, GC trim, checkpoint,
durable-pin write, saga terminal and terminal-tail resumption - remain
independent of signal admission, pacing and quiesce. A terminal-only writer
operation is classified before partition fan-out. Mixed-data writes remain
gated; capacity, storage acknowledgement, cancellation, deadlines and shutdown
still apply. Materialiser drain replays and checkpoints leaf WAL work; the
additional WAL storage shutdown-drain detector is not a substitute.
Terminal-tail resumption delivers commit/abort without new data; ordinary
resumed Execute data is still quiesced.

The model checks six invariants, sixteen properties and eighteen Next actions.
Only Stutter is non-behavioural. Its 34 one-target paired mutations cover every
checked clause, including TypeOK and every environment action. The 38
behaviour-asserting rows resolve to 36 distinct detector methods. Real sampler,
writer, coordinator, snapshot, receiver and projection/aggregation drain
controls cover Healthy, Throttled and Saturated responses. Coyote controls
execute the real saga quiesce, terminal writer and reminder-driven terminal
resumption over explicit choices, not a duplicate algorithm or a claim about
all CLR-thread schedules.

| Instance | Distinct states | Generated states |
| --- | --- | --- |
| Base | 135,464 | 559,372 |
| Bursty | 262,606 | 1,060,078 |
| BurstyFinite | 368,446 | 1,521,488 |
| Sticky | 658,316 | 2,752,248 |
| SagaReroute | 1,593,944 | 6,708,152 |

Every safety clause is retained in all four variants. Bursty and BurstyFinite
exercise zero and finite recovery with AcuteOnly disabled; Sticky exercises
Infinite recovery; SagaReroute enlarges captured scope and map-epoch cases.
The three temporal properties stay at the base bound. Departure from Saturated
requires stopped acute inputs, including sustained flush latency, finite offered
bursts and real relief opportunities. Healthy additionally requires stopped
throttling inputs and finite recovery. Completing a terminal or checkpoint does
not manufacture disappearance of a sampled pin-latency fault. Fair callback
opportunities do not imply bounded wall time; the shared-window episode
abstraction is explicitly narrower than arbitrary independent signal histories.

Measured direct TLC times were 52.79, 25.15, 31.20, 71.08 and 148.59 seconds
in the table's order: 328.81 seconds for five configurations. The complete
39-case run, including all 34 clean-control/mutant experiments, passed in
729.46 seconds (12 minutes 9.46 seconds). Both measurements used two visible
CPUs, two TLC workers, one slot and a configured 1,800 MB heap under the
unchanged launch policy. They are this module's observed cost, not the whole
scheduling shard's cost, a peak-live-heap measurement or a CI deadline.

The member repairs terminal signal/pacing dependence (#4986), ordered
refinement comment lexing (#4990) and coverage-verdict parsing (#4994), each
with before/after controls. Its own missing Throttled view reduction was
rejected by a paired mutation before certification; independent receiver/view
Throttled, point/batch pacing, tree-trip sum, fresh-routing and actual
quiesce-budget call-site controls reject the corresponding production edits.
The refinement note retains these corrections and the terminal-resumption
detector correction rather than certifying earlier candidate results.

## Saga and reader progress

[`SagaReadProgress`](SagaReadProgress.tla), its
[manifest](SagaReadProgress.manifest.json) and
[refinement](SagaReadProgressRefinement.md) compose AtomicCommit's acknowledged
prepares and immutable decisions with WalBackPressure's cached scoped admission
verdict. A continuous writer cannot force a reader into optimistic retry
exhaustion: after bounded retries, the real fallback acquires an unrenewed
decision gate, reads a whole batch and validates coverage. Successful answers
need available collaborators, stable routing and a live lease; lost lease or
coverage must resolve explicitly rather than certify mixed data.

Terminals may reach any undrained leaf next, independent of pressure.
All terminals, the durable completion checkpoint and original caller
acknowledgement are separate steps. A recorded decision or an in-memory
Completed phase is not caller success. Weak fairness applies to delivered
continuation opportunities with production-selected disjoint branches, not
to a policy choice, competing-reader admission or bounded wall-clock service.

The Counts table records three state invariants, five properties, six Next
actions, eleven one-target paired mutations and twelve behaviour rows.
Stutter is non-behavioural; TypeOK is model-domain sanity. Thirteen distinct
production detector methods cover the behavioural rows, including four Coyote
methods; two of those methods are reused from WalBackPressure.

| Instance | Distinct states | Generated states | Checked boundary |
| --- | ---: | ---: | --- |
| Base | 2,451 | 17,838 | Two leaves, one retry, all safety and temporal checks. |
| ThreeLeaves | 14,076 | 106,179 | Three leaves, three retries, all base safety and temporal checks. |
| LeaseLoss | 3,585 | 26,124 | Arbitrary lease loss; retains resolution and saga completion, not successful answer under an invalid lease. |

Direct wall times including JVM startup were 1.627, 5.907 and 1.980 seconds
in that order, configured Xmx1800m and two workers on two visible processors.
All fourteen base/variant/paired TLC cases passed. Their individual test-case
elapsed times sum to 62.892553 seconds, a measured contribution to CI pricing,
not a whole-shard wall-clock measurement. The larger bound retains liveness.

Real source perturbations reject historical retry exhaustion, mixed decisions,
gate and expiry bypass, lost coverage, omitted terminals, a stranded original
caller, early checkpoint acknowledgement, skipped quiesce and a lost reader
deadline. Fourteen parameter-expanded detector cases cover thirteen method keys.
No new production defect is claimed: these controls exercise already-correct
production seams. The refinement retains the original mixed-snapshot
mutant-green and its correction, arbitrary terminal order, and distinct
checkpoint/acknowledgement steps instead of certifying the earlier model.

## Materialiser drain

[`MaterialiserDrain`](MaterialiserDrain.tla), its
[manifest](MaterialiserDrain.manifest.json) and
[refinement](MaterialiserDrainRefinement.md) distinguish five scenarios in one
discovered module: online snapshot shard-copy slots, admitted leaf replay and
durable checkpoint/snapshot/pin progress, view service, WAL-shard shutdown and
source repair after a split. This is a sum of scenarios, not a concurrent
cross-product or a second replay-permit pool.

MaxConcurrentDrains limits online shard-copy calls; BackgroundDrainLeavesPerPass
limits source leaves in one pass. Neither caps every leaf materialiser.
WalDrainBudget bounds each WAL-shard in-flight drain, with a possible fresh
budget for a pending follow-on batch, not every leaf teardown barrier.
InfiniteTimeSpan intentionally opts out of the finite-budget claim. A provider
may ignore cancellation; delivered deadline processing must still settle or
fault its affected callers with the correct shard identity.

The pure WalBackPressure ViewBatch operator supplies positive Healthy,
Throttled and Saturated service. Unread-row count measures this model's
backlog; the production drain-lag classifier instead observes oldest-unread
age and consecutive windows. Supplied pressure regimes cover all consumer
arms, including retained recovery pressure; the model does not infer a
classifier verdict from its row count.
Stable offered load counts writes between successful service opportunities,
including pauses and storage completion, not between sampler ticks. Minimum
service must cover that offered load. Replay admission must have settled;
successful replay and durability completions are separate progress premises,
not a new deadline imposed on a cold activation owner.

Rebuild and reconcile retain the physical source identity while refreshing
the owning logical row's live routing after a split/stale-root response.
This is not an alias-epoch cursor reset. The module does not establish a
root cause or resolution of the historical deployment investigation #3481.

The Counts table records six invariants, eight properties, twenty-one Next
actions and twenty-one anchored mutations. Stutter is the sole non-behavioural
action; TypeOK remains a model-domain check rather than a production row.
The thirty-three behaviour rows resolve to sixteen distinct detector methods;
one is shared with WalBackPressure. Base and Larger have 1,076 and 3,780
distinct states, with 3,126 and 11,866 generated states respectively. Both
retain all six invariants and eight properties, including all five temporal
branches. Direct base/Larger wall times were 1.7064307s and 2.7617059s.

The complete twenty-three-case TLC interval was 54.083547 seconds, including
case-to-case gaps, at configured Xmx1800m, two workers and two visible CPUs.
That measured contribution is not a measurement of the combined scheduling
shard. All twenty-three TLC and fifty-three structural/refinement cases passed.
The sixty-two focused production/hygiene/routing instances also passed;
eighteen actual production-source perturbation/restoration pairs supply
assertion-red evidence for all sixteen mapped methods. The refinement retains
the original evaluator-error TypeOK run, ambiguous mutation anchor and
false-green slot/pass detectors, not as clean certification.

## Replay permits

[`ReplayPermits`](ReplayPermits.tla), its
[manifest](ReplayPermits.manifest.json) and
[refinement](ReplayPermitsRefinement.md) distinguish physical permit ownership,
memory withholding, priority observations and bounded warm-drive residency.
Returning a caller wait does not return a shared cold owner's physical permit.
The cold owner keeps it until its replay frame unwinds. Genuine cancellation
of that independent owner lifetime can unwind an uncooperative read-only
provider await without consuming its late result. Prefix banking and durability
writes are not detached. This does not turn a client deadline into an owner
deadline.

The model keeps available, held and memory-withheld capacity separate. Memory
pressure never withholds the final circulating permit; recovery restores only
an actually withheld permit. The Bulk margin is floored and depends on the
current circulating capacity and observed queue depth, not a global FIFO
contract or a reservation already granted to the caller.

Progress needs actual returned capacity, continuation opportunities and earned
coverage banking. Warm-drive budget expiry is not a promise to terminate an
uncooperative provider or an intentionally shared cold owner. Permanently hung
holders remain reachable. Fair opportunities are not bounded wall-clock service.

A reserved system lane (issue #5569) gives replays of system-owned trees one
permit outside the shared pool. `SystemLaneProgress` proves a system waiter
acquires while every shared holder is hung, assuming only that system replays
are bounded. `LaneExclusive` proves no other replay ever holds the lane.

| Instance | Distinct states | Generated states |
| --- | --- | --- |
| Base | 1,058,056 | Not recorded |
| TwoPartitions | TWOPART_FMT | Not recorded |
| PartialPrefix | 1,582,264 | Not recorded |

These distinct counts are from the #5569 model in CI run 41274403258, which
prints only the distinct count. The pre-lane model explored 695,064 / 6,628,373,
2,762,408 / 26,786,445 and 1,038,664 / 9,888,749 distinct / generated states.

All three configurations retain all nine invariants, thirteen properties and six
temporal branches. PartialPrefix requires multiple completed slices to finish
one coverage gap. Thirty-two one-target paired experiments cover the checked
clauses and environment steps; Stutter alone is non-behavioural. Before the
#5569 lane, the thirty-eight behaviour rows mapped to sixteen distinct production
methods; the lane adds four rows. The actual-gate
Coyote fixture lives in the core package's BPlusTree/Coyote directory.

The unchanged 1,800MB/two-worker periodic policy completed the three full
configurations in 756.4095982, 4748.3236603 and 1058.1101466 process seconds.
The original 52-case execution passed all 29 model pairs and 21/23 harness
controls; both remaining probes used nested action formulas rejected by pinned
TLC. The repaired probe checks full boxed-action equivalence using arbitrary
Boolean action/stutter observations, with one common stutter bit and a genuine
drop-second-clause negative in both checking modes. A separate 23-control run
passes all 23. These are separate retained artifacts, not a fabricated green 52.
Historical A15/OOM results and rejected evaluator-error negatives remain history,
not acceptance at the current 657769 model hash.

## Replication shipping

[`ReplicationShipping`](ReplicationShipping.tla), its
[manifest](ReplicationShipping.manifest.json) and
[refinement](ReplicationShippingRefinement.md) separate a finite physical-source
window, FIFO acknowledgement folding, receiver feedback and retry-driven
deferral progress. The cursor claim is local to one source lineage; rebind and
reseed may reset it. A rejected head cannot be skipped because a later
in-flight batch succeeds.

The model calls the actual `WalBackPressure.ReceiverShrinkBatch` action. The
supplied logical name and Healthy/Throttled/Saturated 4/2/1 regimes remain
unchanged. A separate finite first-head opportunity supplies valid batch-only
feedback without manufacturing a pause or relabelling a Healthy verdict.
Already launched batches cannot be recalled. Observed shrink or pause stops
replenishment for this pump, including held-terminal release; later null
feedback may change the next pump's offer but cannot erase the current stop.

`EveryDeferralBounded` is conditional on delivered retries and elapsed-time
opportunities, available bounded retention, completed reseeding and a stable
receiver lifetime. A decided prepare is never discarded merely because its
timeout expired: its application fault must clear for stream progress.
Dead-letter, poison and quarantine admission each fail closed when full.
Receiver restart loses volatile first-deferral age, not durable poison or
quarantine. Retry count does not substitute for elapsed time. No-reseed memory
is scoped to retained retirement history, not arbitrary history that evicts
the bounded production marker. Jitter is not fairness or a latency bound.

| Instance | Distinct states | Generated states |
| --- | --- | --- |
| Base | 527,689 | 2,174,007 |
| FourRecords | 3,306,850 | 14,126,738 |

Both full configurations retain all four invariants and twelve properties.
The base completes at depth 57 and FourRecords at depth 71. Their separately
measured processes take 171.2132167s and 1809.0807471s under the unchanged
configured 1,800 MB/two-worker policy. That 1980.2939638s sum is not the complete
paired-cohort or scheduling-shard price.

The complete revised harness subsequently passed 28/28 cases: both full
configurations and all twenty-six paired mutations, with fresh process-local
controls. Its launcher-artifact receipt records 2745.4195361 monotonic seconds
under the same configured resource policy. CI does not compose these
certificates. Its scheduling prices are the means of two measured work-item
times, from the receipt-free bucket push ci.yml run 40909619830 (head 9418a498d)
and the nightly-equivalent coverage.yml run 40923724192 (head 0e4aa6b2a), both
of which passed every case: the dedicated TwoPartitions item took 35.22 and
52.78 minutes (jobs 123577568395 and 123623834893), priced 44.00; the 106-case
general group 31.71 and 30.38, priced 31.05; the 86-case group 12.11 and 20.64,
priced 16.38; Shipping base/pairs 4.37 and 4.20, priced 4.29; and FourRecords
22.29 and 17.23, priced 19.76. A single case varies up to 1.5x between runs on
identical inputs, so these estimates are planning inputs, not runtime
guarantees. The earlier standalone fulls remain
diagnostics. Accepted completion binds the fresh TRX, redirected native output
and immediate exit-code/Stopwatch receipt; it is not a recovered CLI terminal
event, at-launch loaded-binary attestation or peak-RSS measurement.

The model declares 26 one-target pairs and 33 behaviour rows, covering every
Next/environment action except non-behavioural Stutter. Twenty distinct
production detector methods include four names already in the prior area
census, so this module adds 16 distinct names, not 17. Seventeen source-red
methods are newly established relative to the earlier source evidence;
the Healthy method is new source-negative evidence for an already-counted
name. The real pump/applier/policy regressions remain distinct from the formal
experiments. Coyote executes controlled opportunities through those production
seams in the replication test package; it is not a replacement protocol or a
claim to explore every CLR Task schedule.

## WAL GC starvation drive

[`WalGcStarvationDrive`](WalGcStarvationDrive.tla), its
[manifest](WalGcStarvationDrive.manifest.json) and
[refinement](WalGcStarvationDriveRefinement.md) model GC passes driving
starved checkpoints through the shared replay gate against competing coverage
timers and cold owners. The timer holds a reserved slot, a refused touch is
never charged, and the floor holder is eventually driven while the WAL head
stays ahead of its pin.

This core is the CI-bound split of a single model that did not complete within
the CI budget. It abstracts the per-leaf stall clock and the foreground call
queue, and states turn release as `TurnReleased`.
[`WalGcStarvationDriveWitness`](WalGcStarvationDriveWitness.tla), its
[manifest](WalGcStarvationDriveWitness.manifest.json) and
[refinement](WalGcStarvationDriveWitnessRefinement.md) keep both mechanisms at a
small bound. The witness checks the original `TurnNotHeldAcrossDrive`
obligation, checks the marked initial state as the `CoreInitRefined`
invariant, and checks, as TLC properties, that its steps and fairness project
onto the core. TLC checks that refinement at one leaf only, as supporting
evidence; the parametric argument that it holds at any constants, and the
table of checked versus argued claims, are in the core
[refinement](WalGcStarvationDriveRefinement.md) note.

The leaf count is the `LeafCount` constant. The one-leaf base is the fast
gate; the cross-leaf ordering, credit and shared-slot paths are checked by the
two-leaf `TwoLeaf` and `ShareOne` variants and by the seven mutants that carry a
`LeafCount = 2` bound.

| Instance | Distinct states | Checked boundary |
| --- | ---: | --- |
| WalGcStarvationDrive Base | 7,160 | One leaf, all safety and temporal checks. |
| WalGcStarvationDrive TwoLeaf | 849,286 | Two leaves, all safety and temporal checks. |
| WalGcStarvationDrive ShareOne | 171,308 | Two leaves, single-slot share and refusal window, all checks. |
| WalGcStarvationDriveWitness Base | 7,906 | Stall clock and call queue, one leaf, refinement onto the core. |

The TLC gate compares each run with its manifest and fails on any mismatch.

A three-leaf core and a larger refinement instance were measured outside CI.
The liveness and refinement runs did not finish within the twenty-minute cap,
so neither is enrolled and neither carries a claim.

## Routing and evidence

TagIndexReconcile snapshots a finite covered set, probes in tree chunks and
repairs one entire tree per phase callback. Its base explores both audit-only
and repairing instances, one on-demand arrival, finite subject changes and
one transient failed repair/checkpoint. ThreeTrees enlarges the covered set;
all safety/action checks remain, while temporal liveness is bounded-out there
for the TLC budget and retained at the base. Eventual convergence assumes
continued runtime/reminder opportunities, eventual registry/storage success
and a stable final subject version. Fairness never implies latency. ChunkSize
does not bound a tree's row scan or the synchronous on-demand caller's turn.
See [the refinement note](TagIndexReconcileRefinement.md) for the exact
production units, paired counterexamples and cooperative Coyote boundary.

All scheduling-directory bases, variants and mutations route before the broad
Wal prefix to five deterministic-only shards, partitioning the ten-module
population 106/89/1/27/1 (224 cases): TlcShardScheduling holds ReplayPermits
base/PartialPrefix and WalBackPressure, MaterialiserDrain, WalAdmission,
WalGcStarvationDriveWitness and TurnOccupancy; TlcShardSchedulingWorkloadB holds
ReplayPermits mutations plus WalGcStarvationDrive, SagaReadProgress and
TagIndexReconcile; TlcShardSchedulingReplayTwoPartitions holds the single
TwoPartitions case; TlcShardSchedulingShipping holds the ReplicationShipping
base and pairs; and TlcShardSchedulingShippingVariants holds FourRecords.
Each item is priced at the mean of its measured work-item times in ci.yml run
40909619830 (head 9418a498d) and coverage.yml run 40923724192 (head 0e4aa6b2a):
TwoPartitions 44.00 minutes, the 106/86-case groups 31.05/16.38, Shipping 4.29
and FourRecords 19.76. Estimates are planning inputs, not runtime guarantees.
Structural gates remain untagged. The checked-in
mutation files derive each mutant from the live module using exact anchors; the
harness runs a clean control and one configured target per mutant.

Module-local refinement notes classify every property, identify real production
detectors, and record the replaced draft assertions and their limitations.
The cooperative Coyote harness explores explicit choices over production seams;
it does not rewrite tasks or enumerate CLR/Orleans thread schedules.

## How to run

From the repository root, configure Java and the pinned TLC jar as described in
the authoritative [TLC running instructions](../README.md#how-to-run-tlc), then
run only this area's bases, variants and paired mutation experiments:

```powershell
dotnet test test\lattice\Orleans.Lattice.Tests.csproj -c Release --filter "TestCategory=TlcShardScheduling|TestCategory=TlcShardSchedulingWorkloadB|TestCategory=TlcShardSchedulingReplayTwoPartitions|TestCategory=TlcShardSchedulingShipping|TestCategory=TlcShardSchedulingShippingVariants"
```
