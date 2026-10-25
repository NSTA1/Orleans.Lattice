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
| MaterialiserDrain | 6 | 8 | 21 | 21 | 33 | 1076 |
| SagaReadProgress | 3 | 5 | 6 | 11 | 12 | 2451 |
| TagIndexReconcile | 3 | 6 | 12 | 15 | 19 | 2784 |

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

All scheduling-directory bases, variants and mutations route to
TlcShardScheduling before the broad Wal prefix, on the deterministic-only
formal-tlc-scheduling shard. Structural gates remain untagged. The checked-in
mutation files derive each mutant from the live module using exact anchors;
the harness runs a clean control and one configured target per mutant.

Module-local refinement notes classify every property, identify real production
detectors, and record the replaced draft assertions and their limitations.
The cooperative Coyote harness explores explicit choices over production seams;
it does not rewrite tasks or enumerate CLR/Orleans thread schedules.

## How to run

From the repository root, configure Java and the pinned TLC jar as described in
the authoritative [TLC running instructions](../README.md#how-to-run-tlc), then
run only this area's bases, variants and paired mutation experiments:

```powershell
dotnet test test\lattice\Orleans.Lattice.Tests.csproj -c Release --filter "TestCategory=TlcShardScheduling"
```
