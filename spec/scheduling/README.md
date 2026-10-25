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
