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

## Routing and evidence

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
