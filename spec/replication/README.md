# TLA+ specification of plain cross-cluster replication

This directory holds the replication-convergence module of the formal coverage
epic (#4430, issue #4438): a TLA+ specification of plain (non-saga)
cross-cluster replication, a TLC model that checks it exhaustively over a small
bounded instance, a catalogue of mutations that each make one property fire,
and a refinement note mapping the module to the code and to the tests that
detect a regression.

Cross-cluster **atomic** visibility (sagas replicated between clusters) is not
covered here; it is the cross-cluster module's (#4436). Coverage of plain
replication says nothing about it.

## Files

| File | What it is |
|------|-----------|
| [`Replication.tla`](Replication.tla) | The specification: state, actions, safety properties, the liveness property. |
| [`Replication.cfg`](Replication.cfg) | The TLC model: the bounded instance and the properties to check. |
| [`Replication.manifest.json`](Replication.manifest.json) | The module's counts, checked by the Formal gates. |
| [`mutations/`](mutations/) | One or more deliberate defects per checked property and per action, each of which must make its property fire. |
| [`Refinement.md`](Refinement.md) | Every variable, action and property mapped to the production symbols and the detector tests, the production defects the module found, the property classification and the abstraction gaps. |
| [`ReplicationCausalDelivery.tla`](ReplicationCausalDelivery.tla), [`.cfg`](ReplicationCausalDelivery.cfg), [`.manifest.json`](ReplicationCausalDelivery.manifest.json) | The focused companion module: causal-dependency delivery under head-of-line blocking (below). |
| [`causal-delivery-mutations/`](causal-delivery-mutations/) | The companion module's mutation catalogue. |
| [`ReplicationCausalDelivery.Refinement.md`](ReplicationCausalDelivery.Refinement.md) | The companion module's refinement note. |

## What is modelled

- **Authoring.** Clusters commit writes under per-leaf HLCs. A leaf's clock
  rises above everything merged into it, and nothing relates two leaves'
  clocks, so an origin's HLCs are not monotonic in delivery order - the
  condition behind #1060. A write may carry a causal dependency on a foreign
  origin's frontier.
- **The shipper.** A per-edge cursor over the source's WAL, which also holds
  the writes the source applied from peers. The local-origin cycle-break skips
  those (`ShipSkip`); any entry above the cursor may be sent, re-sent and
  delivered in any order, and the cursor moves only on the acknowledgement of
  the next entry (`Deliver`).
- **The receiver's apply pipeline**, in production's order (`Deliver`): the
  receiver-side cycle-break, the shadow-forward identity cache, the causal dependency check (`Park`, `Drain`), the merge and
  the monotone high-water mark; dead-lettering of failed applies and evicted
  buffer entries (`ApplyFails`, `Evict`) and operator replay (`Replay`); a
  receiver restart that loses volatile state (`Restart`).
- **Bootstrap** of one cluster from a snapshot of another, and the stream
  handoff the pin establishes (`Bootstrap`).
- **Merge modes** as join-homomorphisms from the set of merged writes:
  last-writer-wins and a grow-only counter.
- **The transport** loses, duplicates, reorders and delays. A partition is a
  run of loss and delay.

The instance has three clusters: a and b author and ship to each other (a
cycle) and to c, which only receives; a -> b -> c is a multi-hop path
production deliberately does not use. c may bootstrap once from b and is the
only fault site. Two keys (one per merge mode), at most two writes, HLCs up to
2 and one environment fault bound it.

## Properties checked

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | Every variable stays in its declared domain. |
| `NoReflection` | Invariant | No cluster admits its own write, received from a peer, into its apply pipeline. |
| `CursorNeverSkipsUnshipped` | Invariant | Every ship-worthy entry at or below a shipper's cursor has been absorbed by its destination. |
| `BootstrapHandoffLosesNothing` | Invariant | After the handoff, every write the bootstrapped cluster did not author is absorbed there or still on its way and will be accepted. |
| `NoRelay` | Action property | No delivery carries a third cluster's write. |
| `DedupNeverDropsNew` | Action property | An entry is dropped as a duplicate only if the receiver has absorbed it. The #1060 class. |
| `EventualConvergence` | Liveness | Once writing stops, every replica of every key reaches the value of all its writes. |

`EventualConvergence` fails on protocol defects under the fairness the module
asserts, not only without it; the paired mutations leave the fairness intact.

## The causal-delivery companion

`Replication.tla` delivers any entry above a shipper's cursor in any order,
but bounds its instance at two writes. That bounds out one question: can a
receiver deadlock waiting for causal dependencies when shippers block at the
head of their line? In production a not-accepted acknowledgement makes the
shipper back off and re-ship the same batch, so a deferred entry stalls
everything behind it, and shipping order can differ from authoring order
because the merge orders partitions by unordered per-leaf clocks (#1060). A
cross-origin cycle - b2 depends on a1, a2 depends on b1, and each writer ships
its dependent write first - needs four writes.

`ReplicationCausalDelivery.tla` checks exactly that, with two writers, four
writes, shipping order free of authoring order and a shipper that waits on
every entry's acknowledgement. The intended design of #4464 - acknowledge a
parked entry, hold it durably, drain it once its dependency arrives -
converges. Withholding the acknowledgement until the entry can be applied
deadlocks, and stands as `EventualConvergenceDeferredParkStalls`.

The only deferral in `Replication.tla`'s intended design is a duplicate of an
entry whose first delivery is still parking (#4465). Production backs off and
re-ships on it, stalling that line, but the first delivery's park completes on
its own (or a restart aborts it and the entry is re-sent), so the stall always
ends; the main module's freer delivery therefore loses no liveness behaviour
there.

## Production defects this module found

Writing the module turned up three defects. The module checks the intended
design, and each defect stands as a mutation that reproduces the production
shape, kept after the fix lands as the check that reintroducing it is caught:

- #4463, fixed by #4476 - the bootstrap pin installed a drop floor that
  discarded writes the snapshot did not hold
  (`BootstrapHandoffLosesNothingPinnedFloor`).
- #4464 - the causal buffer can strand or lose parked entries (four
  `EventualConvergence*` mutations).
- #4465 - a duplicate of an entry still in flight is acknowledged, so an
  aborted first delivery is lost (`CursorNeverSkipsUnshippedDuplicateOfParkingAcked`).

[`Refinement.md`](Refinement.md#territory-owned-by-other-open-issues) lists
every row they touch.

## How to run TLC

From this directory, with the pinned `tla2tools.jar` (v1.7.4) and a JDK:

```powershell
java -XX:+UseParallelGC -cp C:\path\to\tla2tools.jar tlc2.TLC -workers auto -config Replication.cfg Replication.tla
```

A clean run ends with `Model checking completed. No error has been found.`
The `TlcModelCheckTests` fixture runs the same check, and every mutation's
two-arm experiment, in CI; see [the spec README](../README.md#ci-decision).

## Last checked

On tla2tools v1.7.4 with a Temurin-compatible 17 JDK, every property in
`Replication.cfg` held with deadlock checking on, over 228,266 distinct states
(1,171,757 generated) at a complete-search depth of 17. Every property in
`ReplicationCausalDelivery.cfg` held over 19,753 distinct states (64,330
generated) at a depth of 16.

## Counts

The module's current totals. `SpecModuleDiscoveryTests` checks this table
against [`Replication.manifest.json`](Replication.manifest.json), and the
other Formal gates check the manifest against the specification, the cfg, the
mutation catalogue, the refinement note and TLC's own state count.
This table is the one place this directory states them; see
[module layout](../README.md#module-layout).

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `Replication` | 4 | 3 | 11 | 17 | 16 | 228,266 |
| `ReplicationCausalDelivery` | 1 | 1 | 5 | 4 | 5 | 19,753 |
