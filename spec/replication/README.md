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
| [`ReplicationReBootstrap.tla`](ReplicationReBootstrap.tla), [`.cfg`](ReplicationReBootstrap.cfg), [`.manifest.json`](ReplicationReBootstrap.manifest.json) and the [`Floor`](ReplicationReBootstrap.Floor.cfg) variant configuration | The second companion module: an in-place re-bootstrap after the source reaped a delete, and the bootstrap drop floor (below). |
| [`rebootstrap-mutations/`](rebootstrap-mutations/) | The second companion module's mutation catalogue. |
| [`ReplicationReBootstrap.Refinement.md`](ReplicationReBootstrap.Refinement.md) | The second companion module's refinement note. |
| [`ReplicationLowWatermark.tla`](ReplicationLowWatermark.tla), [`.cfg`](ReplicationLowWatermark.cfg), [`.manifest.json`](ReplicationLowWatermark.manifest.json) and four variant configurations | The third companion module: the low watermark that makes the causal-dependency check sound (below). |
| [`lowwatermark-mutations/`](lowwatermark-mutations/) | The third companion module's mutation catalogue. |
| [`ReplicationLowWatermark.Refinement.md`](ReplicationLowWatermark.Refinement.md) | The third companion module's refinement note. |

## What is modelled

- **Authoring.** Clusters commit writes under per-leaf HLCs. A leaf's clock
  rises above everything merged into it, and nothing relates two leaves'
  clocks, so an origin's HLCs are not monotonic in delivery order - the
  condition behind #1060. A write may carry a causal dependency on a foreign
  origin's frontier. A last-writer-wins write may be a delete (a tombstone).
- **The shipper.** A per-edge cursor over the source's WAL, which also holds
  the writes the source applied from peers. The local-origin cycle-break skips
  those (`ShipSkip`); any entry above the cursor may be sent, re-sent and
  delivered in any order, and the cursor moves only on the acknowledgement of
  the next entry (`Deliver`). A trim past the cursor (`Trim`) and a batch the
  shipper cannot encode (`ShipDeadLetter`) move the cursor past entries it
  never sent, and each requests a re-seed of the peer. Content-hash elision
  lets the receiver acknowledge an entry it already reflects without its
  payload (`Elide`).
- **The receiver's apply pipeline**, in production's order (`Deliver`): the
  receiver-side cycle-break, the shadow-forward identity cache, the causal dependency check (`Park`, `Drain`), the merge and
  the monotone high-water mark; dead-lettering of failed applies and evicted
  buffer entries (`ApplyFails`, `Evict`) and operator replay (`Replay`); a
  receiver restart that loses volatile state (`Restart`).
- **Bootstrap** of one cluster from a snapshot of another, and the stream
  handoff the pin establishes (`Bootstrap`). The bootstrap may run in place
  over a copy the receiver already holds, after a re-seed request: what the
  stream skipped arrives only in the snapshot. An operator may also request it
  at any time.
- **Merge modes** as join-homomorphisms from the set of merged writes:
  last-writer-wins and a grow-only counter.
- **The transport** loses, duplicates, reorders and delays. A partition is a
  run of loss and delay.

The instance has three clusters: a and b author and ship to each other (a
cycle) and to c, which only receives; a -> b -> c is a multi-hop path
production deliberately does not use. c may bootstrap once from b and is the
only fault site. Two keys (one per merge mode), at most two writes, HLCs up to
2 and one environment fault bound it. Only b deletes, and only a key it
holds.

## Properties checked

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | Every variable stays in its declared domain. |
| `NoReflection` | Invariant | No cluster admits its own write, received from a peer, into its apply pipeline. |
| `CursorNeverSkipsUnshipped` | Invariant | Every ship-worthy entry at or below a shipper's cursor has been absorbed by its destination. |
| `BootstrapHandoffLosesNothing` | Invariant | After the handoff, every write the bootstrapped cluster did not author is absorbed there or still on its way and will be accepted. |
| `CausalOrder` | Invariant | No replica holds a write without the write its dependency names. |
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
every entry's acknowledgement. The design #4483 implemented for #4464 -
acknowledge a parked entry, hold it durably, drain it once its dependency
arrives - converges. Withholding the acknowledgement until the entry can be applied
deadlocks, and stands as `EventualConvergenceDeferredParkStalls`.

The only deferral in `Replication.tla`'s intended design is a duplicate of an
entry whose first delivery is still parking (#4465, fixed by #4477). Production backs off and
re-ships on it, stalling that line, but the first delivery's park completes on
its own (or a restart aborts it and the entry is re-sent), so the stall always
ends; the main module's freer delivery therefore loses no liveness behaviour
there.

## The re-bootstrap companion

`Replication.tla` does not model tombstone garbage collection, so its snapshot
can carry every delete the source ever made. Production reaps a tombstone after
`TombstoneGracePeriod`, and a receiver that fell off the source's log is behind
a trim that did not wait for it. A delete that is both behind the trim point and
reaped then reaches the receiver by no path: not the stream, not the export.

`ReplicationReBootstrap.tla` checks the reconcile in the re-bootstrap's
drain, over one key and two clusters. A live source-origin entry the receiver
pre-captured at export open, and the export does not carry, is deleted at the
captured HLC when the receiver is aligned with the source's lineage (#4537,
built by #4647). A row of another origin the export does not carry is deleted
when its HLC is below the source's low watermark for that origin at open
(#4549, built by #4675). The reconcile fabricates a write, so it is gated on every other way a
key can be missing from an export: the export's scope, a reshard, resize or
soft delete during the scan, and a restore, purge or rebind since the
receiver's copy was aligned. `ReconcileDeletesOnlyDeleted` checks that every
fabricated tombstone is dominated by a delete the source really authored.

A full re-bootstrap also installs a drop floor at the source's per-origin
applied low watermark (#4549, built by #4675), so a third cluster's write
still on its way to the receiver, which the source had applied, deleted and
reaped, cannot resurrect the key. The floor defers rather than drops until
its import closes stable, is cleared when the import closes unstable, never
covers the source's own origin, and arms the receiver's shard roots so a
write admitted before it is refused rather than land after the reconcile
scan. The Floor variant configuration adds the third cluster that needs.

The module also checks the reap guard (#4615, built by #4678), the refusal of
a batch read under a source lineage the receiver has left (#4673, built by
#4681), the re-seed a receiver
restore and a detach force, and the source-restore contract: a unilateral
source restore never fabricates a delete, peers may diverge after one, and a
coordinated restore converges them. `EventualConvergence` holds on every
behaviour with no unilateral source restore, with no other carve-out, and
after one the receiver keeps every write of another origin until a
coordinated restore runs.

## The low-watermark companion

A causal dependency names one write: the other origin's write at an HLC the
author held. `Replication.tla` and `ReplicationCausalDelivery.tla` check
`CausalOrder` with dependencies met on the exact write merged, which is how
production meets them since #4640. The per-origin high-water mark used before
cannot answer the question, because an origin's HLCs arrive out of order
(#1060); `CausalOrderMaxHlcFrontier` reproduces it.

Production also meets a dependency once its HLC is strictly below the low
watermark the origin ships and the write is not held back unapplied, so a
receiver that has forgotten an identity still releases its dependents.
`ReplicationLowWatermark.tla` checks that half (#4586): one origin shipping over
two WAL partitions, each sealing a clock floor and refusing a fresh stamp
below it, the watermark published with its offset and counted only once the
acknowledged cursor passes it, the minimum over partitions clamped below open
prepares, the capability gate, dead-letter backpressure and lost marks
(#4603), the bounded identity record, and the bootstrap pin. Four variant
configurations each add sagas, transport loss, a second dead letter or a
bootstrap on one partition.

## Production defects this module found

The module checks the intended design, and each production defect it
reproduces stands as a mutation of the production shape, kept after the fix
lands as the check that reintroducing it is caught:

- #4463, fixed by #4476 - the bootstrap pin installed a drop floor that
  discarded writes the snapshot did not hold
  (`BootstrapHandoffLosesNothingPinnedFloor`).
- #4464, fixed by #4483 - the causal buffer could strand or lose parked
  entries: a lost wakeup between the dependency check and the park, a buffer
  held only in memory, a bootstrap pin that replaced the vector, and a pin
  that did not drain (`EventualConvergenceParkLostWakeup`,
  `EventualConvergenceVolatileCausalBuffer`,
  `EventualConvergencePinSkipsDrain`). The pin's vector is no longer read by
  the dependency check, so `EventualConvergencePinRegressesVector` was
  retired with #4586's fix.
- #4465, fixed by #4477 - a duplicate of an entry still in flight was
  acknowledged, so an aborted first delivery was lost
  (`CursorNeverSkipsUnshippedDuplicateOfParkingAcked`).
- #4504, fixed by #4544 - a snapshot bootstrap shipped no deletes, so a
  receiver re-bootstrapped in place after the source trimmed its log past a
  delete kept the deleted key's old value
  (`EventualConvergenceSnapshotDropsDeletes`).
- #4585, fixed by #4602 - content-hash elision elided on the key's bytes
  alone, so a newer write with the same bytes was never applied
  (`DedupNeverDropsNewElidesByContent`).
- #4587, fixed by #4599 - the only fall-off probe read the receiver's own
  WAL, so a source trim past the receiver's cursor requested no re-bootstrap
  (`EventualConvergenceTrimNeverRebootstraps`, and
  `EventualConvergenceFallOffUndetected` in the re-bootstrap companion).
- #4614, fixed by #4651 - a batch the shipper could not encode was
  dead-lettered on the sender and skipped, and nothing re-seeded the peer
  (`EventualConvergenceShipDeadLetterNeverReseeds`).
- #4604 - the snapshot drain ignored a deferred apply, so a row the applier
  deferred (a coordinated restore's receive fence, an in-flight duplicate) was
  dropped and the handoff pinned past it
  (`BootstrapHandoffLosesNothingDrainDropsDeferred`).
- #4586, fixed by #4650, #4640, #4663, #4658 and #4674 - a dependency was met
  once the origin's high-water mark reached its HLC, which is not
  downward-closed, so a dependent was released before its dependency
  (`CausalOrderMaxHlcFrontier`, `CausalOrderDeliveryMaxHlcFrontier` in the
  causal-delivery companion, and `CausalOrderBootstrapInstallsHighWaterMark` in
  the low-watermark companion).
- #4603, fixed by #4612 - a full dead-letter queue evicted its oldest entry,
  and a discarded dead letter left no lost mark
  (`EventualConvergenceDeadLetterEvictsOldest` and
  `CausalOrderDiscardLeavesNoMark`, in the low-watermark companion).
- #4537 (fixed by #4647), #4549 (#4675), #4615 (#4678) and #4673 (#4681) - the
  re-bootstrap companion's: a reaped delete the drain did not reconcile, of a
  source-origin key and then of any origin's; a tombstone reaped on the wall
  clock alone; and a stale-lineage batch applied after realignment
  (`EventualConvergenceReapedDeleteNotReconciled`,
  `EventualConvergenceForeignRowNotReconciled` and, for a third origin's
  write still in flight, `EventualConvergenceNoBootstrapFloor`,
  `EventualConvergenceReapInsideGrace`,
  `ReconcileDeletesOnlyDeletedStaleLineageApplied`).

[`Refinement.md`](Refinement.md#defects-found-and-fixed) lists them with the
fixes.

## Open production gaps

None: every row of the four refinement notes is Yes. Saga atomicity across a
bootstrap (#4683, #4684, #4685) is the atomic-commit cross-cluster module's,
not this directory's; no row here depends on it.

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
`Replication.cfg` held with deadlock checking on, over 306,494 distinct states
(1,592,864 generated) at a complete-search depth of 17. Every property in
`ReplicationCausalDelivery.cfg` held over 19,161 distinct states (62,250
generated) at a depth of 16. Every property in `ReplicationReBootstrap.cfg`
held over 633,326 distinct states (1,898,971 generated) at a depth of 26,
and in its Floor variant over 449,383 (1,327,241 generated) at a depth of
21.
Every property in `ReplicationLowWatermark.cfg` held over 330,842 distinct
states (1,669,854 generated) at a depth of 26, and in its Sagas, Loss,
DeadLetters and Bootstrap variants over 181,862, 27,522, 19,449 and 43,757.

## Counts

The module's current totals. `SpecModuleDiscoveryTests` checks this table
against [`Replication.manifest.json`](Replication.manifest.json), and the
other Formal gates check the manifest against the specification, the cfg, the
mutation catalogue, the refinement note and TLC's own state count.
This table is the one place this directory states them; see
[module layout](../README.md#module-layout).

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `Replication` | 5 | 3 | 14 | 22 | 20 | 306,494 |
| `ReplicationCausalDelivery` | 2 | 1 | 5 | 6 | 6 | 19,161 |
| `ReplicationReBootstrap` | 2 | 1 | 18 | 33 | 19 | 633,326 |
| `ReplicationLowWatermark` | 2 | 1 | 18 | 21 | 19 | 330,842 |
