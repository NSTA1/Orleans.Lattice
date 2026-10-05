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
| [`ReplicationReBootstrap.tla`](ReplicationReBootstrap.tla), [`.cfg`](ReplicationReBootstrap.cfg), [`.manifest.json`](ReplicationReBootstrap.manifest.json) | The second companion module: an in-place re-bootstrap after the source reaped a delete (below). |
| [`rebootstrap-mutations/`](rebootstrap-mutations/) | The second companion module's mutation catalogue. |
| [`ReplicationReBootstrap.Refinement.md`](ReplicationReBootstrap.Refinement.md) | The second companion module's refinement note. |

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

`ReplicationReBootstrap.tla` checks the design of #4537, a reconcile in the
re-bootstrap's drain, over one key and two clusters. Before the export opens,
the receiver pre-captures each live entry whose origin is the source. A
pre-captured key the export does not carry is deleted, attributed to the source
at the captured HLC. The reconcile fabricates a write, so it is gated on every
other way a key can be missing from an export: the export's scope, a reshard
during the scan, and a restore, purge or rebind since the receiver's copy was
aligned with the source. `ReconcileDeletesOnlyDeleted` checks that every
fabricated tombstone is dominated by a delete the source really authored.
`EventualConvergenceReapedDeleteNotReconciled` is current production, with no
reconcile.

A key the receiver holds under another origin cannot be reconciled: the
receiver cannot prove the source held that value. The module states that
residual exactly, as `Residual`, and #4549 owns it.

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
  `EventualConvergencePinRegressesVector`,
  `EventualConvergencePinSkipsDrain`).
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

[`Refinement.md`](Refinement.md#defects-found-and-fixed) lists them with the
fixes.

## Open production gaps

- #4615 - a tombstone is reaped on the wall clock alone, so a write it beats
  that arrives later resurrects the key (`EventualConvergenceReapInsideGrace`,
  in the re-bootstrap companion).
- #4586 - the causal dependency check compares against a high-water mark that
  is not downward-closed; no property checks causal order until its fix.
- #4537 - an in-place re-bootstrap cannot reconcile a delete whose source
  tombstone was reaped (`EventualConvergenceReapedDeleteNotReconciled`, in
  the re-bootstrap companion).
- #4549 - the residual of #4537: a reaped delete of a key the receiver holds
  under another origin.
[`ReplicationReBootstrap.Refinement.md`](ReplicationReBootstrap.Refinement.md#territory-owned-by-other-open-issues)
marks the rows they touch.

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
`Replication.cfg` held with deadlock checking on, over 308,258 distinct states
(1,604,442 generated) at a complete-search depth of 17. Every property in
`ReplicationCausalDelivery.cfg` held over 19,753 distinct states (64,330
generated) at a depth of 16. Every property in `ReplicationReBootstrap.cfg`
held over 56,135 distinct states (163,127 generated) at a depth of 17.

## Counts

The module's current totals. `SpecModuleDiscoveryTests` checks this table
against [`Replication.manifest.json`](Replication.manifest.json), and the
other Formal gates check the manifest against the specification, the cfg, the
mutation catalogue, the refinement note and TLC's own state count.
This table is the one place this directory states them; see
[module layout](../README.md#module-layout).

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `Replication` | 4 | 3 | 14 | 22 | 19 | 308,258 |
| `ReplicationCausalDelivery` | 1 | 1 | 5 | 4 | 5 | 19,753 |
| `ReplicationReBootstrap` | 2 | 1 | 10 | 12 | 11 | 56,135 |
