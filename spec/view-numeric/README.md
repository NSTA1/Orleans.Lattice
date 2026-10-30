# Numeric projection companion

`MaterialisedViewNumeric` checks a Count/Sum aggregation view on its own, with
no schema component installed: that numeric correctness holds as a property of
the projection and the maintainer, not of a validation layer in front of them.
It is a focused companion of the [materialised-view area](../materialised-views/README.md),
kept in its own directory so editing it reruns only itself rather than the five
modules that share `spec/materialised-views/`.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `MaterialisedViewNumeric` | 6 | 3 | 12 | 20 | 19 | 8,208 |

## What is modelled

Two source keys and two groups follow a fixed script of five source units:
unit 1 creates key 1 in A, unit 2 creates key 2 in A, atomic batch 3 regroups
key 1 to B and updates key 2 in place, unit 4 moves key 2 to a reserved group
key, and atomic batch 5 deletes key 1 and recreates key 2 in B with numeric 0.
The two batches overlap on both keys. Each batch is committed or aborted by the
environment, and source HLC is the unit's position, so the last-writer winner
of each key is the newest committed write.

Each event is extracted from its own kind and bytes. A delete, a reserved group
key or a value the filter rejects contributes nothing, so it retracts the key's
prior member; otherwise the event contributes its group and numeric. The view
publishes each group as a Count or Sum, and a group with no member has no row,
while a live Sum group whose total is zero is published as `0`.

An ordinary event is applied by the read-before-write flip: it first needs the
key's prior obligation retired, drops a write the source already superseded,
skips an exactly installed membership, and otherwise moves membership, both
accumulator slots and a durable affected-group intent together under an
operation id of generation, source key and source HLC. Each affected group is
then republished from its slots and the intent retired. A batch flips its whole
captured net slice, public groups included, in one write keyed by the batch.
The checkpoint advances only once every intent is retired.

The environment loses a flip or write response or crashes the maintainer, which
replays the pass from the checkpoint over the durable rows, and, depending on
`Topology`, either changes the projection's filter definition (and so its
version) or swaps the source's physical copy. A changed version or a swapped
copy makes the view rebind: it rebuilds from current source under a fresh
generation and binds to the current copy. A reconcile does the same for a live
view. The rebuild's scan applies each winner under the new generation's
operation ids, so reusing the old generation would let retained sagas swallow
the scan's contributions.

## Instances

| Instance | Kind | Topology change | Faults | Reconciles | Checked |
|----------|------|-----------------|--------|------------|---------|
| Base | Sum | Filter definition | 1 | 0 | Every property |
| `Swap` | Count | Physical copy | 1 | 1 | Every property |
| `TwoFaults` | Sum | Filter definition | 2 | 1 | Invariants and the action property |

The distinct-state census of each instance is recorded in the module's manifest.

Liveness at the `TwoFaults` bound is classified bounded-out for the TLC budget;
both temporal properties are checked on the base and on `Swap`.

## Scope and limits

The model is a bounded intended protocol, not an implementation proof. It does
not model prepare staging before a batch's decision, terminal horizons, the
omitted-member replacement gate or the multi-group public snapshot of a batch
(the [aggregation, horizon and ready-closure companions](../materialised-views/README.md#focused-aggregation-and-physical-rebind-companions)
own those), interrupted rebuild scans, read holds or generation reclaim (the
rebind and tail models), or the accumulative history contract, which adopts a
changed version forward and is checked by the aggregation companion. Non-finite
and non-integral numerics, a throwing selector, fanout, range deletes and the
other aggregation kinds are outside it. The
[refinement note](RefinementNumeric.md) maps every action and property to its
production seam and caller-level detectors and registers each limit.

## How to run

From this directory, with the pinned toolchain described in the
[spec README](../README.md#how-to-run-tlc):

```text
java -cp <repo>/tools/tlc/build/compiled:<repo>/tools/tla2tools.jar tlc2.TLC -config MaterialisedViewNumeric.cfg MaterialisedViewNumeric.tla
```

Through the NUnit harness, every base, variant and mutation case of this module:

```text
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=TlcModule_MaterialisedViewNumeric"
```
