# TLA+ specification of B+ tree topology

This directory holds the formal specification of how the Orleans.Lattice B+ tree
changes shape. It is the deliverable of issue #4795, "Tree Topology TLA
Exploration". It follows the pattern the other modules set: a TLA+ design checked
by TLC in CI, every property and every action paired with a mutation, and a
refinement note mapping each construct to production and to detector tests.

## Modules

| Module | What it specifies |
|--------|-------------------|
| [`SplitLink.tla`](SplitLink.tla) | A leaf split completing on the donor, the shard root recording and applying the child link, and a root crash between them. |
| [`BPlusTopology.tla`](BPlusTopology.tla) | Concurrent writes across split and fold transfers, sibling-chain publication, parent routing, and recovery of an unlinked sibling. |
| [`BPlusCascade.tla`](BPlusCascade.tla) | Rightmost leaf splits cascading through level-indexed internal nodes and repeated root promotions. |
| [`BPlusReclaimRecovery.tla`](BPlusReclaimRecovery.tla) | The durable retirement, unlink, route removal, successor back-link repair, clear, and recovery ordering for a reclaimed leaf. |

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `SplitLink` | 8 | 1 | 12 | 11 | 18 | 260 |
| `BPlusTopology` | 8 | 0 | 14 | 14 | 21 | 232 |
| `BPlusCascade` | 10 | 1 | 9 | 13 | 19 | 1271 |
| `BPlusReclaimRecovery` | 6 | 1 | 9 | 9 | 15 | 24 |

`Actions` counts the disjuncts of `Next`, including the non-behavioural `Stutter`.
`Behaviour rows` counts the action rows of the refinement note, excluding
non-behavioural actions, plus its property rows. `Distinct states` is TLC's count
for the module's cfg.

## Files

| File | What it is |
|------|-----------|
| `SplitLink.tla` / `.cfg` / `.manifest.json` | The module, its TLC model and its manifest. |
| [`mutations/`](mutations/) | One or more deliberate defects per property and per action. |
| [`SplitLinkRefinement.md`](SplitLinkRefinement.md) | `SplitLink` mapped to production: variables, actions, properties, detectors and gaps. |
| [`BPlusTopology.cfg`](BPlusTopology.cfg) / [`BPlusTopology.manifest.json`](BPlusTopology.manifest.json) | The topology model's bounds and gate counts. |
| [`topology-mutations/`](topology-mutations/) | Controlled defects for every behavioral action and invariant in `BPlusTopology`. |
| [`BPlusTopologyRefinement.md`](BPlusTopologyRefinement.md) | The topology model mapped to production and detector tests. |
| [`BPlusCascade.cfg`](BPlusCascade.cfg) / [`BPlusCascade.manifest.json`](BPlusCascade.manifest.json) | Fan-out-three cascade instance; fair progress reaches the configured six-level bound. |
| [`cascade-mutations/`](cascade-mutations/) | Controlled defects for every behavior in `BPlusCascade`. |
| [`BPlusCascadeRefinement.md`](BPlusCascadeRefinement.md) | The cascading split model mapped to production and detector tests. |
| [`BPlusReclaimRecovery.cfg`](BPlusReclaimRecovery.cfg) / [`BPlusReclaimRecovery.manifest.json`](BPlusReclaimRecovery.manifest.json) | The reclaim crash-boundary instance and its verified gate counts. |
| [`reclaim-recovery-mutations/`](reclaim-recovery-mutations/) | Controlled defects for every behavior in `BPlusReclaimRecovery`. |
| [`BPlusReclaimRecoveryRefinement.md`](BPlusReclaimRecoveryRefinement.md) | The reclaim recovery model mapped to production and detector tests. |

## Properties checked

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `NoKeyLostOrDuplicated` | Invariant | Every acknowledged key is held by exactly one leaf. |
| `LinkObligationDurable` | Invariant | A completed, unlinked split is always recorded durably, at the root or on the donor. |
| `IntentOnlyForCompleteSplit` | Invariant | The root records a link intent only for a completed split. |
| `LinkedClearsIntent` | Invariant | A linked sibling carries no pending intent. |
| `ChainedImpliesBorn` | Invariant | The donor never chains to a sibling that does not exist. |
| `BornImpliesIntent` | Invariant | A sibling is born only for a recorded split. |
| `SplitOnlyOfNonEmptyLeaf` | Invariant | Only a leaf holding rows is split. |
| `SplitEventuallyLinked` | Liveness | A completed split is eventually linked into the parent. |
| `ReachMaxHeight` | Liveness | Under fair progress, the bounded cascade reaches its configured maximum height. |
| `SortedLeafChain` | Invariant | Cascading splits preserve one ordered, reciprocal leaf chain. |
| `ParentChildAccounting` | Invariant | Settled nodes have matching parent/child edges; pending links explain the transient gap. |
| `SeparatorRanges` | Invariant | Separators route every key into its owning child range. |
| `FanoutBound` | Invariant | Settled nodes stay within configured fan-out. |
| `RootHasOneRoot` | Invariant | Root promotion preserves the unique parentless root. |
| `HeightBound` | Invariant | The checked model instance does not exceed six levels. |
| `RecoverablePending` | Invariant | Every interrupted split retains durable evidence to resume. |
| `RoutedKeysOwned` | Invariant | Separator descent reaches the unique leaf owning each acknowledged key. |
| `RetirementGrantIsLatched` | Invariant | Retirement cannot proceed until mutation refusal is durable. |
| `UnlinkedVictimHasMarker` | Invariant | An unlinked victim retains durable recovery evidence. |
| `NoRouteToClearedVictim` | Invariant | A victim is not cleared while a parent route can still select it. |
| `ClearedVictimHasRepairedBackLink` | Invariant | A cleared victim is never still named by its successor's backward link. |
| `CompletedReclaimCleared` | Invariant | Reclaim completion cannot retire its marker before clear succeeds. |
| `PendingEventuallyCompletes` | Liveness | With retry opportunities and eventual dependency availability, reclaim completes. |
| `NoKeyLost` | Invariant | Every acknowledged key remains present on a live leaf. |
| `ParentSiblingConsistency` | Invariant | A published sibling has a complete chain and parent route. |
| `OrphanRecoverable` | Invariant | A born, unlinked sibling retains active or durable recovery evidence. |
| `PendingOnlyForCompleteSplit` | Invariant | A parent link obligation names only a completed split. |
| `SplitRequiresData` | Invariant | A split starts only when the donor holds rows. |
| `BornImpliesSplit` | Invariant | A sibling exists only as part of a split or settled topology. |
| `StableNoDuplicate` | Invariant | A settled split or fold has no duplicate keys across leaves. |

## Defects this specification found

| Issue | Defect | Mutation |
|-------|--------|----------|
| #4795 | A root crash after a leaf split completes but before the root records its link intent strands a populated, chained sibling no parent routes to, and its WAL pin freezes the trim floor. Fixed: the donor persists a marker that recovery re-surfaces until the root acknowledges. | `LinkObligationDurableNoMarker` |
| #4815 | A reclaimed leaf could be unlinked from its predecessor while still routed, then reactivate and accept a write before the parent route was retired. Fixed: persist a retirement latch before granting unlink and restore it on activation. | `RetirementGrantWithoutLatch` |
| #4818 | A failed successor back-link write during reclaim could be followed by victim clear and marker completion, stranding a stale backward pointer without durable repair intent. Fixed: retain the victim and marker until repair succeeds. | `ClearBeforeBackLinkRepair` |

## What the assurance covers, and what it does not

`SplitLink` covers one split of one leaf with one root crash. `BPlusTopology`
covers one bounded split/fold with writes interleaved at transfer boundaries and
models marker-based recovery of a born, unlinked sibling. `BPlusCascade` starts
at three levels and reaches six in its checked instance; its transitions are
level-indexed, but the finite TLC run is not an unbounded-height proof.
`BPlusReclaimRecovery` checks that a routed victim remains durable and
write-protected until the route is retired, that unlink retains its recovery
marker until the successor back link is repaired and the victim is cleared, and
that crash recovery eventually completes the obligation. The models do not
cover the orphan audit/repair algorithm itself. The existing `ShardOwnership`
models cover range ownership through adaptive shard split, online reshard and
resize. See the refinement notes for bounded claims and abstraction gaps.

## How to run TLC

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config SplitLink.cfg SplitLink.tla
```

Pass `-metadir` with a directory outside the repository, or delete the `states/`
directory TLC leaves beside the module. It takes seconds.
