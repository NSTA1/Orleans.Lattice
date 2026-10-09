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

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `SplitLink` | 8 | 1 | 12 | 11 | 18 | 260 |
| `BPlusTopology` | 8 | 0 | 14 | 14 | 21 | 232 |

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

## What the assurance covers, and what it does not

`SplitLink` covers one split of one leaf with one root crash. `BPlusTopology`
covers one bounded split/fold with writes interleaved at transfer boundaries and
models marker-based recovery of a born, unlinked sibling; it does not model the
orphan audit/repair algorithm itself or cascading parent splits. The existing
`ShardOwnership` models cover range ownership through adaptive shard split,
online reshard and resize. See the refinement notes for bounded claims and
abstraction gaps.

## How to run TLC

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config SplitLink.cfg SplitLink.tla
```

Pass `-metadir` with a directory outside the repository, or delete the `states/`
directory TLC leaves beside the module. It takes seconds.
