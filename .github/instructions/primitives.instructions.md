---
applyTo: "src/lattice/Primitives/**"
---

# Primitives & CRDT Patterns

## Design Principles

All primitive types in this folder are **CRDT building blocks** - they must be:

- **Commutative**: `merge(a, b) == merge(b, a)`
- **Associative**: `merge(merge(a, b), c) == merge(a, merge(b, c))`
- **Idempotent**: `merge(a, a) == a`

Document these properties in the `<summary>` of every merge method.

## Namespace Placement

Although these types live in the `Primitives/` folder, **public** CRDT primitives declare `namespace Orleans.Lattice` (not `Orleans.Lattice.Primitives`) so the whole public surface sits behind a single `using Orleans.Lattice;` - the repo convention that public API lives in the root namespace. The public primitives are `HybridLogicalClock`, `ICrdt<TSelf>`, `MvRegister`, `MvRegisterEntry`, `OrMap`, `OrMapEntry`, `OrSet`, `OrSetDot`, `PnCounter`, `Rga`, `RgaNode`, and `VersionVector`.

Area-internal helpers in the same folder (`LwwValue<T>`, `LeafDeliveryCursor`, `SplitState`, `SplitStateExtensions`, `StateDelta`) stay `internal` in `namespace Orleans.Lattice.Primitives`.

## Type Shape

**Immutable value types** - use `readonly record struct` with `[Immutable]`:

```csharp
[GenerateSerializer]
[Alias(TypeAliases.X)]
[Immutable]
public readonly record struct MyType
{
    [Id(0)] public long SomeField { get; init; }
    [Id(1)] public int AnotherField { get; init; }
}
```

**Mutable aggregate types** (e.g. `VersionVector`) - use `sealed class` without `[Immutable]`:

```csharp
[GenerateSerializer]
[Alias(TypeAliases.X)]
public sealed class MyAggregate
{
    [Id(0)] public Dictionary<string, HybridLogicalClock> Entries { get; set; } = [];
}
```

- Prefer `readonly record struct` for single-value types (`HybridLogicalClock`, `LwwValue<T>`).
- Use `sealed class` when the type has mutable collections or in-place mutation methods.
- Provide a static `Zero` or `Empty` property for the identity element when applicable.

## Existing Primitives

| Type | Purpose |
|---|---|
| `HybridLogicalClock` | Totally-ordered timestamp (wall clock + counter) |
| `LwwValue<T>` | Last-writer-wins register resolved by HLC. Carries optional `ExpiresAtTicks` ( TTL) - `0` means no expiry; reads filter entries where `IsExpired(nowUtcTicks)` returns `true`. Carries optional `OriginClusterId` (cluster-of-origin stamp, authored from the ambient `LatticeOriginContext`) and optional `VectorClock` (sparse `{originClusterId → HLC}` commit-time frontier, authored from the ambient `LatticeVectorClockContext`); both default to `null` for legacy persisted state. |
| `VersionVector` | Per-replica version tracking for delta extraction. **Public** - also exposed through `ILattice.VersionVector(key)` as a value-surface CRDT accessor. |
| `OrSet` | Observed-remove set CRDT (public). State-level merge unions both sides' adds and tombstones; concurrent adds and removes survive a later remove that did not observe them. Exposed through `ILattice.OrSet(key)`. |
| `OrSetDot` | `(replicaId, counter)` dot tagged on each `OrSet` add. |
| `PnCounter` | Positive-negative counter CRDT (public). Per-replica monotonic positive/negative components; merge is pointwise-max per side. Exposed through `ILattice.PnCounter(key)`. |
| `MvRegister` | Multi-value register CRDT (public). Dot-context-tagged set of live values; concurrent writes from different replicas survive merge as conflict candidates rather than collapsing under last-writer-wins. Exposed through `ILattice.MvRegister<T>(key)`. |
| `MvRegisterEntry` | Dot-tagged `(replicaId, counter, value)` triple inside an `MvRegister`. |
| `OrMap<TKey, TValue>` | Observed-remove map CRDT (public) of recursively-mergeable CRDT values. Keys follow add-wins observed-remove semantics; per-key values are folded through `ICrdt<TValue>.MergeFrom`, so concurrent writes under the same map key converge into a single recursively-merged value rather than being collapsed by last-writer-wins. Exposed through `ILattice.OrMap<TKey, TValue>(key)`. |
| `OrMapEntry<TValue>` | Dot-tagged `(replicaId, counter, value)` slot inside an `OrMap`. |
| `Rga` | Replicated Growable Array sequence CRDT (public). Stores a tree of dot-tagged nodes linked through `parentDot`; the materialised order is a depth-first walk with descending `(Counter, ReplicaId)` sibling tie-break. Removes tombstone nodes but preserve them in the tree so concurrent inserts under the same parent still resolve. Exposed through `ILattice.Sequence<T>(key)`. |
| `RgaNode` | Tree node carrying `(replicaId, counter, parentDot, value, isTombstone)` inside an `Rga`. |
| `ICrdt<TSelf>` | Internal-feeling but `public` interface declaring `MergeFrom(TSelf)` plus `IsBottom`. Implemented by `OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`, and `Rga`; the constraint that lets `OrMap<TKey, TValue>` recurse through nested CRDT values without reflection. |
| `StateDelta` | Captures entries changed since a given version vector |
| `SplitState` | Enum tracking leaf/internal split lifecycle |

## Adding a New Primitive

1. Define the `readonly record struct` in this folder.
2. Add a constant to `TypeAliases.cs` and apply `[Alias]`.
3. Write unit tests in `test/lattice/Primitives/` verifying commutativity, associativity, and idempotency of merge.
