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

Although these types live in the `Primitives/` folder, **public** CRDT primitives declare `namespace Orleans.Lattice` (not `Orleans.Lattice.Primitives`) so the whole public surface sits behind a single `using Orleans.Lattice;` - the repo convention that public API lives in the root namespace. The public primitives are `BoundedRegister`, `GCounter`, `GSet`, `HybridLogicalClock`, `ICrdt<TSelf>`, `MvRegister`, `MvRegisterEntry`, `OrFlag`, `OrMap`, `OrMapEntry`, `OrSet`, `OrSetDot`, `PnCounter`, `Rga`, `RgaNode`, `RwFlag`, `RwSet`, and `VersionVector`. Every typed replication delta DTO lives under `Crdt/` (in the same `Orleans.Lattice` namespace) rather than here: `BoundedRegisterDelta`, `GCounterDelta`, `GSetDelta`, `LwwRegisterDelta`, `MvRegisterDelta`, `OrFlagDelta`, `OrMapDelta` (with `OrMapDeltaEntry` and `OrMapDeltaTombstone`), `OrSetDelta` (with `OrSetDeltaDot`), `PnCounterDelta`, `RgaDelta` (with `RgaDeltaNode`), `RwFlagDelta`, `RwSetDelta`, and `VersionVectorDelta`.

> Keep the list above as **one** paragraph. It previously existed as three
> near-identical copies, each added by a different change and each naming a
> different subset of the primitives, so all three were wrong. Edit this
> paragraph in place when a primitive is added; do not append another copy.

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

## Buffer Ownership (`byte[]` payloads)

Every primitive stores opaque `byte[]` payloads. Who owns a given array is decided by its **provenance**, not by the type holding it. All three legs are mandatory - this is the rule `ICrdt<TSelf>` documents, and the one `BoundedRegister`, `Rga` and `MvRegister` are the reference implementations of.

| Seam | Rule | Why |
|---|---|---|
| **Ingress** from the caller (`Set`, `Add`, `InsertAfter`) | **Hand-off** - store by reference, no copy | The caller just authored the array and has no reason to retain it, so copying is pure waste. Document the hand-off on the parameter, and state that the caller must not mutate afterwards. |
| **Fold** from a peer or a delta (`MergeFrom`, `MergeDelta`) | **Copy** the winning candidate | The array belongs to a peer replica still using it, or a producer that may retry or fan out. Adopting it aliases durable state to another owner's buffer. A *losing* candidate must still allocate nothing. |
| **Egress** to a caller (`Clone`, composite `Get`, materialised projections) | **Copy** | Otherwise a caller can write through the returned value into durable state without passing any mutation API. |

Copy with a span copy (`value.AsSpan().ToArray()`), never `Array.Clone` - the two allocate identically, but `Array.Clone` goes through the non-generic `Array` path and measured roughly 3-4x slower on the `ordedup` microbench suite. An empty span's `ToArray()` returns the shared `Array.Empty<T>()` singleton, so empty and tombstoned payloads cost nothing.

Copy only what the seam actually requires:

- **Only a winning candidate.** A fold that keeps the local side must allocate nothing, which is what keeps the steady-state (idempotent, duplicate-delivery) replication merge free.
- **Keep the expensive part cached, copy the cheap part.** Where a projection is cached (`Rga.ToList`, `MvRegister.Values`), cache the *ordering* - the traversal and the sort - and pay only the per-value copy on the way out. Caching the buffers instead is not a valid substitute for copying: a cached shared buffer lets one reader corrupt every later read, and nothing invalidates the cache.
- **Give internal consumers a shared view rather than skipping the copy.** When a call site inside the assembly immediately consumes the bytes (deserialising them, or reading only the dots), expose an `internal` aliasing view next to the public projection - `Rga.MaterializeShared`, `MvRegister.ValuesShared` - and route it there. The public seam stays contract-pure; the internal hot path pays nothing. Never widen such a view to `public`, and never let one escape to an external caller.

The **set primitives are payload-free** and must stay that way: `GSet`, `OrSet` and `RwSet` encode each element to a base64 string key on ingress and decode a fresh array on egress, so they retain no caller buffer and satisfy all three legs by construction. `CrdtBufferOwnershipContractTests` pins this, so a change to raw `byte[]` element storage fails until the three legs are implemented explicitly.

Both legs have drawn blood, each time in a different primitive. Egress: `OrMap.Clone` (#1705), `OrMap.Get` (#1709), `Rga.Clone` (#1724), then `Rga.ToList`, `MvRegister.Clone` and `MvRegister.Values`. Fold: `Rga.MergeFrom`/`MergeDelta`, then `MvRegister.MergeFrom`/`MergeDelta`.

The pattern is the recurring one: each fix was pinned by a hand-written test for that one type, so the next sibling was free to repeat it, and every violation was justified in a comment by the values being immutable "by convention" or "by every production call site". That reasoning is not accepted - the contract is about ownership, not about current call sites. Enforcement is therefore structural: `CrdtBufferOwnershipContractTestsBase` (in the shared testing library, bound per package) walks each registered CRDT's object graph and compares `byte[]` instances by reference identity across clone, state fold, delta fold, and every public projection. It also fails when a CRDT type in the package has no specimen, or grows a public `byte[]`-bearing projection nothing covers, so a new primitive cannot join the family without picking the contract up. Register a new primitive there in the same change that adds it.

## Source of Truth for Mode-Carried State

Where a primitive carries a field that duplicates something the registered `LatticeMergeMode` already determines - `BoundedRegister.IsMin` is the only current instance - the **mode is the authority** and the field is a wire-carried cache of it. Re-stamp the field from the mode at **every decode seam** (the `CrdtShapeRegistry` shape decode *and* any accessor read path), so state that reaches the store without passing through a directional accessor self-heals on read rather than folding wrongly forever. Stamp in place on the just-decoded instance; it costs no allocation. Do not resolve such a disagreement by throwing on the merge path - a throw there can wedge replication on a single bad payload and still leaves the stored payload wrong.

## Existing Primitives

| Type | Purpose |
|---|---|
| `HybridLogicalClock` | Totally-ordered timestamp (wall clock + counter) |
| `LwwValue<T>` | Last-writer-wins register resolved by HLC. Carries optional `ExpiresAtTicks` ( TTL) - `0` means no expiry; reads filter entries where `IsExpired(nowUtcTicks)` returns `true`. Carries optional `OriginClusterId` (cluster-of-origin stamp, authored from the ambient `LatticeOriginContext`) and optional `VectorClock` (sparse `{originClusterId → HLC}` commit-time frontier, authored from the ambient `LatticeVectorClockContext`); both default to `null` for legacy persisted state. |
| `VersionVector` | Per-replica version tracking for delta extraction. **Public** - also exposed through `ILattice.VersionVector(key)` as a value-surface CRDT accessor. |
| `OrSet` | Observed-remove set CRDT (public). State-level merge unions both sides' adds and tombstones; concurrent adds and removes survive a later remove that did not observe them. Exposed through `ILattice.OrSet(key)`. |
| `GSet` | Grow-only set CRDT (public). The simplest set CRDT: a set of opaque `byte[]` elements with value-equality by content, no dots and no tombstones. Add is idempotent; state-level merge is set union, which is commutative, associative, and idempotent; `IsBottom` is the empty set. There is no remove operation by design - choose `OrSet` or `RwFlag` when removal is needed. Exposed through `ILattice.GSet(key)`. |
| `OrFlag` | Observed-remove (enable-wins) flag CRDT (public). The single-element specialisation of `OrSet`: state is the set of enable dots minus the set of observed-remove (disable) dots, and the flag is present when at least one enable dot survives. Concurrent enable and disable converge enable-wins. The minimal observed-remove primitive for composite-key membership rows (e.g. a tag/key secondary index). Exposed through `ILattice.OrFlag(key)`. |
| `RwFlag` | Remove-wins (disable-wins) flag CRDT (public). The inverse of `OrFlag`: three grow-only dot lists (enables, disables, and the disable dots an observed enable has tombstoned); the flag is present when an enable dot survives and no live disable remains. Concurrent enable and disable converge disable-wins, so ties and unobserved withdrawals fail closed. Exposed through `ILattice.RwFlag(key)`. |
| `RwSet` | Remove-wins observed-remove set CRDT (public). The set-granularity generalisation of `RwFlag` (`RwFlag` is its single-element specialisation, exactly as `OrFlag` is to `OrSet`): per element three grow-only dot lists (adds, removes, and the remove dots an observed add has tombstoned); an element is present when an add dot survives and no live remove remains. Merge unions the three dot lists per element; commutative, associative, idempotent. Concurrent add and remove of the same element converge remove-wins, so an unobserved revoke is never resurrected. Exposed through `ILattice.RwSet(key)`. |
| `OrSetDot` | `(replicaId, counter)` dot tagged on each `OrSet` add. Also carries `OrFlag` and `RwFlag` enable/disable dots and `RwSet` add/remove dots. |
| `PnCounter` | Positive-negative counter CRDT (public). Per-replica monotonic positive/negative components; merge is pointwise-max per side. Exposed through `ILattice.PnCounter(key)`. |
| `GCounter` | Grow-only counter CRDT (public). Per-replica monotonic grow-only component `{replicaId -> long}`; value is the sum across replicas; merge is pointwise-max per replica (commutative, associative, idempotent). The positive half of `PnCounter`, and the correct primitive when a value only ever increments. Rejects negative increments at the accessor boundary; `IsBottom` is the empty map. Exposed through `ILattice.GCounter(key)`. |
| `MvRegister` | Multi-value register CRDT (public). Dot-context-tagged set of live values; concurrent writes from different replicas survive merge as conflict candidates rather than collapsing under last-writer-wins. Exposed through `ILattice.MvRegister<T>(key)`. |
| `MvRegisterEntry` | Dot-tagged `(replicaId, counter, value)` triple inside an `MvRegister`. |
| `OrMap<TKey, TValue>` | Observed-remove map CRDT (public) of recursively-mergeable CRDT values. Keys follow add-wins observed-remove semantics; per-key values are folded through `ICrdt<TValue>.MergeFrom`, so concurrent writes under the same map key converge into a single recursively-merged value rather than being collapsed by last-writer-wins. Exposed through `ILattice.OrMap<TKey, TValue>(key)`. |
| `OrMapEntry<TValue>` | Dot-tagged `(replicaId, counter, value)` slot inside an `OrMap`. |
| `Rga` | Replicated Growable Array sequence CRDT (public). Stores a tree of dot-tagged nodes linked through `parentDot`; the materialised order is a depth-first walk with descending `(Counter, ReplicaId)` sibling tie-break. Removes tombstone nodes but preserve them in the tree so concurrent inserts under the same parent still resolve. Exposed through `ILattice.Sequence<T>(key)`. |
| `RgaNode` | Tree node carrying `(replicaId, counter, parentDot, value, isTombstone)` inside an `Rga`. |
| `RgaDelta` | Public typed replication delta DTO for `Rga`: dot-explicit inserted nodes (`RgaDeltaNode`) plus tombstoned dots. Folded by `Rga.MergeDelta(RgaDelta)`; dispatched under `LatticeMergeMode.Sequence`. Lives under `Crdt/`. |
| `RgaDeltaNode` | A single inserted node inside an `RgaDelta`: the `(replicaId, counter)` dot, its parent dot, and the value bytes. |
| `OrFlagDelta` | Public typed replication delta DTO for `OrFlag`: the enable dots added (`OrSetDot`) plus the observed-remove (disable) dots. Folded by `OrFlag.MergeDelta(OrFlagDelta)`; dispatched under `LatticeMergeMode.OrFlag`. Lives under `Crdt/`. |
| `RwFlagDelta` | Public typed replication delta DTO for `RwFlag`: the enable dots added, the disable dots added, plus the disable dots an observed enable has tombstoned (all `OrSetDot`). Folded by `RwFlag.MergeDelta(RwFlagDelta)`; dispatched under `LatticeMergeMode.RwFlag`. Lives under `Crdt/`. |
| `GCounterDelta` | Public typed replication delta DTO for `GCounter`: the per-replica cumulative grow-only components `{replicaId -> long}`. Applied by pointwise-max (the value is the cumulative count, not an increment to sum). Folded by `GCounter.MergeDelta(GCounterDelta)`; dispatched under `LatticeMergeMode.GCounter`. Lives under `Crdt/`. |
| `GSetDelta` | Public typed replication delta DTO for `GSet`: the grow-only set of added elements only (no removes by design). Folded by `GSet.MergeDelta(GSetDelta)`; dispatched under `LatticeMergeMode.GSet`. Lives under `Crdt/`. |
| `ICrdt<TSelf>` | Internal-feeling but `public` interface declaring `MergeFrom(TSelf)` plus `IsBottom`. Implemented by `OrSet`, `GSet`, `OrFlag`, `RwFlag`, `PnCounter`, `GCounter`, `VersionVector`, `MvRegister`, `OrMap`, and `Rga`; the constraint that lets `OrMap<TKey, TValue>` recurse through nested CRDT values without reflection. |
| `RwSetDelta` | Public typed replication delta DTO for `RwSet`: the add dots, the remove dots, plus the remove dots an observed add has tombstoned (all element-tagged `OrSetDeltaDot`). Folded by `RwSet.MergeDelta(RwSetDelta)`; dispatched under `LatticeMergeMode.RwSet`. Lives under `Crdt/`. |
| `ICrdt<TSelf>` | Internal-feeling but `public` interface declaring `MergeFrom(TSelf)` plus `IsBottom`. Implemented by `OrSet`, `OrFlag`, `RwFlag`, `RwSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`, and `Rga`; the constraint that lets `OrMap<TKey, TValue>` recurse through nested CRDT values without reflection. |
| `BoundedRegister` | Monotone bounded register CRDT (public), directional. A single value plus an explicit total-order key and a direction bit (`IsMin`): a `Max` register keeps the greatest value ever written, a `Min` register the smallest. `Set(value, orderKey)` advances the register only when the candidate beats the current value under the configured direction (backwards writes are durable no-ops); merge is the directional max/min of the two values - a commutative, associative, idempotent total-order semilattice - and the receiver never needs the domain comparer because the order key travels on the wire. `IsBottom` until first written. Exposed through `ILattice.MaxRegister<T>(key, orderKeySelector)` and `ILattice.MinRegister<T>(key, orderKeySelector)`. Lives under `Primitives/`. |
| `BoundedRegisterDelta` | Public typed replication delta DTO shared by both bounded-register directions: the candidate value plus its total-order key. Direction-free on the wire - the receiver's empty state carries the correct direction from the mode-keyed shape. Folded by `BoundedRegister.MergeDelta(BoundedRegisterDelta)`; dispatched under `LatticeMergeMode.MaxRegister` or `LatticeMergeMode.MinRegister`. Lives under `Crdt/`. |
| `ICrdt<TSelf>` | Internal-feeling but `public` interface declaring `MergeFrom(TSelf)` plus `IsBottom`. Implemented by `OrSet`, `OrFlag`, `RwFlag`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`, `Rga`, and `BoundedRegister`; the constraint that lets `OrMap<TKey, TValue>` recurse through nested CRDT values without reflection. |
| `StateDelta` | Captures entries changed since a given version vector |
| `SplitState` | Enum tracking leaf/internal split lifecycle |

## Adding a New Primitive

1. Define the type in this folder - a `readonly record struct` (with `[Immutable]`) for an immutable value type, or a `sealed class` for a mutable aggregate, per the Type Shape guidance above.
2. Add a constant to `TypeAliases.cs` and apply `[Alias]`.
3. Write unit tests in `test/lattice/Primitives/` verifying commutativity, associativity, and idempotency of merge.
