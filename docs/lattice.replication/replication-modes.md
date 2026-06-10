# Replication modes

Every tree replicated by `Orleans.Lattice.Replication` declares a
**`LatticeMergeMode`** at configuration time. The mode tells receivers how
to merge the captured value bytes; the producer stamps it onto every
emitted `WalRecord` so the receiver never has to guess.

There is no implicit fallback. A tree that is not declared in
`LatticeReplicationOptions.ReplicatedTrees` is **not replicated**. This is
deliberate - the core library stores every value as opaque `byte[]`, so
the producer cannot recognise CRDT primitives by inspection. Implicit
opt-in would silently fall back to last-writer-wins on bytes and risk
concurrent-update data loss; explicit declaration removes the footgun.

## Declaring a mode

```csharp verify
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["users"] = LatticeMergeMode.LwwRegister,
        ["orders"] = LatticeMergeMode.LwwRegister,
    };
});
```

`null` and an empty dictionary both mean "no trees are replicated" - the
commit-time observer short-circuits before any sink call.

## Available modes

| Mode | Status | Convergence guarantee |
|------|--------|-----------------------|
| `LwwRegister` | **Available** | Last-writer-wins ordered by `(HybridLogicalClock, OriginClusterId)`. Concurrent writes from different clusters silently drop the loser; safe under single-writer-per-key discipline. |
| `OrSet` | **Available** | Observed-remove set. State-based merge - concurrent active-active adds and removes from multiple clusters survive convergence with their causal dot context preserved. |
| `PnCounter` | **Available** | Positive-negative counter. Pointwise-max merge on each replica's positive and negative components - concurrent increments and decrements from multiple clusters sum correctly. |
| `VersionVector` | **Available** | Version vector. Pointwise-max merge on each replica's `HybridLogicalClock` entry. Late or duplicate delivery is a no-op. |
| `MvRegister` | **Available** | Multi-value register. Dot-tagged state-based merge - concurrent writes from different clusters survive convergence as a conflict set the application resolves via `MvRegisterAccessor<T>.ValuesAsync()`. |
| `OrMap` | **Available** | Observed-remove map of `(TKey, TValue)` where `TValue` is itself a CRDT. Per-key values converge recursively through `ICrdt<TValue>.MergeFrom`. Requires a one-time `siloBuilder.AddOrMapShape<TKey, TValue>(treeName)` registration on each receiving silo so the applier can resolve the generic shape; an unregistered shape faults the apply rather than silently dropping the entry. |

The validator accepts every defined `LatticeMergeMode` value; only undefined integer values fail validation.

## When `LwwRegister` is the right choice

`LwwRegister` is the right answer for keys with overwrite-with-latest semantics, but only under **single-writer-per-key discipline**. Each key must have at most one authoritative cluster at any given time (e.g. routed by tenant, by shard, or by ownership token). Under this discipline, last-writer-wins is correct: there is never a genuinely-concurrent write to resolve, and the HLC-plus-origin tiebreaker just orders the unambiguous successor.

If your workload allows concurrent writes from multiple clusters to the same key, last-writer-wins **silently drops the loser** - both writes return success on their respective clusters, but only one survives the merge. For those workloads, declare a typed CRDT mode (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, or `OrMap`) and author values through the matching accessor on `ILattice` (`OrSet(key)`, `PnCounter(key)`, `VersionVector(key)`, `MvRegister<T>(key)`, `OrMap<TKey, TValue>(key)`).

## How typed CRDT modes apply on the receiver

For every typed CRDT mode (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`) the producer-side accessor authors a public typed delta DTO (`OrSetDelta`, `PnCounterDelta`, `VersionVectorDelta`, `MvRegisterDelta`, `OrMapDelta<TKey, TValue>`) into the single `WalRecord.Delta` slot at commit time. The `WalRecord.Value` slot is omitted on the wire for CRDT modes - the canonical payload travels only as the typed delta, not as a serialised post-merge snapshot. The receiver-side applier reads the typed delta from `Delta`, deserialises it through the matching DTO, reads the locally-stored primitive under optimistic concurrency, calls the primitive's instance `MergeDelta(delta)` operation, and writes the merged state back. The merge is wrapped in a `LatticeOriginContext.With(originClusterId)` scope so the receiver's commit-time observer publishes the foreign origin and the producer-side ship loop filters the resulting entry out - the same cycle-break semantics as LWW. Change-feed consumers that historically read `Value` directly on CRDT-mode entries must migrate to either reading `Delta` and folding it against their own prior observed state, or reading the post-merge state through the public lattice surface (`ILattice.GetAsync` / typed accessors).

Typed-delta merge is commutative, associative, and idempotent: late or duplicate delivery converges to the same set / counter / vector / map regardless of arrival order. The per-origin high-water-mark still gates re-delivery to short-circuit redundant work, but correctness does not depend on it for typed CRDT modes.

### OR-Map shape registration

`OrMap<TKey, TValue>` is generic, so the receiver cannot infer `(TKey, TValue)` from the `WalRecord` alone. Each silo that may apply an OR-Map entry must register the concrete shape once at startup:

```csharp verify
using Orleans.Lattice;

siloBuilder.AddOrMapShape<string, OrSet>("tags-by-user");
```

The registration installs a deserialiser / merger descriptor into the `CrdtShapeRegistry` singleton before silo activation; an apply against a tree configured for `LatticeMergeMode.OrMap` with no matching registration faults the apply with a clear configuration-error message rather than silently dropping the entry.

## How the mode is resolved at commit time

The commit-time observer routes every mutation through
`ILatticeMergeModeResolver.Resolve(treeId)`:

- The default implementation reads
  `LatticeReplicationOptions.ReplicatedTrees` and caches the per-tree
  outcome until `IOptionsMonitor.OnChange` fires.
- Hosts can replace the registration to source the mode map from
  elsewhere (a control plane, a feature flag system, or a permissive
  test stub that opts every tree in to `LwwRegister`).
- A `null` return value means "this tree is not replicated" and the
  observer returns immediately, before any sink call.

The resolved mode is written to `WalRecord.Mode` so receivers can pick
the correct apply algorithm without re-inspecting the value bytes.
