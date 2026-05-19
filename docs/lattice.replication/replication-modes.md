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

The validator accepts every defined `LatticeMergeMode` value; only undefined integer values fail validation.

## When `LwwRegister` is the right choice

`LwwRegister` is the right answer for keys with overwrite-with-latest semantics, but only under **single-writer-per-key discipline**. Each key must have at most one authoritative cluster at any given time (e.g. routed by tenant, by shard, or by ownership token). Under this discipline, last-writer-wins is correct: there is never a genuinely-concurrent write to resolve, and the HLC-plus-origin tiebreaker just orders the unambiguous successor.

If your workload allows concurrent writes from multiple clusters to the same key, last-writer-wins **silently drops the loser** - both writes return success on their respective clusters, but only one survives the merge. For those workloads, declare a typed CRDT mode (`OrSet`, `PnCounter`, `VersionVector`, or `MvRegister`) and author values through the matching accessor on `ILattice` (`OrSet(key)`, `PnCounter(key)`, `VersionVector(key)`, `MvRegister<T>(key)`).

## How typed CRDT modes apply on the receiver

For `OrSet`, `PnCounter`, `VersionVector`, and `MvRegister` modes the receiver-side applier deserialises the captured value bytes as the typed primitive, reads the locally-stored state under optimistic concurrency, calls the primitive's in-place `MergeFrom` operation, and writes the merged state back. The merge is wrapped in a `LatticeOriginContext.With(originClusterId)` scope so the receiver's commit-time observer publishes the foreign origin and the producer-side ship loop filters the resulting entry out - the same cycle-break semantics as LWW.

State-based merge is commutative, associative, and idempotent: late or duplicate delivery converges to the same set / counter / vector regardless of arrival order. The per-origin high-water-mark still gates re-delivery to short-circuit redundant work, but correctness does not depend on it for typed CRDT modes.

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
