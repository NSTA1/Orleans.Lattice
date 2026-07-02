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
| `Sequence` | **Available** | Replicated Growable Array (RGA) sequence for collaborative ordered lists / text. Each insert ships the dot-explicit triple `(dot, parentDot, value)` and each remove ships the tombstoned dot, so concurrent active-active inserts and deletes from multiple clusters converge on an **identical ordered traversal** via the descending `(Counter, ReplicaId)` sibling tie-break. Author values through `ILattice.Sequence<T>(key)`. The descriptor is a global closed shape, so no per-tree registration is required. |
| `OrFlag` | **Available** | Observed-remove (enable-wins) flag. Each key carries a single presence bit whose state is the set of enable dots minus the set of observed-remove (disable) dots. Concurrent enable and disable from multiple clusters converge enable-wins with their causal dot context preserved. The minimal observed-remove primitive for composite-key membership rows (e.g. a tag/key secondary index). Author through `ILattice.OrFlag(key)`. The descriptor is a global closed shape, so no per-tree registration is required. |
| `RwFlag` | **Available** | Remove-wins (disable-wins) flag - the inverse of `OrFlag`. Each key carries a single presence bit tracked by three grow-only dot lists (enables, disables, and the disable-dots an observed enable has tombstoned); the flag is enabled only when an enable dot survives and no live disable remains. Concurrent enable and disable from multiple clusters converge disable-wins, so ties and unobserved withdrawals fail closed. Author through `ILattice.RwFlag(key)`. The descriptor is a global closed shape, so no per-tree registration is required. |

The validator accepts every defined `LatticeMergeMode` value; only undefined integer values fail validation.

## When `LwwRegister` is the right choice

`LwwRegister` is the right answer for keys with overwrite-with-latest semantics, but only under **single-writer-per-key discipline**. Each key must have at most one authoritative cluster at any given time (e.g. routed by tenant, by shard, or by ownership token). Under this discipline, last-writer-wins is correct: there is never a genuinely-concurrent write to resolve, and the HLC-plus-origin tiebreaker just orders the unambiguous successor.

If your workload allows concurrent writes from multiple clusters to the same key, last-writer-wins **silently drops the loser** - both writes return success on their respective clusters, but only one survives the merge. For those workloads, declare a typed CRDT mode (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`, `Sequence`, `OrFlag`, or `RwFlag`) and author values through the matching accessor on `ILattice` (`OrSet(key)`, `PnCounter(key)`, `VersionVector(key)`, `MvRegister<T>(key)`, `OrMap<TKey, TValue>(key)`, `Sequence<T>(key)`, `OrFlag(key)`, `RwFlag(key)`).

## How typed CRDT modes apply on the receiver

For every typed CRDT mode (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`, `Sequence`, `OrFlag`, `RwFlag`) the producer-side accessor authors a public typed delta DTO (`OrSetDelta`, `PnCounterDelta`, `VersionVectorDelta`, `MvRegisterDelta`, `OrMapDelta<TKey, TValue>`, `RgaDelta`, `OrFlagDelta`, `RwFlagDelta`) into the single `WalRecord.Delta` slot at commit time. The `WalRecord.Value` slot is omitted on the wire for CRDT modes - the canonical payload travels only as the typed delta, not as a serialised post-merge snapshot. The receiver-side applier reads the typed delta from `Delta`, deserialises it through the matching DTO, reads the locally-stored primitive under optimistic concurrency, calls the primitive's instance `MergeDelta(delta)` operation, and writes the merged state back. The merge is wrapped in a `LatticeOriginContext.With(originClusterId)` scope so the receiver's commit-time observer publishes the foreign origin and the producer-side ship loop filters the resulting entry out - the same cycle-break semantics as LWW. Change-feed consumers that historically read `Value` directly on CRDT-mode entries must migrate to either reading `Delta` and folding it against their own prior observed state, or reading the post-merge state through the public lattice surface (`ILattice.GetAsync` / typed accessors).

Typed-delta merge is commutative, associative, and idempotent: late or duplicate delivery converges to the same set / counter / vector / map regardless of arrival order. The per-origin high-water-mark still gates re-delivery to short-circuit redundant work, but correctness does not depend on it for typed CRDT modes.

### OR-Map shape registration

`OrMap<TKey, TValue>` is generic, so the receiver cannot infer `(TKey, TValue)` from the `WalRecord` alone. Each silo that may apply an OR-Map entry must register the concrete shape once at startup:

```csharp verify
using Orleans.Lattice;

siloBuilder.AddOrMapShape<string, OrSet>("tags-by-user");
```

The registration installs a deserialiser / merger descriptor into the `CrdtShapeRegistry` singleton before silo activation; an apply against a tree configured for `LatticeMergeMode.OrMap` with no matching registration faults the apply with a clear configuration-error message rather than silently dropping the entry.

## Sequence mode back-pressure hazard

`Sequence` mode replicates an RGA at **operation granularity**: every `InsertAtAsync` / `InsertAfterAsync` / `RemoveAtAsync` / `RemoveAsync` call commits one CRDT-delta WAL entry, and every WAL entry is one unit of replication ship work. This is the right granularity for convergence - the dot-explicit insert/tombstone is the minimal structural intent a receiver needs - but it makes a **high-frequency editor against a single sequence key a WAL-amplification hazard**: a collaborative text buffer that commits one keystroke per `InsertAfterAsync` generates one WAL entry, one change-feed event, and one shipped delta per keystroke. A sustained fast typist (or a programmatic bulk import that inserts character-by-character) can therefore saturate the per-shard WAL writer and the outbound shipper for that key.

Mitigations, in order of preference:

- **Producer-side debounce / coalescing.** Buffer keystrokes in the editor for a short window (for example 50-150 ms) and commit the batch as a run of inserts under the last-resolved parent dot, or as a single multi-character element when the application's element granularity permits. Coalescing N keystrokes into one commit cuts the WAL and ship rate by N without changing the converged order. The debounce lives in the application's edit loop, not in the lattice - the lattice deliberately commits exactly what it is told so the convergence contract stays exact.
- **Coarser-grained `Snapshot` / LWW mode for cold sequences.** A sequence that is read-mostly and only occasionally rewritten wholesale (a rarely-edited document, a config list rebuilt on save) does not need per-keystroke convergence. Storing it as an opaque value under `LwwRegister` (or rebuilding and writing the whole list on save) replaces the per-edit WAL storm with one entry per save, at the cost of last-writer-wins on concurrent whole-document writes. Reserve `Sequence` mode for keys that are genuinely concurrently edited at fine granularity.

The WAL saturation back-pressure surface (`IWalSaturationSignal` / `IWalSaturationObserver`) still applies: a producer that ignores the debounce guidance and drives a single hot sequence key past the per-tree WAL admission budget observes the standard saturation signal and `LatticeSaturatedException`, the same as any other write-amplifying workload.

## Single shape per tree

A replicated tree is **single-shape**: every value in it is authored and
shipped under the one `LatticeMergeMode` the tree is declared with. The mode is
a property of the *tree*, not of the individual write - the producer stamps the
declared mode onto every `WalRecord`, hoists it once per batch into the encoded
batch header, and the receiver re-stamps it onto every decoded entry before
dispatching the typed apply. There is nowhere on the wire to carry a second
shape, and the receiver never re-inspects the bytes to guess one.

This means a write whose shape disagrees with the declared mode cannot converge
on the peer. A plain last-writer-wins write to a tree declared as a CRDT mode
ships value bytes the receiver tries to decode as a typed delta; a CRDT write
under the wrong mode ships a delta the receiver decodes with the wrong shape.
Either way the receiver's typed apply throws during `DeserializeDelta`, the
entry is retried, and after `MaxApplyRetries` it is parked on the dead-letter
queue. The origin cluster's own copy stays correct, so the divergence is
silent - the peer simply never receives that key.

To turn that silent, receiver-side divergence into a loud, origin-side failure,
the public `ILattice` write surface **fails fast**. When a tree is declared for
replication, any write whose shape does not match the declared mode throws
`LatticeReplicationModeMismatchException` before it commits - nothing is written
locally and nothing is shipped:

- A plain last-writer-wins write (`SetAsync`, `SetManyAsync`,
  `SetManyAtomicAsync`, `SetIfVersionAsync`, `GetOrSetAsync`, `DeleteAsync`,
  `DeleteRangeAsync`, `BulkLoadAsync`, and their predicate variants) to a tree
  declared as any typed CRDT mode is rejected. Author the value through the
  matching accessor (`OrSet(key)`, `PnCounter(key)`, and so on) instead.
- A CRDT write (`ApplyCrdtDeltaAsync`, reached through any CRDT accessor) whose
  mode differs from the declared mode is rejected - whether the tree is declared
  as a different CRDT mode or as `LwwRegister`.

```csharp verify
try
{
    // 'tree' is declared for replication as a typed CRDT mode (for example
    // OrSet). A plain last-writer-wins write is rejected before it commits,
    // because the receiver could not decode the bytes under the declared shape.
    await tree.SetAsync("k", new byte[] { 1 }, cancellationToken);
}
catch (LatticeReplicationModeMismatchException ex)
{
    Console.WriteLine($"{ex.TreeId}: declared {ex.DeclaredMode}, attempted {ex.AttemptedMode}");
}
```

The guard is a no-op for trees that are not declared in
`LatticeReplicationOptions.ReplicatedTrees` (the resolver returns `null`, so
single-cluster hosts are never affected) and for writes whose shape already
matches the declared mode, including a plain write to a tree declared as
`LwwRegister`. It costs a single cached resolver reference and one per-tree
dictionary read per write.

The rejection covers the direct `ILattice` write surface. The
[cross-tree atomic write](../lattice/api.md) builder stages writes through a
separate coordinator saga; when a slice of a cross-tree batch targets a
replicated tree, the same single-shape rule applies - stage the slice with the
matching accessor's `Stage*` method rather than a plain value write.

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
