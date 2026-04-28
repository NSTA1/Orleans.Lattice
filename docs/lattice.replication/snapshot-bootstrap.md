# Snapshot / bootstrap export

`Orleans.Lattice.Replication` ships an `ISnapshotProvider` seam used by
the snapshot/bootstrap protocol to seed a newly-joining peer (or a
peer that has fallen off the WAL) before switching it to incremental
replication.

The seam is registered by `AddLatticeReplication` and resolved per
host via `TryAddSingleton`, so a host that needs a more efficient
storage-specific export can pre-register its own implementation
before calling `AddLatticeReplication`.

## Public surface

| Type | Shape | Purpose |
|------|-------|---------|
| `ISnapshotProvider` | `Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken ct)` | Streaming as-of-HLC export of a tree's primary state. |
| `SnapshotStream` | sealed class with `TreeName`, `AsOfHlc`, `CausalStableFrontier` (`VersionVector`), `Entries` (`IAsyncEnumerable<SnapshotEntry>`) | Carries the export metadata + entry stream produced by `ExportAsync`. |
| `SnapshotEntry` | `readonly record struct` with `Key`, `Value`, `Timestamp` | A single live key-value record stamped with its commit-time HLC so the receiver can pin the value at exactly that timestamp. |

`SnapshotEntry` is alias `olr.se`.

## Semantics

- **`asOfHlc = HybridLogicalClock.Zero`** disables the upper-bound
  filter and includes every live entry in the tree. This is the
  common case when seeding a fresh peer that has no incremental
  cursor yet.
- **`asOfHlc > Zero`** filters out entries whose stamped commit-time
  HLC is strictly greater than `asOfHlc`. The receiver resumes
  incremental replication from `asOfHlc`, and the per-origin
  high-water-mark dedupe in `IReplicationApplier` makes the handoff
  exactly-once across the snapshot/incremental boundary.
- **`CausalStableFrontier`** is the producer's causal-stable frontier
  at snapshot time — the pointwise minimum `VersionVector` across
  every consumer that has reported a vector through
  `ILatticeReplicationCursorRegistry.GetCausalStableAsync`. When no
  consumer has reported a VC-shaped cursor (single-peer cluster, fresh
  deployment, host using the legacy HLC-only overload), the provider
  falls back to the producer's per-tree local vector clock from
  `IReplicationHighWaterMarkGrain.GetVectorAsync` — a strict superset
  of the meet that is safe as a snapshot cut-point. Receivers pin
  this on `IReplicationHighWaterMarkGrain.PinSnapshotAsync(asOfHlc, frontier)`
  before draining the entry stream so the causal dependency check on
  the first incremental entry runs from a non-empty frontier.
- **Tombstoned and expired keys are not emitted.** Only live entries
  reach the receiver; the tombstone state is reconstructed from the
  incremental WAL after the snapshot completes.

## Default implementation

The default `LatticeSnapshotProvider` enumerates the tree via the
public `ILattice.EntriesAsync` surface and stamps each entry with its
commit-time HLC via `ILattice.GetWithVersionAsync`. It is correct but
pays a per-key version round-trip on top of the leaf-chain
enumeration. A future revision will swap to a single-pass streaming
HLC-threshold scan once the core library exposes a version-bearing
leaf-scan primitive (a streaming entries-newer-than-HLC scan tracked on the core roadmap); hosts
that need a faster export today can register their own
`ISnapshotProvider` via DI.

## Sample usage

```csharp
ISnapshotProvider provider = new LatticeSnapshotProvider(grainFactory);
SnapshotStream snapshot = await provider.ExportAsync("orders", HybridLogicalClock.Zero, cancellationToken);

await foreach (SnapshotEntry entry in snapshot.Entries.WithCancellation(cancellationToken))
{
    // Apply each entry on the receiver. Use the entry's commit-time
    // Timestamp so transitive replication paths (A -> B -> C) preserve
    // the originating HLC.
    _ = entry.Key;
    _ = entry.Value;
    _ = entry.Timestamp;
}

VersionVector frontier = snapshot.CausalStableFrontier;
HybridLogicalClock asOf = snapshot.AsOfHlc;
_ = (frontier, asOf);
```

In a host the `ISnapshotProvider` is resolved from DI on the sender
side; `LatticeSnapshotProvider` is shown above for illustration. The
receiver pins the snapshot's `CausalStableFrontier` on its per-tree
`IReplicationHighWaterMarkGrain` via `PinSnapshotAsync` before
draining the entry stream so the causal dependency check on the first
incremental entry runs from a non-empty frontier.
