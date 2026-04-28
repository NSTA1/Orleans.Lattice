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

## Receiver-side bootstrap state machine

The bootstrap state machine that drains an `ISnapshotProvider` export
on the receiver, applies every entry through the local apply seam
preserving the source HLC, and pins the snapshot's causal-stable
frontier on the per-tree high-water-mark grain ships as the public
`ILatticeBootstrapCoordinator` seam. Triggered by the auto-bootstrap
detector (when the inbound apply path observes the sender's cursor
has fallen off the WAL) and by operator-driven re-seed flows.

| Type | Shape | Purpose |
|------|-------|---------|
| `LatticeBootstrapState` | `enum` with members `Idle`, `RequestingSnapshot`, `ApplyingSnapshot`, `IncrementalHandoff`, `LiveIncremental`, `Failed` | The state machine's observable position for a single tree. |
| `ILatticeBootstrapCoordinator` | `Task<LatticeBootstrapState> GetStateAsync(string treeName, CancellationToken ct)` + `Task BootstrapAsync(string treeName, string sourceClusterId, CancellationToken ct)` | Public façade over the per-tree bootstrap coordinator grain. Registered as a singleton by `AddLatticeReplication`; the state machine itself lives in a per-tree internal grain whose cluster-wide single activation provides cross-silo mutual exclusion. |

### State transitions

```text
Idle
  └─► RequestingSnapshot     (BootstrapAsync invoked; ExportAsync issued)
        └─► ApplyingSnapshot (snapshot stream open; draining Entries)
              └─► IncrementalHandoff (entries drained; pinning AsOfHlc + CausalStableFrontier)
                    └─► LiveIncremental (terminal — incremental replication is live)

Any state ──► Failed         (any thrown exception; restart is a fresh BootstrapAsync call)
```

### Semantics

- **Kickoff-and-poll API.** `BootstrapAsync` is an idempotent
  kickoff: it persists the bootstrap intent, schedules the
  background phase pump on a 2-second grain timer plus a 1-minute
  keepalive reminder, and returns. Callers poll `GetStateAsync`
  for progress. This avoids the 30-second Orleans RPC timeout for
  long-running snapshot drains and decouples caller liveness from
  the bootstrap workflow.
- **One bootstrap per tree at a time, cluster-wide.** The state
  machine is hosted in an internal per-tree Orleans grain
  (`ILatticeBootstrapCoordinatorGrain`), so every silo's
  `BootstrapAsync` call for a given tree id routes to the same
  activation. The grain reads the persisted `InProgress` flag on
  entry: a concurrent call from the same source cluster is a no-op
  (idempotent retry); a concurrent call from a different source
  cluster throws `InvalidOperationException`. No distributed lock
  or external coordination is required — Orleans' single-activation
  invariant plus the durable in-progress flag is the synchronisation
  primitive. Concurrent bootstraps of different trees route to
  different activations and run in parallel.
- **Durable, crash-resumable state.** The grain inherits the same
  `CoordinatorGrain<TSelf>` reminder + phase-timer pattern used
  by the core tree-resize coordinator. Phase, source cluster id,
  and a `LastAppliedHlc` cursor are persisted to the
  `LatticeOptions.StorageProviderName` storage provider. After a
  silo crash, Orleans reactivates the grain on a surviving silo
  within the keepalive reminder period and the phase pump resumes
  from the persisted phase. During `ApplyingSnapshot` the cursor
  is persisted every 100 entries; on resume, the snapshot stream
  is re-opened at `LastAppliedHlc` (not `Zero`), so the cost
  of a crash is bounded re-application of at most ~100 entries —
  and the per-origin HWM dedupe makes that re-application a
  correctness no-op.
- **`Failed` is restartable.** On any thrown exception inside the
  phase pump the state transitions to `Failed` (persisted) and
  the pump tears down. A subsequent `BootstrapAsync` call
  restarts the cycle from `RequestingSnapshot`.
- **Source HLC + origin preservation.** Every snapshot entry is
  applied through `IReplicationApplyGrain.ApplySetAsync` carrying the
  entry's commit-time `Timestamp` and the supplied
  `sourceClusterId`. Transitive replication paths (A → B → C) preserve
  the originating HLC.
- **Snapshot/incremental handoff is exactly-once.** The coordinator
  pins `(AsOfHlc, CausalStableFrontier)` on
  `IReplicationHighWaterMarkGrain.PinSnapshotAsync` *after* every
  snapshot entry has been applied. The per-origin HWM dedupe in
  `IReplicationApplier` then makes any incremental entry whose
  timestamp is at or below the pinned frontier a no-op, so the
  snapshot/incremental boundary is exactly-once regardless of
  overlap.
- **Tombstones in custom providers are skipped.** Snapshot entries
  whose `Value` is `null` (not emitted by the default provider, but
  permissible from a host-supplied `ISnapshotProvider`) are skipped
  rather than applied as deletes.

### Sample usage

```csharp
ILatticeBootstrapCoordinator coordinator = client.ServiceProvider
    .GetRequiredService<ILatticeBootstrapCoordinator>();

await coordinator.BootstrapAsync("orders", sourceClusterId: "site-a", cancellationToken);

LatticeBootstrapState state = await coordinator.GetStateAsync("orders", cancellationToken);
_ = state; // LatticeBootstrapState.LiveIncremental once the bootstrap completes
