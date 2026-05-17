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
  at snapshot time - the pointwise minimum `VersionVector` across
  every consumer that has reported a vector through
  `IWalCursorRegistry.GetCausalStableAsync`. When no
  consumer has reported a VC-shaped cursor (single-peer cluster, fresh
  deployment, host using the legacy HLC-only overload), the provider
  falls back to the producer's per-tree local vector clock from
  `IReplicationHighWaterMarkGrain.GetVectorAsync` - a strict superset
  of the meet that is safe as a snapshot cut-point. Receivers pin
  this on `IReplicationHighWaterMarkGrain.PinSnapshotAsync(asOfHlc, frontier)`
  before draining the entry stream so the causal dependency check on
  the first incremental entry runs from a non-empty frontier.
- **Tombstoned and expired keys are not emitted.** Only live entries
  reach the receiver; the tombstone state is reconstructed from the
  incremental WAL after the snapshot completes.

## Default implementation

The default `LatticeSnapshotProvider` enumerates the **local** tree
via the public `ILattice.EntriesAsync` surface and stamps each entry
with its commit-time HLC via `ILattice.GetWithVersionAsync`. It is
correct for **intra-cluster** seeding (snapshot-as-a-tool: an operator
snapshots a tree and restores it later in the same cluster, where the
local tree is the authoritative source) but pays a per-key version
round-trip on top of the leaf-chain enumeration. A future revision
will swap to a single-pass streaming HLC-threshold scan once the core
library exposes a version-bearing leaf-scan primitive (tracked on the
[core roadmap](../../src/lattice/roadmap.md)); hosts that need a
faster export today can register their own `ISnapshotProvider` via DI.

**Cross-cluster bootstrap is not yet covered by the default provider.**
On a receiver whose local tree is empty (e.g. a fresh cluster joining
an existing federation), the default `LatticeSnapshotProvider` yields
zero entries because it reads the receiver's own tree rather than the
sender's. The "Cross-cluster transport contract" section below
documents the `IRemoteSnapshotTransport` abstraction a cross-cluster
`ISnapshotProvider` adapter draws from. The scoped
[`roadmap-cross-cluster-bootstrap.md`](../../src/lattice.replication/roadmap-cross-cluster-bootstrap.md)
tracks the remaining sender-side handler, receiver-side adapter, and
gRPC binding required to wire the contract end-to-end; until those
items land, multi-cluster federations must register a custom
`ISnapshotProvider` (typically backed by an `IRemoteSnapshotTransport`
implementation) or seed the receiver out of band.

## Cross-cluster transport contract

The first step of the cross-cluster bootstrap pipeline is the
transport-shaped seam that delivers a snapshot stream from a sender
cluster to a receiver cluster. It is a separate abstraction from the
live-incremental `IReplicationTransport` so a host can plug a different
binding for the bulk snapshot path (HTTP, blob-store, gRPC) without
disturbing the live tail pipeline.

| Type | Shape | Purpose |
|------|-------|---------|
| `IRemoteSnapshotTransport` | `Task<RemoteSnapshotMetadata> GetMetadataAsync(string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken ct)` + `IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken ct)` | Transport-shaped sub-interface used by a cross-cluster `ISnapshotProvider` adapter to fetch a snapshot from a sender cluster. |
| `RemoteSnapshotMetadata` | `readonly record struct` with `TreeName`, `SourceClusterId`, `AsOfHlc`, `CausalStableFrontier` | Snapshot cut-point captured atomically with the start of the entry stream; alias `olr.sm`. |

### Semantics

- **Two RPCs, one cut-point.** The receiver invokes `GetMetadataAsync`
  first to capture the sender's cut-point, then invokes
  `RequestSnapshotAsync` with the same `treeName` /
  `sourceClusterId` / `fromAsOfHlc` tuple to drain the stream. The
  metadata RPC returns the `(AsOfHlc, CausalStableFrontier)` pair the
  receiver pins on `IReplicationHighWaterMarkGrain.PinSnapshotAsync`
  before the drain begins, so the snapshot/incremental handoff stays
  exactly-once even though metadata and stream travel on separate
  calls.
- **Point-in-time view.** Implementations MUST guarantee that entries
  committed on the sender after the metadata cut-point do not leak
  into the corresponding stream call. Receivers treat the stream as a
  point-in-time view at `metadata.AsOfHlc`; a moving-target stream
  would violate the cut-point pin and break the causal-stable handoff
  of the first incremental entry.
- **Concurrency.** Implementations are safe to invoke concurrently
  across distinct `(treeName, sourceClusterId)` pairs. Concurrent
  invocation against the same pair is implementation-defined;
  receivers serialise per pair through the bootstrap coordinator.
- **Argument validation.** Both methods throw `ArgumentException`
  when `treeName` or `sourceClusterId` is null or whitespace-only.
- **Atomic-batch coordination is deferred.** The metadata DTO
  intentionally omits prepared-transaction state. Reconstructing
  receiver-side prepared-tx visibility across a cross-cluster
  bootstrap is tracked as a follow-on; until it lands, a producer
  running an in-flight multi-key transaction concurrent with a
  cross-cluster bootstrap may deliver a split view to the
  bootstrapping peer.

### Contract test fixture

Implementations import `RemoteSnapshotTransportContractTests` from
the replication test project and derive a concrete fixture overriding
`CreateTransportAsync` to plug the transport in front of a
sender-side `StubSenderSnapshotProvider`. The inherited acceptance
suite pins:

- `GetMetadataAsync` returns `treeName` / `sourceClusterId` /
  `AsOfHlc` / `CausalStableFrontier` matching the staged sender
  snapshot.
- `RequestSnapshotAsync` streams every staged entry verbatim.
- `RequestSnapshotAsync` yields an empty stream when the sender has
  no entries.
- Metadata-then-stream is consistent under concurrent sender writes:
  entries staged after the metadata cut-point do not leak.
- `ArgumentException` invariants hold for both RPCs.
- The stream observes cancellation tokens during enumeration.

The exemplar `InMemoryRemoteSnapshotTransport` in the replication
test project wraps a local `ISnapshotProvider` and is the smallest
reference shape for what a wire-bound implementation must preserve.

## Sample usage

```csharp verify
using Orleans.Lattice.Primitives;

ISnapshotProvider provider = client.ServiceProvider.GetRequiredService<ISnapshotProvider>();
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
                    └─► LiveIncremental (terminal - incremental replication is live)

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
  or external coordination is required - Orleans' single-activation
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
  of a crash is bounded re-application of at most ~100 entries - 
  and the per-origin HWM dedupe makes that re-application a
  correctness no-op.
- **`Failed` is restartable.** On any thrown exception inside the
  phase pump the state transitions to `Failed` (persisted) and
  the pump tears down. A subsequent `BootstrapAsync` call
  restarts the cycle from `RequestingSnapshot`.
- **Source HLC + origin preservation.** Every snapshot entry is
  applied through `IReplicationApplier.ApplyAsync`, the same canonical
  inbound apply seam used by live-incremental replication, carrying
  the entry's commit-time `Timestamp` and the supplied
  `sourceClusterId`. Transitive replication paths (A -> B -> C)
  preserve the originating HLC.
- **Bootstrap and live-incremental share the apply seam.** Routing
  the snapshot drain through `IReplicationApplier` means every host
  decorator stacked on the applier - dead-letter tracking, the
  causal-apply buffer, and any host-supplied per-key change observer
  - fires identically for bootstrap-arrived entries and
  live-incremental entries. A receiver that catches up via bootstrap
  therefore raises the same observable side-effects as a receiver
  that catches up via the WAL tail, so UI live-update hooks and
  audit observers see the bootstrap window rather than missing it.
  The per-origin HWM dedupe in the applier suppresses any
  re-delivery of bootstrap-arrived entries through the live-incremental
  path.
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

```csharp verify
ILatticeBootstrapCoordinator coordinator = client.ServiceProvider
    .GetRequiredService<ILatticeBootstrapCoordinator>();

await coordinator.BootstrapAsync("orders", sourceClusterId: "site-a", cancellationToken);

LatticeBootstrapState state = await coordinator.GetStateAsync("orders", cancellationToken);
_ = state; // LatticeBootstrapState.LiveIncremental once the bootstrap completes
```


## Operator-driven re-seed

Beyond the receiver-driven auto-bootstrap path (`ILatticeFallOffLogDetector`), the package exposes an explicit operator-facing entry point for scheduled bootstraps - a new peer joining, a bandwidth-constrained initial sync, or a post-disaster re-bootstrap. The seam is `ILatticeReplicationAdmin.RequestSnapshotAsync`; honoured requests delegate to the same `ILatticeBootstrapCoordinator.BootstrapAsync` driving the auto-bootstrap path, so every re-seed - operator-driven or detector-driven - flows through one state machine.

| Type | Shape | Purpose |
|------|-------|---------|
| `ILatticeReplicationAdmin` | `Task<OperatorReseedDecision> RequestSnapshotAsync(string treeName, string sourceClusterId, CancellationToken ct)` | Public façade that gates the request behind a per-`(tree, sourceClusterId)` rate limit before delegating to the bootstrap coordinator. |
| `OperatorReseedDecision` | `readonly record struct` with `Triggered`, `LastRequestedAt`, `RetryAfter` | Diagnostic return value indicating whether the call invoked the coordinator and, when denied, how long the operator should wait before retrying. |

### Semantics

- **Per-`(tree, sourceClusterId)` rate limit.** `LatticeReplicationOptions.OperatorReseedMinInterval` (default `1 minute`) bounds the minimum gap between honoured requests for the same pair. A second request inside the window returns `Triggered = false` with `RetryAfter` set to the remaining time; the coordinator is not invoked and no exception is thrown. `TimeSpan.Zero` disables the rate limit entirely (every request reaches the coordinator).
- **Process-local rate-limit table.** The default implementation tracks honoured requests in process memory only; a silo restart resets the rate-limit window for every pair. Cross-silo coordination is not required because `ILatticeBootstrapCoordinator` is itself idempotent under concurrent invocations against the same tree from the same source cluster (the per-tree internal grain absorbs the second call as a no-op) and rejects mismatched-source concurrent kickoffs as `InvalidOperationException`. The rate limit is therefore a fairness mechanism, not a correctness one.
- **Timestamp updates only on success.** The dictionary timestamp is stamped only after the coordinator call returns successfully, so a thrown coordinator exception (transport failure, conflicting in-flight bootstrap from a different source) does not consume the rate-limit budget against the operator.
- **Per-tree options resolution.** The minimum interval is resolved per-tree via `IOptionsMonitor<LatticeReplicationOptions>.Get(treeName)`, so different replicated trees can run different re-seed cadences without separate seam instances.
- **Argument validation.** `treeName` and `sourceClusterId` must be non-null and non-empty (`ArgumentException` otherwise); the cancellation token is observed before the rate-limit check and propagated to the underlying coordinator.

### Sample usage

```csharp verify
ILatticeReplicationAdmin admin = client.ServiceProvider
    .GetRequiredService<ILatticeReplicationAdmin>();

OperatorReseedDecision decision = await admin.RequestSnapshotAsync(
    "orders", sourceClusterId: "site-a", cancellationToken);

if (!decision.Triggered)
{
    // Rate-limited: the operator should wait `decision.RetryAfter` before
    // retrying. The previously honoured request is still driving the
    // bootstrap coordinator if one was kicked off recently.
    _ = decision.RetryAfter;
    _ = decision.LastRequestedAt;
    return;
}

// Triggered: poll the coordinator for state-machine progress.
LatticeBootstrapState state = await client.ServiceProvider
    .GetRequiredService<ILatticeBootstrapCoordinator>()
    .GetStateAsync("orders", cancellationToken);
_ = state;
```

## Snapshot and in-flight atomic visibility

A snapshot export reads the producer's committed
tree state at the moment `ExportAsync` is invoked. The snapshot
captures only the per-leaf `Entries` projection - the per-tx pending
bucket that holds an in-flight saga's prepared writes is **not**
exported, because prepared entries are deliberately invisible to
readers and to the snapshot exporter. After bootstrap apply, the
incremental WAL replay drives the receiver-side prepared / terminal
apply hops described in
[`replication-apply.md`](replication-apply.md), which populate the
receiver's pending bucket and flip visibility through the per-tree
`ITxRegistryGrain` linearization point. Steady-state cross-cluster
atomic visibility therefore holds across the bootstrap-to-incremental
boundary: no in-flight saga's keys appear in the snapshot, and any
saga that commits before the receiver's incremental cursor reaches its
terminal mark stays invisible until that mark is replayed locally.

The remaining narrow window: a saga whose **terminal mark on the
producer** lands during the export window may have its `Entries`
captured for some leaves (those exported after the terminal flip)
but not others (those exported before). The receiver's incremental
phase converges the missing keys under causal+/LWW as the producer
ships them, but a bootstrapping reader may briefly observe a partial
view across that specific commit-during-export window. Hosts that
need strict atomic visibility across the bootstrap boundary itself
should quiesce writes on the producer for the duration of the
export.