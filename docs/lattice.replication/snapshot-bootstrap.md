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

```csharp
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

## Snapshot-time saga quiesce

When per-tree atomic-batch delivery is enabled (`LatticeReplicationOptions.AtomicBatchDelivery = true`), in-progress atomic sagas (`SetManyAtomicAsync`) are coordinated with snapshot export so a remote bootstrap never observes a partial-saga view. The producer-side `LatticeSnapshotProvider.ExportAsync` performs a polling quiesce loop (50 ms cadence, capped by `LatticeReplicationOptions.SnapshotSagaQuiesceTimeout`, default 30 s) over the in-process saga tracker before scanning the tree:

- **All sagas drained within the timeout** — the export proceeds with an empty `SnapshotStream.SagaBlacklist`. Receiver behaviour is unchanged from the pre-opt-in case.
- **Some sagas still in flight at the deadline** — the still-running transaction ids are returned in `SnapshotStream.SagaBlacklist`. The receiver-side bootstrap state machine captures the list into `BootstrapCoordinatorState.SagaBlacklist`, persists it across crash boundaries, and on transition to `LiveIncremental` registers it with the per-tree `IReplicationTxBufferGrain` via `RegisterBlacklistedTransactionsAsync`.

Subsequent incremental entries on a blacklisted `TransactionId` bypass the staging buffer and apply as point writes — degrading those specific sagas' cross-cluster atomic visibility to causal+ as a last resort, rather than stalling indefinitely on orphan-timeout because some siblings already landed via the snapshot drain. Non-blacklisted sagas continue to receive full atomic visibility under the normal staging-buffer-then-atomic-apply path.

`SnapshotSagaQuiesceTimeout` is tunable per tree via `IOptionsMonitor<LatticeReplicationOptions>`. The default 30 s suits typical write workloads; raise it on trees with long-running sagas, lower it (or set very small) to prefer faster bootstraps over atomic visibility for the bootstrapping peer's first few sagas.

### Producer-side observer call-site contract

The producer's `IInFlightSagaTracker.ObserveEmission` is invoked from the replication-side commit-time mutation observer **before** any per-emit short-circuit (mode resolver, per-key filter, sink write). This is deliberate: the tracker's count is a proxy for "the producer's tree state has committed this many of the saga's keys", **not** "this many of the saga's keys reached the WAL". The two are equivalent in the steady-state replicated case but diverge when individual siblings are filter-rejected or mode-skipped — the tracker must observe every committed sibling so the in-flight count reflects the producer's tree state, not the WAL projection. Maintenance-category mutations (structural rewrites such as shard splits and saga compensates) are excluded at the call site because they are not user-authored causal events.

The default `InMemoryInFlightSagaTracker` evicts entries older than 10 minutes on every observation and read so a producer-side crash that strands an in-flight count cannot block subsequent snapshots indefinitely.

### Receiver-side blacklist persistence

Blacklist tokens registered via `IReplicationTxBufferGrain.RegisterBlacklistedTransactionsAsync` are persisted to the per-tree backing system tree under the disjoint `x/` key prefix (the staged-entry rows live under `b/`; `b` < `x` in ASCII, so range scans over either prefix do not collide). Each row's key is `x/{transactionId in 'N' format}` and its value is a single-byte sentinel. On grain reactivation the buffer's `BulkLoadAsync` issues a second range scan over `[x/, x0)` to rehydrate the in-memory blacklist set, so a buffer-grain crash mid-bootstrap does not lose the blacklist and re-admit blacklisted entries to the staging buffer.