# Replication drivers

This document describes the **production drivers** that turn the dormant
replication primitives - the change feed, the WAL storage provider, the
WAL garbage collector, and the fall-off-the-log detector - into a running
end-to-end pipeline. Without these drivers, calling `AddLatticeReplication`
yields the seam set but emits nothing on the wire and trims nothing from
disk: every metric on `LatticeReplicationMetrics` other than
`wal.entries_appended` and `dead_letter.*` stays at zero.

The drivers are wired automatically when the host calls
`siloBuilder.AddLatticeReplication(...)`. There is no separate registration
step.

---

## Architecture

Two Orleans-native grain types, both registered as cluster singletons via
their grain key. Cluster-singleton placement gives automatic activation
migration on silo loss without leader election.

| Grain | Key | Cadence | Purpose |
|---|---|---|---|
| `IReplicationShipperGrain` | `{treeName}/{peerClusterId}` | 100 ms phase timer + 90 s reminder backstop + writer-side doorbell | Drains the per-tree change feed from the per-peer cursor, applies producer-side filters and the cycle-break, calls `IReplicationTransport.SendAsync`, advances the cursor on ack, applies exponential backoff on transient failure, parks malformed batches on the per-tree DLQ. |
| `IReplicationMaintenanceGrain` | `{treeName}` | 5 s phase timer + 60 s reminder backstop | Schedules WAL garbage collection (`ILatticeWalGc.RunOnceAsync`) and per-peer fall-off-the-log probes (`ILatticeFallOffLogDetector.CheckAndTriggerAsync`) on independent cadences. |

The shipper is per-peer because per-peer back-pressure isolation must not
couple peers to each other; one slow peer cannot block any other.
The maintenance grain is per-tree because GC and fall-off probing are
tree-scoped, not peer-scoped - running them once per tree avoids
N-fold-redundant work that scales with peer count.

### Activation

A hosted background service (`ReplicationDriverActivationService`,
`BackgroundService`) calls `EnsureActiveAsync` on the cluster-singleton
grain for every replicated tree on startup. Calls are idempotent - Orleans
deduplicates concurrent activations via grain identity, and
`StartCoordinatorAsync` short-circuits when a reminder + phase timer are
already wired.

The activation loop is **retry-with-backoff**: a freshly-started silo may
race the Orleans runtime's own `IHostedService` ordering, so the first
`EnsureActiveAsync` call can throw transiently. The service starts with a
250 ms inter-attempt delay, doubles on each consecutive miss up to a 30 s
cap, and resets the delay on the first successful activation. The loop
only exits when every pending grain is active or the host's
`stoppingToken` is cancelled.

```csharp
// Hosts opt into the drivers transparently - registration is part
// of AddLatticeReplication.
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["catalog"] = LatticeMergeMode.LwwRegister,
    };
    opts.ReplicationPeers = new[] { "site-b", "site-c" };
});
```

### Runtime topology changes

Peer membership is sourced from `IReplicationTopology` (the new
runtime-observable peer topology seam), not snapshotted once at silo
startup. The default implementation, `OptionsReplicationTopology`,
projects `LatticeReplicationOptions.ReplicationPeers` via
`IOptionsMonitor<LatticeReplicationOptions>.OnChange` and diffs each
reload against the last-seen set so callers see one `PeerChanged` event
per net add and per net remove.

A consumer that needs to react to peer arrivals - for example, a
custom dashboard or a metrics publisher - takes a dependency on
`IReplicationTopology` and calls `Subscribe`:

```csharp verify
// Resolve the topology and observe peer membership at runtime.
// In real code this would be injected via constructor parameter.
var topology = client.ServiceProvider.GetRequiredService<IReplicationTopology>();

// Read the current snapshot before subscribing so the consumer does
// not need to replay arrivals it already knows about.
IReadOnlyCollection<string> initialPeers = topology.CurrentPeers;

// Subscribe receives one PeerChanged event per net membership change.
// The returned IDisposable unsubscribes when disposed.
using IDisposable subscription = topology.Subscribe(change =>
{
    if (change.Kind == PeerChangeKind.Added)
    {
        // React to a peer arriving at runtime.
    }
    else if (change.Kind == PeerChangeKind.Removed)
    {
        // React to a peer being removed.
    }
});
```

Hosts that source their topology from a service registry, configuration
provider, or any other dynamic surface can replace the registration by
pre-registering their own `IReplicationTopology` singleton before
`AddLatticeReplication` runs - the default registration uses
`TryAddSingleton`, so a pre-registered implementation wins. The custom
topology only needs to implement the two-member surface:
`IReadOnlyCollection<string> CurrentPeers` and
`IDisposable Subscribe(Action<PeerChanged>)`.

`ReplicationDriverActivationService` subscribes for the lifetime of the
silo: when a peer arrives at runtime (`PeerChangeKind.Added`), one
shipper grain is activated per replicated tree under the same
retry-with-backoff loop as the startup pass, without a silo restart.
Removal events (`PeerChangeKind.Removed`) intentionally do not tear
down the shipper grain - it stays activated to drain its remaining
backlog.

#### Topology vs. `ReplicationPeers`: who owns what

`IReplicationTopology` is **not** the single source of truth for peer
membership across the whole replication pipeline. It governs the
*activation* side of the pipeline; several operational reads still
consult `LatticeReplicationOptions.ReplicationPeers` directly via
`IOptionsMonitor<LatticeReplicationOptions>`. The matrix below lists
every consumer and the source it reads from:

| Consumer | Source it reads | What it does with the peer list |
|---|---|---|
| `ReplicationDriverActivationService` (startup pass + runtime adds) | `IReplicationTopology` | Activates one `IReplicationShipperGrain` per `(replicated tree, peer)`. Runtime adds activate new shippers without a silo restart. |
| `ShardedReplogSink` (doorbell fan-out per WAL append) | `LatticeReplicationOptions.ReplicationPeers` (live snapshot) | Rings every shipper's doorbell after a successful WAL append to drive sub-second ship latency. Best-effort; a missed ring only delays the next phase tick. |
| `ReplicationMaintenanceGrain.ProbeFallOffAsync` (per-cadence fall-off probe) | `LatticeReplicationOptions.ReplicationPeers` (live snapshot) | Walks the peer list each cadence to check whether any peer's persisted cursor has fallen off the local WAL retention window. |
| `ReplicationShipperGrain` (per-peer pump) | Grain key (one activation per `(tree, peer)`) - neither source is re-read | Ships its own backlog. Once activated it is bound to a specific peer for its lifetime; topology and options reloads do not migrate or retarget an existing shipper. |

This split is intentional in the default configuration and does not
produce mismatches: `OptionsReplicationTopology` is a diffed projection
of the exact same `IOptionsMonitor<LatticeReplicationOptions>` instance
that `ShardedReplogSink` and `ReplicationMaintenanceGrain` read, so both
sides observe the same writes within one option-reload tick. Removing
`"site-c"` from `ReplicationPeers` causes the doorbell ring to stop
firing for `"site-c"` on the next WAL append *and* causes the topology
to emit `PeerChanged(site-c, Removed)` from the same `OnChange`
callback.

**The two sources can diverge only when a host replaces the default
topology** by pre-registering its own `IReplicationTopology` singleton
(typically a service-registry-backed source) without keeping
`LatticeReplicationOptions.ReplicationPeers` in sync. When they do
diverge, the conflict resolution is per-concern, not global - there is
no "winner" that overrides the other:

- **Activation follows the topology.** A peer that appears only in the
  custom topology will get a shipper grain activated; a peer that
  appears only in `ReplicationPeers` will not.
- **Doorbells follow `ReplicationPeers`.** A peer that appears only in
  the custom topology will *not* receive doorbell rings - its shipper
  will still drain on the phase timer (default 100 ms), so ship
  latency degrades from sub-second-via-doorbell to bounded-by-the-
  phase-timer but correctness is preserved. A peer that appears only
  in `ReplicationPeers` will get a doorbell ring per WAL append, but
  the ring lands on a shipper grain that the activation service never
  activated; the ring will idly activate the shipper on demand (gRPC
  / Orleans on-demand activation) and the pump will start from cursor
  zero.
- **Fall-off probes follow `ReplicationPeers`.** A peer that appears
  only in the custom topology is not walked by the fall-off probe and
  will not get a `FallOffLogTriggered` notification if its cursor
  drops below the local WAL retention window. This is the only
  divergence that has a *correctness* consequence at the protocol
  level: an unprotected peer can silently fall off the log and need
  manual reseeding.

**Recommended discipline for custom topologies.** Treat
`LatticeReplicationOptions.ReplicationPeers` as the canonical
configuration surface and have the custom `IReplicationTopology`
mirror it (for example, by writing peer registrations back into
options on the way in). The narrow interpretation - the custom
topology purely *observes* membership changes that some other
component has already reflected into `ReplicationPeers` - keeps the
three consumers in lockstep and avoids the asymmetries above. Widening
`IReplicationTopology` to be the single source of truth for doorbell
fan-out and fall-off probing is tracked as a follow-up to the original
topology seam and is not yet implemented.

#### Why not `IObservable<PeerChanged>`?

The seam is intentionally a callback (`IDisposable Subscribe(Action<PeerChanged>)`)
rather than `IObservable<PeerChanged>`. Full Rx semantics
(`OnCompleted`, `OnError`, schedulers, replay buffering) buy nothing for
membership diffs, and an `IObservable<T>`-shaped seam tempts callers to
pull in `System.Reactive` for what is otherwise a one-line lambda. An
`IObservable<PeerChanged>` adapter can be added later as a thin
extension method (`topology.AsObservable()`) without breaking the
primary surface; the reverse change would be a breaking one.

---

## Shipper grain

### Pump loop

Every phase tick (default 100 ms, `LatticeReplicationOptions.ShipPhaseTimerPeriod`) the shipper:

1. Honours the backoff budget set by the previous failed attempt - if
   `_nextRetryAtUtc > now`, the tick returns immediately.
2. Drains a batch up to `LatticeReplicationOptions.ShipBatchSize` entries
   past the persisted cursor from the change feed.
3. Filters each entry through the producer-side filter chain:
   - **Cycle-break:** skip entries whose `OriginClusterId` matches the
     peer's own cluster id (peer never receives its own writes back).
   - **`KeyFilter`:** skip entries whose key fails the configured
     predicate.
   - **`KeyPrefixes`:** skip entries whose key does not start with any
     configured prefix.
4. Encodes the batch via the configured `IReplicationBatchEncoder` into
   an activation-scoped `ArrayBufferWriter<byte>` (see "Buffer reuse"
   below).
5. Calls `IReplicationTransport.SendAsync` with the framed batch.
6. On positive ack, advances the durable cursor to
   `min(ack.HighestAppliedHlc, lastShippedHlc)` and reports the new
   cursor through the `IWalCursorRegistry`.

A successful round-trip resets `ConsecutiveFailures` to `0` and clears
the backoff budget.

### Doorbell

In addition to the phase timer, the shipper exposes
`OnDoorbellAsync(CancellationToken)`. The producer-side `ShardedReplogSink`
rings the doorbell after every successful WAL append for the affected
`(tree, peer)` activations, so steady-state ship latency is sub-second
The doorbell is
best-effort - a missed call only delays the next ship by one timer tick (default 100 ms).

### Backoff schedule

Transient failures (drain throw, transport throw, ack rejected) feed an
exponential backoff sized by:

- `ShipBackoffInitial` (default 100 ms) - base delay on the first failure.
- `ShipBackoffMax` (default 30 s) - upper bound regardless of consecutive
  failure count.
- `ShipBackoffJitter` (default 0.2) - symmetric `[1 - jitter, 1 + jitter]`
  multiplier applied to the computed delay so a fleet of shippers sharing
  a transient outage does not resynchronise on retry.

`Random.Shared` is the jitter source - sufficient for distribution
purposes, not cryptographic.

### Permanent encode failure: dead-letter routing

When `IReplicationBatchEncoder.Encode` throws an `ArgumentException` or
`InvalidOperationException` - schema-shape failures the bytes can never
recover from in their current form - the shipper:

1. Parks every entry in the offending batch on the per-tree
   `IReplicationDeadLetterGrain` tagged with
   `LatticeReplicationMetrics.ReasonSchema` so a single poison entry never
   stalls the stream forever.
2. Advances the cursor past the batch so the stream makes forward
   progress.
3. Logs a warning with the entry count and the new cursor position.

The DLQ enqueue is best-effort; a deterministically-failing DLQ does not
pin the ship loop. The original entries remain in the WAL until the GC
pass trims them, so an operator can still recover off the WAL even when
the DLQ is unavailable.

### Buffer reuse

The shipper maintains two activation-scoped buffers reused across pump
ticks:

- `_drainBuffer` (`List<WalRecord>`) - cleared in place at the start
  of every `PumpOnceAsync`. The encoder consumes the list synchronously
  inside `Encode`, so reuse is safe (no aliasing past the call).
- `_writeBuffer` (`ArrayBufferWriter<byte>`) - reset via
  `ResetWrittenCount()` between ticks, which keeps the underlying array
  and rewinds the write index to 0. A one-time spike that grows the
  buffer at-or-past 4 MB recreates the writer so a single outlier batch
  does not pin a multi-MB array on the heap forever. The 4 MB threshold
  matches the WAL's per-batch byte budget so the typical steady-state
  path always reuses.

Net effect: zero per-tick heap allocation on the steady-state path,
modulo Orleans-internal serializer wrappers.

### Partition resume cursor

The steady-state ship loop bypasses `IChangeFeed` and reads each WAL
partition directly via `IWalShardGrain.ReadAsync(fromSequence, …)`
starting at a durable per-partition resume cursor stored on
`ReplicationShipperState.PartitionCursors`. Per pump tick the shipper
fetches up to `ShipPartitionPageSize` (default 256) entries from each
partition and merges them by HLC ascending via a heap-free O(P) linear
scan-for-min over partition heads. The merge collapses to O(1) for the
canonical single-partition case.

Sequence-based (not HLC-based) resume converts every pump tick from an
O(N) rescan-from-zero walk over the WAL into an O(page) read past the
last successfully shipped offset. `IChangeFeed` is retained verbatim
for bootstrap, test, and future-materialiser consumers that have no
notion of partition routing.

A defensive HLC predicate at the top of the merge loop drops any entry
whose timestamp is at-or-below the durable HLC `Cursor`. This is the
single insurance line that handles the legacy-state-decode case (an
upgraded shipper resuming with a populated HLC cursor but an empty
`PartitionCursors` dictionary), the bootstrap case (the receiver
applies a snapshot that pushes the HLC cursor past pending WAL
entries), and the cross-shipper-HWM case (another peer advanced the
receiver's frontier past ours and the next ack reflects that). Steady
state never matches the predicate because partition cursors move
strictly forward on every positive ack.

Wire-compat is additive: the new `[Id(2)] PartitionCursors` slot on
`ReplicationShipperState` decodes as the empty dictionary for legacy
persisted state, which the cold-start path treats identically to a
fresh activation. Setting `ReplogPartitions=1` (the default) reduces
the merge to a single read per tick.

### Deferred cursor persistence

Cursor advances are amortised across `ShipCursorWriteInterval`
(default 16) successful acks rather than persisted per-ack. The
`_pendingCursorWrites` counter increments on every advance and the
durable `WriteStateAsync` only fires when the counter reaches the
configured interval (or on graceful deactivation). Receiver-side apply
is HLC-monotonic and dedupes on `(originClusterId, originHlc)`, so a
silo crash inside the deferred-persist window costs at most
`ShipCursorWriteInterval × ShipBatchSize` entries of wasteful
re-shipping - the receiver no-ops the duplicates and no data is lost.

Setting `ShipCursorWriteInterval=1` recovers the persist-every-ack
behaviour for hosts that prefer the smaller replay window over the
amortised storage cost.

### Persist-then-report ordering (load-bearing)

`IWalCursorRegistry.ReportCursorAsync` is called
strictly **after** `WriteStateAsync` completes. The WAL GC consumes
the reported cursor to compute the trim frontier, so reporting before
persistence would risk trimming entries the shipper cannot recover
after a crash. This ordering is preserved across the deferred-persist
change: only flushes that durably advance the HLC cursor produce a new
registry report.

A registry-side failure during the report does not unwind the durable
cursor advance and does not retry - the shipper updates
`_lastReportedCursor` to the durable value regardless. The
suppression check inside `FlushCursorAsync` then skips the next
report attempt until the durable cursor moves further forward, at
which point the next flush re-reports the new frontier through the
recovered registry. Operators monitoring the WAL GC trim frontier
should expect this lag to clear on the next post-outage ack rather
than immediately when the registry recovers.

### Graceful deactivation

`OnDeactivateCoreAsync` flushes any pending cursor advance before the
activation tears down so a clean shutdown (e.g. operator silo drain)
eliminates the deferred-persist replay window entirely. A storage
failure during the flush is logged and swallowed - deactivation must
not block - and the next activation recovers by re-shipping at most
`ShipCursorWriteInterval × ShipBatchSize` entries the receiver
dedupes.

### `ShipMaxInFlight` is v1-inert

The validator accepts any value `>= 1`, but the shipper grain hard-codes
strict serial sends per `(tree, peer)` in this release. Multi-batch
pipelining is gated on the typed-envelope transport seam (which removes
the sender-side decode round-trip the gRPC push transport currently pays)
and on multi-batch in-flight WAL flush landing first.

---

## Maintenance grain

### Independent cadences

The two scheduled passes run on independent cadences with their own
last-run timestamps in persistent state:

- **GC pass** - calls `ILatticeWalGc.RunOnceAsync(treeName)` every
  `MaintenanceGcInterval` (default 5 s). The GC consults the
  cursor registry for the slowest-ack frontier across `IChangeFeed`
  consumers and trims the WAL up to that frontier (or the
  `WalRetention` TTL ceiling, whichever is later).
- **Fall-off-the-log probe** - calls
  `ILatticeFallOffLogDetector.CheckAndTriggerAsync(treeName, peer, oldestHlc)`
  every `MaintenanceFallOffCheckInterval` (default 30 s) for each
  configured peer. The sender-oldest HLC is computed via
  `ILatticeWalIntrospection.GetOldestAvailableHlcAsync`. On positive
  detection, the detector drives the bootstrap kickoff itself -
  the maintenance grain is a pure scheduler.

### Failure handling

The cadence stamp advances **only on a successful pass**. A thrown
`RunOnceAsync` or probe pass is logged as a warning and retried on the
next phase tick rather than waiting a full cadence interval. The keepalive
reminder (60 s) is the backstop against a deterministically-failing pass
so the activation cannot stall indefinitely.

This is the opposite of "log and skip" - a steady-state maintenance error
is visible in logs immediately and recovers as soon as the underlying
condition clears.

---

## Options

| Option | Default | Validator | Purpose |
|---|---|---|---|
| `ShipBatchSize` | 256 | `>= 1` | Maximum entries per ship loop iteration. |
| `ShipMaxInFlight` | 1 | `>= 1` | **v1-inert.** Reserved for future multi-batch pipelining. |
| `ShipBackoffInitial` | 100 ms | `> TimeSpan.Zero` | Base delay on first transient failure. |
| `ShipBackoffMax` | 30 s | `>= ShipBackoffInitial` | Upper bound on backoff regardless of consecutive failure count. |
| `ShipBackoffJitter` | 0.2 | `[0.0, 1.0]` | Symmetric jitter multiplier. |
| `MaintenanceGcInterval` | 5 s | `> TimeSpan.Zero` | Cadence between WAL GC passes. |
| `MaintenanceFallOffCheckInterval` | 30 s | `> TimeSpan.Zero` | Cadence between per-peer fall-off-the-log probes. |
| `ShipDoorbellEnabled` | `true` | - | Master switch for the writer-side doorbell. |

All options resolve via `IOptionsMonitor<LatticeReplicationOptions>.Get(treeName)`,
so per-tree overrides are honoured.

---

## Metric activation

These instruments stay at zero until the drivers light them up; the table
shows which driver is the source of each.

| Metric | Source | When it fires |
|---|---|---|
| `wal.entries_appended` | (already wired by `ShardedReplogSink`) | Successful WAL append. |
| `wal.entries_shipped` | Shipper grain via `IReplicationTransport.SendAsync` | Outbound batch acknowledged. |
| `wal.entries_trimmed` (on the core `orleans.lattice` meter, not `orleans.lattice.replication` - see `LatticeMetrics.WalEntriesTrimmed`) | Maintenance grain GC pass | GC trim removed at least one entry. |
| `ship.duration` | Shipper grain via `IReplicationTransport.SendAsync` | Every send call (success or failure). |
| `peer.fell_off_log` | Maintenance grain fall-off probe | Detector confirms peer's HWM precedes sender's oldest HLC. |
| `apply.lag` / `apply.duration` / `apply.fifo_violations` / `apply.buffered_entries` / `apply.buffer_bytes` / `apply.dependency_wait_ms` / `apply.causal_violations_blocked` | Receiver-side `IReplicationApplier` | Lit transitively once the peer is shipping real traffic. |
| `dead_letter.enqueued` (reason=schema) | Shipper grain (permanent encode failure) | Schema-shape encode throw. |
| `dead_letter.removed` | (already wired) | Operator discards/replays. |

---

## Local-apply materialiser reuses the same scheduler

The shipper grain is the canonical scheduler skeleton for the
**local-apply materialiser** in the core library - the
same `IChangeFeed` consumer shape, the same per-consumer cursor on
`IWalCursorRegistry`, the same phase-timer + doorbell +
reminder triad. The materialiser is just another change-feed consumer
with its own cursor; the `IReplicationTransport.SendAsync` call is
replaced with a local `IReplicationApplier.ApplyAsync` call (or the
commit-log apply seam) and the rest of the scheduling skeleton is
verbatim. The maintenance grain is similarly the natural home for any
future per-tree background pass (compaction, snapshot pruning, projection
rebuild) without inventing a third scheduler shape.
