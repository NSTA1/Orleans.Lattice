# Replication drivers

This document describes the **production drivers** that turn the dormant
replication primitives — the change feed, the WAL storage provider, the
WAL garbage collector, and the fall-off-the-log detector — into a running
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
| `IReplicationShipperGrain` | `{treeName}/{peerClusterId}` | 200 ms phase timer + 90 s reminder backstop + writer-side doorbell | Drains the per-tree change feed from the per-peer cursor, applies producer-side filters and the cycle-break, calls `IReplicationTransport.SendAsync`, advances the cursor on ack, applies exponential backoff on transient failure, parks malformed batches on the per-tree DLQ. |
| `IReplicationMaintenanceGrain` | `{treeName}` | 5 s phase timer + 60 s reminder backstop | Schedules WAL garbage collection (`ILatticeWalGc.RunOnceAsync`) and per-peer fall-off-the-log probes (`ILatticeFallOffLogDetector.CheckAndTriggerAsync`) on independent cadences. |

The shipper is per-peer because per-peer back-pressure isolation must not
couple peers to each other; one slow peer cannot block any other.
The maintenance grain is per-tree because GC and fall-off probing are
tree-scoped, not peer-scoped — running them once per tree avoids
N-fold-redundant work that scales with peer count.

### Activation

A hosted background service (`ReplicationDriverActivationService`,
`BackgroundService`) calls `EnsureActiveAsync` on the cluster-singleton
grain for every replicated tree on startup. Calls are idempotent — Orleans
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
// Hosts opt into the drivers transparently — registration is part
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

---

## Shipper grain

### Pump loop

Every phase tick (default 200 ms) the shipper:

1. Honours the backoff budget set by the previous failed attempt — if
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
rather than waiting for the next 200 ms timer tick. The doorbell is
best-effort — a missed call only delays the next ship by one timer tick.

### Backoff schedule

Transient failures (drain throw, transport throw, ack rejected) feed an
exponential backoff sized by:

- `ShipBackoffInitial` (default 100 ms) — base delay on the first failure.
- `ShipBackoffMax` (default 30 s) — upper bound regardless of consecutive
  failure count.
- `ShipBackoffJitter` (default 0.2) — symmetric `[1 - jitter, 1 + jitter]`
  multiplier applied to the computed delay so a fleet of shippers sharing
  a transient outage does not resynchronise on retry.

`Random.Shared` is the jitter source — sufficient for distribution
purposes, not cryptographic.

### Permanent encode failure: dead-letter routing

When `IReplicationBatchEncoder.Encode` throws an `ArgumentException` or
`InvalidOperationException` — schema-shape failures the bytes can never
recover from in their current form — the shipper:

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

- `_drainBuffer` (`List<WalRecord>`) — cleared in place at the start
  of every `PumpOnceAsync`. The encoder consumes the list synchronously
  inside `Encode`, so reuse is safe (no aliasing past the call).
- `_writeBuffer` (`ArrayBufferWriter<byte>`) — reset via
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
re-shipping — the receiver no-ops the duplicates and no data is lost.

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
cursor advance and does not retry — the shipper updates
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
failure during the flush is logged and swallowed — deactivation must
not block — and the next activation recovers by re-shipping at most
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

- **GC pass** — calls `ILatticeWalGc.RunOnceAsync(treeName)` every
  `MaintenanceGcInterval` (default 5 s). The GC consults the
  cursor registry for the slowest-ack frontier across `IChangeFeed`
  consumers and trims the WAL up to that frontier (or the
  `WalRetention` TTL ceiling, whichever is later).
- **Fall-off-the-log probe** — calls
  `ILatticeFallOffLogDetector.CheckAndTriggerAsync(treeName, peer, oldestHlc)`
  every `MaintenanceFallOffCheckInterval` (default 30 s) for each
  configured peer. The sender-oldest HLC is computed via
  `ILatticeWalIntrospection.GetOldestAvailableHlcAsync`. On positive
  detection, the detector drives the bootstrap kickoff itself —
  the maintenance grain is a pure scheduler.

### Failure handling

The cadence stamp advances **only on a successful pass**. A thrown
`RunOnceAsync` or probe pass is logged as a warning and retried on the
next phase tick rather than waiting a full cadence interval. The keepalive
reminder (60 s) is the backstop against a deterministically-failing pass
so the activation cannot stall indefinitely.

This is the opposite of "log and skip" — a steady-state maintenance error
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
| `ShipDoorbellEnabled` | `true` | — | Master switch for the writer-side doorbell. |

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
| `wal.entries_trimmed` | Maintenance grain GC pass | GC trim removed at least one entry. |
| `ship.duration` | Shipper grain via `IReplicationTransport.SendAsync` | Every send call (success or failure). |
| `peer.fell_off_log` | Maintenance grain fall-off probe | Detector confirms peer's HWM precedes sender's oldest HLC. |
| `apply.lag` / `apply.duration` / `apply.fifo_violations` / `apply.buffered_entries` / `apply.buffer_bytes` / `apply.dependency_wait_ms` / `apply.causal_violations_blocked` | Receiver-side `IReplicationApplier` | Lit transitively once the peer is shipping real traffic. |
| `dead_letter.enqueued` (reason=schema) | Shipper grain (permanent encode failure) | Schema-shape encode throw. |
| `dead_letter.removed` | (already wired) | Operator discards/replays. |

---

## Local-apply materialiser reuses the same scheduler

The shipper grain is the canonical scheduler skeleton for the
**local-apply materialiser** in the core library — the
same `IChangeFeed` consumer shape, the same per-consumer cursor on
`IWalCursorRegistry`, the same phase-timer + doorbell +
reminder triad. The materialiser is just another change-feed consumer
with its own cursor; the `IReplicationTransport.SendAsync` call is
replaced with a local `IReplicationApplier.ApplyAsync` call (or the
commit-log apply seam) and the rest of the scheduling skeleton is
verbatim. The maintenance grain is similarly the natural home for any
future per-tree background pass (compaction, snapshot pruning, projection
rebuild) without inventing a third scheduler shape.
