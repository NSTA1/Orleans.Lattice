# WAL garbage collection

The per-shard write-ahead log (WAL) backing every replicated tree grows
unboundedly until something trims its head. `Orleans.Lattice.Replication`
ships a built-in garbage collector — `ILatticeReplicationGc` — that
trims the head of each shard up to the largest contiguous prefix that
**every** registered consumer has already acknowledged.

## Predicate

A WAL entry is trim-eligible when **either** of the following holds:

| Condition | Meaning |
|---|---|
| `entry.Timestamp <= minCursor` | Every registered consumer has reported a cursor at or beyond this entry HLC. |
| `entry.Timestamp <= ttlCeiling` | The entry wall-clock component is older than `now - WalRetention` (when configured). |

`minCursor` is the minimum HLC across all `(treeName, consumerId)`
entries published to the `ILatticeReplicationCursorRegistry`. The
cursor branch is gated on `minCursor > HybridLogicalClock.Zero` so that
range-delete entries (which carry `HybridLogicalClock.Zero` by design)
are never trimmed under an unset / zero cursor.

`ttlCeiling` is the hard ceiling configured by
`LatticeReplicationOptions.WalRetention`. When set, a lagging consumer
that pins the log past the ceiling is intentionally allowed to "fall
off the log" so disk usage stays bounded; that consumer detects the
gap on its next read and re-bootstraps (Phase 5 protocol).

The scan is conservative: the first non-eligible entry per shard stops
the walk for that shard. WAL offsets are dense and append-only but HLC
`WallClockTicks` is mostly-monotonic-with-skew, so a stop-at-first-miss
walk preserves correctness while a more aggressive scan would risk
trimming an entry younger than a still-pinned later entry.

## Consumer registration

Every consumer of the change feed — the outbound replication ship loop,
in-process bridges, custom transports, and (in v2) the local
materialiser — must publish its acked HLC to the registry so its
progress contributes to `minCursor`. A consumer that never registers
does not pin the log; the GC will trim under it and the consumer must
detect the gap on the next read.

```csharp
var registry = client.ServiceProvider.GetRequiredService<ILatticeReplicationCursorRegistry>();

// After successfully applying a batch acknowledged through the HLC
// `appliedHlc`, the consumer reports its progress. Subsequent reports
// must be monotonically non-decreasing per (treeName, consumerId).
HybridLogicalClock appliedHlc = default;
await registry.ReportCursorAsync(
    treeName: "orders",
    consumerId: "peer:site-b",
    cursor: appliedHlc,
    cancellationToken: cancellationToken);

// On graceful shutdown the consumer unregisters so its stale cursor
// stops pinning the log.
await registry.UnregisterAsync("orders", "peer:site-b", cancellationToken);
```

The default `InMemoryReplicationCursorRegistry` is process-local and
loses its state on silo restart. A host that needs cross-restart
durability registers its own `ILatticeReplicationCursorRegistry`
implementation via DI before calling `AddLatticeReplication(...)`.

## Scheduling

`ILatticeReplicationGc.RunOnceAsync(treeName)` is a single-pass GC
invocation. The library does **not** install a background timer — the
host owns the cadence so it can integrate with whatever scheduling
infrastructure it already uses (Orleans reminders, hosted services,
external schedulers). A typical inner-loop period is 30–60 seconds
per replicated tree.

```csharp
var gc = client.ServiceProvider.GetRequiredService<ILatticeReplicationGc>();

ReplicationGcReport report = await gc.RunOnceAsync(
    treeName: "orders",
    cancellationToken: cancellationToken);

// The report exposes the inputs and the outcome:
//   - report.MinCursor       — minimum cursor across registered consumers, or null
//   - report.TtlCeilingHlc   — TTL ceiling synthesised from WalRetention, or null
//   - report.ShardsScanned   — number of WAL shards walked
//   - report.EntriesTrimmed  — total entries removed across all shards
_ = report.EntriesTrimmed;
```

## Metrics

The GC publishes one counter on the
`orleans.lattice.replication` meter:

| Instrument | Tags | Description |
|---|---|---|
| `orleans.lattice.replication.wal.entries_trimmed` | `tree` | Total WAL entries removed by a GC pass. Incremented only when the pass trimmed at least one entry. |

## Forward compatibility

The GC predicate is expressed as `min(cursor across registered
consumers)` — not `min(cursor across remote peers)` — so a future
local materialiser (the v2 WAL-only direction) is just another
consumer. The materialiser registers under its own `consumerId` and a
lagging materialiser pins the log exactly the same way a lagging
remote peer does.