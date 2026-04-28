# WAL garbage collection

The per-shard write-ahead log (WAL) backing every replicated tree grows
unboundedly until something trims its head. `Orleans.Lattice.Replication`
ships a built-in garbage collector — `ILatticeReplicationGc` — that
trims the head of each shard up to the largest contiguous prefix that
**every** registered consumer has already acknowledged.

## Predicate

A WAL entry is trim-eligible when the HLC clause **and** the causal-stable clause both accept it.

The HLC clause is satisfied when **either** of the following holds:

| Condition | Meaning |
|---|---|
| `entry.Timestamp <= minCursor` | Every registered consumer has reported a cursor at or beyond this entry HLC. |
| `entry.Timestamp <= ttlCeiling` | The entry wall-clock component is older than `now - WalRetention` (when configured). |

The causal-stable clause is satisfied when **either** of the following holds:

| Condition | Meaning |
|---|---|
| `causalStable is null` | No consumer has reported a per-origin `VersionVector` through the causal+ overload of `ReportCursorAsync`. The clause degrades to a no-op so the GC behaves identically to the legacy HLC-only predicate. |
| `causalStable.DominatesOrEquals(entry.VectorClock)` | Every consumer that reported a vector has fully observed the entry's causal predecessors. Entries with a `null` `VectorClock` (legacy peers, pre-causal+ entries, range deletes) are treated as the empty frontier and pass automatically. |

The two clauses are AND-ed: the existing cursor / TTL predicate is kept for safety so a stale or mis-configured causal-stable computation cannot cause the GC to over-trim past a consumer that is still pinning the HLC half.

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

## Causal-stable frontier

A consumer that has stamped vector clocks on the entries it applies can also report its full per-origin frontier through the causal+ overload of `ReportCursorAsync`. The GC then computes `causalStable` as the **pointwise minimum** of every reported `VersionVector`: an origin is retained in the meet only when every reporting consumer has named it, and the value at that origin is the smallest HLC across the reports.

Consumers that only report HLC (the legacy overload) continue to pin the cursor half of the predicate but are excluded from the meet. When **no** consumer has reported a vector, `causalStable` is `null` and the GC behaves identically to the legacy HLC-only predicate.

The frontier is cached in the registry behind a per-tree generation counter that bumps on every accepted report or unregister, so a high-frequency GC pass that observes a stable registry reads the frontier in O(1).

A consumer registers a vector by passing the additional `VersionVector` argument:

```csharp
var registry = client.ServiceProvider.GetRequiredService<ILatticeReplicationCursorRegistry>();

HybridLogicalClock appliedHlc = default;
VersionVector appliedFrontier = new();
await registry.ReportCursorAsync(
    treeName: "orders",
    consumerId: "peer:site-b",
    cursor: appliedHlc,
    vector: appliedFrontier,
    cancellationToken: cancellationToken);
```

The registry takes a defensive clone of the supplied vector, so callers may continue to mutate their local frontier after the report returns.

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
//   - report.CausalStable    — pointwise-min VersionVector across consumers, or null
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