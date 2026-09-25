# Change feed (`IChangeFeed`)

`IChangeFeed` is the public, in-process subscriber API over the per-shard write-ahead log. It lets in-process consumers - custom bridges and transports, integration tests, and in-process projections - read every locally-authored `WalRecord` for a tree without touching the primary state and without depending on transport-shaped acks. Entries installed on this cluster by inbound replication apply are filtered out; decorate `IReplicationApplier` to observe those.

The contract is deliberately neutral: there is no peer id, no per-call ack envelope, no notion of "live" vs. "snapshot" mode.

## API

The interface lives in `Orleans.Lattice.Replication` and has three members:

```text
public interface IChangeFeed
{
    IAsyncEnumerable<WalRecord> Subscribe(
        string treeName,
        ChangeFeedCursor cursor,
        bool includeLocalOrigin = true,
        CancellationToken cancellationToken = default);

    Task<ChangeFeedCursor> GetCurrentCursorAsync(
        string treeName,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<WalRecord> Subscribe(
        string treeName,
        HybridLogicalClock cursor,
        bool includeLocalOrigin = true,
        CancellationToken cancellationToken = default);
}
```

| Parameter | Default | Semantics |
|---|---|---|
| `treeName` | required | Logical tree id whose change feed is being consumed. Only entries with `WalRecord.TreeId` equal to this value are yielded. |
| `cursor` (`ChangeFeedCursor`) | required | Per-partition exclusive lower bound: for each WAL partition, the offset of the next entry to read. The feed yields every entry whose partition offset is greater than or equal to the matching cursor entry; a partition absent from the cursor reads from offset `0`. `ChangeFeedCursor.Initial` reads every partition from the start. |
| `cursor` (`HybridLogicalClock`) | required | Kept for source compatibility only. The default implementation ignores the value and reads every partition from the start, so a consumer on this overload re-reads the whole retained feed on every call and must de-duplicate entries it has already seen. |
| `includeLocalOrigin` | `true` | When `false`, entries whose `OriginClusterId` matches the local `LatticeReplicationOptions.ClusterId` are filtered out - the cycle-break used by remote shippers. Defaults to `true` because in-process projections and background materialisers need to observe local-origin mutations. |
| `cancellationToken` | `default` | Observed between every page read and every yielded entry. |

`GetCurrentCursorAsync` returns a snapshot of every partition's next append position. Passed back to the `ChangeFeedCursor` overload, it yields exactly the entries committed after the call.

## Pull semantics

Each `Subscribe` call takes a snapshot of the WAL at invocation time and completes when that snapshot is exhausted. To pick up later commits, the consumer re-subscribes with a fresh cursor. The offset cursor is the lossless resume shape: capture it with `GetCurrentCursorAsync`, then read the entries committed after that point:

```csharp verify
using Orleans.Lattice;

IChangeFeed feed = client.ServiceProvider.GetRequiredService<IChangeFeed>();
string treeName = "orders";

// Capture "now"; a later Subscribe from this cursor yields exactly the
// entries committed after this call.
ChangeFeedCursor resumeFrom = await feed.GetCurrentCursorAsync(treeName, cancellationToken);

await foreach (var entry in feed.Subscribe(treeName, resumeFrom, cancellationToken: cancellationToken))
{
    // process entry
}
```

An offset cursor never skips an entry: WAL offsets are monotonic per partition, whereas hybrid-logical clocks are stamped per leaf and can arrive out of order at a shared partition, which is why the HLC cursor is no longer honoured as a filter.

Pure-pull means there are no callbacks, no events, and no live-streaming guarantees - every consumer drives its own cadence.

## Ordering

Entries are yielded in `HybridLogicalClock` ascending order, merged across every WAL partition for the requested tree. Ties under equal HLCs are broken by the order in which the merge consumes them, which is unspecified - consumers must treat the feed as a multiset under equal HLCs.

## Caveats

- Tombstone-reap envelopes (`MutationKind.Tombstone`) are local structural clean-up records with no receiver-side apply rule, so the feed skips them.
- `DeleteRange` entries carry the producer's authoring HLC. Entries persisted by older producers carry `HybridLogicalClock.Zero` and therefore sort ahead of every timestamped entry in the HLC-ordered output.
- The current implementation merges by collecting every entry that passes the cursor and origin filters into a list and sorting it - `O(N log N)` in that entry count. That is adequate for tests and in-process projections; the outbound shipper does not use the feed at all - it tails the WAL partitions directly with its own durable per-partition cursors and streaming merge.
- `WalRecord.Value` is `null` on CRDT-mode `Set` entries that carry a `Delta` (prepared saga entries keep it) - the canonical encoder strips the slot at WAL append time because the receiver-side apply path dispatches every typed CRDT mode through `WalRecord.Delta` and the primitive's `MergeDelta` operation. Consumers that previously read `Value` on CRDT entries must either (a) read `Delta` and apply it against their own prior observed state, or (b) read the producer's leaf store via the public lattice surface (`ILattice.GetAsync` or the typed accessor). `LwwRegister` entries are unaffected - `Value` remains the canonical payload. See `docs/lattice.replication/wire-format.md` for the encoder-side strip rules.

## Registration

`AddLatticeReplication` registers the default `IChangeFeed` implementation as a singleton against the silo's `IGrainFactory`. Resolve it via DI:

```csharp verify
var feed = client.ServiceProvider.GetRequiredService<IChangeFeed>();
```

## Why a separate seam from the transport

The outbound shipper does not read the change feed; custom bridges, tests, and in-process projections or background materialisers do. Keeping `IChangeFeed` free of peer ids, acks, and transport options means such a consumer can plug in at the same seam without depending on the replication transport.

## Cursor shape - offset cursor on the public surface

`IChangeFeed` originally exposed only a `HybridLogicalClock` cursor. That shape assumed HLC monotonicity per WAL partition, which does not hold: each leaf stamps its own HLC, so two leaves appending to one partition can interleave out-of-order clocks, and an HLC lower bound silently dropped the lower-clock entry that arrived second. The public surface therefore carries a per-partition offset cursor, `ChangeFeedCursor`, as the canonical resume shape:

| Shape | Status | Properties |
|---|---|---|
| **`ChangeFeedCursor`** (per-partition offsets) | Canonical | Monotonic per partition by construction (offsets are assigned under the WAL partition's own ordering), so no entry is skipped. `GetCurrentCursorAsync` captures one; `PartitionOffsets` exposes the map for persistence. |
| **`HybridLogicalClock`** | Source-compatible shim | Ignored by the default implementation, which reads every partition from the start; the consumer de-duplicates. |

Downstream of the feed, record identity stays HLC-based whichever cursor shape a consumer used: the receiver suppresses a repeated `(origin, hlc, key, op)` record and tracks a per-origin high-water mark of source HLCs, and the bootstrap snapshot pin takes a `HybridLogicalClock` argument, not an offset (see `ISnapshotProvider.ExportAsync`).
