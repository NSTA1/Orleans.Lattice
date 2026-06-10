# Change feed (`IChangeFeed`)

`IChangeFeed` is the public, in-process subscriber API over the per-shard write-ahead log. It lets consumers - the outbound ship loop, custom transports, integration tests, and any in-process projection - read every captured `WalRecord` for a tree without touching the primary state and without depending on transport-shaped acks.

The contract is deliberately neutral: there is no peer id, no per-call ack envelope, no notion of "live" vs. "snapshot" mode.

## API

The interface lives in `Orleans.Lattice.Replication`:

```text
public interface IChangeFeed
{
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
| `cursor` | required | Strict lower-bound timestamp. The feed yields entries with `entry.Timestamp > cursor`. Pass `HybridLogicalClock.Zero` to read from the start of the WAL. |
| `includeLocalOrigin` | `true` | When `false`, entries whose `OriginClusterId` matches the local `LatticeReplicationOptions.ClusterId` are filtered out - the cycle-break used by remote shippers. Defaults to `true` because in-process projections and background materialisers need to observe local-origin mutations. |
| `cancellationToken` | `default` | Observed between every page read and every yielded entry. |

## Pull semantics

Each `Subscribe` call takes a snapshot of the WAL at invocation time and completes when that snapshot is exhausted. To pick up entries committed after the call, the consumer remembers the timestamp of the last entry it observed and re-subscribes with that value as the new cursor:

```csharp verify
using Orleans.Lattice;

IChangeFeed feed = client.ServiceProvider.GetRequiredService<IChangeFeed>();
string treeName = "orders";
var cursor = HybridLogicalClock.Zero;
while (!cancellationToken.IsCancellationRequested)
{
    await foreach (var entry in feed.Subscribe(treeName, cursor, cancellationToken: cancellationToken))
    {
        // process entry
        cursor = entry.Timestamp;
    }
    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
}
```

Pure-pull means there are no callbacks, no events, and no live-streaming guarantees - every consumer drives its own cadence.

## Ordering

Entries are yielded in `HybridLogicalClock` ascending order, merged across every WAL partition for the requested tree. Ties under equal HLCs are broken by the order in which the merge consumes them, which is unspecified - consumers must treat the feed as a multiset under equal HLCs.

## Caveats

- `DeleteRange` entries currently carry `HybridLogicalClock.Zero` (a known property of `WalRecord`). A non-`Zero` cursor therefore filters them out; this is fixed at the `WalRecord` layer in a later phase, not at the change-feed layer.
- The current implementation merges by collecting filtered entries into a list and sorting them - `O(N log N)` in the number of entries that pass the cursor filter. Adequate for bootstrap and for the test surface this seam enables; the outbound shipper will swap to a streaming k-way merge if the change-feed consumer count grows.
- `WalRecord.Value` is `null` on CRDT-mode `Set` entries (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`) - the canonical encoder strips the slot at WAL append time because the receiver-side apply path dispatches every typed CRDT mode through `WalRecord.Delta` and the primitive's `MergeDelta` operation. Consumers that previously read `Value` on CRDT entries must either (a) read `Delta` and apply it against their own prior observed state, or (b) read the producer's leaf store via the public lattice surface (`ILattice.GetAsync` or the typed accessor). `LwwRegister` entries are unaffected - `Value` remains the canonical payload. See `docs/lattice.replication/wire-format.md` for the encoder-side strip rules.

## Registration

`AddLatticeReplication` registers the default `IChangeFeed` implementation as a singleton against the silo's `IGrainFactory`. Resolve it via DI:

```csharp verify
var feed = client.ServiceProvider.GetRequiredService<IChangeFeed>();
```

## Why a separate seam from the transport

The outbound shipper is one consumer of the change feed; an in-process projection or background materialiser is another. Keeping `IChangeFeed` free of peer ids, acks, and transport options means such a consumer can plug in at the same seam without replication being installed.

## Cursor shape - HLC on the public surface

The public `IChangeFeed.Subscribe` signature accepts a `HybridLogicalClock` cursor, not a per-shard offset. Both shapes were considered:

| Shape | Advantages | Disadvantages |
|---|---|---|
| **HLC cursor** *(chosen)* | Preserves transitive replication HLC fidelity; aligns 1:1 with the per-origin high-water-mark dedup table; cross-tree consumers (a future cross-tree materialiser) have no notion of per-shard offset. | HLC-skew at reconnect time is a (rare) edge case the consumer must handle. |
| **`(shardIndex, offset)` cursor** | Trivially monotonic per shard; matches the underlying `WalEntry.Offset` shape 1:1; no HLC-skew edge cases at reconnect time. | Couples the public API to the internal partitioning scheme; cross-tree consumers can't use it without an HLC translation layer. |

**The decision:** keep cursors HLC-shaped on every public surface. Per-shard offsets are exposed only on the internal transport-side seam used by the gRPC push transport, where reconnect resume points genuinely benefit from the offset shape. The internal `WalResumeToken` (`ShardIndex`, `Offset`) carries the offset cursor across that seam without leaking into the public API.

This decision affects three downstream items:

- The wire envelope's "resume from" field is HLC-shaped on the public transport contract; the internal gRPC stream may carry an opaque `WalResumeToken` alongside as a diagnostic fast-path.
- The per-origin high-water-mark table continues to key on `(tree, originClusterId) → HLC` regardless of cursor shape - HLC is the dedup key, cursor shape only governs *resume* tokens.
- The bootstrap snapshot pin takes a `HybridLogicalClock` argument, not an offset (see `ISnapshotProvider.ExportAsync`).
