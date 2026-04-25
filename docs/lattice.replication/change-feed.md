# Change feed (`IChangeFeed`)

`IChangeFeed` is the public, in-process subscriber API over the per-shard write-ahead log. It lets consumers — the outbound ship loop in later phases, custom transports, integration tests, and the future local materialiser — read every captured `ReplogEntry` for a tree without touching the primary state and without depending on transport-shaped acks.

The contract is deliberately neutral: there is no peer id, no per-call ack envelope, no notion of "live" vs. "snapshot" mode. It is the seam later phases plug into.

## API

The interface lives in `Orleans.Lattice.Replication`:

```text
public interface IChangeFeed
{
    IAsyncEnumerable<ReplogEntry> Subscribe(
        string treeName,
        HybridLogicalClock cursor,
        bool includeLocalOrigin = true,
        CancellationToken cancellationToken = default);
}
```

| Parameter | Default | Semantics |
|---|---|---|
| `treeName` | required | Logical tree id whose change feed is being consumed. Only entries with `ReplogEntry.TreeId` equal to this value are yielded. |
| `cursor` | required | Strict lower-bound timestamp. The feed yields entries with `entry.Timestamp > cursor`. Pass `HybridLogicalClock.Zero` to read from the start of the WAL. |
| `includeLocalOrigin` | `true` | When `false`, entries whose `OriginClusterId` matches the local `LatticeReplicationOptions.ClusterId` are filtered out — the cycle-break used by remote shippers. Defaults to `true` because in-process consumers (e.g. a future local materialiser) need to observe local-origin mutations. |
| `cancellationToken` | `default` | Observed between every page read and every yielded entry. |

## Pull semantics

Each `Subscribe` call takes a snapshot of the WAL at invocation time and completes when that snapshot is exhausted. To pick up entries committed after the call, the consumer remembers the timestamp of the last entry it observed and re-subscribes with that value as the new cursor:

```text
var cursor = HybridLogicalClock.Zero;
while (!cancellationToken.IsCancellationRequested)
{
    await foreach (var entry in feed.Subscribe(tree, cursor, cancellationToken: cancellationToken))
    {
        // process entry
        cursor = entry.Timestamp;
    }
    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
}
```

Pure-pull means there are no callbacks, no events, and no live-streaming guarantees — every consumer drives its own cadence.

## Ordering

Entries are yielded in `HybridLogicalClock` ascending order, merged across every WAL partition for the requested tree. Ties under equal HLCs are broken by the order in which the merge consumes them, which is unspecified — consumers must treat the feed as a multiset under equal HLCs.

## Caveats

- `DeleteRange` entries currently carry `HybridLogicalClock.Zero` (a known property of `ReplogEntry`). A non-`Zero` cursor therefore filters them out; this is fixed at the `ReplogEntry` layer in a later phase, not at the change-feed layer.
- The current implementation merges by collecting filtered entries into a list and sorting them — `O(N log N)` in the number of entries that pass the cursor filter. Adequate for bootstrap and for the test surface this seam enables; the outbound shipper introduced in later phases will swap to a streaming k-way merge if the consumer count grows.

## Registration

`AddLatticeReplication` registers the default `IChangeFeed` implementation as a singleton against the silo's `IGrainFactory`. Resolve it via DI:

```text
var feed = serviceProvider.GetRequiredService<IChangeFeed>();
```

## Why a separate seam from the transport

The outbound shipper introduced in later phases is one consumer of the change feed; the future local materialiser is another. Keeping `IChangeFeed` free of peer ids, acks, and transport options means a future projection or background materialiser can plug in at the same seam without replication being installed.
