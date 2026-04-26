# Per-shard WAL (write-ahead log)

Every replicated mutation in `Orleans.Lattice.Replication` is committed to a per-shard write-ahead log before any downstream replication consumer observes it. The WAL is the single source of truth for replication: shipping, snapshotting, and recovery all read from the WAL — never from the primary tree.

## Topology

A WAL grain is keyed by `{treeId}/{partition}` and persists an append-only list of `ReplogShardEntry` records. Each entry has a dense, monotonically increasing `Sequence` (starts at 0 and increments by one per append) and the captured `ReplogEntry`.

Routing of a mutation to a partition is deterministic and process-independent: a stable FNV-1a 32-bit hash of the entry's key, modulo `LatticeReplicationOptions.ReplogPartitions` (default `1`). A `null` key hashes as the empty string.

```text
        commit (BPlusLeafGrain / ShardRootGrain)
                       │
                       ▼
            IMutationObserver chain
                       │
                       ▼
              ShardedReplogSink            ← default IReplogSink
                       │
              hash(key) % partitions
                       │
                       ▼
   IReplogShardGrain "{treeId}/{partition}"
                       │
                       ▼
              IPersistentState<ReplogShardState>
```

## Configuration

```text
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplogPartitions = 8; // default 1
});
```
`ReplogPartitions` must be `>= 1`; the validator rejects lower values. The current implementation reads the partition count from `IOptionsMonitor<LatticeReplicationOptions>.CurrentValue`, so per-tree partition-count overrides are not honoured today.

## Producer-side filters

Three options on `LatticeReplicationOptions` decide whether a mutation reaches the WAL at all. Filters run on the producer side at commit time, so a non-replicated mutation never touches a `ReplogShardGrain`:

| Option | Default | Semantics |
|---|---|---|
| `ReplicatedTrees` | `null` | `null` = every tree is replicated; an empty collection = no trees are replicated; a non-empty collection restricts replication to the listed tree ids. |
| `KeyFilter` | `null` | Optional `Func<string, bool>` evaluated against the mutation's key. `null` = accept every key. |
| `KeyPrefixes` | `null` | Optional declarative prefix allowlist. `null` or empty = no prefix restriction; otherwise the key must start with at least one listed prefix (ordinal, case-sensitive). |

The three filters combine with logical AND — a mutation must satisfy every configured filter to be appended. For `DeleteRange` mutations, `KeyFilter` and `KeyPrefixes` are evaluated against the inclusive start key.

Per-tree overrides are honoured: the observer resolves options via `IOptionsMonitor<LatticeReplicationOptions>.Get(treeId)`, so `siloBuilder.ConfigureLatticeReplication("my-tree", o => o.KeyFilter = ...)` overrides the global default for that tree only.

Filters are precompiled per tree id and cached on the observer so the commit-time hot path is bounded by a `ConcurrentDictionary` lookup, a single bool, and at most one delegate plus a linear prefix scan. The cache is invalidated on `IOptionsMonitor.OnChange`, so reconfiguring filters at runtime takes effect on the next mutation per tree.

## Failure semantics

WAL-append failures propagate. A failure inside `IReplogSink.WriteAsync` flows back out of the commit-time observer, and because the observer fires inside the originating grain's write path, the failure surfaces as the same exception the underlying storage provider threw — the calling `ILattice.SetAsync` / `DeleteAsync` / `DeleteRangeAsync` observes it. This guarantees that every committed mutation is also captured for replication.

There is intentionally no opt-in "best-effort" mode that would catch the exception and let the primary write report success while silently dropping the change-feed record. Silent change-feed drops are exactly the hazard commit-time capture exists to remove; a host that wants different semantics for a specific tree should compose its own `IMutationObserver` rather than configure correctness away.

## API

`IReplogShardGrain` is internal to the replication package. Members:

| Member | Purpose |
|---|---|
| `AppendAsync(ReplogEntry, CancellationToken)` | Append a captured mutation. Returns the assigned sequence number. |
| `ReadAsync(long fromSequence, int maxEntries, CancellationToken)` | Read a contiguous page from `fromSequence`. Returns a `ReplogShardPage` with the entries and `NextSequence` cursor. |
| `GetNextSequenceAsync(CancellationToken)` | Returns the sequence the next append will use. |
| `GetEntryCountAsync(CancellationToken)` | Returns the total number of entries persisted. |

`ReadAsync` validates: `fromSequence >= 0` and `maxEntries >= 1`. Out-of-range reads return `ReplogShardPage.Empty(fromSequence)` instead of throwing.

## Persistence

State is stored under the persistent state name `replog-shard` against the standard lattice storage provider (`LatticeOptions.StorageProviderName`). Every successful append performs a `WriteStateAsync` before returning, making the WAL append the commit point for replication.

## Why a WAL grain rather than ship-time read

This design fixes three sample-pipeline shortcuts called out in the [replication design](./replication-design.md):

- **No ship-time value read.** The captured `ReplogEntry` already carries the value (or delta) at commit-time HLC; the ship loop never re-reads the primary.
- **No host-level outgoing-call filter.** Capture happens grain-side via `IMutationObserver`, so the WAL append is atomic with the write rather than a best-effort post-write hook.
- **No silent coalescing between append and ship.** Every mutation gets its own monotonic sequence number; a later overwrite cannot retroactively shadow an earlier WAL entry.

## Testing

- Unit tests against the grain (`ReplogShardGrainTests`) instantiate it with `FakePersistentState<ReplogShardState>` and `Substitute.For<IGrainContext>()`.
- Integration tests (`ReplogShardWalIntegrationTests`) bring up a single-silo `TestCluster` with `AddLattice` + `AddLatticeReplication` and assert that WAL entries appear after `ILattice.SetAsync` / `DeleteAsync`.

## Reading from the WAL

Direct grain access is the low-level entry point; in-process consumers should use [`IChangeFeed`](./change-feed.md) instead. The change feed walks every WAL partition for a tree, filters by HLC cursor and origin, and merges the result in HLC ascending order — the seam the outbound shipper and the future local materialiser plug into.

## Pluggable durability — `IWalStorageProvider`

The grain shape above (`IReplogShardGrain` + Orleans grain persistence) is the WAL's **logical** contract. The **durability backend** is pluggable via `IWalStorageProvider`, a public seam in `Orleans.Lattice.Replication`:

```text
public interface IWalStorageProvider
{
    Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken ct);
    IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken ct);
    Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken ct);
    Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken ct);
}
```

| Member | Contract |
|---|---|
| `AppendBatchAsync` | All-or-nothing per call. Backends that cannot meet that for a particular batch (e.g. a multi-partition write on a backend without cross-partition transactions) must reject the batch at validation time rather than silently fragmenting it. Supplied offsets must be dense relative to the persisted tail. |
| `ReadAsync` | Yields entries strictly above `fromOffsetExclusive` in ascending offset order, up to `maxEntries`. Pass `-1` to read from the start of the log. |
| `GetHighestOffsetAsync` | Returns the highest persisted `WalEntry.Offset`, or `-1` when the shard is empty. Used by the WAL grain on activation to recover its next-offset counter. |
| `TrimAsync` | Removes entries with offset `<= throughOffsetInclusive`. Idempotent; safe to call concurrently with reads. Trimming through an offset that does not yet exist reserves the trim point for a future append. |

The exchanged `WalEntry` is a public `readonly record struct` carrying the dense per-shard `Offset` and the captured `ReplogEntry`. It is intentionally distinct from the internal `ReplogShardEntry` grain RPC envelope so the in-cluster grain protocol can evolve without breaking host-supplied storage backends.

### Configuration

The provider is configurable per-tree via a resolver delegate on `LatticeReplicationOptions`:

```text
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    // Per-tree provider resolver. Returning the same instance for every
    // tree is fine; returning different instances lets a host pick
    // different durability/cost trade-offs per tree.
    opts.WalStorageProvider = treeId => myCustomProvider;
});
```

When `LatticeReplicationOptions.WalStorageProvider` is `null` (the default), the WAL grain falls back to the DI-registered `IWalStorageProvider` singleton. `AddLatticeReplication` registers `InMemoryWalStorageProvider` as the default fallback; replace it by registering your own implementation before calling `AddLatticeReplication` (the registration uses `TryAddSingleton`, so a pre-registered singleton wins).

### Shipped implementations

| Provider | Use case |
|---|---|
| `InMemoryWalStorageProvider` | Default. Stores every appended entry in process memory. Suitable for tests, single-process samples, and pilot deployments before a durable backend is wired up. State is lost on silo restart. |

The grain itself is **not yet rewired** to call the provider — that is the next phase of work (turn-safe batching protocol). The seam ships dormant so host configuration is stable before the grain's internal commit hot path moves over to it. Today's persistence still flows through `IPersistentState<ReplogShardState>`; configuring `WalStorageProvider` does not yet change observable behaviour.

A canonical Azure Table Storage implementation (matching the `(PartitionKey, RowKey) = ({TreeId}/{ShardIndex}, zero-padded 19-digit Offset)` layout described in [`wal-design.md`](./wal-design.md)) is planned as a separate package so the core replication library does not pull in an Azure dependency.

## Forward compatibility

The `IWalStorageProvider` contract is identical between today's replication-only WAL and the future log-first commit-point model in which the WAL becomes the sole durability mechanism. Implementations authored against this interface today are reusable in v2 without API change.
