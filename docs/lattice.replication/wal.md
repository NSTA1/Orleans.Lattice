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
