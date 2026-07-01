# Per-shard replication WAL (write-ahead log)

Every replicated mutation in `Orleans.Lattice.Replication` is committed to a per-shard write-ahead log before any downstream replication consumer observes it. The WAL is the single source of truth for replication: shipping, snapshotting, and recovery all read from the WAL - never from the primary tree.

> This document is the **replication-side overlay** - the per-shard sharded sink, the producer-side filters, the change-feed consumer model, and the replication-only configuration knobs. The cross-cutting WAL semantics shared with the core library - the WAL grain API, the commit pipeline, the durability boundary, the turn-safe batching protocol, recovery and rebuild, projection checkpointing, trim and GC, and origin-cluster-id stamping - all live in [`../lattice/wal.md`](../lattice/wal.md). The pluggable storage backend (in-memory vs Azure Table) lives in [`../lattice/wal-storage-providers.md`](../lattice/wal-storage-providers.md). The causal+ entry-schema extension (vector clock + dependency summary slots on `WalRecord`) lives in [`../lattice/wal-causal-plus.md`](../lattice/wal-causal-plus.md).

## Topology

A WAL grain is keyed by `{treeId}/{partition}` and persists an append-only list of `WalShardSequencedEntry` records. Each entry has a dense, monotonically increasing `Sequence` (starts at 0 and increments by one per append) and the captured `WalRecord`.

Routing of a mutation to a partition is deterministic and process-independent: a stable FNV-1a 32-bit hash of the entry's key, modulo `LatticeReplicationOptions.ReplogPartitions` (default `8`, kept in lockstep with `LatticeOptions.WalPartitions` so the shipper reads every partition the commit-log writer fanned across). A `null` key hashes as the empty string.

```text
        commit (BPlusLeafGrain / ShardRootGrain)
                       │
                       ▼
       commit-log writer (WalCommitLogWriter)   <- single WAL appender
                       │
              hash(key) % partitions
                      │
                      ▼
   IWalShardGrain "{treeId}/{partition}"
                      │
                      ▼
                IWalStorageProvider

  (in parallel, off the same commit - no WAL append)
        IMutationObserver chain
                       │
                       ▼
        ReplicationMutationObserver
                      │
                      ▼
               ShardedReplogSink
        commit-time nudge: ring each peer
        shipper's doorbell to wake it if idle
```

The leaf commit-log writer is the single WAL appender: every commit reaches the per-shard `IWalShardGrain` exactly once through it. The commit-time `ShardedReplogSink` does **not** write the WAL and maintains no producer-side vector clock state - it is reduced to a low-latency tree-id doorbell nudge that rings each per-`(tree, peer)` shipper's doorbell. The shipper is the log-first replication producer: it tails the same leaf WAL from a durable per-partition cursor and ships to peers. The causal frontier the shipper sends is read from the leaf WAL itself, not from any in-memory commit-time mirror.

For the `IWalShardGrain` API surface (`AppendAsync`, `ReadAsync`, `GetNextSequenceAsync`, `GetLiveEntryCountAsync`) and the turn-safe batching

## Configuration

```csharp verify
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplogPartitions = 8; // default 8
});
```

`ClusterId` is also the value the producer-side `ILatticeOriginClusterIdResolver` returns when the replication package is registered - every WAL record stamped on this silo carries `OriginClusterId = "site-a"` unless the originating mutation already carried a non-null `OriginClusterId` from upstream. See [`../lattice/wal.md`](../lattice/wal.md) for the resolver contract.

`ReplogPartitions` must be `>= 1`; the validator rejects lower values. The current implementation reads the partition count from `IOptionsMonitor<LatticeReplicationOptions>.CurrentValue`, so per-tree partition-count overrides are not honoured today.

## Producer-side filters

Three options on `LatticeReplicationOptions` decide whether a committed mutation is replicated to peers. The leaf commit-log writer appends every commit to the per-shard WAL regardless; these filters gate the commit-time replication nudge and are re-applied by the shipper as it tails the WAL, so a mutation that fails a filter stays in the local WAL but is never shipped:

| Option | Default | Semantics |
|---|---|---|
| `ReplicatedTrees` | `null` | `null` = every tree is replicated; an empty collection = no trees are replicated; a non-empty collection restricts replication to the listed tree ids. |
| `KeyFilter` | `null` | Optional `Func<string, bool>` evaluated against the mutation's key. `null` = accept every key. |
| `KeyPrefixes` | `null` | Optional declarative prefix allowlist. `null` or empty = no prefix restriction; otherwise the key must start with at least one listed prefix (ordinal, case-sensitive). |

The three filters combine with logical AND - a mutation must satisfy every configured filter to be shipped. For `DeleteRange` mutations, `KeyFilter` and `KeyPrefixes` are evaluated against the inclusive start key.

Per-tree overrides are honoured: the observer resolves options via `IOptionsMonitor<LatticeReplicationOptions>.Get(treeId)`, so `siloBuilder.ConfigureLatticeReplication("my-tree", o => o.KeyFilter = ...)` overrides the global default for that tree only.

Filters are precompiled per tree id and cached on the observer so the commit-time hot path is bounded by a `ConcurrentDictionary` lookup, a single bool, and at most one delegate plus a linear prefix scan. The cache is invalidated on `IOptionsMonitor.OnChange`, so reconfiguring filters at runtime takes effect on the next mutation per tree.

## Maintenance writes are skipped from replication

Beyond the per-tree / per-key filters above, the observer skips a second class of mutation from replication: writes classified as `MutationCategory.Maintenance` on the `LatticeMutation.Category` slot. These are library-internal structural rewrites - resize, rebalance, compaction, internal node splits / merges - that operate on state the user never authored directly and that every converged peer will run independently against its own copy of the data. Replicating them would (a) inflate every peer's vector clock with edges the writer never authored, (b) pollute the dependency graph with non-user-authored edges, and (c) generate wire traffic for events that have no semantic causal meaning.

User-driven writes - `SetAsync`, `DeleteAsync`, `DeleteRangeAsync`, `SetIfVersionAsync`, `GetOrSetAsync`, `SetManyAsync`, `SetManyAtomicAsync`, bulk-load, and saga compensation rolls - emit with `MutationCategory.User` (the default) and follow the existing per-tree / per-key filter path unchanged. The classification is stamped on the mutation at the producing leaf grain and arrives at the observer pre-stamped; users do not interact with the classification mechanism directly.

The maintenance gate runs **before** mode resolution and per-key filters: a maintenance emit pays nothing more than a single enum compare on the commit-time hot path. The classification is also independent of `OriginClusterId` - a remote-origin maintenance emit (from a peer's apply path that itself ran under maintenance) is still `Maintenance` and is still excluded from replication. (The entry is appended to the WAL like any other commit; the shipper re-applies the same maintenance exclusion as it tails the log, so a maintenance entry is never shipped to a peer.)

## Durability and commit-time nudge failure semantics

WAL-append failures propagate. The leaf commit-log writer's append runs inside the originating grain's foreground commit path, so a storage-provider failure surfaces as the same exception the calling `ILattice.SetAsync` / `DeleteAsync` / `DeleteRangeAsync` observes. Because the WAL is the single source of truth for replication, this guarantees that every committed mutation is durably captured for replication before the write reports success.

The commit-time replication nudge is, by contrast, best-effort. `ShardedReplogSink` does not append to the WAL and holds no producer-side vector clock state; it rings each peer shipper's doorbell fire-and-forget. A doorbell ring that fails (silo loss, transient fault, missing activation) is logged at `Trace` and swallowed, so the commit path never fails on a nudge failure - a missed doorbell only delays the affected peer by one shipper timer tick.

There is intentionally no opt-in "best-effort" mode that would catch the WAL-append exception and let the primary write report success while silently dropping the log record. Silent log drops are exactly the hazard commit-time capture exists to remove; a host that wants different semantics for a specific tree should compose its own `IMutationObserver` rather than configure correctness away.

The append-time failure semantics inside the WAL grain itself (offset rollback, per-caller TCS faulting, drain-on-deactivation) live in the core [`../lattice/wal.md`](../lattice/wal.md) under "Turn-safe batching protocol".

## Why a WAL grain rather than ship-time read

Capturing each mutation into a WAL grain at commit time, rather than reading values at ship time, guarantees three properties:

- **No ship-time value read.** The captured `WalRecord` already carries the value (or delta) at commit-time HLC; the ship loop never re-reads the primary.
- **No host-level outgoing-call filter.** Capture happens grain-side via `IMutationObserver`, so the WAL append is atomic with the write rather than a best-effort post-write hook.
- **No silent coalescing between append and ship.** Every mutation gets its own monotonic sequence number; a later overwrite cannot retroactively shadow an earlier WAL entry. (The outbound shipper does apply *pre-ship coalescing* by default - collapsing redundant per-key versions off the cross-cluster wire - but that is a convergent transform over what the ship loop reads, never a mutation of the durable WAL: a last-writer-wins tree keeps the highest-HLC version per key, registered CRDT shapes delta-merge, and generic / unregistered OR-Map plus opaque payloads ship verbatim. Every WAL entry retains its sequence and the resume cursor advances past every elided version. Opt out per tree with `PreShipCoalescingEnabled = false`.)

## Reading from the WAL

Direct grain access is the low-level entry point; in-process consumers should use [`IChangeFeed`](./change-feed.md) instead. The change feed walks every WAL partition for a tree, filters by HLC cursor and origin, and merges the result in HLC ascending order - the seam the outbound shipper and the future local materialiser plug into.

## Pluggable durability (replication-only override)

The replication WAL grain shape (`IWalShardGrain`) is the WAL's **logical** contract; the **durability backend** is the same pluggable `IWalStorageProvider` seam the core library uses. See [`../lattice/wal-storage-providers.md`](../lattice/wal-storage-providers.md) for the provider contract and the shipped in-memory / Azure Table implementations.

`LatticeReplicationOptions` adds one replication-only override on top of that seam: a per-tree resolver delegate that lets a host pick a different provider per tree.

```csharp verify
IWalStorageProvider myCustomProvider = new InMemoryWalStorageProvider();
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

The exchanged `WalEntry` carries the dense per-shard `Offset` and the captured `LatticeMutation`. The replication-only metadata that `WalRecord` carries (`Mode`, `DependencySummary`) is reconstructed at ship time inside `WalShardGrain.ReadAsync` via `ILatticeMergeModeResolver` and the mutation's `VectorClock`, so the on-disk WAL stays storage-pluggable for both single-cluster and multi-cluster hosts.

## Testing

- Unit tests against the grain (`WalShardGrainTests`) instantiate it with substituted `IGrainContext`/`IServiceProvider`/`IOptionsMonitor<LatticeReplicationOptions>` and an `InMemoryWalStorageProvider`, calling the internal `InitializeForTestingAsync` test seam to bypass Orleans activation.
- Integration tests (`WalShardWalIntegrationTests`) bring up a single-silo `TestCluster` with `AddLattice` + `AddLatticeReplication` and assert that WAL entries appear after `ILattice.SetAsync` / `DeleteAsync`.

