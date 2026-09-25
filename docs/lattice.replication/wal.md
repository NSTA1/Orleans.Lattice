# Per-shard replication WAL (write-ahead log)

Every replicated mutation in `Orleans.Lattice.Replication` is committed to a per-shard write-ahead log before any downstream replication consumer observes it. The WAL is the single source of truth for replication: shipping, snapshotting, and recovery all read from the WAL - never from the primary tree.

> This document is the **replication-side overlay** - the per-shard sharded sink, the producer-side filters, the change-feed consumer model, and the replication-only configuration knobs. The cross-cutting WAL semantics shared with the core library - the WAL grain API, the commit pipeline, the durability boundary, the turn-safe batching protocol, recovery and rebuild, projection checkpointing, trim and GC, and origin-cluster-id stamping - all live in [`../lattice/wal.md`](../lattice/wal.md). The pluggable storage backend (in-memory vs Azure Table) lives in [`../lattice/wal-storage-providers.md`](../lattice/wal-storage-providers.md). The causal+ entry-schema extension (vector clock + dependency summary slots on `WalRecord`) lives in [`../lattice/wal-causal-plus.md`](../lattice/wal-causal-plus.md).

## Topology

A WAL grain is keyed by `{treeId}/{partition}` and persists an append-only list of sequenced WAL-entry records. Each entry has a dense, monotonically increasing `Sequence` (starts at 0 and increments by one per append) and the captured `WalRecord`.

Routing of a mutation to a partition is deterministic and process-independent: a stable FNV-1a 32-bit hash of the entry's key, modulo the tree's WAL partition count (saga terminal marks route by shard index modulo the same count instead). A `null` key hashes as the empty string. That count is pinned in the tree's registry entry when the tree is first registered, from its `LatticeOptions.WalPartitions` (default `8`), and is not changed afterwards. The shipper reads a separate setting, `LatticeReplicationOptions.ReplogPartitions` (default `8`), so the two agree only by configuration - see [Configuration](#configuration) below.

```text
        commit (leaf / shard-root grains)
                       │
                       ▼
       commit-log writer   <- single WAL appender
                       │
              hash(key) % partitions
                      │
                      ▼
   per-shard WAL grain "{treeId}/{partition}"
                      │
                      ▼
                IWalStorageProvider

  (in parallel, off the same commit - no WAL append)
        IMutationObserver chain
                       │
                       ▼
        replication mutation observer
                      │
                      ▼
              commit-time doorbell sink
        commit-time nudge: ring each peer
        shipper's doorbell to wake it if idle
```

The leaf commit-log writer is the single WAL appender: every commit reaches the per-shard WAL grain exactly once through it. The commit-time doorbell sink does **not** write the WAL and maintains no producer-side vector clock state - it is reduced to a low-latency tree-id doorbell nudge that rings each per-`(tree, peer)` shipper's doorbell. The shipper is the log-first replication producer: it tails the same leaf WAL from a durable per-partition cursor and ships to peers. The causal frontier the shipper sends is read from the leaf WAL itself, not from any in-memory commit-time mirror.

For the per-shard WAL grain API surface (append, read, next-sequence, and live-entry-count operations) and the turn-safe batching protocol, see [`../lattice/wal.md`](../lattice/wal.md).

## Configuration

```csharp verify
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplogPartitions = 8; // default 8
});
```

`ClusterId` is also the value the producer-side `ILatticeOriginClusterIdResolver` returns when the replication package is registered - every WAL record stamped on this silo carries `OriginClusterId = "site-a"` unless the originating mutation already carried a non-null `OriginClusterId` from upstream. See [`../lattice/wal.md`](../lattice/wal.md) for the resolver contract.

`ReplogPartitions` must be `>= 1`; the validator rejects lower values, and nothing checks it against the WAL partition count. The value is resolved per tree (`IOptionsMonitor<LatticeReplicationOptions>.Get(treeId)`), and the shipper, the change feed, fall-off detection, and anti-entropy leaf re-replay all read only partitions `[0, ReplogPartitions)` of the tree's WAL, so it must equal the tree's pinned WAL partition count. The two match only when both options stay at their default of `8`, or when the count is set here: a non-default `ReplogPartitions` is mirrored onto the same tree's `LatticeOptions.WalPartitions` unless that value is already non-default, and the tree pins it at first registration. Nothing copies a core-only `LatticeOptions.WalPartitions` override back. When they differ, a `ReplogPartitions` below the pinned count leaves every WAL record routed to a higher partition unread, so it is never shipped to peers - with no error, and nothing in the shipper's lag gauges to show it; only the anti-entropy digest probe, when enabled, detects the resulting divergence. A `ReplogPartitions` above the pinned count only adds reads of partitions that never receive writes. Because the pin is fixed at first registration, changing either option later does not re-partition an existing tree: keep each replicated tree's `ReplogPartitions` equal to the count it was first registered with. See [`ReplogPartitions`](configuration.md#replogpartitions).

## Producer-side filters

Three options on `LatticeReplicationOptions` decide whether a committed mutation is replicated to peers. The leaf commit-log writer appends every commit to the per-shard WAL regardless; these filters gate the commit-time replication nudge and are re-applied by the shipper as it tails the WAL, so a mutation that fails a filter stays in the local WAL but is never shipped:

| Option | Default | Semantics |
|---|---|---|
| `ReplicatedTrees` | `null` | Per-tree opt-in map from tree id to `LatticeMergeMode`. `null` and an empty map both mean no tree is replicated - there is no implicit "all trees" wildcard; only the listed tree ids replicate, under their declared mode. |
| `KeyFilter` | `null` | Optional `Func<string, bool>` evaluated against the mutation's key. `null` = accept every key. |
| `KeyPrefixes` | `null` | Optional declarative prefix allowlist. `null` or empty = no prefix restriction; otherwise the key must start with at least one listed prefix (ordinal, case-sensitive). |

The three filters combine with logical AND - a mutation must satisfy every configured filter to be shipped. For `DeleteRange` mutations, `KeyFilter` and `KeyPrefixes` are evaluated against the inclusive start key. Saga terminal records (`TxCommit` / `TxAbort`) bypass `KeyFilter` and `KeyPrefixes` on the shipper, because their key is an internal shard-routing token and cross-cluster atomic visibility needs every terminal delivered.

Per-tree overrides are honoured: the observer resolves options via `IOptionsMonitor<LatticeReplicationOptions>.Get(treeId)`, so `siloBuilder.ConfigureLatticeReplication("my-tree", o => o.KeyFilter = ...)` overrides the global default for that tree only.

Filters are precompiled per tree id and cached on the observer so the commit-time hot path is bounded by a `ConcurrentDictionary` lookup, a single bool, and at most one delegate plus a linear prefix scan. The cache is invalidated on `IOptionsMonitor.OnChange`, so reconfiguring filters at runtime takes effect on the next mutation per tree.

## Maintenance writes are skipped from replication

Beyond the per-tree / per-key filters above, the observer skips a second class of mutation from replication: writes classified as `MutationCategory.Maintenance` on the `LatticeMutation.Category` slot - library-internal clean-up of state the user never authored directly, which every converged peer runs independently against its own copy of the data. Replicating such writes would (a) inflate every peer's vector clock with edges the writer never authored, (b) pollute the dependency graph with non-user-authored edges, and (c) generate wire traffic for events that have no semantic causal meaning. Today the only producer of that category is tombstone compaction: the reap envelopes it appends (`MutationKind.Tombstone`) are stamped `Maintenance`. Structural rewrites that move entries between leaves or shards - leaf splits, cross-shard migration during online reshard and shard splits, and tree merges - are not maintenance-classified: they re-append each moved entry as a last-writer-wins merge envelope that keeps the `User` category together with the entry's original origin and HLC, so on a peer that already applied the original it is a no-op - suppressed by the receiver's exact-identity dedup of repeated `(origin, hlc, key, op)` records, or re-applied idempotently if it has aged out of that bounded cache.

User-driven writes - `SetAsync`, `DeleteAsync`, `DeleteRangeAsync`, `SetIfVersionAsync`, `GetOrSetAsync`, `SetManyAsync`, `SetManyAtomicAsync`, bulk-load, and the compensating atomic write an `IAtomicActionGrain` saga issues to restore a completed tree-write step's pre-images - emit with `MutationCategory.User` (the default) and follow the existing per-tree / per-key filter path unchanged. An aborted `SetManyAtomicAsync` writes no per-key rollback at all: its prepared writes were never visible, so the abort is recorded in the tree's transaction registry and broadcast as a `TxAbort` terminal mark that drops them on every shard the batch touched, and that terminal ships to peers like a commit terminal. The classification is stamped on the mutation at the producing leaf grain and arrives at the observer pre-stamped; users do not interact with the classification mechanism directly.

The maintenance gate runs **before** mode resolution and per-key filters: a maintenance emit pays nothing more than a single enum compare on the commit-time hot path. The classification is also independent of `OriginClusterId` - a remote-origin maintenance emit (from a peer's apply path that itself ran under maintenance) is still `Maintenance` and is still excluded from replication. (The WAL record does carry the category, but the shipper does not consult it as it tails the log; its tail-side filter excludes the tombstone-reap envelopes compaction appends by their `MutationKind.Tombstone` operation.)

## Durability and commit-time nudge failure semantics

WAL-append failures propagate. The leaf commit-log writer's append runs inside the originating grain's foreground commit path, so a storage-provider failure surfaces as the same exception the calling `ILattice.SetAsync` / `DeleteAsync` / `DeleteRangeAsync` observes. Because the WAL is the single source of truth for replication, this guarantees that every committed mutation is durably captured for replication before the write reports success.

The commit-time replication nudge is, by contrast, best-effort. The commit-time doorbell sink does not append to the WAL and holds no producer-side vector clock state; it rings each peer shipper's doorbell fire-and-forget. A doorbell ring that fails (silo loss, transient fault, missing activation) is logged at `Trace` and swallowed, so the commit path never fails on a nudge failure - a missed doorbell only delays the affected peer by one shipper timer tick.

There is intentionally no opt-in "best-effort" mode that would catch the WAL-append exception and let the primary write report success while silently dropping the log record. Silent log drops are exactly the hazard commit-time capture exists to remove; a host that wants different semantics for a specific tree should compose its own `IMutationObserver` rather than configure correctness away.

The append-time failure semantics inside the WAL grain itself (offset rollback, per-caller TCS faulting, drain-on-deactivation) live in the core [`../lattice/wal.md`](../lattice/wal.md) under "Turn-safe batching protocol".

## Why a WAL grain rather than ship-time read

Capturing each mutation into a WAL grain at commit time, rather than reading values at ship time, guarantees three properties:

- **No ship-time value read.** The captured `WalRecord` already carries the value (or delta) at commit-time HLC; the ship loop never re-reads the primary.
- **No host-level outgoing-call filter.** Capture happens grain-side via `IMutationObserver`, so the WAL append is atomic with the write rather than a best-effort post-write hook.
- **No silent coalescing between append and ship.** Every mutation gets its own monotonic sequence number; a later overwrite cannot retroactively shadow an earlier WAL entry. (The outbound shipper does apply *pre-ship coalescing* by default - collapsing redundant per-key versions off the cross-cluster wire - but that is a convergent transform over what the ship loop reads, never a mutation of the durable WAL: a last-writer-wins tree keeps the highest-HLC version per key, registered CRDT shapes delta-merge, and generic / unregistered OR-Map plus opaque payloads ship verbatim. Every WAL entry retains its sequence and the resume cursor advances past every elided version. Opt out per tree with `PreShipCoalescingEnabled = false`.)

## Reading from the WAL

Direct grain access is the low-level entry point; in-process consumers should use [`IChangeFeed`](./change-feed.md) instead. The change feed walks every WAL partition for a tree from a per-partition offset cursor, filters by origin, and merges the result in HLC ascending order. The outbound shipper does not use it: it tails the WAL partitions directly from its own durable per-partition cursors.

## Pluggable durability (replication-only override)

The replication WAL grain shape is the WAL's **logical** contract; the **durability backend** is the same pluggable `IWalStorageProvider` seam the core library uses. See [`../lattice/wal-storage-providers.md`](../lattice/wal-storage-providers.md) for the provider contract and the shipped in-memory / Azure Table implementations.

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

When `LatticeReplicationOptions.WalStorageProvider` is `null` (the default), the WAL grain falls back to the DI-registered `IWalStorageProvider` singleton. `AddLattice` installs `InMemoryWalStorageProvider` as that fallback (a first-wins registration); replace it with `AddWalStorage(factory)` or a storage package such as `AddAzureTableWalStorage`, which replace the baseline whether they are called before or after `AddLattice`.

The exchanged `WalEntry` carries the dense per-shard `Offset` and the captured `LatticeMutation`. When the WAL is read back, the authored merge mode comes from the durable record itself (the record persists it since wire id 26), falling back to `ILatticeMergeModeResolver` only when it holds the default `LwwRegister` (a plain LWW write or a legacy record); `DependencySummary` is rebuilt from the mutation's `VectorClock`. The on-disk WAL therefore stays storage-pluggable for both single-cluster and multi-cluster hosts.

## Testing

- Unit tests against the grain (`WalShardGrainTests`) instantiate it with substituted `IGrainContext`/`IServiceProvider`/`IOptionsMonitor<LatticeReplicationOptions>` and an `InMemoryWalStorageProvider`, calling the internal `InitializeForTestingAsync` test seam to bypass Orleans activation.
- Integration tests (`WalShardWalIntegrationTests`) bring up a single-silo `TestCluster` with `AddLattice` + `AddLatticeReplication` and assert that WAL entries appear after `ILattice.SetAsync` / `DeleteAsync`.

