using System.Diagnostics.Metrics;

namespace Orleans.Lattice;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/> instruments
/// for Orleans.Lattice. All instruments are published on a single <see cref="Meter"/>
/// named <see cref="MeterName"/> so an OpenTelemetry pipeline can subscribe once and
/// receive every Lattice metric.
/// </summary>
/// <remarks>
/// Instruments fall into five tiers:
/// <list type="bullet">
///   <item>
///     <b>Shard-level</b> - per-shard read / write / split counters, sourced from
///     <c>ShardRootGrain</c>. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
///   </item>
///   <item>
///     <b>Leaf-level</b> - write-state latency, scan latency, compaction duration,
///     tombstone churn (explicit deletes + TTL expiries reported separately),
///     and leaf-split counters sourced from <c>BPlusLeafGrain</c>. Tagged with
///     <see cref="TagTree"/> (leaf grain ids are too high-cardinality to publish directly).
///   </item>
///   <item>
///     <b>Cache</b> - hit / miss counters from <c>LeafCacheGrain</c>.
///   </item>
///   <item>
///     <b>Saga / coordinator / lifecycle</b> - terminal-state counters for
///     <c>SetManyAtomicAsync</c> sagas, long-running coordinator completions
///     (snapshot / resize / reshard / merge / compaction), and tree-lifecycle
///     transitions (deleted / recovered / purged).
///   </item>
///   <item>
///     <b>Events &amp; configuration</b> - event-publisher health (dispatches
///     succeeded vs. dropped) and per-tree configuration-change counters.
///   </item>
/// </list>
/// All durations are reported in <em>milliseconds</em> as <c>double</c>.
/// </remarks>
public static class LatticeMetrics
{
    /// <summary>
    /// The root meter / instrument / activity-source name for all Orleans.Lattice telemetry.
    /// All internal telemetry hooks must reference this constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice";

    /// <summary>Tag key for the logical tree id.</summary>
    public const string TagTree = "tree";

    /// <summary>Tag key for the physical shard index.</summary>
    public const string TagShard = "shard";

    /// <summary>Tag key for the operation kind (e.g. <c>keys</c> or <c>entries</c> on scan histograms).</summary>
    public const string TagOperation = "operation";

    /// <summary>
    /// Tag key for the terminal outcome of a saga / coordinator (e.g.
    /// <c>committed</c>, <c>compensated</c>, <c>failed</c> on
    /// <see cref="AtomicWriteCompleted"/>).
    /// </summary>
    public const string TagOutcome = "outcome";

    /// <summary>
    /// Tag key for a discriminated-kind dimension (e.g. coordinator kind,
    /// tree-lifecycle kind, event kind).
    /// </summary>
    public const string TagKind = "kind";

    /// <summary>Tag key for the reason a publication / operation was dropped.</summary>
    public const string TagReason = "reason";

    /// <summary>
    /// Tag key for a configuration dimension name (e.g.
    /// <c>publish_events</c> on <see cref="ConfigChanged"/>).
    /// </summary>
    public const string TagConfig = "config";

    /// <summary>
    /// Tag key for a per-step dimension on the leaf commit path
    /// (e.g. <c>wal</c>, <c>apply</c>, <c>observer</c> on
    /// <see cref="LeafCommitDuration"/>) so operators can attribute
    /// total commit latency to its constituent stages.
    /// </summary>
    public const string TagStep = "step";

    /// <summary>
    /// The meter that owns every Lattice instrument. Exposed publicly so integration
    /// tests and custom OpenTelemetry exporters can subscribe by reference rather
    /// than by name.
    /// </summary>
    public static readonly Meter Meter = new(MeterName);

    // --- Shard-level counters (ShardRootGrain) -----------------------------------

    /// <summary>Counter incremented on every read operation observed by a shard root.</summary>
    public static readonly Counter<long> ShardReads =
        Meter.CreateCounter<long>("orleans.lattice.shard.reads", unit: "{op}",
            description: "Read operations served by a shard root (GetAsync, ExistsAsync, scan, count, etc.).");

    /// <summary>Counter incremented on every write operation observed by a shard root.</summary>
    public static readonly Counter<long> ShardWrites =
        Meter.CreateCounter<long>("orleans.lattice.shard.writes", unit: "{op}",
            description: "Write operations served by a shard root (SetAsync, DeleteAsync, MergeManyAsync, etc.).");

    /// <summary>
    /// Counter incremented once per <c>IShardRootGrain.GetShardProjectionDigestAsync</c>
    /// call, tagged with <see cref="TagTree"/> and <see cref="TagShard"/>. Lets operators
    /// (and integration tests) verify that a whole-tree poll of
    /// <see cref="ILattice.GetLeafProjectionDigestAsync"/> issues exactly one grain
    /// call per physical shard - the chained-fold design's headline operational
    /// invariant - rather than degrading to an O(shardCount x leafCount) walk.
    /// </summary>
    public static readonly Counter<long> ShardDigestReads =
        Meter.CreateCounter<long>("orleans.lattice.shard.digest_reads", unit: "{op}",
            description: "Projection-digest reads served by a shard root (one per GetShardProjectionDigestAsync call).");

    /// <summary>
    /// Counter incremented once per adaptive shard-split commit, fired from
    /// <c>TreeShardSplitGrain.FinaliseAsync</c> immediately after the shard
    /// map swap succeeds.
    /// </summary>
    public static readonly Counter<long> ShardSplitsCommitted =
        Meter.CreateCounter<long>("orleans.lattice.shard.splits_committed", unit: "{split}",
            description: "Adaptive shard-split commits (ShardMap swap published).");

    // --- Leaf-level instruments (BPlusLeafGrain) ---------------------------------

    /// <summary>
    /// Histogram of <c>IPersistentState.WriteStateAsync</c> durations observed by
    /// <c>BPlusLeafGrain</c>. Captures storage-provider write latency from the
    /// perspective of the leaf grain that issued the persist.
    /// </summary>
    public static readonly Histogram<double> LeafWriteDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.write.duration", unit: "ms",
            description: "Duration of IPersistentState.WriteStateAsync calls from BPlusLeafGrain.");

    /// <summary>
    /// Histogram of leaf-level scan durations. Tagged with <see cref="TagOperation"/>
    /// = <c>keys</c> (for <c>GetKeysAsync</c>) or <c>entries</c> (for
    /// <c>GetEntriesAsync</c>).
    /// </summary>
    public static readonly Histogram<double> LeafScanDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.scan.duration", unit: "ms",
            description: "Duration of leaf-level range scans (GetKeysAsync / GetEntriesAsync).");

    /// <summary>Histogram of <c>CompactTombstonesAsync</c> durations.</summary>
    public static readonly Histogram<double> LeafCompactionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.compaction.duration", unit: "ms",
            description: "Duration of tombstone compaction passes on a single leaf.");

    /// <summary>Counter of tombstone entries removed by <c>CompactTombstonesAsync</c>.</summary>
    public static readonly Counter<long> LeafTombstonesReaped =
        Meter.CreateCounter<long>("orleans.lattice.leaf.tombstones.reaped", unit: "{tombstone}",
            description: "Tombstone entries permanently removed by compaction.");

    /// <summary>
    /// Counter of tombstones created - incremented on every <c>DeleteAsync</c>
    /// success and once per deleted key in <c>DeleteRangeAsync</c>.
    /// </summary>
    public static readonly Counter<long> LeafTombstonesCreated =
        Meter.CreateCounter<long>("orleans.lattice.leaf.tombstones.created", unit: "{tombstone}",
            description: "Tombstone entries newly written by delete operations.");

    /// <summary>
    /// Counter of live entries removed by compaction because their per-entry
    /// TTL (set via the TTL overload of <c>SetAsync</c>) elapsed past the
    /// configured grace period. Separate from <see cref="LeafTombstonesReaped"/>
    /// so operators can distinguish explicit-delete reap throughput from TTL
    /// churn.
    /// </summary>
    public static readonly Counter<long> LeafTombstonesExpired =
        Meter.CreateCounter<long>("orleans.lattice.leaf.tombstones.expired", unit: "{tombstone}",
            description: "Live entries reaped by compaction because their TTL elapsed past the grace period.");

    /// <summary>Counter of leaf-level splits (leaf capacity exceeded, sibling allocated).</summary>
    public static readonly Counter<long> LeafSplits =
        Meter.CreateCounter<long>("orleans.lattice.leaf.splits", unit: "{split}",
            description: "Leaf-node splits triggered by MaxLeafKeys overflow.");

    /// <summary>
    /// Histogram of per-step latency on the leaf commit path
    /// (build-and-WAL-append, in-memory Apply, observer-publish,
    /// parent-digest publish). Tagged with <see cref="TagStep"/> =
    /// <c>wal</c>, <c>apply</c>, <c>observer</c>, or <c>digest</c> so
    /// operators can attribute total commit latency to its constituent
    /// stages. The <c>digest</c> step covers the awaited cross-grain
    /// <c>OnChildDigestPublishedAsync</c> RPC to the parent internal
    /// node emitted from every foreground write path (single-key
    /// <c>SetAsync</c> / <c>DeleteAsync</c>, per-leaf
    /// <c>DeleteRangeAsync</c>); cold / structural digest publishes
    /// (leaf-split topology, projection-checkpoint flush, saga
    /// terminal) are deliberately excluded so the histogram remains
    /// attributable to the per-write pipeline.
    /// </summary>
    public static readonly Histogram<double> LeafCommitDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.commit.duration", unit: "ms",
            description: "Per-step latency on the BPlusLeafGrain commit path.");

    // --- Cache instruments (LeafCacheGrain) --------------------------------------

    /// <summary>
    /// Counter of cache hits observed by <c>LeafCacheGrain</c> - a key was present
    /// and live in the local cache after (possibly) refreshing the delta.
    /// </summary>
    public static readonly Counter<long> CacheHits =
        Meter.CreateCounter<long>("orleans.lattice.cache.hits", unit: "{hit}",
            description: "LeafCacheGrain reads served by a live, cached entry.");

    /// <summary>
    /// Counter of cache misses observed by <c>LeafCacheGrain</c> - the key was
    /// absent or tombstoned in the local cache after the delta refresh.
    /// </summary>
    public static readonly Counter<long> CacheMisses =
        Meter.CreateCounter<long>("orleans.lattice.cache.misses", unit: "{miss}",
            description: "LeafCacheGrain reads that did not find a live cached entry.");

    // --- Saga / coordinator / lifecycle instruments ------------------------------

    /// <summary>
    /// Counter incremented once per terminal transition of an <c>AtomicWriteGrain</c>
    /// saga. Tagged with <see cref="TagOutcome"/> = <c>committed</c> (all writes
    /// applied), <c>compensated</c> (prepare / execute failure rolled back via LWW),
    /// or <c>failed</c> (post-compensation surrogate failure).
    /// </summary>
    public static readonly Counter<long> AtomicWriteCompleted =
        Meter.CreateCounter<long>("orleans.lattice.atomic_write.completed", unit: "{saga}",
            description: "Terminal transitions of SetManyAtomicAsync sagas, tagged by outcome.");

    /// <summary>
    /// Histogram of end-to-end <c>SetManyAtomicAsync</c> saga durations,
    /// recorded once per terminal transition of an <c>AtomicWriteGrain</c>
    /// saga next to <see cref="AtomicWriteCompleted"/>. The duration is
    /// measured from the wall-clock time the saga's first
    /// <c>AtomicWritePhase.Prepare</c> ran (persisted on the saga state
    /// so it survives a silo crash) to the time the saga reached
    /// <c>AtomicWritePhase.Completed</c>. Tagged with <see cref="TagOutcome"/>
    /// = <c>committed</c>, <c>compensated</c>, or <c>failed</c> so operators
    /// can plot rollback-path latency separately from happy-path latency.
    /// <para>
    /// Combine with <see cref="AtomicWriteBatchSize"/> when building dashboards:
    /// duration is meaningful only relative to the size of the batch that
    /// produced it. A duration spike with a constant batch-size distribution
    /// is a regression; a duration spike accompanied by a batch-size spike is
    /// a workload change.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> AtomicWriteDuration =
        Meter.CreateHistogram<double>("orleans.lattice.atomic_write.duration", unit: "ms",
            description: "End-to-end SetManyAtomicAsync saga duration, tagged by outcome.");

    /// <summary>
    /// Histogram of saga batch sizes, recorded once per terminal transition of
    /// an <c>AtomicWriteGrain</c> saga next to <see cref="AtomicWriteCompleted"/>.
    /// The value is the entry count submitted to <c>SetManyAtomicAsync</c>
    /// (or the per-entry list length on apply-mode sagas). Tagged with
    /// <see cref="TagOutcome"/> = <c>committed</c>, <c>compensated</c>, or
    /// <c>failed</c>. Lets operators interpret <see cref="AtomicWriteDuration"/>
    /// in context - a 10-entry batch and a 1000-entry batch both appear as one
    /// data point on the duration histogram, and only the batch-size histogram
    /// disambiguates them.
    /// </summary>
    public static readonly Histogram<int> AtomicWriteBatchSize =
        Meter.CreateHistogram<int>("orleans.lattice.atomic_write.batch_size", unit: "{entry}",
            description: "Entry count of each SetManyAtomicAsync saga, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per successful coordinator-grain completion.
    /// Tagged with <see cref="TagKind"/> = <c>snapshot</c>, <c>resize</c>,
    /// <c>reshard</c>, <c>merge</c>, or <c>compaction</c>.
    /// </summary>
    public static readonly Counter<long> CoordinatorCompleted =
        Meter.CreateCounter<long>("orleans.lattice.coordinator.completed", unit: "{operation}",
            description: "Long-running coordinator-grain completions (snapshot, resize, reshard, merge, compaction).");

    /// <summary>
    /// Counter incremented once per tree-lifecycle transition. Tagged with
    /// <see cref="TagKind"/> = <c>deleted</c>, <c>recovered</c>, or <c>purged</c>.
    /// </summary>
    public static readonly Counter<long> TreeLifecycle =
        Meter.CreateCounter<long>("orleans.lattice.tree.lifecycle", unit: "{event}",
            description: "Tree-lifecycle transitions emitted by TreeDeletionGrain.");

    /// <summary>
    /// Counter incremented once per successfully-dispatched
    /// <see cref="LatticeTreeEvent"/>. Tagged with <see cref="TagKind"/> =
    /// the event kind name (e.g. <c>Set</c>, <c>SnapshotCompleted</c>).
    /// </summary>
    public static readonly Counter<long> EventsPublished =
        Meter.CreateCounter<long>("orleans.lattice.events.published", unit: "{event}",
            description: "LatticeTreeEvent instances successfully dispatched to the configured stream provider.");

    /// <summary>
    /// Counter incremented once per event drop. Tagged with <see cref="TagReason"/>
    /// = <c>missing_provider</c> (no stream provider by the configured name) or
    /// <c>publish_error</c> (stream provider threw during dispatch).
    /// </summary>
    public static readonly Counter<long> EventsDropped =
        Meter.CreateCounter<long>("orleans.lattice.events.dropped", unit: "{event}",
            description: "LatticeTreeEvent instances dropped because the stream provider was missing or threw.");

    /// <summary>
    /// Counter incremented once per per-tree configuration change applied at
    /// runtime. Tagged with <see cref="TagConfig"/> = the configuration
    /// dimension (e.g. <c>publish_events</c>) and <see cref="TagTree"/>.
    /// </summary>
    public static readonly Counter<long> ConfigChanged =
        Meter.CreateCounter<long>("orleans.lattice.config.changed", unit: "{change}",
            description: "Per-tree configuration changes applied at runtime via ILattice overrides.");

    // --- Leaf-projection replay instruments ----------------------------------

    /// <summary>
    /// Histogram of activation-time leaf-projection replay durations,
    /// emitted by <c>BPlusLeafGrain.OnActivateAsync</c> when the
    /// activation path consults the persisted projection checkpoint
    /// and drives <c>ILeafProjection.Apply</c> over the WAL slice.
    /// Tagged with <see cref="TagOutcome"/> = <c>tail</c> (caught up by
    /// replaying the slice <c>(checkpoint, head]</c>),
    /// <c>snapshot_then_wal</c> (a fall-off-log trigger fired and the
    /// snapshot-then-WAL recovery path was taken), or
    /// <c>full_rebuild</c> (a full WAL rebuild was forced via
    /// <see cref="ProjectionRebuildPolicy.FullRebuildFromWal"/>).
    /// </summary>
    public static readonly Histogram<double> LeafReplayDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.replay.duration", unit: "ms",
            description: "Activation-time leaf-projection replay duration, tagged by recovery outcome.");

    /// <summary>
    /// Counter of mutations encountered during activation-time leaf
    /// projection replay. Tagged with <see cref="TagOutcome"/> =
    /// <c>applied</c> (fed to <c>ILeafProjection.Apply</c>) or
    /// <c>skipped</c> (filtered by the leaf's key-range responsibility
    /// before reaching <c>Apply</c>).
    /// </summary>
    public static readonly Counter<long> LeafReplayEntries =
        Meter.CreateCounter<long>("orleans.lattice.leaf.replay.entries", unit: "{entry}",
            description: "Mutations seen by activation-time leaf-projection replay, tagged by outcome.");

    // --- Snapshot-cursor instruments ----------------------------------------

    /// <summary>
    /// Histogram of per-shard WAL-replay duration observed during
    /// snapshot-leaf open. Emitted by <c>SnapshotLeafGrain</c> after a
    /// successful replay over <c>[0, capturedOffset)</c>. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/> (the virtual
    /// shard the snapshot leaf materialises).
    /// </summary>
    public static readonly Histogram<double> SnapshotReplayDuration =
        Meter.CreateHistogram<double>("orleans.lattice.snapshot.replay.duration", unit: "ms",
            description: "Per-shard WAL-replay duration observed during zero-observable-writes snapshot-leaf open.");

    /// <summary>
    /// Counter of WAL entries fed to the snapshot-leaf replay engine
    /// during a snapshot-leaf open. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>. One increment per
    /// <c>CommitLogSliceEntry</c> processed; filtered or skipped
    /// records are still counted because they contribute to wall-clock
    /// replay cost.
    /// </summary>
    public static readonly Counter<long> SnapshotReplayEntries =
        Meter.CreateCounter<long>("orleans.lattice.snapshot.replay.entries", unit: "{entry}",
            description: "WAL entries consumed by the zero-observable-writes snapshot-leaf replay engine.");

    /// <summary>
    /// Up-down counter tracking the number of live WAL retention pins
    /// registered by snapshot cursors against
    /// <see cref="IWalCursorRegistry"/>. Incremented on
    /// <c>OpenSnapshotKeyCursorAsync</c> / <c>OpenSnapshotEntryCursorAsync</c>
    /// after a successful pin report and decremented on close /
    /// idle-TTL eviction. Tagged with <see cref="TagTree"/>.
    /// </summary>
    public static readonly UpDownCounter<long> SnapshotPinCount =
        Meter.CreateUpDownCounter<long>("orleans.lattice.snapshot.pins", unit: "{pin}",
            description: "Live WAL retention pins held by zero-observable-writes snapshot cursors.");

    // --- WAL garbage-collector instruments ----------------------------------

    /// <summary>
    /// Counter of WAL entries removed by a <see cref="ILatticeWalGc.RunOnceAsync"/>
    /// pass, tagged with <see cref="TagTree"/>. Emitted from
    /// <see cref="LatticeWalGc"/> after every pass that trims at least one
    /// entry; a zero-trim pass does not emit so a high-frequency GC pass
    /// against an empty WAL produces no measurement traffic.
    /// </summary>
    public static readonly Counter<long> WalEntriesTrimmed =
        Meter.CreateCounter<long>("orleans.lattice.wal.entries_trimmed", unit: "{entry}",
            description: "WAL entries removed by the per-tree garbage collector, tagged by tree.");

    // --- Retroactive shard-split sweep instruments ----------------

    /// <summary>
    /// Counter of in-flight prepared mutations retroactively
    /// shadow-forwarded from a source shard's leaf chain to the
    /// destination shard at the start of an adaptive split's
    /// <c>BeginShadowWrite</c> phase. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/> (the source shard index). One
    /// increment per <see cref="PendingMutationSnapshot"/> replayed.
    /// </summary>
    public static readonly Counter<long> SplitRetroactiveForwardEntries =
        Meter.CreateCounter<long>("orleans.lattice.split.retroactive_forward.entries", unit: "{entry}",
            description: "Pending prepared mutations retroactively shadow-forwarded at the start of a shard split.");

    /// <summary>
    /// Histogram of the wall-clock duration the split coordinator
    /// spends inside the retroactive shadow-forward sweep before
    /// transitioning to the <c>Drain</c> phase. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/> (the source
    /// shard index).
    /// </summary>
    public static readonly Histogram<double> SplitRetroactiveForwardDuration =
        Meter.CreateHistogram<double>("orleans.lattice.split.retroactive_forward.duration", unit: "ms",
            description: "Wall-clock duration of the retroactive prepared-mutation sweep at shard-split BeginShadowWrite entry.");
}

