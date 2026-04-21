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
///     <b>Shard-level</b> — per-shard read / write / split counters, sourced from
///     <c>ShardRootGrain</c>. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
///   </item>
///   <item>
///     <b>Leaf-level</b> — write-state latency, scan latency, compaction duration,
///     tombstone churn (explicit deletes + TTL expiries reported separately),
///     and leaf-split counters sourced from <c>BPlusLeafGrain</c>. Tagged with
///     <see cref="TagTree"/> (leaf grain ids are too high-cardinality to publish directly).
///   </item>
///   <item>
///     <b>Cache</b> — hit / miss counters from <c>LeafCacheGrain</c>.
///   </item>
///   <item>
///     <b>Saga / coordinator / lifecycle</b> — terminal-state counters for
///     <c>SetManyAtomicAsync</c> sagas, long-running coordinator completions
///     (snapshot / resize / reshard / merge / compaction), and tree-lifecycle
///     transitions (deleted / recovered / purged).
///   </item>
///   <item>
///     <b>Events &amp; configuration</b> — event-publisher health (dispatches
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
    /// Counter of tombstones created — incremented on every <c>DeleteAsync</c>
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

    // --- Cache instruments (LeafCacheGrain) --------------------------------------

    /// <summary>
    /// Counter of cache hits observed by <c>LeafCacheGrain</c> — a key was present
    /// and live in the local cache after (possibly) refreshing the delta.
    /// </summary>
    public static readonly Counter<long> CacheHits =
        Meter.CreateCounter<long>("orleans.lattice.cache.hits", unit: "{hit}",
            description: "LeafCacheGrain reads served by a live, cached entry.");

    /// <summary>
    /// Counter of cache misses observed by <c>LeafCacheGrain</c> — the key was
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
}

