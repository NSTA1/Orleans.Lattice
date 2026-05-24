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
    /// Tag key for the trigger that initiated a tombstone-compaction pass
    /// (e.g. <c>reminder</c>, <c>ratio</c>, <c>size</c>, <c>operator</c>
    /// on <see cref="CompactionPassDuration"/>). The tag is also emitted on
    /// <see cref="LeafCompactionDuration"/>, <see cref="LeafTombstonesReaped"/>,
    /// and <see cref="LeafTombstonesExpired"/> when at least one policy knob
    /// (<c>MinTombstoneRatioForCompaction</c> or
    /// <c>MaxLeafEntriesBeforeForcedCompaction</c>) is non-default; when
    /// every policy knob holds its default the tag is omitted so existing
    /// dashboards that filter on <c>trigger=""</c> keep matching.
    /// </summary>
    public const string TagTrigger = "trigger";

    /// <summary>
    /// Tag key for the path a tombstone-compaction pass took through a
    /// shard's leaves: <c>walk</c> for the legacy chain walk and
    /// <c>dirty-set</c> for the dirty-leaves fast path that consults
    /// the shard-root dirty-leaves snapshot. Emitted on
    /// <see cref="CompactionLeavesVisited"/>.
    /// </summary>
    public const string TagPath = "path";

    /// <summary>
    /// Tag key for a leaf-grain identifier on per-leaf instruments
    /// (e.g. <see cref="LeafTombstoneRatio"/>). Cardinality follows the
    /// same caveats as any per-leaf tag - operators that run very wide
    /// trees should expect to either drop the tag at the OpenTelemetry
    /// view layer or sample it.
    /// </summary>
    public const string TagLeaf = "leaf";

    /// <summary>
    /// Tag key for the storage-provider commit phase
    /// (e.g. <c>phase1</c> = per-batch partition transaction,
    /// <c>phase2</c> = manifest partition transaction). Emitted on
    /// <see cref="ProviderCommitDuration"/> and
    /// <see cref="ProviderRetryExhausted"/>.
    /// </summary>
    public const string TagPhase = "phase";

    /// <summary>
    /// Tag key for an Azure Tables HTTP status string on
    /// <see cref="ProviderRetryExhausted"/>. Cardinality is bounded by
    /// the small set of HTTP status codes the SDK surfaces on the WAL
    /// hot path; an unmapped status reports as <c>unknown</c>.
    /// </summary>
    public const string TagStatus = "status";

    /// <summary>
    /// Tag key for the activation's effective <c>WalPartitions</c>
    /// setting. Emitted on the Phase A WAL / saga instruments so a
    /// single Prometheus / dashboard query can pivot the same metric
    /// stream across the diagnostic attribution sweep
    /// (<c>WalPartitions in {1, 4, 16}</c>). Captured once on grain
    /// activation and reused per record - no per-call allocation.
    /// </summary>
    public const string TagWalPartitions = "wal_partitions";

    /// <summary>
    /// Tag key for the activation's effective <c>WalMaxPendingBatches</c>
    /// setting. Same allocation-free, activation-cached pattern as
    /// <see cref="TagWalPartitions"/>; lets the attribution sweep
    /// distinguish runs that vary the in-flight-flush ceiling.
    /// </summary>
    public const string TagWalMaxPendingBatches = "wal_max_pending_batches";

    /// <summary>
    /// Tag key for the Azure Tables provider's effective
    /// <c>PipelinePhaseTwoCommits</c> setting (the values <c>true</c>
    /// / <c>false</c>). Emitted by the Azure Tables WAL provider on
    /// the Phase A provider instruments so dashboards can pivot the
    /// same series between synchronous and pipelined phase-2 modes.
    /// </summary>
    public const string TagPipelinePhaseTwo = "pipeline_phase2";

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

    // --- WAL append diagnostic instruments (WalShardGrain) ------------------
    //
    // Phase A horizontal-scaling diagnostics. These instruments are *only*
    // emitted on the WAL append hot path; they attribute caller-visible
    // append latency to grain-side queueing (turn wait), batching depth
    // (batch entries / bytes), and storage-provider time (provider
    // duration). They are intentionally split from the leaf-level
    // <see cref="LeafCommitDuration"/> histogram because the leaf grain
    // measures total commit duration *including* the cross-grain RPC to
    // the WAL shard - this set isolates the WAL grain's own contribution.

    /// <summary>
    /// Histogram of per-flush batch entry counts, observed at the point
    /// <c>WalShardGrain</c> hands a pending batch to the storage
    /// provider. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// A flat distribution near <see cref="LatticeOptions.WalMaxBatchEntries"/>
    /// indicates the WAL is batching effectively; a distribution
    /// concentrated near 1 indicates the per-batch caps are never
    /// reached and the in-flight cap is the actual throughput limit.
    /// </summary>
    public static readonly Histogram<int> WalAppendBatchEntries =
        Meter.CreateHistogram<int>("orleans.lattice.wal.append.batch_entries", unit: "{entry}",
            description: "Entry count per WAL grain flush, observed at provider hand-off.");

    /// <summary>
    /// Histogram of per-flush batch payload bytes, observed at the
    /// point <c>WalShardGrain</c> hands a pending batch to the storage
    /// provider. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// </summary>
    public static readonly Histogram<long> WalAppendBatchBytes =
        Meter.CreateHistogram<long>("orleans.lattice.wal.append.batch_bytes", unit: "By",
            description: "Encoded-payload bytes per WAL grain flush, observed at provider hand-off.");

    /// <summary>
    /// Histogram of the in-flight flush count snapshot taken at the
    /// moment <c>WalShardGrain</c> starts a new flush. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>. A distribution
    /// pinned at <c>0</c> means the WAL is fully serialised under the
    /// configured <see cref="LatticeOptions.WalMaxPendingBatches"/>;
    /// non-zero values prove pipelined provider calls.
    /// </summary>
    public static readonly Histogram<int> WalAppendInFlight =
        Meter.CreateHistogram<int>("orleans.lattice.wal.append.in_flight", unit: "{flush}",
            description: "In-flight flush count snapshot taken at the start of a new WAL flush.");

    /// <summary>
    /// Histogram of <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>
    /// wall-clock duration, observed by <c>WalShardGrain</c>. Tagged
    /// with <see cref="TagTree"/> and <see cref="TagShard"/>. This is
    /// the storage-provider's contribution to caller-visible append
    /// latency; subtracting it from <see cref="WalAppendTurnWait"/>
    /// gives the grain-side queueing tax.
    /// </summary>
    public static readonly Histogram<double> WalAppendProviderDuration =
        Meter.CreateHistogram<double>("orleans.lattice.wal.append.provider.duration", unit: "ms",
            description: "Wall-clock duration of IWalStorageProvider.AppendEncodedBatchAsync, observed by WalShardGrain.");

    /// <summary>
    /// Histogram of caller-visible WAL append latency, measured from
    /// the moment <c>AppendAsync</c> / <c>AppendBatchAsync</c> admits
    /// an entry to the moment the corresponding ack TCS completes.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Includes time spent waiting for the in-flight cap to drain,
    /// time spent in the pending batch before cutover, the
    /// provider's <see cref="WalAppendProviderDuration"/>, and any
    /// grain-turn dispatch overhead.
    /// </summary>
    public static readonly Histogram<double> WalAppendTurnWait =
        Meter.CreateHistogram<double>("orleans.lattice.wal.append.turn_wait", unit: "ms",
            description: "Caller-visible WAL append duration (entry admission to ack), observed by WalShardGrain.");

    /// <summary>
    /// Histogram of the pending-segments queue depth observed at the
    /// moment a per-entry <c>AppendAsync</c> call enqueues its
    /// segment. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagShard"/>. The value is <c>_pendingSegments.Count</c>
    /// *after* the new segment has been added, so a value of 1 means
    /// the entry arrived to an empty pending batch.
    /// </summary>
    public static readonly Histogram<int> WalAppendQueueDepth =
        Meter.CreateHistogram<int>("orleans.lattice.wal.append.queue_depth", unit: "{entry}",
            description: "Pending-batch depth observed at the moment a WAL append enqueues its segment.");

    /// <summary>
    /// Histogram of the cross-grain dispatch duration into
    /// <c>IWalShardGrain.AppendAsync</c> / <c>AppendBatchAsync</c>,
    /// observed by <c>WalCommitLogWriter</c>. Clocked around the
    /// awaited grain RPC on the caller side, so the value includes the
    /// Orleans turn-queue wait on the target <c>WalShardGrain</c>
    /// activation, the RPC serialisation overhead, and the WAL grain's
    /// own body time. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagShard"/> (the WAL partition index, identical to
    /// the <c>WalShardGrain</c>'s own shard tag) plus the Phase A
    /// attribution tags <see cref="TagWalPartitions"/> and
    /// <see cref="TagWalMaxPendingBatches"/>.
    /// <para>
    /// Subtracting <see cref="WalAppendTurnWait"/> (the WAL grain's
    /// own self-clock) from this histogram isolates the Orleans
    /// scheduling tax on the single WAL activation per partition: the
    /// time spent in the activation's turn queue plus the RPC
    /// dispatch overhead. Under <c>WalPartitions = 1</c> every leaf
    /// commit funnels through one activation and any commit-path
    /// throughput regression is expected to show up here first.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalShardDispatchDuration =
        Meter.CreateHistogram<double>("orleans.lattice.wal.shard.dispatch.duration", unit: "ms",
            description: "Wall-clock duration of the cross-grain IWalShardGrain.AppendAsync / AppendBatchAsync RPC, observed by WalCommitLogWriter.");

    // --- Storage-provider commit instruments --------------------------------

    /// <summary>
    /// Histogram of storage-provider commit phase duration. Tagged
    /// with <see cref="TagPhase"/> = <c>phase1</c> (per-batch
    /// partition transaction) or <c>phase2</c> (manifest partition
    /// transaction). Emitted by the Azure Table WAL provider; other
    /// providers may emit it too. The phase-2 measurement covers a
    /// single coalesced commit transaction, not the per-shard
    /// worker's whole drain loop.
    /// </summary>
    public static readonly Histogram<double> ProviderCommitDuration =
        Meter.CreateHistogram<double>("orleans.lattice.provider.commit.duration", unit: "ms",
            description: "Storage-provider commit-transaction wall-clock duration, tagged by phase.");

    /// <summary>
    /// Histogram of the number of coalesced phase-2 commits the
    /// per-shard provider worker bundled into a single transaction.
    /// A distribution concentrated near 1 means the worker is never
    /// catching up against backed-up arrivals; values closer to the
    /// 49-commit per-transaction cap indicate the worker is the
    /// shard's effective rate limiter.
    /// </summary>
    public static readonly Histogram<int> ProviderPhase2BatchSize =
        Meter.CreateHistogram<int>("orleans.lattice.provider.phase2.batch_size", unit: "{commit}",
            description: "Coalesced phase-2 commits per provider-worker transaction.");

    /// <summary>
    /// Counter incremented once per provider call whose retry budget
    /// was exhausted and surfaced an exception. Tagged with
    /// <see cref="TagPhase"/> and <see cref="TagStatus"/> (the HTTP
    /// status string the SDK observed, or <c>unknown</c>). A non-zero
    /// rate signals the storage backend is throttling the shard at
    /// its ceiling.
    /// </summary>
    public static readonly Counter<long> ProviderRetryExhausted =
        Meter.CreateCounter<long>("orleans.lattice.provider.retry.exhausted", unit: "{call}",
            description: "Provider commit calls that exhausted the SDK retry budget and surfaced an exception.");

    /// <summary>
    /// Counter incremented once per individual retry attempt the
    /// storage SDK performs on a provider call, regardless of whether
    /// the retry ultimately succeeds. Tagged with <see cref="TagStatus"/>
    /// (the HTTP status string of the response that triggered the
    /// retry, e.g. <c>503</c>, <c>429</c>; <c>0</c> when the trigger
    /// was a transport-level exception with no HTTP status). Phase A
    /// (see <c>scaling.md</c>) discovered a 5-100x gap between wall
    /// p99 (700-1,700 ms) and Azure Tables server-timing p99
    /// (10-130 ms) on the WAL hot path - the canonical signature of
    /// retry storms whose retries ultimately succeed and therefore
    /// never increment <see cref="ProviderRetryExhausted"/>. This
    /// instrument is the counterpart that captures *attempted*
    /// retries so dashboards can attribute wall-time inflation to
    /// SDK backoff without inferring it from the gap.
    /// <para>
    /// Cardinality is intentionally bounded: only the status tag is
    /// emitted (small bounded set of HTTP status codes), not
    /// <see cref="TagTree"/> / <see cref="TagShard"/>. Per-tree /
    /// per-shard attribution is covered by
    /// <see cref="ProviderRetryExhausted"/>, which fires rarely.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ProviderRetryAttempts =
        Meter.CreateCounter<long>("orleans.lattice.provider.retry.attempts", unit: "{attempt}",
            description: "Individual retry attempts performed by the storage SDK on provider calls, tagged by the HTTP status that triggered each retry.");

    // --- Saga fan-out diagnostic instruments (AtomicWriteGrain) -------------

    /// <summary>
    /// Histogram of <c>SetManyAtomicAsync</c> saga entry counts
    /// observed at the moment the saga enters its execute phase
    /// (i.e. once per saga activation, not once per retry). Tagged
    /// with <see cref="TagTree"/>. Distinct from
    /// <see cref="AtomicWriteBatchSize"/>, which is emitted at
    /// terminal transition: this one is emitted at execute-phase
    /// entry so a diagnostic dashboard can correlate fan-out size
    /// with the per-key duration histogram below regardless of
    /// terminal outcome.
    /// </summary>
    public static readonly Histogram<int> SagaFanoutSize =
        Meter.CreateHistogram<int>("orleans.lattice.saga.fanout.size", unit: "{entry}",
            description: "Entry count per atomic-write saga, observed at execute-phase entry.");

    /// <summary>
    /// Histogram of per-key <c>lattice.SetAsync</c> wall-clock
    /// duration inside an atomic-write saga's execute loop. Tagged
    /// with <see cref="TagTree"/>. One observation per successful
    /// or failing key-level await, regardless of whether the saga
    /// later compensates. The 99th-percentile of this histogram is
    /// the dominant signal for whether the saga's serial fan-out
    /// pattern is the throughput limit: it must be added across
    /// all keys to recover the saga's end-to-end duration, so a
    /// 10-entry saga's duration is bounded below by 10 x p50 of
    /// this histogram.
    /// </summary>
    public static readonly Histogram<double> SagaPerKeyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.perkey.duration", unit: "ms",
            description: "Per-key SetAsync duration inside an atomic-write saga execute loop.");

    /// <summary>
    /// Histogram of the wall-clock gap between consecutive per-key
    /// awaits inside an atomic-write saga's execute loop, i.e. the
    /// time the saga spends between one successful key-level commit
    /// and the next key-level await. Tagged with <see cref="TagTree"/>.
    /// Captures the saga-side per-iteration overhead
    /// (<c>WriteStateAsync</c> of the saga checkpoint plus loop
    /// bookkeeping) that the per-key duration histogram does *not*
    /// see. A non-trivial value at p50 here indicates the saga
    /// checkpoint persist is contributing as much to end-to-end
    /// latency as the data writes themselves; near-zero values mean
    /// the saga's overhead is negligible and the per-key duration
    /// is the dominant cost.
    /// </summary>
    public static readonly Histogram<double> SagaWaitSerialGap =
        Meter.CreateHistogram<double>("orleans.lattice.saga.wait.serial_gap", unit: "ms",
            description: "Wall-clock gap between consecutive per-key awaits inside an atomic-write saga.");

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

    // --- Compaction policy instruments (TombstoneCompactionGrain) -----------

    /// <summary>
    /// Histogram of full <c>RunCompactionPassAsync</c> wall-clock duration
    /// recorded by <c>TombstoneCompactionGrain.CompleteCompactionAsync</c>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagTrigger"/>
    /// (<c>reminder</c> for the periodic reminder, <c>ratio</c> /
    /// <c>size</c> for the corresponding policy triggers, or
    /// <c>operator</c> for an explicit <c>RequestCompactionAsync</c>
    /// call). Distinct from <see cref="LeafCompactionDuration"/>, which
    /// is per-leaf.
    /// </summary>
    public static readonly Histogram<double> CompactionPassDuration =
        Meter.CreateHistogram<double>("orleans.lattice.compaction.pass.duration", unit: "ms",
            description: "Full tombstone-compaction pass duration, tagged by tree and trigger.");

    /// <summary>
    /// Counter of leaves visited by a compaction pass, tagged with
    /// <see cref="TagTree"/> and <see cref="TagOutcome"/> = <c>reaped</c>
    /// (the leaf removed at least one tombstone or expired entry),
    /// <c>noop</c> (the leaf short-circuited because nothing has changed
    /// since its last compaction), or <c>skipped</c> (the leaf threw and
    /// the pass advanced past it). Lets operators distinguish work-done
    /// from work-skipped on a single rate panel.
    /// </summary>
    public static readonly Counter<long> CompactionLeavesVisited =
        Meter.CreateCounter<long>("orleans.lattice.compaction.leaves.visited", unit: "{leaf}",
            description: "Leaves visited by a tombstone-compaction pass, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per per-shard retry inside
    /// <c>TombstoneCompactionGrain.ProcessNextShardAsync</c>. Tagged with
    /// <see cref="TagTree"/>. A non-zero rate means at least one shard's
    /// per-leaf compaction call threw and the pass deferred a fresh
    /// attempt within the same activation.
    /// </summary>
    public static readonly Counter<long> CompactionShardRetries =
        Meter.CreateCounter<long>("orleans.lattice.compaction.shard.retries", unit: "{retry}",
            description: "Per-shard compaction retries inside a single pass.");

    /// <summary>
    /// Counter incremented once per shard whose retry budget was
    /// exhausted and whose cursor advanced without a successful
    /// compaction. Tagged with <see cref="TagTree"/>. A persistent
    /// non-zero rate is the operational alert signal that a shard is
    /// consistently failing past <c>MaxRetriesPerShard</c>.
    /// </summary>
    public static readonly Counter<long> CompactionShardSkipped =
        Meter.CreateCounter<long>("orleans.lattice.compaction.shard.skipped", unit: "{shard}",
            description: "Shards whose per-pass retry budget was exhausted, tagged by tree.");

    /// <summary>
    /// Histogram of the dirty-leaf snapshot size pulled from each shard
    /// root at the start of every compaction shard pass. Tagged with
    /// <see cref="TagTree"/>. A value of <c>0</c> means the shard
    /// activated only its shard-root grain on the pass; a non-zero
    /// value reflects the count of leaves the coordinator activated
    /// via the dirty-leaves fast path. Capacity-planning signal for the
    /// "<c>O(shards + dirty_leaves)</c>" pass-cost target.
    /// </summary>
    public static readonly Histogram<int> CompactionShardDirtyLeaves =
        Meter.CreateHistogram<int>("orleans.lattice.compaction.shard.dirty_leaves", unit: "{leaf}",
            description: "Per-shard dirty-leaf snapshot size at compaction shard-pass start.");

    /// <summary>
    /// Histogram of per-leaf tombstone-to-total ratio
    /// (<c>tombstones / max(liveKeys + tombstones, 1)</c>) sampled
    /// inside a tombstone-compaction pass, just before
    /// <c>CompactTombstonesAsync</c> performs its scan. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagLeaf"/>. Surfaces
    /// space-amplification hot spots without requiring an
    /// <c>ObservableGauge</c> over a registry of live activations -
    /// the histogram is observed lazily inside the pass, so it costs
    /// nothing on the read or write hot paths.
    /// </summary>
    public static readonly Histogram<double> LeafTombstoneRatio =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.tombstone.ratio", unit: "{ratio}",
            description: "Per-leaf tombstone-to-total ratio sampled inside compaction passes.");

    // --- Cached constant tag pairs (allocation-free hot-path helpers) -------
    //
    // KeyValuePair<string, object?> is a value type and the corresponding
    // Histogram<T>.Record / Counter<T>.Add overloads with explicit 1/2/3
    // KeyValuePair parameters never allocate. Caching the constant-valued
    // pairs as static-readonly fields elides the per-call struct construction
    // (a handful of CPU cycles) and the dictionary lookup inside the metric
    // handler, and is more readable than inline `new KeyValuePair<...>(name,
    // literal)` repeated dozens of times.

    /// <summary><see cref="TagOutcome"/> = <c>noop</c>.</summary>
    public static readonly KeyValuePair<string, object?> OutcomeNoop = new(TagOutcome, "noop");

    /// <summary><see cref="TagOutcome"/> = <c>reaped</c>.</summary>
    public static readonly KeyValuePair<string, object?> OutcomeReaped = new(TagOutcome, "reaped");

    /// <summary><see cref="TagOutcome"/> = <c>skipped</c>.</summary>
    public static readonly KeyValuePair<string, object?> OutcomeSkipped = new(TagOutcome, "skipped");

    /// <summary><see cref="TagKind"/> = <c>compact</c> (per-leaf WAL-write attribution).</summary>
    public static readonly KeyValuePair<string, object?> KindCompact = new(TagKind, "compact");

    /// <summary><see cref="TagKind"/> = <c>compaction</c> (coordinator-completion attribution).</summary>
    public static readonly KeyValuePair<string, object?> KindCompaction = new(TagKind, "compaction");

    /// <summary><see cref="TagTrigger"/> = <c>reminder</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerReminderTag = new(TagTrigger, "reminder");

    /// <summary><see cref="TagTrigger"/> = <c>ratio</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerRatioTag = new(TagTrigger, "ratio");

    /// <summary><see cref="TagTrigger"/> = <c>size</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerSizeTag = new(TagTrigger, "size");

    /// <summary><see cref="TagTrigger"/> = <c>operator</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerOperatorTag = new(TagTrigger, "operator");

    /// <summary><see cref="TagPath"/> = <c>walk</c> (legacy / fallback leaf-chain walk).</summary>
    public static readonly KeyValuePair<string, object?> PathWalkTag = new(TagPath, "walk");

    /// <summary><see cref="TagPath"/> = <c>dirty-set</c> (dirty-leaves fast path).</summary>
    public static readonly KeyValuePair<string, object?> PathDirtySetTag = new(TagPath, "dirty-set");

    /// <summary>String label for the legacy leaf-chain walk path.</summary>
    public const string PathWalk = "walk";

    /// <summary>String label for the shard-root dirty-leaves fast path.</summary>
    public const string PathDirtySet = "dirty-set";

    /// <summary><see cref="TagPhase"/> = <c>phase1</c>.</summary>
    public static readonly KeyValuePair<string, object?> PhasePhase1Tag = new(TagPhase, "phase1");

    /// <summary><see cref="TagPhase"/> = <c>phase2</c>.</summary>
    public static readonly KeyValuePair<string, object?> PhasePhase2Tag = new(TagPhase, "phase2");
}
