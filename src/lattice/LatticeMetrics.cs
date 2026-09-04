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

    /// <summary>Tag key for the participating-tree count of a cross-tree atomic write.</summary>
    public const string TagTreeCount = "tree_count";

    /// <summary>Tag key for the physical shard index.</summary>
    public const string TagShard = "shard";

    /// <summary>
    /// Tag key for the admission-control quota dimension
    /// (<see cref="DimensionKeys"/> or <see cref="DimensionBytes"/>) on the
    /// <c>orleans.lattice.admission.*</c> instruments.
    /// </summary>
    public const string TagDimension = "dimension";

    /// <summary><see cref="TagDimension"/> = <c>keys</c> (live-key admission dimension).</summary>
    public static readonly KeyValuePair<string, object?> DimensionKeys = new(TagDimension, "keys");

    /// <summary><see cref="TagDimension"/> = <c>bytes</c> (estimated-byte admission dimension).</summary>
    public static readonly KeyValuePair<string, object?> DimensionBytes = new(TagDimension, "bytes");

    /// <summary>
    /// Tag key for the WAL writer partition index. Distinct from
    /// <see cref="TagShard"/>: the writer partition is the producer-side
    /// routing key (one entry-batch per partition per call into
    /// <c>WalCommitLogWriter.AppendForPartitionAsync</c>) and lines up
    /// 1:1 with the destination shard's index, but is reported on the
    /// writer-layer instruments so a future fan-out shape that decouples
    /// the two does not silently overload <see cref="TagShard"/>.
    /// </summary>
    public const string TagPartition = "partition";

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
    /// Tag key for the decision a control loop reached on one observation
    /// pass (e.g. <c>admitted</c>, <c>not_over_split</c>, <c>backpressure</c>
    /// on <see cref="ShardHealingDecisions"/>). Distinct from
    /// <see cref="TagReason"/>, which names only why something was refused:
    /// a decision dimension carries the admitting value too, so the series is
    /// a complete account of every pass rather than of the failures.
    /// </summary>
    public const string TagDecision = "decision";

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
    /// Tag key for the concrete <see cref="IMutationObserver"/> implementation
    /// a measurement is attributed to on <see cref="ObserverDuration"/>. The
    /// value is the observer's CLR type name (<see cref="Type.FullName"/>,
    /// falling back to the short <c>Type.Name</c> for a type that reports
    /// none), matching the identifier the dispatcher already writes into its
    /// swallow-and-log warning. Cardinality is bounded by the number of
    /// observers registered in the silo's DI container, which is a
    /// deployment-time constant.
    /// </summary>
    public const string TagObserver = "observer";

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
    /// Tag key for the sub-stage label inside
    /// <c>ShardRootGrain.AppendTxTerminalAsync</c>. Emitted on
    /// <see cref="SagaBroadcastShardStageDuration"/>. Values:
    /// <c>resolve</c> (step 1 affected-leaves resolution),
    /// <c>hlc</c> (step 2 <c>ComputeTerminalHlcAsync</c> fan-out + tick),
    /// <c>wal</c> (step 3 commit-log adapter append; absent when no
    /// adapter is registered), and <c>fanout</c> (step 4 per-leaf
    /// <c>ApplyTxTerminalAsync</c> dispatch + shadow-forward).
    /// </summary>
    public const string TagStage = "stage";

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

    /// <summary>
    /// Counter incremented once per write <b>operation</b> observed by a shard root.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is a per-<b>operation</b> counter, not a per-record one. A batched or
    /// bulk operation - <c>SetManyAsync</c>, <c>MergeManyAsync</c>,
    /// <c>SetManyWherePredicateAsync</c>, <c>DeleteRangeAsync</c>,
    /// <c>BulkLoadAsync</c>, <c>BulkLoadRawAsync</c>, <c>BulkAppendAsync</c> -
    /// contributes exactly <b>one</b> increment regardless of how many entries it
    /// carries, so a 5000-record bulk import ticks this counter on the order of
    /// (shards touched x bulk operations), not 5000.
    /// </para>
    /// <para>
    /// Use <see cref="ShardRecordsWritten"/> when you need the record rate. Plotting
    /// this instrument as "write throughput" on a batch-heavy or bulk-ingesting
    /// estate under-represents the real volume; plot both, or label this one
    /// explicitly as operations per second.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> ShardWrites =
        Meter.CreateCounter<long>("orleans.lattice.shard.writes", unit: "{op}",
            description: "Write operations served by a shard root (SetAsync, DeleteAsync, MergeManyAsync, BulkLoadAsync, etc.). One increment per operation: a batched or bulk operation counts once regardless of entry count - see orleans.lattice.shard.records_written for the per-record rate.");

    /// <summary>
    /// Counter incremented by the number of individual <b>records</b> each write
    /// operation carried - the per-record companion to <see cref="ShardWrites"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A single-key write (<c>SetAsync</c>, <c>DeleteAsync</c>,
    /// <c>SetIfVersionAsync</c>, <c>GetOrSetAsync</c>, <c>ApplyCrdtDeltaAsync</c>)
    /// contributes 1. A batched or bulk operation contributes its entry count, so a
    /// 5000-record bulk import contributes 5000 here while
    /// <see cref="ShardWrites"/> advances by only the number of bulk operations.
    /// Together the two make both the operation rate and the record rate
    /// observable, and their ratio is the effective batch size.
    /// </para>
    /// <para>
    /// For the two operations whose affected-record count is only known once the
    /// operation completes - <c>DeleteRangeAsync</c> and
    /// <c>SetManyWherePredicateAsync</c> - the increment is the number of records
    /// actually tombstoned or matched, and is published after the operation
    /// succeeds. Every other path publishes the entry count it was handed. A write
    /// that throws before completing contributes to <see cref="ShardWrites"/> but
    /// not here.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> ShardRecordsWritten =
        Meter.CreateCounter<long>("orleans.lattice.shard.records_written", unit: "{record}",
            description: "Individual records written by a shard root, incremented by the entry count of each write operation (1 for a single-key write, the batch size for SetManyAsync / MergeManyAsync / bulk load, the affected count for DeleteRangeAsync / SetManyWherePredicateAsync). The per-record companion to orleans.lattice.shard.writes.");

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
    /// Counter incremented once for every leaf-side projection-digest decision
    /// point, tagged with <see cref="TagTree"/> and <see cref="TagPath"/>:
    /// <list type="bullet">
    ///   <item><description><c>coalesced_scheduled</c> - the leaf scheduled a
    ///   fresh one-shot timer (first dirty mutation inside a new coalescing
    ///   window). Future <c>coalesced_skipped</c> increments share its
    ///   eventual <c>coalesced_fired</c> publish.</description></item>
    ///   <item><description><c>coalesced_skipped</c> - a dirty mutation arrived
    ///   while a coalesced publish was already scheduled, so the
    ///   cross-grain hop was deferred onto the existing window. This is the
    ///   "publishes saved" surface that justifies the coalescing default.</description></item>
    ///   <item><description><c>coalesced_fired</c> - the coalescing timer
    ///   tick issued the cross-grain
    ///   <c>OnChildDigestPublishedAsync</c> RPC to the parent. One per
    ///   window per leaf (unless an inline publish or a graceful flush
    ///   cancelled the timer first).</description></item>
    ///   <item><description><c>inline</c> - the leaf issued the cross-grain
    ///   publish synchronously (either because <c>DigestCoalescingWindowMs</c>
    ///   is zero, the timer registration failed in a test harness, or the
    ///   call came from a structural caller via
    ///   <c>PublishDigestUpwardInlineAsync</c>).</description></item>
    ///   <item><description><c>deactivation_flush</c> - the leaf's graceful
    ///   <c>OnDeactivateAsync</c> drained a pending coalesced publish before
    ///   the activation tore down.</description></item>
    /// </list>
    /// <para>
    /// The headline operational invariant the coalescing path was designed
    /// for is "N writes inside one window produce one cross-grain hop". That
    /// translates to <c>coalesced_scheduled + coalesced_fired</c> per window
    /// regardless of write count, with <c>coalesced_skipped</c> absorbing
    /// the remaining N-1 dirtying mutations.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafDigestPublishes =
        Meter.CreateCounter<long>("orleans.lattice.leaf.digest.publishes", unit: "{publish}",
            description: "Leaf-side projection-digest publish decisions, partitioned by path (coalesced scheduling, skip, fire, inline, deactivation flush).");

    /// <summary><see cref="TagPath"/> = <c>coalesced_scheduled</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathCoalescedScheduledTag = new(TagPath, "coalesced_scheduled");

    /// <summary><see cref="TagPath"/> = <c>coalesced_skipped</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathCoalescedSkippedTag = new(TagPath, "coalesced_skipped");

    /// <summary><see cref="TagPath"/> = <c>coalesced_fired</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathCoalescedFiredTag = new(TagPath, "coalesced_fired");

    /// <summary><see cref="TagPath"/> = <c>inline</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathInlineTag = new(TagPath, "inline");

    /// <summary><see cref="TagPath"/> = <c>deactivation_flush</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathDeactivationFlushTag = new(TagPath, "deactivation_flush");

    /// <summary>
    /// Counter incremented once per adaptive shard-split commit, fired from
    /// <c>TreeShardSplitGrain.FinaliseAsync</c> immediately after the shard
    /// map swap succeeds.
    /// </summary>
    public static readonly Counter<long> ShardSplitsCommitted =
        Meter.CreateCounter<long>("orleans.lattice.shard.splits_committed", unit: "{split}",
            description: "Adaptive shard-split commits (ShardMap swap published).");

    /// <summary>
    /// Counter incremented once per online shard-consolidation commit, fired
    /// from <c>TreeShardConsolidationGrain.FinaliseAsync</c> immediately after
    /// the terminal state write succeeds, so an increment always corresponds to
    /// a durably-committed fold.
    /// <para>
    /// The exact inverse of <see cref="ShardSplitsCommitted"/>, and the metric
    /// that proves a tree an over-eager splitter shattered is actually being
    /// healed: plotted together, a sustained gap between the two is a tree
    /// whose physical shard count is still climbing. Tagged
    /// <see cref="TagShard"/> with the <em>donor</em> shard index - the shard
    /// being retired from the routing map.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardConsolidationsCommitted =
        Meter.CreateCounter<long>("orleans.lattice.shard.consolidations_committed", unit: "{consolidation}",
            description: "Online shard-consolidation commits (donor shard retired from the ShardMap).");

    /// <summary>
    /// Per-tree count of physical shards above the tree's configured base
    /// shard count, sampled once every healing-orchestrator sweep. The healing
    /// <em>work outstanding</em>: how many folds separate the tree from its
    /// intended shape.
    /// <para>
    /// A tree is healed exactly when this reaches zero, so "trees healed" is
    /// read off this instrument directly (<c>count</c> of series at zero) with
    /// no second instrument to keep consistent. Plotted alongside
    /// <see cref="ShardConsolidationsCommitted"/> - the reclaimed-shard rate -
    /// it answers both halves of the question: how much damage is left, and is
    /// it going down. Tagged <see cref="TagTree"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> ShardHealingBacklog =
        Meter.CreateHistogram<int>("orleans.lattice.shard.healing.backlog", unit: "{shard}",
            description: "Per-tree physical shards above the configured base count, sampled every healing sweep.");

    /// <summary>
    /// Counter incremented exactly once per healing-orchestrator sweep with
    /// the decision that sweep reached, tagged <see cref="TagTree"/> and
    /// <see cref="TagDecision"/>.
    /// <para>
    /// The series whose rate is currently non-zero for a tree <em>is</em> that
    /// tree's current healing decision, so an operator can tell a tree that
    /// needs no healing (<c>not_over_split</c>) from one that needs healing and
    /// is being held back (<c>skewed_load</c>, <c>backpressure</c>,
    /// <c>cooldown</c>, <c>split_in_flight</c>, <c>at_capacity</c>) from one
    /// where the mechanism is off (<c>disabled</c>, <c>admission_closed</c>).
    /// Without it, a tree that never heals is indistinguishable from a tree
    /// that never needed to.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardHealingDecisions =
        Meter.CreateCounter<long>("orleans.lattice.shard.healing.decisions", unit: "{decision}",
            description: "Healing-orchestrator sweeps by decision (one increment per tree per sweep).");

    /// <summary><see cref="TagDecision"/> = <c>admitted</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingAdmittedDecisionTag = new(TagDecision, "admitted");

    /// <summary><see cref="TagDecision"/> = <c>disabled</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingDisabledDecisionTag = new(TagDecision, "disabled");

    /// <summary><see cref="TagDecision"/> = <c>admission_closed</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingAdmissionClosedDecisionTag = new(TagDecision, "admission_closed");

    /// <summary><see cref="TagDecision"/> = <c>not_over_split</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingNotOverSplitDecisionTag = new(TagDecision, "not_over_split");

    /// <summary><see cref="TagDecision"/> = <c>skewed_load</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingSkewedLoadDecisionTag = new(TagDecision, "skewed_load");

    /// <summary><see cref="TagDecision"/> = <c>split_in_flight</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingSplitInFlightDecisionTag = new(TagDecision, "split_in_flight");

    /// <summary><see cref="TagDecision"/> = <c>tree_maintenance</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingTreeMaintenanceDecisionTag = new(TagDecision, "tree_maintenance");

    /// <summary><see cref="TagDecision"/> = <c>cooldown</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingCooldownDecisionTag = new(TagDecision, "cooldown");

    /// <summary><see cref="TagDecision"/> = <c>backpressure</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingBackpressureDecisionTag = new(TagDecision, "backpressure");

    /// <summary><see cref="TagDecision"/> = <c>at_capacity</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingAtCapacityDecisionTag = new(TagDecision, "at_capacity");

    /// <summary><see cref="TagDecision"/> = <c>no_foldable_pair</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingNoFoldablePairDecisionTag = new(TagDecision, "no_foldable_pair");

    /// <summary><see cref="TagDecision"/> = <c>not_observed</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingNotObservedDecisionTag = new(TagDecision, "not_observed");

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

    /// <summary>
    /// Histogram of the in-flight commit-count snapshot taken at the
    /// moment a <c>BPlusLeafGrain</c> commit (either <c>CommitSetAsync</c>
    /// or <c>CommitSetManyAsync</c>) enters the commit path. The
    /// recorded value is the number of commits already in flight on the
    /// same leaf activation at the entry instant (i.e. zero on the very
    /// first concurrent commit, one on the second, and so on). Tagged
    /// with <see cref="TagTree"/> so operators can plot leaf-side
    /// commit concurrency per tree.
    /// <para>
    /// Under the default Orleans non-reentrant grain scheduling - the
    /// shipping shape of <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetAsync(string, byte[])"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/>, neither marked
    /// <c>[AlwaysInterleave]</c> - this histogram pins at <c>0</c>: the
    /// next commit cannot enter until the current one has returned. A
    /// non-zero quantile therefore signals one of two things:
    /// (i) a future change has applied <c>[AlwaysInterleave]</c> to the
    /// leaf-side commit entrypoint and disjoint-key sub-batches now
    /// overlap on the same leaf activation, or
    /// (ii) a reentrant-by-design code path (saga terminal write under
    /// commit-log scope) routed back through the commit-set path while
    /// an outer commit was still awaiting a WAL append.
    /// Either reading is informative for the U9m / leaf-side-commit-concurrency
    /// probe: a steady pin at <c>0</c> falsifies
    /// the leaf turn-queue hypothesis and routes the next probe to
    /// WAL-side fan-in (U9n); a steady lift above <c>0</c> identifies
    /// the leaf grain as the binding constraint.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> LeafCommitInFlight =
        Meter.CreateHistogram<int>("orleans.lattice.leaf.commit.in_flight", unit: "{commit}",
            description: "In-flight commit count snapshot at the moment a BPlusLeafGrain commit enters the commit path.");

    // --- Warm-up instruments (ILattice.WarmUpAsync) ------------------------------

    /// <summary>
    /// Counter of completed <see cref="Orleans.Lattice.ILattice.WarmUpAsync"/>
    /// invocations observed on this silo. Tagged with <see cref="TagTree"/> so
    /// operators can confirm per-tree warm-up fired exactly once before the
    /// first hot-path write. A zero value on a tree whose first
    /// <c>SetManyAsync</c> coincides with steady-state means warm-up was
    /// skipped and the cold-start placement-directory storm landed against
    /// producer-driven flush concurrency.
    /// </summary>
    public static readonly Counter<long> WarmUpInvocations =
        Meter.CreateCounter<long>("orleans.lattice.warmup.invocations", unit: "{call}",
            description: "Completed ILattice.WarmUpAsync calls observed on this silo.");

    /// <summary>
    /// Histogram of <see cref="Orleans.Lattice.ILattice.WarmUpAsync"/>
    /// wall-clock duration, in milliseconds. One observation per call,
    /// covering routing resolution plus every bounded-concurrency per-shard
    /// probe round-trip. Tagged with <see cref="TagTree"/> and <c>shard_count</c>
    /// so the per-tree warm-start cost is attributable in phase-A scrapes.
    /// Useful as the headline "did warm-up actually fire and how long did it
    /// take" signal alongside <see cref="WarmUpInvocations"/>.
    /// </summary>
    public static readonly Histogram<double> WarmUpDurationMs =
        Meter.CreateHistogram<double>("orleans.lattice.warmup.duration", unit: "ms",
            description: "Wall-clock duration of ILattice.WarmUpAsync including all per-shard probes.");

    /// <summary>
    /// Counter of leaf caches successfully primed by a shard root's opt-in
    /// post-restart pre-warm (<c>LatticeOptions.LeafCachePreWarmCount</c>).
    /// Tagged with <see cref="TagTree"/>, <c>shard</c>, and the tenant label.
    /// Stays at zero while the feature is disabled, which is the default. A
    /// value materially below the configured pre-warm count means individual
    /// priming calls are failing - each failure is swallowed by design, so this
    /// counter is the only signal that they happened.
    /// </summary>
    public static readonly Counter<long> LeafCachePreWarmed =
        Meter.CreateCounter<long>("orleans.lattice.warmup.leaf_cache.prewarmed", unit: "{leaf}",
            description: "Leaf caches successfully primed by a shard root's post-restart pre-warm.");

    /// <summary>
    /// Histogram of the wall-clock duration, in milliseconds, of a shard root's
    /// leaf-cache pre-warm fan-out. One observation per shard per warm-up when
    /// the feature is enabled and the access model ranked at least one leaf.
    /// Read alongside <see cref="WarmUpDurationMs"/> to attribute how much of a
    /// tree's warm-up cost is leaf priming.
    /// </summary>
    public static readonly Histogram<double> LeafCachePreWarmDurationMs =
        Meter.CreateHistogram<double>("orleans.lattice.warmup.leaf_cache.duration", unit: "ms",
            description: "Wall-clock duration of a shard root's leaf-cache pre-warm fan-out.");

    /// <summary>
    /// Histogram of the number of leaves resident in a shard root's leaf-access
    /// histogram, observed each time the model is persisted. Bounded above by
    /// the model's own tracked-leaf cap, so a distribution pinned at that cap
    /// means the shard's read set is wider than the model can represent and the
    /// ranking is being drawn from a pruned view.
    /// </summary>
    public static readonly Histogram<int> LeafAccessModelLeaves =
        Meter.CreateHistogram<int>("orleans.lattice.leaf_access.model.leaves", unit: "{leaf}",
            description: "Leaves resident in a shard root's leaf-access histogram at persist time.");

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
    /// <c>failed</c> (post-compensation surrogate failure), or <c>shutdown_refused</c>
    /// (the saga's batched dispatch tripped the writer-side drain refusal
    /// because the silo is shutting down; the saga short-circuited the retry loop
    /// and the compensate-broadcast pass and surfaced
    /// <see cref="LatticeShuttingDownException"/> to the caller without persisting
    /// post-detection state).
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
    /// = <c>committed</c>, <c>compensated</c>, <c>failed</c>, or
    /// <c>shutdown_refused</c> so operators can plot rollback-path latency
    /// separately from happy-path latency, and shutdown-coincidence sagas
    /// separately from genuine commit-conflict sagas.
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
    /// <see cref="TagOutcome"/> = <c>committed</c>, <c>compensated</c>,
    /// <c>failed</c>, or <c>shutdown_refused</c>. Lets operators interpret
    /// <see cref="AtomicWriteDuration"/>
    /// in context - a 10-entry batch and a 1000-entry batch both appear as one
    /// data point on the duration histogram, and only the batch-size histogram
    /// disambiguates them.
    /// </summary>
    public static readonly Histogram<int> AtomicWriteBatchSize =
        Meter.CreateHistogram<int>("orleans.lattice.atomic_write.batch_size", unit: "{entry}",
            description: "Entry count of each SetManyAtomicAsync saga, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per terminal completion of a <b>cross-tree</b>
    /// atomic write (the coordinator grain), tagged with <see cref="TagOutcome"/>
    /// = <c>committed</c> or <c>precondition_failed</c> / <c>aborted</c> and a
    /// <see cref="TagTreeCount"/> bucket. Distinguishes multi-tree saga volume
    /// from the single-tree <see cref="AtomicWriteCompleted"/> stream.
    /// </summary>
    public static readonly Counter<long> CrossTreeAtomicWriteCompleted =
        Meter.CreateCounter<long>("orleans.lattice.atomic_write.cross_tree.completed", unit: "{saga}",
            description: "Terminal transitions of cross-tree atomic-write sagas, tagged by outcome and tree count.");

    /// <summary>
    /// Histogram of end-to-end cross-tree atomic-write coordinator latency,
    /// measured from first submit to the terminal phase. Tagged with
    /// <see cref="TagOutcome"/> so commit-path and abort-path latency can be
    /// plotted separately.
    /// </summary>
    public static readonly Histogram<double> CrossTreeAtomicWriteDuration =
        Meter.CreateHistogram<double>("orleans.lattice.atomic_write.cross_tree.duration", unit: "ms",
            description: "End-to-end cross-tree atomic-write coordinator duration, tagged by outcome.");

    /// <summary>
    /// Histogram of the number of participating trees in each cross-tree atomic
    /// write, recorded once per terminal completion next to
    /// <see cref="CrossTreeAtomicWriteCompleted"/>. Lets operators interpret
    /// <see cref="CrossTreeAtomicWriteDuration"/> relative to fan-out width.
    /// </summary>
    public static readonly Histogram<int> CrossTreeAtomicWriteParticipants =
        Meter.CreateHistogram<int>("orleans.lattice.atomic_write.cross_tree.participants", unit: "{tree}",
            description: "Participating-tree count of each cross-tree atomic-write saga, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per <c>AcquireAsync</c> / <c>TryAcquireAsync</c>
    /// terminal outcome on <c>LatticeLockGrain</c>, tagged with
    /// <see cref="TagOutcome"/> = <c>granted</c> (the caller received the lease),
    /// <c>timeout</c> (the FIFO wait elapsed before a grant), or
    /// <c>unavailable</c> (a non-blocking <c>TryAcquireAsync</c> found the lock
    /// held). Lets operators watch lock contention as the ratio of non-granted to
    /// granted outcomes.
    /// </summary>
    public static readonly Counter<long> LockAcquired =
        Meter.CreateCounter<long>("orleans.lattice.lock.acquired", unit: "{acquire}",
            description: "Distributed-lock acquire outcomes (granted / timeout / unavailable), tagged by outcome.");

    /// <summary>
    /// Counter incremented once per honoured <c>ReleaseAsync</c> on
    /// <c>LatticeLockGrain</c> - a release presenting the current holder's fencing
    /// token that actually freed the lock. A stale-token release is a no-op and is
    /// not counted here.
    /// </summary>
    public static readonly Counter<long> LockReleased =
        Meter.CreateCounter<long>("orleans.lattice.lock.released", unit: "{release}",
            description: "Distributed-lock releases that freed the lock under the current holder's fencing token.");

    /// <summary>
    /// Counter incremented once per lease reclamation on <c>LatticeLockGrain</c> -
    /// a holder whose lease expired without a renew or release, whose lock was
    /// reclaimed (and handed to the next FIFO waiter, if any). A sustained non-zero
    /// rate indicates holders crashing or pausing past their lease duration, the
    /// exact condition the fencing token protects downstream resources against.
    /// </summary>
    public static readonly Counter<long> LockLeaseReclaimed =
        Meter.CreateCounter<long>("orleans.lattice.lock.lease_reclaimed", unit: "{lease}",
            description: "Distributed-lock leases reclaimed after expiry without renew or release.");

    /// <summary>
    /// Histogram of the wall-clock time a granted acquire spent waiting in the
    /// FIFO queue, recorded once per <c>granted</c> outcome on
    /// <c>LatticeLockGrain</c> (zero for an uncontended immediate grant). Lets
    /// operators plot lock-wait latency percentiles distinctly from the
    /// granted/timeout counts on <see cref="LockAcquired"/>.
    /// </summary>
    public static readonly Histogram<double> LockAcquireWait =
        Meter.CreateHistogram<double>("orleans.lattice.lock.acquire.wait", unit: "ms",
            description: "Time a granted distributed-lock acquire spent waiting in the FIFO queue.");

    /// <summary>
    /// Counter incremented once per terminal transition of an <c>AtomicActionGrain</c>
    /// saga (the generic atomic-action / TCC coordinator). Tagged with
    /// <see cref="TagOutcome"/> = <c>committed</c> (every forward step committed),
    /// <c>compensated</c> (a forward step faulted and every committed step was
    /// rolled back in reverse order), or <c>compensation_failed</c> (a compensating
    /// effect itself faulted, so the saga parked for operator intervention). Lets
    /// operators watch the rollback and parked-saga rates as fractions of total
    /// saga volume.
    /// </summary>
    public static readonly Counter<long> AtomicActionCompleted =
        Meter.CreateCounter<long>("orleans.lattice.atomic_action.completed", unit: "{saga}",
            description: "Terminal transitions of atomic-action (saga / TCC) coordinators, tagged by outcome.");

    /// <summary>
    /// Histogram of end-to-end atomic-action saga durations, recorded once per
    /// terminal transition of an <c>AtomicActionGrain</c> next to
    /// <see cref="AtomicActionCompleted"/>. The duration is measured from the
    /// wall-clock time the saga first started (persisted on the saga state so it
    /// survives a silo crash) to the time it reached its terminal outcome. Tagged
    /// with <see cref="TagOutcome"/> = <c>committed</c>, <c>compensated</c>, or
    /// <c>compensation_failed</c> so operators can plot rollback-path latency
    /// separately from happy-path latency.
    /// </summary>
    public static readonly Histogram<double> AtomicActionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.atomic_action.duration", unit: "ms",
            description: "End-to-end atomic-action (saga / TCC) coordinator duration, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per step effect an <c>AtomicActionGrain</c> saga
    /// runs. Tagged with <see cref="TagPhase"/> = <c>forward</c> (a forward effect
    /// committed) or <c>compensate</c> (a compensating effect committed), and
    /// <see cref="TagOutcome"/> = <c>ok</c> (the effect succeeded) or <c>fault</c>
    /// (the effect threw). Lets operators watch the compensation rate and per-phase
    /// fault rate at step granularity, below the per-saga
    /// <see cref="AtomicActionCompleted"/> stream.
    /// </summary>
    public static readonly Counter<long> AtomicActionStep =
        Meter.CreateCounter<long>("orleans.lattice.atomic_action.step", unit: "{step}",
            description: "Atomic-action saga step effects, tagged by phase (forward / compensate) and outcome (ok / fault).");

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

    // --- Mutation-observer instruments (MutationObserverDispatcher) ----------

    /// <summary>
    /// Histogram of the wall-clock time one registered
    /// <see cref="IMutationObserver"/> spent inside a single
    /// <c>OnMutationAsync</c> callback. Observers run <em>inline</em> on the
    /// grain write path, so every millisecond recorded here is a millisecond
    /// added to the caller's write latency - this instrument attributes that
    /// cost to the specific observer that incurred it, on the same pipeline as
    /// the traffic it slows down.
    /// <para>
    /// Tagged with <see cref="TagObserver"/> (the observer's CLR type name) and
    /// <see cref="TagTree"/> (the mutated tree). Recorded on the faulting path
    /// too: an observer that throws slowly is exactly the misbehaviour this
    /// instrument exists to surface, and the dispatcher still suppresses the
    /// exception. The sample spans only the callback - the dispatcher's own
    /// swallow-and-log work is excluded, so a slow log sink cannot be
    /// mistaken for a slow observer.
    /// </para>
    /// <para>
    /// Zero-cost when unused. The dispatcher's no-observer fast path returns
    /// before any timing work, and the remaining timestamp capture is elided
    /// when <see cref="Instrument.Enabled"/> is <c>false</c> - so a caller who
    /// registers an observer but attaches no metrics listener pays one boolean
    /// read per publish.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> ObserverDuration =
        Meter.CreateHistogram<double>("orleans.lattice.observer.duration", unit: "ms",
            description: "Inline duration of one IMutationObserver callback on the write path, tagged by observer type and tree.");

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

    /// <summary>
    /// Counter of WAL garbage-collection passes the per-silo scheduler drove for a
    /// tree, tagged with <see cref="TagTree"/> and <see cref="TagOutcome"/>
    /// (<see cref="OutcomeReclaimed"/> when the pass trimmed at least one entry,
    /// <see cref="OutcomeIdle"/> when it found nothing above the trim floor, and
    /// <see cref="OutcomeFailed"/> when the pass threw). Pairing the reclaimed rate
    /// against the total pass rate gives the per-tree reclaim rate, and the failed
    /// rate isolates a wedged tree without needing to read the scheduler's logs.
    /// </summary>
    public static readonly Counter<long> WalGcPasses =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.passes", unit: "{pass}",
            description: "WAL garbage-collection passes driven by the per-silo scheduler, tagged by tree and outcome.");

    /// <summary>
    /// Histogram of the adaptive WAL garbage-collection cadence, in seconds: the
    /// interval the scheduler selected for a tree after its most recent pass,
    /// tagged with <see cref="TagTree"/>. The value moves inside the configured
    /// band <c>[<see cref="LatticeOptions.WalGcMinInterval"/>,
    /// <see cref="LatticeOptions.WalGcInterval"/>]</c>, so a sustained reading at
    /// the floor is a tree whose log is growing faster than one pass reclaims and
    /// a reading at the ceiling is a quiet tree. This is the instrument that shows
    /// the cadence responding to backlog.
    /// </summary>
    public static readonly Histogram<double> WalGcInterval =
        Meter.CreateHistogram<double>("orleans.lattice.wal.gc.interval", unit: "s",
            description: "Adaptive WAL garbage-collection interval selected per tree, tagged by tree.");

    /// <summary>
    /// Histogram of retained WAL bytes remaining after a garbage-collection pass,
    /// tagged with <see cref="TagTree"/>. Sampled from the pass's own
    /// <see cref="LatticeWalGcReport.RetainedBytesAfter"/>, so it costs no extra
    /// I/O. Read against <see cref="WalGcInterval"/> it answers the operational
    /// question this instrument pair exists for: is the backlog falling, and is
    /// the cadence tightening while it does.
    /// <para>
    /// <b>Byte accounting is a capability, and its absence is knowable rather
    /// than silent.</b> The series exists for a tree only when the byte-pressure
    /// policy is enabled (<see cref="LatticeOptions.WalMaxRetainedBytes"/> is
    /// set) <i>and</i> the configured <see cref="IWalStorageProvider"/> reports a
    /// retained byte size. <see cref="WalGcPasses"/> is emitted unconditionally
    /// for every pass, so a tree that is reporting passes but no backlog bytes is
    /// positively identifying a host without byte accounting - the two series are
    /// read together, and no consumer has to distinguish "no backlog" from "not
    /// measured". Reclaimed volume in that configuration is still observable in
    /// records through <see cref="WalEntriesTrimmed"/> and
    /// <see cref="OutcomeReclaimed"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalGcBacklogBytes =
        Meter.CreateHistogram<long>("orleans.lattice.wal.gc.backlog_bytes", unit: "By",
            description: "Retained WAL bytes remaining after a garbage-collection pass, tagged by tree.");

    /// <summary><see cref="TagOutcome"/> = <c>reclaimed</c> (a WAL GC pass that trimmed at least one entry).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeReclaimed = new(TagOutcome, "reclaimed");

    /// <summary><see cref="TagOutcome"/> = <c>idle</c> (a WAL GC pass that found nothing above the trim floor).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeIdle = new(TagOutcome, "idle");

    /// <summary><see cref="TagOutcome"/> = <c>failed</c> (a WAL GC pass that threw).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeFailed = new(TagOutcome, "failed");

    // --- Leaf-materialiser durable pin instruments (issue #1030) ------------

    /// <summary>
    /// Counter of durable writes to the leaf-materialiser pin store, emitted by
    /// <c>WalMaterialiserPinGrain</c> on every <c>WriteStateAsync</c>. Tagged
    /// with <see cref="TagOutcome"/> = <c>birth</c> (a synchronous through-write
    /// seeded by a new leaf's block pin) or <c>coalesced</c> (a debounced flush
    /// draining one or more advancing reports). The pre-#1030 shape wrote once
    /// per advancing report through a single per-tree grain; coalescing collapses
    /// a report burst to one write per shard per flush window, so a sustained
    /// <c>coalesced</c> rate far below the report rate confirms the fan-in
    /// hotspot fix is engaged.
    /// </summary>
    public static readonly Counter<long> MaterialiserPinDurableWrites =
        Meter.CreateCounter<long>("orleans.lattice.materialiser.pin.durable_writes", unit: "{write}",
            description: "Durable writes to the leaf-materialiser pin store, tagged by birth/coalesced outcome.");

    /// <summary>
    /// Histogram of leaf-materialiser drain lag, in milliseconds, sampled by the
    /// per-tree WAL GC pass as <c>now - slowest durable materialiser checkpoint</c>.
    /// A rising drain lag is the back-pressure signal (issue #1030): when it
    /// exceeds <see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/>
    /// for <see cref="LatticeOptions.WalSaturationMaterialiserLagSampleWindows"/>
    /// consecutive passes the tree escalates to Saturated. Tagged with
    /// <see cref="TagTree"/>. Only emitted when a non-zero materialiser frontier
    /// exists (a never-checkpointed tree produces no measurement).
    /// </summary>
    public static readonly Histogram<double> MaterialiserDrainLag =
        Meter.CreateHistogram<double>("orleans.lattice.materialiser.drain_lag", unit: "ms",
            description: "Leaf-materialiser drain lag sampled by the WAL GC pass, tagged by tree.");

    /// <summary>
    /// Counter of activation-time leaf materialiser replays started, emitted by
    /// <c>BPlusLeafGrain.OnActivateAsync</c> once a per-silo replay permit
    /// (<see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/>) is
    /// acquired. Tagged with <see cref="TagTree"/>. A reactivation storm (issue
    /// #1030) shows as a spike in this counter; pairing it with
    /// <see cref="LeafReplayDuration"/> reveals whether the per-silo concurrency
    /// ceiling is queueing replays under load.
    /// </summary>
    public static readonly Counter<long> LeafActivationReplays =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation_replays", unit: "{replay}",
            description: "Activation-time leaf materialiser replays started, tagged by tree.");

    /// <summary>
    /// Counter of activation-time leaf materialiser replays that ran <b>beyond</b>
    /// the configured <see cref="LatticeOptions.MaxLeafReplayEntries"/> budget (or
    /// past <see cref="LatticeOptions.LeafProjectionRetention"/>) while the
    /// write-ahead log still covered the whole needed window. Tagged with
    /// <see cref="TagTree"/>.
    /// <para>
    /// These replays converge correctly - they are simply longer than the budget
    /// anticipated - so the condition is a capacity signal, not a fault. A tree
    /// that trips this persistently is checkpointing too slowly relative to its
    /// write rate: raise the budget, shorten the materialiser checkpoint cadence
    /// (<see cref="LatticeOptions.MaterialiserCheckpointInterval"/> /
    /// <see cref="LatticeOptions.MaterialiserCheckpointEntries"/>), or accept the
    /// longer activation. Before issue #1738 this condition was fatal and bricked
    /// the tree, so this counter also measures how often that would have fired.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafActivationOverBudgetReplays =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation_replays_over_budget", unit: "{replay}",
            description: "Activation-time leaf replays that exceeded the configured replay budget with an intact WAL, tagged by tree.");

    /// <summary>
    /// Counter of activation-time eager cursor-publish failures, emitted by
    /// <c>BPlusLeafGrain.OnActivateAsync</c> when the post-replay cursor report
    /// throws (a non-fatal failure the next foreground flush recovers from).
    /// Under a reactivation storm against a saturated silo (issue #1030) these
    /// failures fan out across every reactivating leaf; the warning log is
    /// rate-limited per silo to avoid a self-amplifying log flood, but this
    /// counter records every occurrence so the true rate stays observable.
    /// Tagged with <see cref="TagTree"/>.
    /// </summary>
    public static readonly Counter<long> LeafActivationCursorPublishFailures =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation_cursor_publish_failures", unit: "{failure}",
            description: "Activation-time eager cursor-publish failures, tagged by tree.");

    // --- Storage-usage instruments (byte-accurate retained footprint) ------
    //
    // The four byte gauges and the over-threshold gauge are observable gauges
    // registered lazily by LatticeStorageUsageMetrics (so they cost nothing
    // when no listener is attached and are not created at all when the host
    // never wires the storage-usage singleton). Their canonical names are
    // exposed here as `...Name` constants so the dashboards drift-guard test
    // recognises the PromQL token forms even though the instruments are not
    // statically constructed on this meter. The two policy counters are
    // ordinary counters constructed on the meter below.

    /// <summary>Canonical name of the observable gauge reporting per-tree retained WAL bytes (tagged <see cref="TagTree"/>).</summary>
    public const string StorageWalBytesName = "orleans.lattice.storage.wal_bytes";

    /// <summary>Canonical name of the observable gauge reporting per-tree snapshot blob bytes (tagged <see cref="TagTree"/>).</summary>
    public const string StorageSnapshotBytesName = "orleans.lattice.storage.snapshot_bytes";

    /// <summary>Canonical name of the observable gauge reporting per-tree summed leaf/shard-root state bytes (tagged <see cref="TagTree"/>).</summary>
    public const string StorageLeafStateBytesName = "orleans.lattice.storage.leaf_state_bytes";

    /// <summary>Canonical name of the observable gauge reporting the per-tree sum of the three storage surfaces (tagged <see cref="TagTree"/>).</summary>
    public const string StorageTotalBytesName = "orleans.lattice.storage.total_bytes";

    /// <summary>Canonical name of the observable 0/1 gauge that flags a tree whose retained WAL bytes currently breach the advisory ceiling (tagged <see cref="TagTree"/>).</summary>
    public const string StoragePolicyOverThresholdName = "orleans.lattice.storage.policy.over_threshold";

    /// <summary>
    /// Counter incremented once per <see cref="ILatticeWalGc.RunOnceAsync"/>
    /// pass that observes a tree's pre-trim retained WAL bytes exceeding the
    /// configured advisory ceiling
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>) and therefore
    /// schedules a byte-pressure trim, tagged with <see cref="TagTree"/> and
    /// <see cref="TagReason"/> (<c>byte_pressure</c>). The trim itself never
    /// crosses the safe frontier; this counter records that the policy acted,
    /// not that the ceiling was met. Not emitted when the policy is disabled
    /// or the WAL provider does not support byte accounting.
    /// </summary>
    public static readonly Counter<long> StoragePolicyTrimTriggered =
        Meter.CreateCounter<long>(StoragePolicyTrimTriggeredName, unit: "{trim}",
            description: "Byte-pressure trim passes scheduled by the advisory storage policy, tagged by tree and reason.");

    /// <summary>Canonical name of <see cref="StoragePolicyTrimTriggered"/>.</summary>
    public const string StoragePolicyTrimTriggeredName = "orleans.lattice.storage.policy.trim_triggered";

    /// <summary>
    /// Counter of WAL bytes freed by a byte-pressure-triggered trim pass
    /// (pre-trim retained bytes minus post-trim retained bytes), tagged with
    /// <see cref="TagTree"/>. Emitted alongside
    /// <see cref="StoragePolicyTrimTriggered"/>; zero-reclaim passes (a
    /// lagging consumer pinned every byte) do not emit so a perpetually
    /// over-ceiling tree with no caught-up consumer produces no reclaim
    /// traffic.
    /// </summary>
    public static readonly Counter<long> StoragePolicyBytesReclaimed =
        Meter.CreateCounter<long>(StoragePolicyBytesReclaimedName, unit: "By",
            description: "WAL bytes freed by byte-pressure-triggered trim passes, tagged by tree.");

    /// <summary>Canonical name of <see cref="StoragePolicyBytesReclaimed"/>.</summary>
    public const string StoragePolicyBytesReclaimedName = "orleans.lattice.storage.policy.bytes_reclaimed";

    /// <summary><see cref="TagReason"/> = <c>byte_pressure</c> (advisory storage-policy trim attribution).</summary>
    public static readonly KeyValuePair<string, object?> ReasonBytePressure = new(TagReason, "byte_pressure");

    // --- Per-tree admission-control instruments ----------------------------
    //
    // Four observable gauges (live_keys, estimated_bytes, over_advisory,
    // utilization) are registered lazily by LatticeAdmissionMetrics - they read
    // the cached per-tree admission aggregate on scrape and cost nothing when no
    // listener is attached, exactly like the storage gauges - so their canonical
    // names are exposed here as `...Name` constants for the dashboard drift
    // guard. The two would_reject / rejected counters are ordinary counters
    // constructed on the meter below. All are tagged with `tree`; the utilisation
    // gauge and the two counters additionally carry the low-cardinality
    // `dimension` = keys | bytes tag.

    /// <summary>Canonical name of the observable gauge reporting a tree's current live (non-tombstone) key count (tagged <see cref="TagTree"/>).</summary>
    public const string AdmissionLiveKeysName = "orleans.lattice.admission.live_keys";

    /// <summary>Canonical name of the observable gauge reporting a tree's current estimated retained bytes (tagged <see cref="TagTree"/>). May alias <see cref="StorageTotalBytesName"/>.</summary>
    public const string AdmissionEstimatedBytesName = "orleans.lattice.admission.estimated_bytes";

    /// <summary>Canonical name of the observable 0/1 gauge that flags a tree currently exceeding its advisory admission ceiling (tagged <see cref="TagTree"/>).</summary>
    public const string AdmissionOverAdvisoryName = "orleans.lattice.admission.over_advisory";

    /// <summary>Canonical name of the observable ratio gauge reporting current / ceiling per <see cref="TagDimension"/> (tagged <see cref="TagTree"/>).</summary>
    public const string AdmissionUtilizationName = "orleans.lattice.admission.utilization";

    /// <summary>
    /// Counter incremented once per write that <i>would</i> have been rejected
    /// at a tree's advisory admission ceiling (the dry-run blast radius of a
    /// candidate cap), tagged with <see cref="TagTree"/> and
    /// <see cref="TagDimension"/>. Never rejects a write; pairs with
    /// <see cref="AdmissionOverAdvisoryName"/> to right-size a cap before
    /// enforcement is enabled.
    /// </summary>
    public static readonly Counter<long> AdmissionWouldReject =
        Meter.CreateCounter<long>(AdmissionWouldRejectName, unit: "{write}",
            description: "Writes that would be rejected at the advisory admission ceiling, tagged by tree and dimension.");

    /// <summary>Canonical name of <see cref="AdmissionWouldReject"/>.</summary>
    public const string AdmissionWouldRejectName = "orleans.lattice.admission.would_reject";

    /// <summary>
    /// Counter incremented once per write actually rejected by an enforced
    /// admission cap (<see cref="LatticeQuotaExceededException"/> thrown), tagged
    /// with <see cref="TagTree"/> and <see cref="TagDimension"/>. Confirms
    /// enforcement is live and surfaces the offending tree(s).
    /// </summary>
    public static readonly Counter<long> AdmissionRejected =
        Meter.CreateCounter<long>(AdmissionRejectedName, unit: "{write}",
            description: "Writes rejected by an enforced admission cap, tagged by tree and dimension.");

    /// <summary>Canonical name of <see cref="AdmissionRejected"/>.</summary>
    public const string AdmissionRejectedName = "orleans.lattice.admission.rejected";

    // --- WAL compression-savings instruments (per-row payload compression) --
    //
    // Three ordinary counters constructed on the meter, emitted once per
    // append batch by a WAL provider that compresses entry payloads (e.g.
    // the Azure Table provider's default-on Zstd path). The savings ratio is
    // derived in the dashboard as 1 - stored/uncompressed; exposing two
    // monotonic byte totals (rather than an observable savings gauge) means
    // no staleness-horizon handling and the totals survive activation churn.

    /// <summary>
    /// Counter of pre-compression encoded WAL payload bytes a provider
    /// attempted to store, summed per append batch and tagged with
    /// <see cref="TagTree"/>. Paired with <see cref="StorageWalStoredBytes"/>:
    /// the compression savings ratio for a tree is
    /// <c>1 - stored_bytes / uncompressed_bytes</c>. Counts the encoded
    /// length regardless of whether compression was applied, so a tree whose
    /// payloads all skip compression reports equal uncompressed and stored
    /// totals.
    /// </summary>
    public static readonly Counter<long> StorageWalUncompressedBytes =
        Meter.CreateCounter<long>(StorageWalUncompressedBytesName, unit: "By",
            description: "Pre-compression encoded WAL payload bytes, summed per append batch and tagged by tree.");

    /// <summary>Canonical name of <see cref="StorageWalUncompressedBytes"/>.</summary>
    public const string StorageWalUncompressedBytesName = "orleans.lattice.storage.wal.uncompressed_bytes";

    /// <summary>
    /// Counter of post-compression WAL payload bytes a provider actually
    /// stored, summed per append batch and tagged with <see cref="TagTree"/>.
    /// When a row skips compression (disabled, below the size threshold, or
    /// caught by the inflation guard) the verbatim length is counted, so this
    /// total never exceeds <see cref="StorageWalUncompressedBytes"/> for the
    /// same tree.
    /// </summary>
    public static readonly Counter<long> StorageWalStoredBytes =
        Meter.CreateCounter<long>(StorageWalStoredBytesName, unit: "By",
            description: "Post-compression stored WAL payload bytes, summed per append batch and tagged by tree.");

    /// <summary>Canonical name of <see cref="StorageWalStoredBytes"/>.</summary>
    public const string StorageWalStoredBytesName = "orleans.lattice.storage.wal.stored_bytes";

    /// <summary>
    /// Counter of WAL rows stored verbatim instead of compressed, tagged with
    /// <see cref="TagTree"/> and <see cref="TagReason"/>
    /// (<c>below_threshold</c>, <c>inflation_guard</c>, or <c>disabled</c>).
    /// Lets a dashboard attribute a low savings ratio to the dominant skip
    /// cause so an operator can tune
    /// <see cref="LatticeOptions"/>-adjacent provider thresholds rather than
    /// guess. Rows that were actually compressed do not increment this
    /// counter.
    /// </summary>
    public static readonly Counter<long> StorageWalCompressionSkipped =
        Meter.CreateCounter<long>(StorageWalCompressionSkippedName, unit: "{row}",
            description: "WAL rows stored verbatim instead of compressed, tagged by tree and skip reason.");

    /// <summary>Canonical name of <see cref="StorageWalCompressionSkipped"/>.</summary>
    public const string StorageWalCompressionSkippedName = "orleans.lattice.storage.wal.compression_skipped";

    /// <summary><see cref="TagReason"/> = <c>below_threshold</c> (payload shorter than the provider's compression size threshold).</summary>
    public static readonly KeyValuePair<string, object?> ReasonBelowThreshold = new(TagReason, "below_threshold");

    /// <summary><see cref="TagReason"/> = <c>inflation_guard</c> (compressing did not shrink the payload, so it was stored verbatim).</summary>
    public static readonly KeyValuePair<string, object?> ReasonInflationGuard = new(TagReason, "inflation_guard");

    /// <summary><see cref="TagReason"/> = <c>disabled</c> (compression is not enabled on the provider).</summary>
    public static readonly KeyValuePair<string, object?> ReasonCompressionDisabled = new(TagReason, "disabled");

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

    /// <summary>
    /// Histogram of the per-dispatch entry count handed to
    /// <c>IWalShardGrain.AppendAsync</c> / <c>AppendBatchAsync</c>,
    /// observed by <c>WalCommitLogWriter</c> at the caller side.
    /// Tagged with <see cref="TagTree"/>, <see cref="TagShard"/>
    /// (the WAL partition index), and the Phase A attribution tags
    /// <see cref="TagWalPartitions"/> and
    /// <see cref="TagWalMaxPendingBatches"/>. The single-entry
    /// overload records <c>1</c>; the batched overload records the
    /// per-partition slice size that <c>AppendForPartitionAsync</c>
    /// forwards as one <c>AppendBatchAsync</c> call.
    /// <para>
    /// Pair with <see cref="WalAppendBatchEntries"/> (the WAL grain's
    /// observed per-flush packing) to detect a missing
    /// cross-AppendBatchAsync coalescing window: if the writer-side
    /// dispatch entry count equals the WAL grain's per-flush packing
    /// under steady-state fan-in, each leaf's dispatch flushes as its
    /// own batch and concurrent leaves never merge into a single
    /// pending batch (the <c>WalShardGrain</c> kick predicate
    /// <c>isLast == true</c> triggers a flush at the end of every
    /// caller's batch).
    /// </para>
    /// </summary>
    public static readonly Histogram<int> WalShardDispatchEntries =
        Meter.CreateHistogram<int>("orleans.lattice.wal.shard.dispatch.entries", unit: "{entry}",
            description: "Per-dispatch entry count handed to IWalShardGrain.AppendAsync / AppendBatchAsync, observed by WalCommitLogWriter.");

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
    /// Counter incremented once per phase-2 manifest commit that
    /// exceeded the per-commit deadline and was abandoned by the
    /// per-shard worker. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagShard"/>. A non-zero rate is the direct signal
    /// that the phase-2 drain loop would otherwise have wedged: the
    /// commit's underlying Azure Tables transaction stopped making
    /// progress (a hung socket, a server-side partition stall, or an
    /// SDK retry storm running past the deadline) and the worker
    /// bounded it instead of blocking every later commit on the same
    /// shard indefinitely. Zero on a healthy shard; the pre-fix
    /// behaviour (no deadline) is recoverable by leaving
    /// <c>PhaseTwoCommitTimeout</c> unset, in which case this counter
    /// never increments because no deadline is enforced.
    /// </summary>
    public static readonly Counter<long> ProviderPhase2CommitTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.provider.phase2.commit.timeouts", unit: "{commit}",
            description: "Phase-2 manifest commits abandoned by the per-shard worker after exceeding the configured per-commit deadline.");

    /// <summary>
    /// Counter incremented once per individual retry attempt the
    /// storage SDK performs on a provider call, regardless of whether
    /// the retry ultimately succeeds. Tagged with <see cref="TagStatus"/>
    /// (the HTTP status string of the response that triggered the
    /// retry, e.g. <c>503</c>, <c>429</c>; <c>0</c> when the trigger
    /// was a transport-level exception with no HTTP status). Phase A
    /// discovered a 5-100x gap between wall
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

    /// <summary>
    /// Counter of SDK retry attempts that
    /// <c>SaturationAwareRetryPolicy</c> short-circuited by stamping a
    /// synthetic 503 response onto the message because the silo-scoped
    /// WAL saturation signal reports
    /// <see cref="WalSaturationState.Saturated"/>. Fires only on the
    /// post-saturation-classifier path; first attempts and retries
    /// under <see cref="WalSaturationState.Healthy"/> /
    /// <see cref="WalSaturationState.Throttled"/> never fire it.
    /// Tagged by the synthetic status (<c>503</c> today; reserved for
    /// future expansion).
    /// <para>
    /// Closes the diagnostic gap that the existing
    /// <see cref="ProviderRetryAttempts"/> counter does not
    /// distinguish SDK-driven retries (a transient transport hiccup,
    /// a 429 throttle) from policy-driven short-circuits (the
    /// saturation signal abandoning the retry). A non-zero rate on
    /// this counter is the operator-visible signal that the
    /// saturation policy is doing its job; a zero rate with a
    /// non-zero <see cref="ProviderRetryExhausted"/> means the SDK
    /// burned its full retry budget without the policy intervening,
    /// which is the pre-policy historical behaviour.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ProviderRetryShortCircuited =
        Meter.CreateCounter<long>("orleans.lattice.provider.retry.short_circuited", unit: "{attempt}",
            description: "SDK retry attempts short-circuited by SaturationAwareRetryPolicy because the WAL saturation signal reports Saturated, tagged by the synthetic HTTP status used to abort the retry chain.");

    /// <summary>
    /// Counter incremented once per provider commit call that surfaced a
    /// <c>409 EntityAlreadyExists</c> conflict which was proven to be an
    /// idempotent replay of an already-durable write and therefore
    /// resolved as a success rather than a failure. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagShard"/> and
    /// <see cref="TagPhase"/>. Fires when the storage SDK's retry
    /// pipeline resends a batch whose first attempt committed
    /// server-side but whose response was lost; the provider reads the
    /// resident rows back, confirms they are byte-identical to the
    /// batch it tried to write, and treats the conflict as a no-op
    /// success. A non-zero rate is the operator-visible signal that
    /// lost-response retries are occurring (typically under CPU /
    /// network pressure); crucially these replays are NOT counted on
    /// <see cref="ProviderRetryExhausted"/>, so they never escalate the
    /// WAL saturation classifier. A 409 that is NOT a clean replay (a
    /// genuine offset collision) still surfaces as a hard failure on
    /// <see cref="ProviderRetryExhausted"/> and never increments this
    /// counter.
    /// </summary>
    public static readonly Counter<long> ProviderIdempotentReplays =
        Meter.CreateCounter<long>("orleans.lattice.provider.idempotent_replays", unit: "{call}",
            description: "Provider commit calls whose 409 EntityAlreadyExists conflict was proven to be an idempotent replay of an already-durable write and resolved as a success.");

    /// <summary>
    /// Counts phase-1 commit attempts that the provider re-issued in place after a
    /// <i>transient</i> fault (a timeout, a 408 / 429 / 5xx, or a network-level cancellation
    /// that is not the silo's own drain token). Each retry resubmits the <b>byte-identical</b>
    /// batch at the same offsets, so it never asks the calling <c>WalShardGrain</c> to fault,
    /// resync, and re-drive divergent content - the positive-feedback 409 conflict storm this
    /// counter's retry path exists to prevent. A retry that lands on an already-durable batch
    /// resolves via the idempotent-replay proof (and increments
    /// <see cref="ProviderIdempotentReplays"/>); one that lands on a never-committed batch
    /// simply commits. A non-zero value means the provider absorbed transient phase-1 turbulence
    /// without escalating it to the shard. When the bounded retry budget is exhausted the fault
    /// surfaces on <see cref="ProviderRetryExhausted"/> as before.
    /// </summary>
    public static readonly Counter<long> ProviderPhaseOneTransientRetries =
        Meter.CreateCounter<long>("orleans.lattice.provider.phase1.transient_retries", unit: "{attempt}",
            description: "Phase-1 commit attempts the provider re-issued in place after a transient fault, resubmitting the byte-identical batch at the same offsets rather than faulting the calling WAL shard.");

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

    /// <summary>
    /// Histogram of wall-clock ms spent inside the saga's prepare
    /// phase: from the start of <c>ExecutePhaseAsync</c>'s parallel
    /// batched <c>lattice.SetManyAsync(slice)</c> dispatch to the
    /// moment every per-shard fan-out completes (post-D1c shape -
    /// a single parallel call rather than a per-key loop). Excludes
    /// the saga checkpoint persist that follows the dispatch.
    /// Tagged with <see cref="TagTree"/> and the per-tree WAL
    /// partition count tag.
    /// <para>
    /// Sums with <see cref="SagaTerminalDecisionDuration"/> and
    /// <see cref="SagaBroadcastDuration"/> to approximate the saga's
    /// end-to-end <c>SetManyAtomicAsync</c> p50 (the residue is
    /// saga-checkpoint persist + grain-RPC framing on the public
    /// surface, both negligible at the c2-iii operating point).
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaPrepareDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.prepare.duration", unit: "ms",
            description: "Wall-clock ms inside the saga's parallel-prepare phase (lattice.SetManyAsync(slice) dispatch through per-shard fan-out completion).");

    /// <summary>
    /// Histogram of wall-clock ms spent inside the saga's terminal
    /// decision write: the per-tree
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.MarkCommittedAsync"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.MarkAbortedAsync"/> call that
    /// records the single tree-wide linearization point before the
    /// per-leaf terminal fan-out. Tagged with <see cref="TagTree"/>
    /// and the per-tree WAL partition count tag.
    /// <para>
    /// Per the c2-xv routing memo this is the lowest-prior candidate
    /// for the saga's binding constraint (one grain RPC per saga) but
    /// is instrumented so the attribution is conclusive rather than
    /// inferred.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaTerminalDecisionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.terminal_decision.duration", unit: "ms",
            description: "Wall-clock ms inside the per-tree TxRegistry MarkCommittedAsync / MarkAbortedAsync call.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside the saga's broadcast
    /// terminal phase: from the start of <c>BroadcastTerminalsAsync</c>'s
    /// per-shard fan-out (one <c>IShardRootGrain.AppendTxTerminalAsync</c>
    /// per touched shard, dispatched via <c>Task.WhenAll</c>) to the
    /// moment every per-shard terminal has been appended and the
    /// leaf-side pending-tx buckets drained into the visible
    /// projection. Tagged with <see cref="TagTree"/> and the per-tree
    /// WAL partition count tag.
    /// <para>
    /// Per the c2-xv routing memo this is the highest-prior candidate
    /// for the saga's binding constraint. Each per-shard
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.AppendTxTerminalAsync"/> appends one
    /// WAL record and drains the leaf-side pending-tx bucket; if
    /// per-shard turn-token contention or per-shard WAL-append
    /// serialisation dominates, the histogram's p50 is the per-saga
    /// floor regardless of how parallel the prepare phase is.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.duration", unit: "ms",
            description: "Wall-clock ms inside BroadcastTerminalsAsync's per-shard AppendTxTerminalAsync fan-out.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single
    /// <c>state.WriteStateAsync</c> call on the saga grain
    /// (<c>AtomicWriteGrain</c>). The per-call <see cref="TagPhase"/>
    /// tag identifies which checkpoint site the observation came from
    /// (e.g. <c>prepare</c>, <c>execute-batch-commit</c>,
    /// <c>complete</c>) so dashboards can decompose the per-saga
    /// checkpoint cost across the grain's ~10 distinct persist sites
    /// without joining across instruments.
    /// <para>
    /// Closes the c2-xvi residual-cost attribution gap: the sum of
    /// the three saga-phase histograms
    /// (<see cref="SagaPrepareDuration"/>,
    /// <see cref="SagaTerminalDecisionDuration"/>,
    /// <see cref="SagaBroadcastDuration"/>) accounted for ~1.4s of
    /// the c2-xi-measured 7.7s per-saga p50; the residual ~6.3s
    /// lives in saga-internal state persists which this histogram
    /// attributes. Tagged with <see cref="TagTree"/>,
    /// <see cref="TagWalPartitions"/>, and <see cref="TagPhase"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaCheckpointDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.checkpoint.duration", unit: "ms",
            description: "Wall-clock ms inside a single state.WriteStateAsync on AtomicWriteGrain, tagged with the call-site phase.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside Orleans reminder
    /// registry calls on the saga grain
    /// (<c>AtomicWriteGrain.RegisterKeepaliveAsync</c> and
    /// <c>UnregisterKeepaliveAsync</c>). Each call is an Azure Tables
    /// transaction against the reminder table (one
    /// <c>RegisterOrUpdateReminder</c> at saga entry, one
    /// <c>GetReminder</c> + <c>UnregisterReminder</c> at saga
    /// completion). The per-call <see cref="TagPhase"/> tag
    /// distinguishes the call site (<c>register</c> /
    /// <c>unregister-get</c> / <c>unregister-drop</c>).
    /// <para>
    /// Closes the c2-xvi/c2-xvii unattributed-residual gap: the
    /// c2-xvi-measured sum of phases (~1.4s) plus the c2-xvii-measured
    /// checkpoint persists (~52ms) left ~6.9s of the c2-xi 7.7s saga
    /// p50 unattributed. Reminder I/O is the most plausible
    /// contributor and was not previously instrumented. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagWalPartitions"/>, and
    /// <see cref="TagPhase"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaReminderDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.reminder.duration", unit: "ms",
            description: "Wall-clock ms inside Orleans reminder registry RPCs on AtomicWriteGrain, tagged with the call-site phase.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single
    /// <c>ShardRootGrain.AppendTxTerminalAsync</c> call: the full
    /// per-shard cost of broadcasting one saga's terminal mark, from
    /// the start of step 1 (affected-leaves resolution) through step 4
    /// (per-leaf <c>ApplyTxTerminalAsync</c> fan-out). Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// <para>
    /// Sums (in parallel-Task.WhenAll-fashion) inside the saga grain's
    /// <see cref="SagaBroadcastDuration"/> - the saga p50 of ~880ms is
    /// the max across ~32 parallel shard calls; this histogram surfaces
    /// the per-shard contribution so the broadcast-cost attribution
    /// gap left open by c2-xvii can be closed. Per the c2-xix routing
    /// memo this is the next instrument target before any structural
    /// optimisation of the broadcast path.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastShardDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.shard.duration", unit: "ms",
            description: "Wall-clock ms inside a single ShardRootGrain.AppendTxTerminalAsync call (per-shard broadcast contribution).");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single per-leaf
    /// <c>IBPlusLeafGrain.ApplyTxTerminalAsync</c> RPC dispatched from
    /// <c>ShardRootGrain.BroadcastTerminalToLeavesAsync</c> (step 4
    /// of the per-shard broadcast). Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>.
    /// <para>
    /// The shard-side broadcast fan-out is a single
    /// <c>Task.WhenAll</c> across ~1-2 affected leaves per shard, so
    /// the shard duration is approximately the max of its per-leaf
    /// durations. The gap between
    /// <see cref="SagaBroadcastShardDuration"/> p50 and
    /// <see cref="SagaBroadcastLeafDuration"/> p50 attributes the
    /// non-leaf cost on the shard (affected-leaves resolution, HLC
    /// compute, optional WAL append, the parallel-dispatch scheduler
    /// overhead). Per the c2-xix routing memo.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastLeafDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.leaf.duration", unit: "ms",
            description: "Wall-clock ms inside a single per-leaf ApplyTxTerminalAsync RPC dispatched from the shard's terminal broadcast.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single sub-stage of
    /// <c>ShardRootGrain.AppendTxTerminalAsync</c>. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagShard"/>, and
    /// <see cref="TagStage"/> (<c>resolve</c> | <c>hlc</c> | <c>wal</c>
    /// | <c>fanout</c>).
    /// <para>
    /// Per the c2-xxi memo the c2-xx <see cref="SagaBroadcastShardDuration"/>
    /// p50 of ~143ms could not be attributed to leaf-side turn-token
    /// queueing (<c>[AlwaysInterleave]</c> on <c>GetClockAsync</c> did
    /// not move per-shard p50 down). The four sub-stage spans here
    /// split the per-shard envelope into its constituent pieces so the
    /// dominant cost can be identified before any further structural
    /// attempt.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastShardStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.shard.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (resolve|hlc|wal|fanout) of ShardRootGrain.AppendTxTerminalAsync.");

    // --- Shard-root SetManyAsync split instruments ----------------

    /// <summary>
    /// Histogram of wall-clock ms spent inside the local-apply path
    /// of <c>ShardRootGrain.SetManyAsync</c>: from the moment the
    /// shard-root receives a batch to the moment every per-leaf
    /// <c>IBPlusLeafGrain.SetManyAsync</c> dispatched by
    /// <c>SetManyLocalOnlyAsync</c> has returned. Tagged with
    /// <see cref="TagTree"/>. Includes per-leaf RPC scheduling, leaf
    /// turn-queue wait, leaf commit, WAL append, and the WAL provider's
    /// phase-2 commit. Excludes the lattice-grain's per-shard bucket
    /// build and event publish, and excludes the online-resize
    /// shadow-forward task (measured separately by
    /// <see cref="ShardRootSetManyShadowForwardDuration"/>).
    /// </summary>
    public static readonly Histogram<double> ShardRootSetManyLocalApplyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.shard_root.set_many.local_apply.duration", unit: "ms",
            description: "Wall-clock ms inside ShardRootGrain.SetManyLocalOnlyAsync (per-leaf fan-out, leaf commit, WAL append + phase 2).");

    /// <summary>
    /// Histogram of wall-clock ms spent awaiting the trailing
    /// shadow-forward task in <c>ShardRootGrain.SetManyAsync</c>.
    /// Tagged with <see cref="TagTree"/>. Expected to be near zero in
    /// steady state (no active resize): the shadow-forward task
    /// completes synchronously via <c>TrackShadowForward</c>'s
    /// no-resize fast-path. Material values indicate either an active
    /// online resize or an unexpected wait on the resize tracker.
    /// </summary>
    public static readonly Histogram<double> ShardRootSetManyShadowForwardDuration =
        Meter.CreateHistogram<double>("orleans.lattice.shard_root.set_many.shadow_forward.duration", unit: "ms",
            description: "Wall-clock ms awaiting the shadow-forward task at the tail of ShardRootGrain.SetManyAsync.");

    /// <summary>
    /// Count of outbound shard-to-shard write forwards that were abandoned
    /// because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.ShardForwardTimeout"/>.
    /// Tagged with <see cref="TagTree"/>. A non-zero value indicates a
    /// forward parked against a sibling shard whose ownership was changing
    /// during a reshard swap - the parked forward was faulted as a
    /// <see cref="TimeoutException"/> so the foreground write pipeline could
    /// make forward progress and the operation be retried against refreshed
    /// routing. Expected to be zero in steady state; sustained non-zero
    /// counts during a resize indicate the swap phase is taking longer than
    /// the configured forward deadline.
    /// </summary>
    public static readonly Counter<long> ShardForwardTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.forward.timeouts", unit: "{timeout}",
            description: "Count of outbound shard-to-shard write forwards abandoned after exceeding ShardForwardTimeout.");

    /// <summary>
    /// Count of <c>ShardRootGrain</c> activation-readiness seeds that were
    /// abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.ActivationReadyTimeout"/>.
    /// Tagged with <see cref="TagTree"/>. A non-zero value indicates a
    /// first-activation seed (registry registration or root-leaf
    /// initialization) parked - typically because a startup reshard or
    /// membership change left the target activation not-yet-visible - and
    /// was faulted as a <see cref="TimeoutException"/> so the held
    /// activation gate could release and the foreground write pipeline make
    /// progress, with the seed retried against refreshed routing. Expected
    /// to be zero in steady state; sustained non-zero counts during silo
    /// startup or a reshard indicate the seed envelope is exceeding the
    /// configured deadline.
    /// </summary>
    public static readonly Counter<long> ActivationReadyTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.activation_ready.timeouts", unit: "{timeout}",
            description: "Count of shard-root activation-readiness seeds abandoned after exceeding ActivationReadyTimeout.");

    /// <summary>
    /// Count of <c>ShardRootGrain</c> range-scan page fills abandoned because
    /// they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.MaxScanPageStallDuration"/>.
    /// Tagged with <see cref="TagTree"/>, <see cref="TagShard"/> and
    /// <see cref="TagPhase"/>, the last naming how far the page fill had got -
    /// <c>prologue</c>, <c>descent</c>, or <c>leaf-walk</c> - which is what
    /// distinguishes a slow shard prepare from a single leaf read that never
    /// returned.
    /// <para>
    /// Expected to be flat zero: the cooperative
    /// <see cref="Orleans.Lattice.LatticeOptions.MaxScanPageDuration"/> budget
    /// returns a partial page long before this ceiling, so a non-zero rate
    /// means a page fill was stuck inside a single await and was holding its
    /// deliberately non-reentrant shard root against every other request to
    /// that shard (issue 2002). Treat sustained non-zero as a wedge, and read
    /// the phase tag to place it.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanPageStalls =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.scan_page.stalls", unit: "{stall}",
            description: "Count of shard-root range-scan page fills abandoned after exceeding MaxScanPageStallDuration.");

    /// <summary>
    /// Count of internal-node digest publishes (the upward
    /// <c>ChildDigestSnapshot</c> propagation from a <c>BPlusInternalGrain</c>
    /// to its parent) that were abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.DigestPublishTimeout"/>.
    /// Tagged with <see cref="TagTree"/>. A non-zero value indicates a
    /// publish parked against a parent internal node that was mid-mutation -
    /// the parked publish was faulted as a <see cref="TimeoutException"/> so
    /// the holding turn released the non-reentrant split gate rather than
    /// pinning it indefinitely. The digest is staleness-tolerant, so the next
    /// mutation's publish re-drives convergence; sustained non-zero counts
    /// indicate a contended internal-node chain worth investigating.
    /// </summary>
    public static readonly Counter<long> DigestPublishTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.internal.digest_publish.timeouts", unit: "{timeout}",
            description: "Count of internal-node upward digest publishes abandoned after exceeding DigestPublishTimeout.");

    /// <summary>
    /// Count of outbound <c>IWalShardGrain</c> dispatches
    /// (<c>WalCommitLogWriter.AppendForPartitionAsync</c> /
    /// <c>AppendAsync</c>) that were abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.WalAppendDispatchTimeout"/>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// The dispatch is the writer-side cross-grain RPC into the per-shard
    /// WAL grain; it was historically unbounded on the writer side, so a
    /// wedged shard activation would hold every caller's dispatch parked
    /// until the Orleans response deadline (default 3 minutes) expired.
    /// A non-zero value attributes the wedge to a specific
    /// <c>(tree, shard)</c> pair in O(<see cref="Orleans.Lattice.LatticeOptions.WalAppendDispatchTimeout"/>)
    /// time rather than O(response timeout) time, and the parked dispatch
    /// is faulted as a <see cref="TimeoutException"/> so the request
    /// pipeline releases its slot rather than back-filling behind the
    /// wedge. Sustained non-zero counts on a specific
    /// <c>(tree, shard)</c> identify the wedged shard for follow-up
    /// investigation.
    /// </summary>
    public static readonly Counter<long> WalAppendDispatchTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.wal.append_dispatch.timeouts", unit: "{timeout}",
            description: "Count of writer-side WAL shard dispatches abandoned after exceeding WalAppendDispatchTimeout.");

    /// <summary>
    /// Count of per-shard WAL <c>FlushAsync</c> preflight regions (the
    /// synchronous setup and initial scheduler yield that precede the
    /// bounded provider call) that were abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.WalFlushPreflightTimeout"/>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// The preflight region is normally microseconds; a non-zero count
    /// indicates the activation's grain scheduler did not resume the
    /// flush's post-yield continuation within the deadline, leaving the
    /// in-flight slot pinned with no provider-call deadline armed (the
    /// existing <see cref="Orleans.Lattice.LatticeOptions.WalFlushTimeout"/>
    /// only covers the provider call itself, which has not yet been
    /// issued). The faulted preflight surfaces as a
    /// <see cref="TimeoutException"/> routed through the normal failure
    /// handler, the slot drains, and this counter attributes the trip to
    /// the affected <c>(tree, shard)</c>. Sustained non-zero counts
    /// indicate the activation's scheduler is being held by a startup
    /// reshard / membership change, a non-cooperative work item, or a
    /// mid-flush activation tear-down.
    /// </summary>
    public static readonly Counter<long> WalFlushPreflightTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.wal.flush.preflight.timeouts", unit: "{timeout}",
            description: "Count of WAL shard FlushAsync preflight regions abandoned after exceeding WalFlushPreflightTimeout.");

    /// <summary>
    /// Histogram of <c>_inFlight.Count</c> observed when a per-shard
    /// <c>WalShardGrain</c> activation is being deactivated. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>. Recorded exactly
    /// once per <c>OnDeactivateAsync</c> call. A zero observation is the
    /// healthy steady-state shape (the grain drained cleanly); a non-zero
    /// observation means the activation was torn down with in-flight
    /// flushes still pending - the slot population that defines the
    /// post-#568 residual phase-1/activation wedge fingerprint. Combined
    /// with <see cref="WalFlushPreflightTimeouts"/>, a deactivation with
    /// non-zero in-flight count immediately followed by a preflight
    /// timeout on a successor activation is the smoking gun for the
    /// "mid-call deactivation orphan" hypothesis.
    /// </summary>
    public static readonly Histogram<long> WalShardDeactivateInFlight =
        Meter.CreateHistogram<long>("orleans.lattice.wal.shard.deactivate.in_flight", unit: "{slot}",
            description: "Per-WAL-shard in-flight slot count observed at OnDeactivateAsync time.");

    /// <summary>
    /// Count of per-shard <c>WalShardGrain</c> deactivation drains that
    /// exceeded <see cref="Orleans.Lattice.LatticeOptions.WalDrainBudget"/>
    /// and had to force-fault one or more in-flight slots so the
    /// activation could finish tearing down. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// <para>
    /// Reliability intent: under a saturating-storage-account wedge,
    /// the provider call's await can park behind an SDK retry loop in
    /// pre-attempt back-off where the per-flush
    /// <see cref="Orleans.Lattice.LatticeOptions.WalFlushTimeout"/>
    /// deadline does not fire promptly (the SDK observes cancellation
    /// only between attempts, not during back-off), so a chain with N
    /// in-flight slots could otherwise hold the deactivation
    /// indefinitely. With the drain budget the deactivation force-faults
    /// any slot that has not unlinked within the deadline; this counter
    /// names the wedged shard so operators can attribute the trip
    /// without source-walking the silo log. Zero on a healthy
    /// drain; any non-zero rate identifies a shard whose provider call
    /// could not be cancelled inside the drain budget.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalShardDrainBudgetExpirations =
        Meter.CreateCounter<long>("orleans.lattice.wal.shard.drain.budget.expirations", unit: "{expiration}",
            description: "Count of WalShardGrain deactivation drains that exceeded WalDrainBudget and force-faulted in-flight slots.");

    /// <summary>
    /// Histogram of in-flight slots force-faulted by a per-shard
    /// <c>WalShardGrain</c> deactivation drain after
    /// <see cref="Orleans.Lattice.LatticeOptions.WalDrainBudget"/>
    /// expired. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Recorded exactly once per drain that hit the budget; the value is
    /// the number of slots that had not unlinked when the budget fired and
    /// were force-faulted to release the activation. A zero observation
    /// is not recorded - the histogram only fires on the
    /// <see cref="WalShardDrainBudgetExpirations"/> path, so reading the
    /// histogram's count and the counter's count gives the same number.
    /// </summary>
    public static readonly Histogram<long> WalShardDrainBudgetForceFaultedSlots =
        Meter.CreateHistogram<long>("orleans.lattice.wal.shard.drain.budget.force_faulted_slots", unit: "{slot}",
            description: "Per-WAL-shard in-flight slot count force-faulted by a deactivation drain that exceeded WalDrainBudget.");

    /// <summary>
    /// Count of <c>WalShardGrain.StartFlush</c> invocations per
    /// <c>(tree, shard)</c>. Incremented once at the top of every
    /// <c>StartFlush</c> call, including the follow-on flushes a
    /// completing flush kicks off. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>.
    /// <para>
    /// Diagnostic intent: under the residual phase-1/activation WAL
    /// wedge, the in-flight chain pins at <c>WalMaxPendingBatches</c>
    /// for 120+ seconds with no shipped deadline tripping. This counter
    /// distinguishes two of the three remaining wedge-mechanism classes
    /// in one cohort: if <c>start_flush.calls</c> keeps incrementing
    /// throughout the wedge then new flushes ARE being kicked off, so
    /// the wedge is a slot-leak in the in-flight chain's <c>finally</c>
    /// (slots never removed even after the flush's task settles). If
    /// <c>start_flush.calls</c> stops incrementing during the wedge then
    /// the cap-cutover loop in <c>AppendBatchAsync</c> is itself blocked
    /// and no new flush ever kicks off. Either signal narrows the
    /// remaining investigation to a small handful of code regions.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalShardStartFlushCalls =
        Meter.CreateCounter<long>("orleans.lattice.wal.shard.start_flush.calls", unit: "{call}",
            description: "Count of WalShardGrain.StartFlush invocations per (tree, shard).");

    /// <summary>
    /// Histogram of <c>_pendingSegments.Count</c> observed at every
    /// <c>WalShardGrain.StartFlush</c> entry, sampled <i>before</i> the
    /// pending list is captured into the new in-flight slot. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// <para>
    /// Diagnostic intent: under the wedge, a growing distribution
    /// indicates callers are still arriving and enqueueing into
    /// <c>_pendingSegments</c> even though the chain cannot drain - a
    /// signature of back-pressure absorbing everything but never
    /// releasing. A stuck-at-zero distribution combined with a
    /// <see cref="WalShardStartFlushCalls"/> trickle indicates the
    /// cap-cutover loop blocked itself; combined with a healthy
    /// <c>start_flush.calls</c> rate it indicates the wedge is downstream
    /// of the flush kick-off. Mirrors the existing <c>WalAppendInFlight</c>
    /// histogram's allocation-free emission shape.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalShardPendingSegments =
        Meter.CreateHistogram<long>("orleans.lattice.wal.shard.pending_segments", unit: "{segment}",
            description: "Per-WAL-shard pending-segment count sampled at every StartFlush entry.");

    /// <summary>
    /// Count of <c>TreeReshardGrain.ReshardAsync</c> invocations that
    /// progressed past argument / interlock validation and started a
    /// reshard coordinator. Tagged with <see cref="TagTree"/>.
    /// <para>
    /// Diagnostic intent: the residual WAL wedge is correlated with the
    /// <c>reshard ... REJECTED (Forwarding failed)</c> log storm
    /// (228-540 occurrences per wedged run). This counter is the
    /// Lattice-side initiation signal; pairing it with
    /// <see cref="ShardRootReshardCompleted"/> and
    /// <see cref="ShardRootReshardRejected"/> lets a dashboard correlate
    /// reshard activity with wedge onset directly, without depending on
    /// grep over a rotated silo log. Note: Orleans-side message-routing
    /// rejections ("Forwarding failed") are emitted by Orleans's own
    /// router and are not captured here - they remain log-only until a
    /// separate diagnostic source is added.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardRootReshardInitiated =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.reshard.initiated", unit: "{reshard}",
            description: "Count of TreeReshardGrain.ReshardAsync invocations that started a reshard coordinator.");

    /// <summary>
    /// Count of <c>TreeReshardGrain.ReshardAsync</c> invocations that
    /// were rejected at the Lattice layer before starting a coordinator.
    /// Tagged with <see cref="TagTree"/> and a <c>reason</c> tag
    /// enumerating the rejection cause (e.g. <c>argument_out_of_range</c>,
    /// <c>resize_in_flight</c>, <c>state_write_failed</c>).
    /// <para>
    /// Excludes Orleans-side message-routing rejections, which the
    /// Orleans runtime logs as "Forwarding failed" but does not surface
    /// to <c>TreeReshardGrain</c> as a catchable exception inside
    /// <c>ReshardAsync</c>. See <see cref="ShardRootReshardInitiated"/>.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardRootReshardRejected =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.reshard.rejected", unit: "{rejection}",
            description: "Count of TreeReshardGrain.ReshardAsync rejections, tagged by reason.");

    /// <summary>
    /// Count of <c>TreeReshardGrain</c> coordinator completions that
    /// reached the terminal phase successfully. Tagged with
    /// <see cref="TagTree"/>. The difference between this and
    /// <see cref="ShardRootReshardInitiated"/> over a window is the
    /// number of reshards still in flight or that failed mid-coordinator.
    /// </summary>
    public static readonly Counter<long> ShardRootReshardCompleted =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.reshard.completed", unit: "{reshard}",
            description: "Count of TreeReshardGrain coordinator completions.");

    /// <summary>
    /// Histogram observation of the reshard in-flight state for a tree,
    /// emitted at every <c>ReshardAsync</c> entry as either <c>0</c>
    /// (idle) or <c>1</c> (a reshard is already in progress for this
    /// tree). Tagged with <see cref="TagTree"/>. Bridges the gap left
    /// by not registering an <c>ObservableGauge</c>: a non-zero
    /// observation immediately preceding the wedge onset is the same
    /// signal a periodically-polled gauge would provide.
    /// </summary>
    public static readonly Histogram<long> ShardRootReshardInFlight =
        Meter.CreateHistogram<long>("orleans.lattice.shard_root.reshard.in_flight", unit: "{reshard}",
            description: "Per-tree reshard in-flight (0/1) observation, recorded at ReshardAsync entry.");

    /// <summary>
    /// Count of <c>WalCommitLogWriter</c> append dispatches that started
    /// (one increment per pending-append at the <c>Enqueued</c> stamp).
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Wedge-investigation intent: the saturation-rung wedge has a
    /// dominant mode (cohort 2026-06-03, 5/7 wedged cohorts) where every
    /// shard's in-flight chain is empty (<c>head.IsNull=True</c>) yet
    /// 348+ callers are parked at <c>WalShardGrain.AppendBatchAsync</c>
    /// and 375+ at <c>WalCommitLogWriter.AppendForPartitionAsync</c>.
    /// The wedge mechanism for that mode is upstream of in-flight
    /// insertion, inside this writer's per-partition dispatch plumbing.
    /// This counter is the writer-layer kick-off signal: a healthy rate
    /// rules out "writer never gets called"; a collapse to zero during a
    /// wedge tail localises the stall to the writer's own routing /
    /// option-resolver path; a sustained rate combined with stale
    /// <see cref="WalAppendPendingDispatches"/> p99 readings localises it
    /// to the awaited shard-grain RPC.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendDispatched =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.dispatched", unit: "{dispatch}",
            description: "Count of WalCommitLogWriter append dispatches that reached the Enqueued lifecycle stamp.");

    /// <summary>
    /// Histogram of per-partition pending-append depth observed at every
    /// <c>WalCommitLogWriter</c> append entry, sampled <i>before</i> the
    /// new pending stamp is added to the partition's tracker. Tagged
    /// with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Wedge-investigation intent: the writer's per-partition pending
    /// tracker holds one <c>PendingAppend</c> stamp per in-flight
    /// <c>AppendForPartitionAsync</c> caller. A growing distribution
    /// during the wedge confirms the writer is the choke (callers
    /// enqueuing into a tracker that cannot drain); a stuck-at-zero
    /// distribution combined with sustained
    /// <see cref="WalAppendDispatched"/> rules out a writer-layer
    /// dispatch lifecycle stall and points the next bisect downstream of
    /// the <c>SentToShard</c> stage. Mirrors the
    /// <see cref="WalShardPendingSegments"/> shape one layer up.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalAppendPendingDispatches =
        Meter.CreateHistogram<long>("orleans.lattice.wal.writer.partition.pending_appends", unit: "{dispatch}",
            description: "Per-WAL-writer-partition pending-append-dispatch count sampled at every append entry.");

    /// <summary>
    /// Count of <c>WalCommitLogWriter</c> append dispatches that failed
    /// to acquire a per-partition admission slot before
    /// <see cref="LatticeOptions.WalAppendDispatchTimeout"/> expired.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Reliability intent: the per-partition admission semaphore caps
    /// <c>PartitionTracker._inFlight</c> depth at
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/>, mirroring the
    /// shard-side ceiling. When the shard cannot drain, callers
    /// awaiting an admission slot are released with a typed
    /// <see cref="TimeoutException"/> at the deadline rather than
    /// silently parking forever in an unbounded writer queue. A
    /// non-zero counter under steady-state operation is the signal that
    /// the offered rate exceeds the shard's drain rate - the
    /// saturation regime previously hidden as a silent wedge. Pair with
    /// <see cref="WalAppendAdmissionWait"/> to distinguish "saturation
    /// hit but absorbed cleanly" (wait p99 elevated, zero timeouts)
    /// from "saturation exceeded the deadline" (non-zero timeouts).
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendAdmissionTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.admission_timeouts", unit: "{timeout}",
            description: "Count of WalCommitLogWriter append dispatches whose per-partition admission wait exceeded WalAppendDispatchTimeout.");

    /// <summary>
    /// Histogram of wall-clock ms spent waiting for a per-partition
    /// admission slot before the <c>WalCommitLogWriter</c> dispatch was
    /// allowed to link a new <c>PendingAppend</c> stamp. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Reliability intent: under healthy operation this histogram sits
    /// at the floor (a sub-microsecond uncontended semaphore acquire).
    /// A spreading distribution indicates the per-partition tracker is
    /// approaching its <see cref="LatticeOptions.WalMaxPendingBatches"/>
    /// ceiling, surfacing back-pressure as an honest tail-latency
    /// signal long before any caller hits the
    /// <see cref="WalAppendAdmissionTimeouts"/> deadline. Recorded for
    /// every dispatch that successfully acquired a slot (timed-out
    /// dispatches feed the counter only).
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalAppendAdmissionWait =
        Meter.CreateHistogram<double>("orleans.lattice.wal.writer.append.admission_wait", unit: "ms",
            description: "Wall-clock ms a WalCommitLogWriter dispatch waited for a per-partition admission slot.");

    /// <summary>
    /// Count of writer-side parked admission callers released by a
    /// silo-drain signal on host shutdown. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagPartition"/>. One sample per parked caller
    /// faulted out of <c>PartitionTracker.AcquireAsync</c> when the
    /// owning <see cref="Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter"/>
    /// drains on host shutdown; zero on a healthy shutdown that has
    /// no parked callers.
    /// <para>
    /// Reliability intent: distinct from
    /// <see cref="WalAppendAdmissionTimeouts"/> (which counts
    /// per-call dispatch-deadline expiries during steady-state
    /// operation) and from <see cref="WalShardDrainBudgetExpirations"/>
    /// (which counts shard-grain deactivation drains that had to
    /// force-fault). This counter names writer-side parked callers
    /// released by the silo's drain on shutdown - the surface that
    /// closes the writer-admission-semaphore-wedged-at-SIGTERM
    /// phenotype documented in
    /// <c>benchmark/azure-throughput/throughput.md</c> section 32.6.
    /// A non-zero rate on shutdown is normal when the silo was under
    /// storage saturation at drain entry; a non-zero rate during
    /// steady-state operation indicates the drain hook fired
    /// spuriously and is a regression signal.
    /// </para>
    /// <para>
    /// Per-silo: each silo process emits its own samples for the
    /// trackers its <see cref="Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter"/>
    /// owns. A dashboard <c>sum by (tree, partition)</c> across the
    /// cluster gives the cumulative drain-released-caller count;
    /// a <c>sum by (silo)</c> isolates which silos saw saturated
    /// shutdowns.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendDrainReleases =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.drain.releases", unit: "{release}",
            description: "Count of writer-side parked admission callers released by a silo-drain signal on host shutdown.");

    /// <summary>
    /// Count of writer-side admission dispatches refused with
    /// <see cref="LatticeSaturatedException"/> because the per-tree
    /// saturation signal reported
    /// <see cref="WalSaturationState.Saturated"/> for longer than
    /// <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Reliability intent: distinct from
    /// <see cref="WalAppendAdmissionTimeouts"/> (which counts the
    /// per-call <see cref="LatticeOptions.WalAppendDispatchTimeout"/>
    /// deadline) and from <see cref="WalAppendDrainReleases"/> (which
    /// counts host-shutdown drain releases). This counter names
    /// callers refused fast by the
    /// <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>
    /// gate during steady-state saturation episodes - the surface
    /// that closes the storage-account 409-Conflict-burst phenotype
    /// documented in <c>benchmark/azure-throughput/throughput.md</c>
    /// section 32. A non-zero rate during steady-state operation is
    /// the canonical signal that offered load is exceeding the
    /// storage layer's sustained drain rate; under healthy operation
    /// the counter stays at zero because the saturation signal does
    /// not enter <see cref="WalSaturationState.Saturated"/>.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendAdmissionSaturationRefusals =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.admission_saturation_refusals", unit: "{refusal}",
            description: "Count of writer-side admission dispatches refused with LatticeSaturatedException because the saturation signal stayed Saturated beyond WalAdmissionSaturationWaitBudget.");

    /// <summary>
    /// Tag key for the per-tree saturation state on
    /// <see cref="WalSaturationTransitions"/>. Value is the lowercased
    /// state name (<c>healthy</c>, <c>throttled</c>, <c>saturated</c>).
    /// The <see cref="WalSaturationStateGaugeName"/> observable gauge
    /// deliberately does not carry this tag - its ordinal value already
    /// encodes the regime, so labelling it as well would fragment the
    /// per-tree series on every transition and leave stale elevated
    /// series behind.
    /// </summary>
    public const string TagWalSaturationState = "state";

    /// <summary>
    /// Tag key for the previous saturation state on
    /// <see cref="WalSaturationTransitions"/>. Lets dashboards filter
    /// by direction of transition (e.g. <c>previous_state=healthy</c>
    /// + <c>state=throttled</c> isolates the leading edge of every
    /// saturation episode).
    /// </summary>
    public const string TagWalSaturationPreviousState = "previous_state";

    /// <summary>
    /// Counter incremented once per per-tree WAL saturation-state
    /// transition observed by the silo-scoped sampler. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagWalSaturationState"/>
    /// (the new state), and
    /// <see cref="TagWalSaturationPreviousState"/> (the state the tree
    /// was in before the transition). Optional <see cref="TagPartition"/>
    /// and <see cref="TagShard"/> tags are added when the transition
    /// is attributable to a single partition (admission-depth-driven)
    /// or shard (dispatch-timeout-driven).
    /// <para>
    /// Wedge-investigation intent: a healthy silo's series is a flat
    /// zero. A rising rate of <c>state=throttled</c> transitions on a
    /// specific <c>(tree)</c> is the leading edge of the saturation
    /// regime; a rising rate of <c>state=saturated</c> is the regime
    /// itself. Pair with the observable
    /// <see cref="WalSaturationStateGaugeName"/> gauge for "what is
    /// the current regime" and with this counter for "how often is
    /// the regime changing" - flapping between Throttled and
    /// Saturated is a different operational signal from a sustained
    /// Saturated.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalSaturationTransitions =
        Meter.CreateCounter<long>("orleans.lattice.wal.saturation.transitions", unit: "{transition}",
            description: "Count of per-tree WAL saturation-state transitions observed by the silo-scoped sampler.");

    /// <summary>
    /// Instrument name of the observable gauge that reports the current
    /// per-tree WAL saturation state. Published with
    /// <see cref="TagTree"/> only; the value is the ordinal
    /// of the <see cref="WalSaturationState"/> enum
    /// (<c>0</c> = Healthy, <c>1</c> = Throttled, <c>2</c> = Saturated)
    /// so dashboards can plot the regime as a step function. The regime
    /// is intentionally <b>not</b> also carried as a label: the ordinal
    /// value already encodes it, and adding a redundant state label made
    /// every transition change the series identity, leaving the prior
    /// state's series behind at its last (elevated) value under scrape
    /// staleness - so a recovered tree kept reading as Saturated. The
    /// per-state breakdown lives on <see cref="WalSaturationTransitions"/>.
    /// <para>
    /// Idle cost is zero - the observable callback only runs on scrape
    /// and reads a concurrent-dictionary cache populated by the silo-
    /// scoped sampler. A tree contributes a measurement only after the
    /// sampler has observed at least one signal for it; a tree that
    /// has never been written to does not appear in the gauge series
    /// at all (rather than reporting an incorrect Healthy zero).
    /// </para>
    /// </summary>
    public const string WalSaturationStateGaugeName = "orleans.lattice.wal.saturation.state";

    /// <summary>
    /// Histogram of wall-clock ms for a single per-leaf
    /// <c>IBPlusLeafGrain.SetManyAsync</c> RPC dispatched from
    /// <c>ShardRootGrain.SetManyLocalOnlyAsync</c> via
    /// <c>DispatchLeafBatchWithRetryAsync</c>. Recorded per attempt
    /// (including retries) and per dispatched leaf, so for a single
    /// shard-root <c>SetManyAsync(N)</c> there are up to one
    /// observation per per-leaf bucket. Tagged with <see cref="TagTree"/>.
    /// This is the outbound-call view from the shard-root: it includes
    /// Orleans grain-schedule wait, per-leaf turn-queue wait, leaf
    /// commit, WAL append, and WAL phase-2. Combined with the leaf-side
    /// <c>leaf.commit.duration</c> aggregate, the residual gap localises
    /// pre-turn scheduling cost.
    /// </summary>
    public static readonly Histogram<double> ShardRootSetManyLeafRpcDuration =
        Meter.CreateHistogram<double>("orleans.lattice.shard_root.set_many.leaf_rpc.duration", unit: "ms",
            description: "Wall-clock ms per per-leaf IBPlusLeafGrain.SetManyAsync RPC dispatched from ShardRootGrain.SetManyLocalOnlyAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.SetManyAsync</c>, the user-facing
    /// <see cref="ILattice.SetManyAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// batched write call (includes routing, bucketing, per-shard
    /// parallel fan-out, and event publish). Pair with
    /// <see cref="SetManyStageDuration"/> to attribute the per-call
    /// envelope to one of five sub-spans.
    /// </summary>
    public static readonly Histogram<double> SetManyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set_many.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.SetManyAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.SetManyAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>gate</c> | <c>route</c> |
    /// <c>bucket</c> | <c>fanout</c> | <c>events</c>).
    /// <para>
    /// Mirrors the c2-xxii saga-broadcast sub-stage instrumentation
    /// (<see cref="SagaBroadcastShardStageDuration"/>). Splits the
    /// caller-visible envelope into its constituent spans so the
    /// dominant cost on the set-many path can be identified before any
    /// further structural attempt. Together with the existing
    /// <see cref="ShardRootSetManyLocalApplyDuration"/> /
    /// <see cref="ShardRootSetManyShadowForwardDuration"/> /
    /// <see cref="ShardRootSetManyLeafRpcDuration"/> instruments,
    /// the full envelope from <c>ILattice.SetManyAsync</c> entry down
    /// to the leaf RPC is attributed.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SetManyStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set_many.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (gate|route|bucket|fanout|events) of LatticeGrain.SetManyAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.SetAsync</c>, the user-facing point-write
    /// <see cref="ILattice.SetAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// single-key set call (includes gate, routing, the shard RPC, and
    /// event publish). Pair with <see cref="SetStageDuration"/> to
    /// attribute the per-call envelope to one of four sub-spans.
    /// </summary>
    public static readonly Histogram<double> SetDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.SetAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.SetAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>gate</c> | <c>route</c> |
    /// <c>shard</c> | <c>publish</c>).
    /// <para>
    /// Mirrors <see cref="SetManyStageDuration"/> for the point-write
    /// path. Together with the existing per-leaf instruments
    /// (<c>leaf.commit.duration phase=wal|apply|observer|digest</c>),
    /// <c>wal.shard.dispatch.duration</c>, and <c>wal.append.*</c>
    /// histograms, the full point-write envelope from
    /// <c>ILattice.SetAsync</c> entry down to the Azure provider call is
    /// attributed. The c2-xxvii investigation surfaced this seam.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SetStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (gate|route|shard|publish) of LatticeGrain.SetAsync.");

    // --- Foreground read envelopes (LatticeGrain) ----------------------------

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.GetAsync</c>, the user-facing point-read
    /// <see cref="ILattice.GetAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// single-key read (includes routing resolution, the shard RPC, and
    /// any stale-routing retries). Pair with <see cref="GetStageDuration"/>
    /// to attribute the per-call envelope to one of its sub-spans.
    /// <para>
    /// Mirrors <see cref="SetDuration"/> for the point-read path. Closes
    /// the read-side gap in the foreground-call attribution model: the
    /// existing <c>shard.reads</c> counter only counts reads, not their
    /// latency, and <c>leaf.scan.duration</c> covers range scans only.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> GetDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.GetAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.GetAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>route</c> | <c>shard</c>).
    /// <para>
    /// One observation per stage per inner attempt: under a stale-routing
    /// storm a single <c>GetAsync</c> call records multiple <c>route</c> /
    /// <c>shard</c> data points so the histograms attribute the retry
    /// cost. Mirrors the per-attempt accumulation pattern of
    /// <see cref="SetStageDuration"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> GetStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (route|shard) of LatticeGrain.GetAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.GetManyAsync</c>, the user-facing batched-read
    /// <see cref="ILattice.GetManyAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// batched-read call (includes routing, per-key bucketing, per-shard
    /// parallel fan-out, the registry-snapshot double-check, and any
    /// stale-routing retries). Pair with
    /// <see cref="GetManyStageDuration"/> to attribute the envelope to
    /// one of its sub-spans.
    /// </summary>
    public static readonly Histogram<double> GetManyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get_many.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.GetManyAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.GetManyAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>route</c> | <c>bucket</c> |
    /// <c>fanout</c> | <c>merge</c>).
    /// <para>
    /// Mirrors <see cref="SetManyStageDuration"/> for the batched-read
    /// path. The <c>route</c> stage covers the <c>GetRoutingAsync</c>
    /// fetch; <c>bucket</c> the per-key shard bucketing loop;
    /// <c>fanout</c> the cross-shard <c>Task.WhenAll</c> dispatch;
    /// <c>merge</c> the post-fan-out result merge plus the
    /// snapshot-stability and topology-stability checks. One observation
    /// per stage per inner attempt: under a snapshot retry or
    /// stale-routing storm a single call records multiple data points
    /// per stage so the histogram attributes the retry cost honestly.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> GetManyStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get_many.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (route|bucket|fanout|merge) of LatticeGrain.GetManyAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.ExistsAsync</c>, the user-facing key-existence
    /// <see cref="ILattice.ExistsAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. Lower-traffic than <see cref="GetDuration"/>
    /// in typical workloads but exposed for symmetry with the other
    /// read-side envelopes so a dashboard tile can confirm activity.
    /// </summary>
    public static readonly Histogram<double> ExistsDuration =
        Meter.CreateHistogram<double>("orleans.lattice.exists.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.ExistsAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.GetWithVersionAsync</c>, the user-facing versioned-read
    /// <see cref="ILattice.GetWithVersionAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. Lower-traffic than <see cref="GetDuration"/>
    /// in typical workloads but exposed for symmetry with the other
    /// read-side envelopes so an operator can verify version-probe
    /// activity.
    /// </summary>
    public static readonly Histogram<double> GetWithVersionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get_with_version.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.GetWithVersionAsync call (caller-visible envelope).");

    // --- Retroactive shard-split sweep instruments ----------------

    /// <summary>
    /// Counter of in-flight prepared mutations retroactively
    /// shadow-forwarded from a source shard's leaf chain to the
    /// destination shard at the start of an adaptive split's
    /// <c>BeginShadowWrite</c> phase. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/> (the source shard index). One
    /// increment per <see cref="Orleans.Lattice.BPlusTree.PendingMutationSnapshot"/> replayed.
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

    // --- Autonomic split admission instruments (HotShardMonitorGrain) -------

    /// <summary>
    /// Histogram sampled once per autonomic monitor pass with the number of
    /// splits currently in flight for that tree (derived from each shard's
    /// authoritative <c>IsSplitting</c> status). Tagged with <see cref="TagTree"/>.
    /// Emitted every pass <em>regardless</em> of whether the cluster-wide split
    /// gate (<see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>) is
    /// enabled, so operators can compute the cluster aggregate as a
    /// <c>sum</c> across the <c>tree</c> tag and decide whether they need the
    /// gate and how to size it.
    /// </summary>
    public static readonly Histogram<long> SplitInFlight =
        Meter.CreateHistogram<long>("orleans.lattice.split.in_flight", unit: "{split}",
            description: "Per-tree autonomic splits in flight, sampled every monitor pass (sum across tree for the cluster total).");

    /// <summary>
    /// Counter of hot, otherwise-eligible shards that could not trigger an
    /// autonomic split this pass because a concurrency cap (the per-tree
    /// <see cref="LatticeOptions.MaxConcurrentAutoSplits"/> or the cluster-wide
    /// <see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>) was already
    /// reached. Tagged with <see cref="TagTree"/>. Emitted regardless of whether
    /// the cluster gate is enabled; a chronically non-zero value across many
    /// trees signals aggregate split pressure the per-tree cap alone cannot see.
    /// </summary>
    public static readonly Counter<long> SplitCandidatesSuppressed =
        Meter.CreateCounter<long>("orleans.lattice.split.candidates_suppressed", unit: "{shard}",
            description: "Hot eligible shards that could not split this pass because a concurrency cap was reached.");

    /// <summary>
    /// Counter incremented only when the <em>cluster-wide</em> admission gate
    /// denied an otherwise-eligible autonomic split (no permit available under
    /// <see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>). Tagged with
    /// <see cref="TagTree"/> and <see cref="TagReason"/> (<c>cluster_cap</c>).
    /// Flat-zero means the ceiling never binds; sustained non-zero alongside
    /// rising hot-shard latency means the ceiling is set too low.
    /// </summary>
    public static readonly Counter<long> SplitAdmissionDeferred =
        Meter.CreateCounter<long>("orleans.lattice.split.admission.deferred", unit: "{shard}",
            description: "Otherwise-eligible autonomic splits held back by the cluster-wide admission gate.");

    /// <summary><see cref="TagReason"/> = <c>cluster_cap</c> on <see cref="SplitAdmissionDeferred"/>.</summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredClusterCapReasonTag = new(TagReason, "cluster_cap");

    /// <summary>
    /// <see cref="TagReason"/> = <c>uniform_load</c> on <see cref="SplitAdmissionDeferred"/>.
    /// Emitted for a shard that is above the ops/sec threshold but whose tree is
    /// uniformly loaded, so a split would relieve nothing. Sustained non-zero
    /// values are the signature of a bulk ingest, not of a hot spot.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredUniformLoadReasonTag = new(TagReason, "uniform_load");

    /// <summary>
    /// <see cref="TagReason"/> = <c>low_occupancy</c> on <see cref="SplitAdmissionDeferred"/>.
    /// Emitted for a hot, skewed shard that holds too few live entries for a
    /// split to redistribute anything.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredLowOccupancyReasonTag = new(TagReason, "low_occupancy");

    /// <summary>
    /// <see cref="TagReason"/> = <c>shard_ceiling</c> on <see cref="SplitAdmissionDeferred"/>.
    /// Emitted for a hot shard held back because the tree has reached its
    /// per-tree physical shard ceiling. Sustained non-zero means the ceiling is
    /// binding and should be reviewed alongside the tree's shard count.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredShardCeilingReasonTag = new(TagReason, "shard_ceiling");

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
        Meter.CreateHistogram<double>("orleans.lattice.leaf.tombstone.ratio", unit: "1",
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

    /// <summary>
    /// <see cref="TagPhase"/> = <c>prologue</c> (a range-scan page fill was
    /// still preparing its shard for the operation).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PhaseScanPagePrologueTag = new(TagPhase, "prologue");

    /// <summary>
    /// <see cref="TagPhase"/> = <c>descent</c> (a range-scan page fill was
    /// still traversing down to its start leaf).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PhaseScanPageDescentTag = new(TagPhase, "descent");

    /// <summary>
    /// <see cref="TagPhase"/> = <c>leaf-walk</c> (a range-scan page fill was
    /// reading the leaf chain).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PhaseScanPageLeafWalkTag = new(TagPhase, "leaf-walk");

    /// <summary><see cref="TagStage"/> = <c>resolve</c> (step 1 affected-leaves resolution).</summary>
    public static readonly KeyValuePair<string, object?> StageResolveTag = new(TagStage, "resolve");

    /// <summary><see cref="TagStage"/> = <c>hlc</c> (step 2 ComputeTerminalHlcAsync fan-out + tick).</summary>
    public static readonly KeyValuePair<string, object?> StageHlcTag = new(TagStage, "hlc");

    /// <summary><see cref="TagStage"/> = <c>wal</c> (step 3 commit-log adapter append).</summary>
    public static readonly KeyValuePair<string, object?> StageWalTag = new(TagStage, "wal");

    /// <summary><see cref="TagStage"/> = <c>fanout</c> (step 4 per-leaf ApplyTxTerminalAsync dispatch + shadow-forward).</summary>
    public static readonly KeyValuePair<string, object?> StageFanOutTag = new(TagStage, "fanout");

    /// <summary><see cref="TagStage"/> = <c>gate</c> (LatticeGrain.SetManyAsync pre-flight: compaction reminder + monitor + events-gate probe).</summary>
    public static readonly KeyValuePair<string, object?> StageGateTag = new(TagStage, "gate");

    /// <summary><see cref="TagStage"/> = <c>route</c> (LatticeGrain.SetManyAsync / GetManyAsync routing fetch via GetRoutingAsync; LatticeGrain.GetAsync per-attempt shard resolution).</summary>
    public static readonly KeyValuePair<string, object?> StageRouteTag = new(TagStage, "route");

    /// <summary><see cref="TagStage"/> = <c>bucket</c> (LatticeGrain.SetManyAsync / GetManyAsync per-key shard bucketing loop).</summary>
    public static readonly KeyValuePair<string, object?> StageBucketTag = new(TagStage, "bucket");

    /// <summary><see cref="TagStage"/> = <c>events</c> (LatticeGrain.SetManyAsync trailing per-entry PublishEventAsync foreach).</summary>
    public static readonly KeyValuePair<string, object?> StageEventsTag = new(TagStage, "events");

    /// <summary><see cref="TagStage"/> = <c>shard</c> (LatticeGrain.SetAsync / GetAsync inner shard RPC including stale-routing retries).</summary>
    public static readonly KeyValuePair<string, object?> StageShardTag = new(TagStage, "shard");

    /// <summary><see cref="TagStage"/> = <c>publish</c> (LatticeGrain.SetAsync trailing PublishEventAsync hop).</summary>
    public static readonly KeyValuePair<string, object?> StagePublishTag = new(TagStage, "publish");

    /// <summary><see cref="TagStage"/> = <c>merge</c> (LatticeGrain.GetManyAsync post-fan-out result merge plus snapshot- and topology-stability checks).</summary>
    public static readonly KeyValuePair<string, object?> StageMergeTag = new(TagStage, "merge");

    // --- Auto-trained compression-dictionary instruments -------------------
    //
    // Emitted by AutoTrainingCompressionDictionaryProvider, the opt-in
    // (default-off) provider that samples a bounded reservoir of payloads and
    // periodically trains a Zstandard dictionary off the hot path. When
    // auto-training is disabled the provider emits none of these. The two
    // observable gauges (active_version, reservoir_fill) are registered lazily
    // by the provider instance (so they cost nothing when no provider is
    // constructed and nothing when no listener is attached); their canonical
    // names are exposed as `...Name` constants so the dashboards drift-guard
    // recognises the PromQL token forms even though the instruments are not
    // statically constructed on this meter. The three counters are ordinary
    // counters constructed on the meter below.

    /// <summary>
    /// Counter incremented once per auto-training pass attempt, tagged with
    /// <see cref="TagOutcome"/> = <c>trained</c> (a dictionary was built),
    /// <c>skipped_insufficient_samples</c> (the reservoir held fewer than the
    /// configured minimum, or the underlying builder rejected the corpus), or
    /// <c>skipped_cadence</c> (the minimum training interval had not yet
    /// elapsed since the previous attempt).
    /// </summary>
    public static readonly Counter<long> CompressionDictionaryTrainingRuns =
        Meter.CreateCounter<long>(CompressionDictionaryTrainingRunsName, unit: "{run}",
            description: "Auto-training dictionary pass attempts, tagged by outcome (trained, skipped_insufficient_samples, skipped_cadence).");

    /// <summary>Canonical name of <see cref="CompressionDictionaryTrainingRuns"/>.</summary>
    public const string CompressionDictionaryTrainingRunsName = "orleans.lattice.compress.dictionary.training_runs";

    /// <summary>
    /// Counter of the no-dictionary (plain Zstandard) baseline compressed
    /// bytes of the training probe, summed once per successful training pass.
    /// Paired with <see cref="CompressionDictionaryTrainedBytesOut"/>: the
    /// trained-dictionary compression-ratio delta versus the dictionary-less
    /// baseline is <c>trained_bytes_out / trained_bytes_in</c> (a value below
    /// <c>1</c> means the trained dictionary beats plain Zstandard on the
    /// sampled corpus).
    /// </summary>
    public static readonly Counter<long> CompressionDictionaryTrainedBytesIn =
        Meter.CreateCounter<long>(CompressionDictionaryTrainedBytesInName, unit: "By",
            description: "No-dictionary (plain Zstd) baseline compressed bytes of the training probe, summed per successful auto-training pass.");

    /// <summary>Canonical name of <see cref="CompressionDictionaryTrainedBytesIn"/>.</summary>
    public const string CompressionDictionaryTrainedBytesInName = "orleans.lattice.compress.dictionary.trained_bytes_in";

    /// <summary>
    /// Counter of the trained-dictionary compressed bytes of the training
    /// probe, summed once per successful training pass. See
    /// <see cref="CompressionDictionaryTrainedBytesIn"/> for the ratio
    /// interpretation.
    /// </summary>
    public static readonly Counter<long> CompressionDictionaryTrainedBytesOut =
        Meter.CreateCounter<long>(CompressionDictionaryTrainedBytesOutName, unit: "By",
            description: "Trained-dictionary compressed bytes of the training probe, summed per successful auto-training pass.");

    /// <summary>Canonical name of <see cref="CompressionDictionaryTrainedBytesOut"/>.</summary>
    public const string CompressionDictionaryTrainedBytesOutName = "orleans.lattice.compress.dictionary.trained_bytes_out";

    /// <summary>
    /// Canonical name of the observable gauge reporting the currently active
    /// auto-trained dictionary id (the monotonic version the encoder should
    /// request). <c>0</c> means no dictionary has been trained yet. Registered
    /// lazily by <see cref="AutoTrainingCompressionDictionaryProvider"/>; not
    /// statically constructed on the meter.
    /// </summary>
    public const string CompressionDictionaryActiveVersionName = "orleans.lattice.compress.dictionary.active_version";

    /// <summary>
    /// Canonical name of the observable gauge reporting auto-training reservoir
    /// occupancy. Reports two series tagged with <see cref="TagKind"/>:
    /// <c>samples</c> (retained sample count) and <c>bytes</c> (retained total
    /// bytes). Registered lazily by
    /// <see cref="AutoTrainingCompressionDictionaryProvider"/>; not statically
    /// constructed on the meter.
    /// </summary>
    public const string CompressionDictionaryReservoirFillName = "orleans.lattice.compress.dictionary.reservoir_fill";

    /// <summary><see cref="TagOutcome"/> = <c>trained</c> (an auto-training pass built a dictionary).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeTrained = new(TagOutcome, "trained");

    /// <summary><see cref="TagOutcome"/> = <c>skipped_insufficient_samples</c> (the reservoir held too few samples to train).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeSkippedInsufficientSamples = new(TagOutcome, "skipped_insufficient_samples");

    /// <summary><see cref="TagOutcome"/> = <c>skipped_cadence</c> (the minimum training interval had not elapsed).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeSkippedCadence = new(TagOutcome, "skipped_cadence");

    /// <summary><see cref="TagKind"/> = <c>samples</c> (reservoir-fill sample-count series).</summary>
    public static readonly KeyValuePair<string, object?> ReservoirFillSamplesTag = new(TagKind, "samples");

    /// <summary><see cref="TagKind"/> = <c>bytes</c> (reservoir-fill total-bytes series).</summary>
    public static readonly KeyValuePair<string, object?> ReservoirFillBytesTag = new(TagKind, "bytes");

    // --- Materialised-view instruments (view maintainer) -------------------------

    /// <summary>
    /// Identifies the materialised view a view-maintenance instrument relates to.
    /// Value is the logical view name (the <c>view-{name}</c> tree's name).
    /// </summary>
    public const string TagView = "view";

    /// <summary>
    /// Histogram of view apply lag, recorded each drain pass as the number of
    /// source WAL entries committed but not yet applied to the view at the start
    /// of the pass. Tagged with <see cref="TagView"/>. A persistently high value
    /// indicates the maintainer is falling behind the source write rate.
    /// </summary>
    public static readonly Histogram<long> ViewApplyLag =
        Meter.CreateHistogram<long>("orleans.lattice.view.apply_lag", unit: "{entry}",
            description: "Source WAL entries committed but not yet applied to the view, sampled per drain pass.");

    /// <summary>
    /// Histogram of the per-pass backlog depth: the number of source entries a
    /// single drain pass read before reaching the source head (bounded by the
    /// view's configured batch size). Tagged with <see cref="TagView"/>.
    /// </summary>
    public static readonly Histogram<long> ViewBacklogDepth =
        Meter.CreateHistogram<long>("orleans.lattice.view.backlog_depth", unit: "{entry}",
            description: "Source entries read in a single view drain pass before reaching the source head.");

    /// <summary>
    /// Counter of view writes applied to the view tree (post-coalesce upserts and
    /// deletes). Tagged with <see cref="TagView"/>. Differentiating apply rate from
    /// backlog depth distinguishes coalesce efficiency from raw source throughput.
    /// </summary>
    public static readonly Counter<long> ViewApplied =
        Meter.CreateCounter<long>("orleans.lattice.view.applied", unit: "{write}",
            description: "View writes applied to the view tree after per-batch last-writer-wins coalescing.");

    /// <summary>
    /// Counter of re-key collisions detected in a view drain batch: a view key
    /// produced by two or more distinct source keys under an injective re-map (a
    /// configuration error). Tagged with <see cref="TagView"/>. A non-zero value
    /// means the projection's key re-map is not injective; the maintainer falls
    /// back to source-HLC last-writer-wins so the view stays well-defined, but the
    /// colliding keys' resolution is non-deterministic with respect to intent.
    /// </summary>
    public static readonly Counter<long> ViewKeyCollisions =
        Meter.CreateCounter<long>("orleans.lattice.view.key_collisions", unit: "{collision}",
            description: "View keys produced by more than one distinct source key under an injective re-map, per drain batch.");

    /// <summary>
    /// Counter of aggregation contributions applied to an aggregation view's group
    /// accumulators (folds and retractions). Tagged with <see cref="TagView"/>.
    /// Distinguishes aggregation apply throughput from the filter / re-project
    /// <see cref="ViewApplied"/> upsert/delete counter.
    /// </summary>
    public static readonly Counter<long> ViewAggregationApplied =
        Meter.CreateCounter<long>("orleans.lattice.view.aggregation_applied", unit: "{contribution}",
            description: "Aggregation contributions (folds and retractions) applied to an aggregation view's group accumulators.");

    /// <summary>
    /// Counter of aggregation contributions rejected because the projection's
    /// group-key selector produced a key in the reserved region - empty, or
    /// beginning with the reserved NUL (<c>\u0000</c>) prefix the maintainer uses
    /// for its internal accumulator / inverse / membership rows. Tagged with
    /// <see cref="TagView"/>. The maintainer drops the offending contribution
    /// rather than writing a group value that would be invisible to view reads
    /// (which floor above the reserved region) and could collide with an internal
    /// row; the rejection is deterministic on the key, so every cluster drops the
    /// same members and the view stays convergent. A non-zero value means a
    /// group-key selector is emitting reserved keys and should be corrected.
    /// </summary>
    public static readonly Counter<long> ViewAggregationRejected =
        Meter.CreateCounter<long>("orleans.lattice.view.aggregation_rejected", unit: "{contribution}",
            description: "Aggregation contributions dropped because the group-key selector produced a reserved (empty or NUL-prefixed) key.");

    /// <summary>
    /// Counter of atomic-write staging backstop fall-backs: a drain pass
    /// abandoned incremental atomic-batch staging and forced a rebuild because
    /// the in-flight staging buffer exceeded its configured bound
    /// (<see cref="LatticeViewOptions.MaxStagedTransactions"/> /
    /// <see cref="LatticeViewOptions.MaxStagedBytes"/>) or an un-terminated
    /// batch's blocked-floor pin would sink below the source WAL retention
    /// ceiling. Tagged with <see cref="TagView"/>. A non-zero value means a
    /// saga terminal was lost or the maintainer fell behind the atomic-write
    /// rate; the view still converges via the rebuild, but the
    /// not-visible-until-committed batch was reassembled from current source
    /// state rather than the staged prepares.
    /// </summary>
    public static readonly Counter<long> ViewAtomicStagingBackstop =
        Meter.CreateCounter<long>("orleans.lattice.view.atomic_staging_backstop", unit: "{rebuild}",
            description: "Drain passes that abandoned atomic-batch staging and forced a rebuild because the staging buffer exceeded its bound or its blocked-floor pin would sink below WAL retention.");

    /// <summary>
    /// Counter of cross-tree joint-atomicity-violation degradations: a view
    /// participating in a cross-tree atomic write waited the bounded
    /// <see cref="LatticeViewOptions.CrossTreeReadinessTimeout"/> for every other
    /// participant view to become ready, did not observe a joint flip, and so
    /// degraded to per-tree-slice atomicity - flipping its own slice atomically
    /// into its own view tree and scheduling a reconcile. Tagged with
    /// <see cref="TagView"/>. A non-zero value means a participant view was
    /// permanently unavailable (cluster partition / crashed maintainer) and the
    /// participating views did not flip together for that batch; the views still
    /// converge via the scheduled reconcile, but a reader could momentarily have
    /// observed one view's slice without another's.
    /// </summary>
    public static readonly Counter<long> ViewCrossTreeJointViolation =
        Meter.CreateCounter<long>("orleans.lattice.view.cross_tree_joint_violation", unit: "{degradation}",
            description: "Cross-tree view batches that degraded to per-tree-slice atomicity because a participant view did not become ready within the bounded readiness timeout.");

    /// <summary>
    /// Counter of lag-budget evictions: a view exceeded its per-view
    /// <see cref="LatticeViewOptions.MaxLagBudget"/> (chronically slow, or a crashed
    /// maintainer that only reactivated on a keepalive tick) and was force-evicted -
    /// the maintainer unpinned the source WAL (so a chronically-lagging or dead view
    /// can no longer hold the WAL garbage collector) and re-onboarded the view via a
    /// rebuild from current committed source state, which re-pins at the rebuilt
    /// head. Tagged with <see cref="TagView"/>. A non-zero value means the view fell
    /// further behind than its configured budget at least once; the view still
    /// converges via the rebuild, but the bounded backlog was dropped rather than
    /// tail-replayed.
    /// </summary>
    public static readonly Counter<long> ViewLagBudgetEviction =
        Meter.CreateCounter<long>("orleans.lattice.view.lag_budget_eviction", unit: "{eviction}",
            description: "Views force-evicted (WAL unpinned and rebuilt) because they exceeded their configured MaxLagBudget.");

    /// <summary>
    /// Counter of background drain passes that observed the source tree under WAL
    /// saturation back-pressure and consequently reduced their footprint - a
    /// scaled-down batch size and, for a background timer tick, a deferral of the
    /// next pass - so the asynchronous maintainer hands client concurrency back to
    /// the foreground writer rather than competing with it. Tagged with
    /// <see cref="TagView"/> and <see cref="TagWalSaturationState"/> (the observed
    /// source regime, <c>throttled</c> or <c>saturated</c>). A sustained non-zero
    /// rate means the source tree is hot enough that the view is deliberately
    /// lagging to protect foreground throughput; it converges once the source
    /// recovers. Never recorded while the source is <c>healthy</c> or when
    /// <see cref="LatticeViewOptions.ObeySourceBackpressure"/> is disabled.
    /// </summary>
    public static readonly Counter<long> ViewSourceBackpressure =
        Meter.CreateCounter<long>("orleans.lattice.view.source_backpressure", unit: "{pass}",
            description: "View maintainer drain passes that throttled themselves because the source tree was under WAL saturation back-pressure.");
}
