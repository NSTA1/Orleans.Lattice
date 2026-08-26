namespace Orleans.Lattice;

/// <summary>
/// Per-view tuning, resolved through the named-options pattern
/// (<c>IOptionsMonitor&lt;LatticeViewOptions&gt;.Get(viewName)</c>), mirroring
/// how replication resolves <c>LatticeReplicationOptions</c> per tree. The
/// unnamed (default) instance applies to every view that has no named override.
/// <para>
/// <see cref="BatchSize"/> and <see cref="CoalesceWindow"/> apply to every view;
/// <see cref="AggregationFanout"/> and <see cref="AggregationMaxGroupEntries"/>
/// apply only to aggregation views.
/// </para>
/// </summary>
public sealed class LatticeViewOptions
{
    /// <summary>Default <see cref="BatchSize"/> (256 entries per drain pass).</summary>
    public const int DefaultBatchSize = 256;

    /// <summary>Default <see cref="CoalesceWindow"/> (50 ms between idle poll passes).</summary>
    public static readonly TimeSpan DefaultCoalesceWindow = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Default <see cref="SourceIdentityBackstopInterval"/> (30 s): a coarse
    /// safety-net cadence for a missed source-tree alias-swap notification. The
    /// maintainer rebinds to a swapped source physical identity reactively via the
    /// <see cref="ITreeAliasObserver"/> push, so this backstop only heals the rare
    /// lost notification; it is deliberately coarse so an idle view does not read
    /// the tree registry on every drain tick.
    /// </summary>
    public static readonly TimeSpan DefaultSourceIdentityBackstopInterval = TimeSpan.FromSeconds(30);

    /// <summary>Default <see cref="AggregationFanout"/> (a single accumulator per group).</summary>
    public const int DefaultAggregationFanout = 1;

    /// <summary>
    /// Default <see cref="MaxLagBudget"/> (<c>0</c>): the lag-budget eviction is
    /// disabled, so a view pins the source WAL for as long as it needs and is never
    /// force-evicted for chronic lag. Opt in by setting a positive entry-count
    /// budget.
    /// </summary>
    public const long DefaultMaxLagBudget = 0;

    /// <summary>
    /// Default <see cref="MaxStagedTransactions"/> (1024 in-flight atomic
    /// batches buffered before the maintainer falls back to a rebuild).
    /// </summary>
    public const int DefaultMaxStagedTransactions = 1024;

    /// <summary>
    /// Default <see cref="MaxStagedBytes"/> (64 MiB of buffered prepared-entry
    /// payload before the maintainer falls back to a rebuild).
    /// </summary>
    public const long DefaultMaxStagedBytes = 64L * 1024 * 1024;

    /// <summary>
    /// Default <see cref="ReadHandleCacheTtl"/> (1 second): how long a view read
    /// handle reuses its cached active-generation tree id before re-resolving it
    /// from the maintainer.
    /// </summary>
    public static readonly TimeSpan DefaultReadHandleCacheTtl = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Default <see cref="OldGenerationReclaimGrace"/> (5 seconds): how long a
    /// swapped-out view generation tree is retained before it is reclaimed.
    /// Comfortably exceeds <see cref="DefaultReadHandleCacheTtl"/> so a reader on
    /// the stale prior generation has refreshed before its tree is deleted.
    /// </summary>
    public static readonly TimeSpan DefaultOldGenerationReclaimGrace = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Default <see cref="CrossTreeReadinessTimeout"/> (5 seconds): how long a
    /// view waits for every other participant view of a cross-tree atomic write
    /// to become ready before it degrades to per-tree-slice atomicity.
    /// </summary>
    public static readonly TimeSpan DefaultCrossTreeReadinessTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Default <see cref="LagEvictionCooldown"/> (30 seconds): the minimum interval
    /// between two lag-budget force-evictions of the same maintainer, so a view
    /// kept chronically over budget by sustained writes is not rebuilt on every
    /// drain (thrashing) but at most once per cooldown, draining normally in
    /// between.
    /// </summary>
    public static readonly TimeSpan DefaultLagEvictionCooldown = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Default <see cref="ObeySourceBackpressure"/> (<c>true</c>): the maintainer
    /// honours the source tree's WAL saturation signal and throttles its own drain.
    /// </summary>
    public const bool DefaultObeySourceBackpressure = true;

    /// <summary>
    /// Default <see cref="ThrottledBatchRatio"/> (<c>0.5</c>): drain half the
    /// configured <see cref="BatchSize"/> per pass while the source is throttled.
    /// </summary>
    public const double DefaultThrottledBatchRatio = 0.5d;

    /// <summary>
    /// Default <see cref="ThrottledPauseMs"/> (50 ms): skip background drain ticks
    /// for this long after a pass that observed a throttled source.
    /// </summary>
    public const int DefaultThrottledPauseMs = 50;

    /// <summary>
    /// Default <see cref="SaturatedBatchSize"/> (<c>16</c> entries): the small
    /// drip-feed batch the maintainer drains per pass while the source is saturated.
    /// </summary>
    public const int DefaultSaturatedBatchSize = 16;

    /// <summary>
    /// Default <see cref="SaturatedPauseMs"/> (500 ms): skip background drain ticks
    /// for this long after a pass that observed a saturated source.
    /// </summary>
    public const int DefaultSaturatedPauseMs = 500;

    /// <summary>
    /// Default <see cref="HistoryHybridFullValueWindow"/> (5 minutes): under
    /// <see cref="HistoryRetentionMode.Hybrid"/>, a revision keeps its full value
    /// bytes only while its apply-time age is within this window; older revisions
    /// are shaped to metadata.
    /// </summary>
    public static readonly TimeSpan DefaultHistoryHybridFullValueWindow = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum number of source WAL entries the maintainer reads and applies in a
    /// single drain pass per source shard before checkpointing. Must be positive;
    /// the registered validator rejects a non-positive value at first resolve.
    /// </summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>
    /// Idle poll cadence: how long the maintainer waits before re-checking the
    /// source WAL for new entries once it has drained to the head. Also bounds how
    /// long repeated writes to the same view key are batched together for
    /// last-writer-wins coalescing. Must be greater than zero.
    /// </summary>
    public TimeSpan CoalesceWindow { get; set; } = DefaultCoalesceWindow;

    /// <summary>
    /// Coarse safety-net cadence at which the maintainer re-resolves its source
    /// tree's physical identity from the tree registry to heal a missed alias-swap
    /// notification. In steady state the maintainer does <b>not</b> read the
    /// registry on a drain tick: it binds once per activation and rebinds
    /// reactively when the core registry fires an
    /// <see cref="ITreeAliasObserver"/> alias-change push
    /// (<c>NotifySourceIdentityChangedAsync</c>). This interval only bounds how
    /// long a <i>lost</i> notification (observer transiently unavailable, or a
    /// producer predating this build) can leave the view bound to a retired
    /// physical source before a poll re-resolve catches it. Must be greater than
    /// zero; the registered validator rejects a non-positive value at first
    /// resolve. The default is <see cref="DefaultSourceIdentityBackstopInterval"/>.
    /// </summary>
    public TimeSpan SourceIdentityBackstopInterval { get; set; } = DefaultSourceIdentityBackstopInterval;

    /// <summary>
    /// Number of sub-accumulators an aggregation view shards each group into,
    /// hashed on the source key (<c>group#0..#P-1</c>), to spread the write load
    /// of a hot group whose members would otherwise all fold into one view key.
    /// Reads of a group's aggregate merge the sub-accumulators. Must be at least
    /// 1; the default <c>1</c> is a single accumulator (identical behaviour to no
    /// sharding). Ignored by filter / re-project views.
    /// </summary>
    public int AggregationFanout { get; set; } = DefaultAggregationFanout;

    /// <summary>
    /// Opt-in bound on the per-group exact state an aggregation view keeps for the
    /// kinds that inherently need the full multiset
    /// (<see cref="AggregationKind.Min"/>, <see cref="AggregationKind.Max"/>,
    /// <see cref="AggregationKind.SetUnion"/>). When greater than <c>0</c> the
    /// inverse-contribution rows are capped at this many entries per group shard
    /// (a bounded top-K for min / max; a bounded distinct sample for set-union),
    /// trading exactness for bounded storage on unbounded-cardinality groups. The
    /// default <c>0</c> keeps exact state. Ignored by
    /// <see cref="AggregationKind.Count"/> and <see cref="AggregationKind.Sum"/>,
    /// which never keep per-member state.
    /// </summary>
    public int AggregationMaxGroupEntries { get; set; }

    /// <summary>
    /// Upper bound on the number of in-flight atomic-write transactions the
    /// maintainer stages (prepared-but-not-yet-committed batches buffered by
    /// <see cref="LatticeMutation.TransactionId"/>) before it abandons
    /// incremental staging and falls back to a full rebuild from current
    /// source state. Bounds maintainer memory and the WAL-GC blocked-floor pin
    /// when a flood of atomic writes is in flight or a saga terminal is lost.
    /// Must be at least 1; the default is
    /// <see cref="DefaultMaxStagedTransactions"/>.
    /// </summary>
    public int MaxStagedTransactions { get; set; } = DefaultMaxStagedTransactions;

    /// <summary>
    /// Upper bound, in bytes, on the total prepared-entry payload (key plus
    /// value octets) the maintainer buffers across every in-flight atomic-write
    /// transaction before it abandons incremental staging and falls back to a
    /// full rebuild. Complements <see cref="MaxStagedTransactions"/> for the
    /// few-but-huge-batch shape. Must be at least 1; the default is
    /// <see cref="DefaultMaxStagedBytes"/>.
    /// </summary>
    public long MaxStagedBytes { get; set; } = DefaultMaxStagedBytes;

    /// <summary>
    /// How long a view read handle reuses its cached active-generation tree id
    /// before re-resolving it from the maintainer. After a shadow-swap rebuild a
    /// reader may serve the prior (fully-built, slightly stale) generation for up
    /// to this window before it observes the swap; it never serves a half-built or
    /// empty tree. Must be greater than zero; the default is
    /// <see cref="DefaultReadHandleCacheTtl"/>.
    /// </summary>
    public TimeSpan ReadHandleCacheTtl { get; set; } = DefaultReadHandleCacheTtl;

    /// <summary>
    /// How long a swapped-out view generation tree is retained after a
    /// shadow-swap before the maintainer reclaims (deletes) it. Must exceed
    /// <see cref="ReadHandleCacheTtl"/> so a reader still holding the prior
    /// generation's cached tree id has refreshed before its tree is deleted; the
    /// registered validator rejects a value that does not. The default is
    /// <see cref="DefaultOldGenerationReclaimGrace"/>.
    /// </summary>
    public TimeSpan OldGenerationReclaimGrace { get; set; } = DefaultOldGenerationReclaimGrace;

    /// <summary>
    /// Bounded interval a view's maintainer waits for every other participant
    /// view of a cross-tree atomic write to register its ready slice before it
    /// gives up on the joint all-or-nothing flip and degrades to per-tree-slice
    /// atomicity (each present view flips its own slice atomically into its own
    /// view tree, a joint-atomicity-violation metric is emitted, and a reconcile
    /// is scheduled). The bound exists so a permanently-unavailable participant
    /// view (cluster partition / crashed maintainer) cannot pin the source WAL
    /// indefinitely: liveness is chosen over an indefinite stall. Must be greater
    /// than zero; the registered validator rejects a non-positive value at first
    /// resolve. The default is <see cref="DefaultCrossTreeReadinessTimeout"/>.
    /// </summary>
    public TimeSpan CrossTreeReadinessTimeout { get; set; } = DefaultCrossTreeReadinessTimeout;

    /// <summary>
    /// How this view's tree is made available across replicating clusters. The
    /// default <see cref="LatticeViewReplicationMode.DeriveLocally"/> runs the
    /// maintainer on every cluster and never replicates the view's data;
    /// <see cref="LatticeViewReplicationMode.ShipView"/> runs the maintainer only on
    /// one producer and replicates the view tree to consumers. When the source tree
    /// is also replicated, <see cref="ShipViewProducerClusterId"/> must explicitly
    /// select that producer; otherwise local source-WAL ownership identifies it. A
    /// <see cref="LatticeViewReplicationMode.ShipView"/> view's
    /// <c>view-{name}</c> tree must be declared in the replication configuration's
    /// replicated-trees map (so consumers receive it), and a
    /// <see cref="LatticeViewReplicationMode.DeriveLocally"/> view's tree must
    /// <i>not</i> be (it would create a second writer); the registered startup
    /// validator rejects either misconfiguration at silo start. Replication mode
    /// and producer identity are fixed for a view name; create a new view name when
    /// changing topology so pre-existing view WAL history is never reclassified.
    /// </summary>
    public LatticeViewReplicationMode ReplicationMode { get; set; } = LatticeViewReplicationMode.DeriveLocally;

    /// <summary>
    /// Stable replication cluster id of the single maintainer for a
    /// <see cref="LatticeViewReplicationMode.ShipView"/> whose source tree is also
    /// replicated. Compared case-sensitively with
    /// <see cref="ILatticeReplicationContext.LocalReplicaId"/>. Must be
    /// <see langword="null"/> when the source tree is not replicated, because that
    /// source-less-consumer topology infers its producer from local source-WAL
    /// ownership. The startup and runtime topology guards reject either an
    /// ambiguous replicated-source view without this value or an explicit value
    /// on a non-replicated source.
    /// </summary>
    public string? ShipViewProducerClusterId { get; set; }

    /// <summary>
    /// Opt-in upper bound, in committed-but-unapplied source entries, on how far a
    /// view may fall behind the source before the maintainer force-evicts it: it
    /// unpins the source WAL (so the WAL garbage collector is no longer held by a
    /// chronically-slow or crashed view) and re-onboards the view via a rebuild
    /// from current committed source state, which re-pins at the rebuilt head. This
    /// bounds WAL retention regardless of view health and doubles as
    /// dead-maintainer detection. The default <see cref="DefaultMaxLagBudget"/>
    /// (<c>0</c>) disables eviction (unbounded lag). Must not be negative; the
    /// registered validator rejects a negative value at first resolve. Size
    /// <c>LatticeOptions.WalRetention</c> at or above the expected steady-state
    /// view lag so the budget is a hard backstop rather than a routine trigger.
    /// </summary>
    public long MaxLagBudget { get; set; } = DefaultMaxLagBudget;

    /// <summary>
    /// The minimum interval between two consecutive lag-budget force-evictions of
    /// the same maintainer. Once a view is evicted (unpinned + rebuilt) for
    /// exceeding <see cref="MaxLagBudget"/>, it will not be force-evicted again
    /// until this cooldown elapses; in the meantime it keeps draining normally, so
    /// a view held chronically over budget by sustained writes is not rebuilt on
    /// every drain (thrashing). A non-positive value falls back to
    /// <see cref="DefaultLagEvictionCooldown"/>. Has no effect when
    /// <see cref="MaxLagBudget"/> is <c>0</c> (eviction disabled).
    /// </summary>
    public TimeSpan LagEvictionCooldown { get; set; } = DefaultLagEvictionCooldown;

    /// <summary>
    /// Whether the maintainer obeys the source tree's WAL saturation back-pressure
    /// signal (<c>IWalSaturationSignal</c>, produced by <c>AddLattice</c>). When
    /// <c>true</c> (the default) a maintainer whose source tree is
    /// <see cref="WalSaturationState.Throttled"/> or
    /// <see cref="WalSaturationState.Saturated"/> shrinks its per-pass drain batch
    /// (<see cref="ThrottledBatchRatio"/> / <see cref="SaturatedBatchSize"/>) and
    /// defers its next background drain tick (<see cref="ThrottledPauseMs"/> /
    /// <see cref="SaturatedPauseMs"/>), so the asynchronous view yields client
    /// concurrency to the foreground writer instead of competing with it. Only
    /// background timer drains are deferred; a foreground read-your-writes drain
    /// (<c>WaitForApplyAsync</c>) still makes progress, just with a smaller batch.
    /// Set to <c>false</c> to keep the maintainer draining at full rate regardless
    /// of source pressure (the view never self-throttles). The throttle engages
    /// only while the source is actually saturated, so leaving it on costs nothing
    /// on a healthy source. Defaults to
    /// <see cref="DefaultObeySourceBackpressure"/>.
    /// </summary>
    public bool ObeySourceBackpressure { get; set; } = DefaultObeySourceBackpressure;

    /// <summary>
    /// Fraction of <see cref="BatchSize"/> the maintainer drains per pass while the
    /// source tree is <see cref="WalSaturationState.Throttled"/>. The effective
    /// batch is <c>ceil(BatchSize * ratio)</c> clamped to <c>[1, BatchSize]</c>; the
    /// ratio itself is clamped to <c>[0, 1]</c> so an out-of-range value can never
    /// inflate the batch. Ignored when <see cref="ObeySourceBackpressure"/> is
    /// <c>false</c>. Defaults to <see cref="DefaultThrottledBatchRatio"/>.
    /// </summary>
    public double ThrottledBatchRatio { get; set; } = DefaultThrottledBatchRatio;

    /// <summary>
    /// Milliseconds the maintainer skips background drain ticks after a pass that
    /// observed a <see cref="WalSaturationState.Throttled"/> source, lengthening the
    /// effective poll cadence under pressure. A value less than or equal to zero
    /// disables the deferral (batch scaling still applies). Ignored when
    /// <see cref="ObeySourceBackpressure"/> is <c>false</c>. Defaults to
    /// <see cref="DefaultThrottledPauseMs"/>.
    /// </summary>
    public int ThrottledPauseMs { get; set; } = DefaultThrottledPauseMs;

    /// <summary>
    /// Absolute per-pass drain batch the maintainer uses while the source tree is
    /// <see cref="WalSaturationState.Saturated"/>, clamped to <c>[1, BatchSize]</c>.
    /// A small drip-feed keeps the view converging without piling read/write
    /// concurrency onto an already-saturated source. Ignored when
    /// <see cref="ObeySourceBackpressure"/> is <c>false</c>. Defaults to
    /// <see cref="DefaultSaturatedBatchSize"/>.
    /// </summary>
    public int SaturatedBatchSize { get; set; } = DefaultSaturatedBatchSize;

    /// <summary>
    /// Milliseconds the maintainer skips background drain ticks after a pass that
    /// observed a <see cref="WalSaturationState.Saturated"/> source. A value less
    /// than or equal to zero disables the deferral (batch scaling still applies).
    /// Ignored when <see cref="ObeySourceBackpressure"/> is <c>false</c>. Defaults
    /// to <see cref="DefaultSaturatedPauseMs"/>.
    /// </summary>
    public int SaturatedPauseMs { get; set; } = DefaultSaturatedPauseMs;

    /// <summary>
    /// Under <see cref="HistoryRetentionMode.Hybrid"/> on a durable history view,
    /// the maximum apply-time age of a revision for which the maintainer keeps the
    /// full LWW value bytes; an older revision (drained from a backlog or a
    /// catch-up replay) is shaped to metadata only. Bounds full-byte storage to the
    /// recent tail while keeping an unbounded metadata-only timeline behind it.
    /// A non-positive value degrades hybrid to metadata-only. Ignored by the other
    /// retention modes and by non-history views. Defaults to
    /// <see cref="DefaultHistoryHybridFullValueWindow"/>.
    /// </summary>
    public TimeSpan HistoryHybridFullValueWindow { get; set; } = DefaultHistoryHybridFullValueWindow;
}
