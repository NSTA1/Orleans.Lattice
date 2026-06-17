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

    /// <summary>Default <see cref="AggregationFanout"/> (a single accumulator per group).</summary>
    public const int DefaultAggregationFanout = 1;

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
}
