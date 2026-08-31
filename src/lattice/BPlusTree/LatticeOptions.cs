namespace Orleans.Lattice;

/// <summary>
/// Configuration options for a Lattice tree. Register a named instance to
/// override settings for a specific tree:
/// <code>
/// siloBuilder.Services.Configure&lt;LatticeOptions&gt;("my-tree", o => o.CacheTtl = TimeSpan.FromMilliseconds(100));
/// </code>
/// The unnamed (default) instance applies to all trees that do not have a
/// named override.
/// <para>
/// Structural sizing - the number of keys per leaf, the number of children
/// per internal node, and the shard count - is <em>not</em> configured here.
/// Those values are pinned in the internal tree registry at first-use
/// (seeded from the canonical defaults in
/// <see cref="Orleans.Lattice.BPlusTree.LatticeConstants"/>) and are mutable
/// only through <see cref="ILattice.ResizeAsync"/> (leaf / internal capacity)
/// or <see cref="ILattice.ReshardAsync"/> (shard count). This prevents
/// accidental divergence between the layout a tree was built with and a
/// later configuration change.
/// </para>
/// <para>
/// The virtual shard space is also not configurable here. It is a compile-time
/// constant,
/// <see cref="Orleans.Lattice.BPlusTree.LatticeConstants.DefaultVirtualShardCount"/>
/// (4096), because persisted shard maps reference virtual slots by integer
/// index and changing the constant would invalidate every stored map.
/// </para>
/// </summary>
public class LatticeOptions
{
    /// <summary>Number of keys per page returned by <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetSortedKeysBatchAsync"/>.</summary>
    public int KeysPageSize { get; set; } = DefaultKeysPageSize;

    /// <summary>
    /// Optional bound on the number of entries a cluster-internal
    /// <see cref="ILatticeQueue{T}"/> backed by this tree's options may
    /// hold. When set, enqueueing past the bound evicts the oldest entry
    /// (FIFO eviction) before appending the new one; <see langword="null"/>
    /// (the default) leaves the queue unbounded. When set it must be at
    /// least <c>1</c>, enforced by the options validator. Resolved per
    /// queue via <c>IOptionsMonitor&lt;LatticeOptions&gt;.Get(queueName)</c>.
    /// </summary>
    public int? QueueCapacity { get; set; }

    /// <summary>
    /// Optional upper bound on the number of characters in a key accepted by
    /// the <see cref="ILattice"/> write surface (<see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>
    /// and its TTL overload, <see cref="ILattice.SetIfVersionAsync"/>,
    /// <see cref="ILattice.GetOrSetAsync"/>, <see cref="ILattice.SetManyAsync"/>,
    /// and the CRDT delta-apply path). When set, a write whose key is longer
    /// than this bound is rejected with an <see cref="ArgumentException"/>
    /// before any shard work, so a client cannot drive unbounded heap growth
    /// by writing pathologically large keys (memory-exhaustion DoS).
    /// <see langword="null"/> (the default) leaves key length unbounded,
    /// preserving the historical behaviour. When set it must be at least
    /// <c>1</c>, enforced by the options validator.
    /// </summary>
    public int? MaxKeyLength { get; set; }

    /// <summary>
    /// Optional upper bound, in bytes, on the size of a value (or CRDT delta)
    /// accepted by the <see cref="ILattice"/> write surface. When set, a write
    /// whose value exceeds this many bytes is rejected with an
    /// <see cref="ArgumentException"/> before any shard work, so a client
    /// cannot drive unbounded heap growth by writing pathologically large
    /// values (memory-exhaustion DoS). <see langword="null"/> (the default)
    /// leaves value size unbounded, preserving the historical behaviour. When
    /// set it must be at least <c>1</c>, enforced by the options validator.
    /// </summary>
    public int? MaxValueSizeBytes { get; set; }

    /// <summary>
    /// Optional enforcing cap on the number of live (non-tombstone) keys a
    /// single tree may hold. When set and breached, a locally-authored write to
    /// the tree is rejected with a <see cref="LatticeQuotaExceededException"/>
    /// carrying the <c>keys</c> dimension, so a misbehaving multi-tenant
    /// workload cannot grow one tree without bound and starve storage for the
    /// others. <see langword="null"/> (the default) leaves the live-key count
    /// unbounded; enforcement is strictly opt-in and fail-open. When set it must
    /// be at least <c>1</c>, enforced by the options validator.
    /// <para>
    /// The cap is <b>best-effort and approximate</b>. It is evaluated against a
    /// cached, eventually-consistent per-tree aggregate (the same TTL-coalesced
    /// aggregator that backs the storage-usage gauges), never a per-write
    /// fan-out, so concurrent cross-shard writes can overshoot the cap slightly
    /// before the aggregate refreshes, and a freshly-activated tree fails open
    /// until its first sample lands. Replication and atomic-write-saga apply
    /// paths bypass the cap so an incoming replicated write is never rejected.
    /// Resolved per tree via <c>IOptionsMonitor&lt;LatticeOptions&gt;.Get(treeName)</c>.
    /// </para>
    /// </summary>
    public long? MaxLiveKeys { get; set; } = DefaultMaxLiveKeys;

    /// <summary>
    /// Optional enforcing cap, in bytes, on the estimated retained storage a
    /// single tree may occupy (the same figure the
    /// <c>orleans.lattice.storage.total_bytes</c> gauge reports: WAL rows plus
    /// snapshot blobs plus leaf/shard-root state). When set and breached, a
    /// locally-authored write is rejected with a
    /// <see cref="LatticeQuotaExceededException"/> carrying the <c>bytes</c>
    /// dimension. <see langword="null"/> (the default) leaves estimated bytes
    /// unbounded; enforcement is strictly opt-in and fail-open. When set it must
    /// be at least <c>1</c>, enforced by the options validator. Shares the
    /// best-effort / approximate, replication-bypassing semantics of
    /// <see cref="MaxLiveKeys"/>. Resolved per tree via
    /// <c>IOptionsMonitor&lt;LatticeOptions&gt;.Get(treeName)</c>.
    /// </summary>
    public long? MaxEstimatedBytes { get; set; } = DefaultMaxEstimatedBytes;

    /// <summary>
    /// Optional non-enforcing advisory ceiling on the live (non-tombstone) key
    /// count, used to right-size <see cref="MaxLiveKeys"/> before turning
    /// enforcement on. A tree that exceeds this ceiling is flagged by the
    /// <c>orleans.lattice.admission.over_advisory</c> gauge and every write that
    /// <i>would</i> have been rejected at this ceiling increments the
    /// <c>orleans.lattice.admission.would_reject</c> counter - but no write is
    /// ever rejected. <see langword="null"/> (the default) disables the advisory
    /// signal for the key dimension. When set it must be at least <c>1</c>,
    /// enforced by the options validator.
    /// </summary>
    public long? AdmissionAdvisoryLiveKeys { get; set; } = DefaultAdmissionAdvisoryLiveKeys;

    /// <summary>
    /// Optional non-enforcing advisory ceiling, in bytes, on the estimated
    /// retained storage, used to right-size <see cref="MaxEstimatedBytes"/>
    /// before turning enforcement on. Drives the same non-rejecting
    /// <c>orleans.lattice.admission.over_advisory</c> /
    /// <c>orleans.lattice.admission.would_reject</c> dry-run signals as
    /// <see cref="AdmissionAdvisoryLiveKeys"/>, for the byte dimension.
    /// <see langword="null"/> (the default) disables the advisory signal for the
    /// byte dimension. When set it must be at least <c>1</c>, enforced by the
    /// options validator.
    /// </summary>
    public long? AdmissionAdvisoryBytes { get; set; } = DefaultAdmissionAdvisoryBytes;

    /// <summary>
    /// How long a tombstone must exist before it is eligible for compaction.
    /// A grain reminder fires at this interval; tombstones older than this
    /// grace period are permanently removed. Set to <see cref="Timeout.InfiniteTimeSpan"/>
    /// to disable compaction entirely. Per-tree overrides follow the same
    /// named-options pattern as other properties.
    /// </summary>
    public TimeSpan TombstoneGracePeriod { get; set; } = DefaultTombstoneGracePeriod;

    /// <summary>
    /// Minimum tombstone-to-total ratio (in <c>[0.0, 1.0]</c>) on a single leaf
    /// that triggers an out-of-cycle compaction pass for the leaf's shard,
    /// in addition to the regular reminder-driven cadence governed by
    /// <see cref="TombstoneGracePeriod"/>. The ratio is computed as
    /// <c>tombstones / max(liveKeys + tombstones, 1)</c> on each leaf
    /// commit; when it crosses the threshold, the leaf asks its tree's
    /// compaction grain to schedule a pass for the affected shard.
    /// Set to <c>0.0</c> (the default) to disable ratio-based pre-emption -
    /// only the regular reminder fires.
    /// </summary>
    public double MinTombstoneRatioForCompaction { get; set; } = DefaultMinTombstoneRatioForCompaction;

    /// <summary>
    /// Maximum total entry count (live + tombstones) on a single leaf before
    /// the leaf requests an out-of-cycle compaction pass for its shard.
    /// Provides a size-based safety valve that complements
    /// <see cref="MinTombstoneRatioForCompaction"/>: a small leaf at high
    /// ratio is reaped through the ratio trigger, while a large leaf that
    /// has accumulated tombstones (even at low ratio) is reaped through
    /// this trigger. Only fires when the leaf actually contains at least
    /// one tombstone or expired entry. Set to <c>0</c> (the default) to
    /// disable size-based pre-emption.
    /// </summary>
    public int MaxLeafEntriesBeforeForcedCompaction { get; set; } = DefaultMaxLeafEntriesBeforeForcedCompaction;

    /// <summary>
    /// Minimum interval between consecutive out-of-cycle compaction passes
    /// for the same <c>(treeId, shardIndex)</c> when triggered by
    /// <see cref="MinTombstoneRatioForCompaction"/> or
    /// <see cref="MaxLeafEntriesBeforeForcedCompaction"/>. Prevents a hot
    /// leaf from monopolising the compactor's grain timer with rapid-fire
    /// requests. The cooldown is independent of the regular reminder
    /// cadence: a regular reminder pass always proceeds regardless of any
    /// per-shard cooldown record. Operator-initiated requests via
    /// <c>RequestCompactionAsync</c> bypass the cooldown.
    /// </summary>
    public TimeSpan CompactionTriggerCooldown { get; set; } = DefaultCompactionTriggerCooldown;

    /// <summary>
    /// Per-shard tick interval used by <c>TombstoneCompactionGrain</c>'s
    /// internal grain timer when walking a compaction pass. The compactor
    /// processes one shard per tick and waits this long between ticks so
    /// the grain returns control to the Orleans scheduler between shards
    /// (avoiding a long-running grain call that could hit Orleans timeouts
    /// and starve concurrent <c>RequestCompactionAsync</c> callers). The
    /// interval is a **scheduler-fairness knob, not a grain-deactivation
    /// knob** - leaf activation lifetime is governed by the silo's
    /// <c>GrainCollectionOptions.CollectionAge</c> and is independent of
    /// this value. Lower values speed up full passes (a 1024-shard tree
    /// at 2 s = ~34 min/pass; same tree at 200 ms = ~3.4 min/pass) at
    /// the cost of less scheduler headroom for other grains. Values
    /// below <see cref="MinCompactionShardTickInterval"/> are clamped to
    /// the floor with a one-shot warning per tree per process.
    /// Snapshotted at pass start, so mid-pass option changes do not
    /// retroactively reshape an in-flight pass.
    /// </summary>
    public TimeSpan CompactionShardTickInterval { get; set; } = DefaultCompactionShardTickInterval;

    /// <summary>
    /// Maximum number of leaves the tombstone-compaction coordinator visits
    /// within a single physical shard before yielding for one
    /// <see cref="CompactionShardTickInterval"/>. The leaf walk resumes on
    /// the next timer tick from a persisted in-shard cursor, so progress
    /// survives silo crashes the same way the shard cursor does.
    /// <para>
    /// This is the dominant control on **peak concurrent leaf activations**
    /// during a pass. Within a shard the leaf walk runs back-to-back; the
    /// tick gap applies only between shards. Without batching, a full pass
    /// activates every leaf in the tree at least once, and a pass that
    /// completes inside one <c>GrainCollectionOptions.CollectionAge</c>
    /// window has effectively activated the entire leaf set at once.
    /// Batching caps peak activations to roughly
    /// <c>CompactionLeafBatchSize * (CollectionAge / CompactionShardTickInterval)</c>
    /// regardless of tree size.
    /// </para>
    /// <para>
    /// Default 64 reproduces pre-batching behaviour exactly on shards
    /// with &lt;= 64 leaves (the common case). Values below
    /// <see cref="MinCompactionLeafBatchSize"/> are clamped to the floor
    /// with a one-shot warning per tree per process. Snapshotted at pass
    /// start, so mid-pass option changes do not retroactively reshape an
    /// in-flight pass.
    /// </para>
    /// </summary>
    public int CompactionLeafBatchSize { get; set; } = DefaultCompactionLeafBatchSize;

    /// <summary>
    /// Coalescing window (in milliseconds) for shard-root dirty-leaf marks.
    /// Every routed <c>Delete</c> stamps the destination leaf into an
    /// in-memory pending-marks map; a grain timer on the shard root drains
    /// the map and calls <c>WriteStateAsync</c> exactly once per interval,
    /// regardless of how many distinct leaves were dirtied. Snapshot and
    /// drain calls from the compaction coordinator
    /// (<c>GetDirtyLeavesSinceLastCompactionAsync</c> /
    /// <c>ClearDirtyLeavesUpToAsync</c>) and clean deactivation also flush
    /// pending marks, so persistence is best-effort-coalesced rather than
    /// best-effort-lost.
    /// <para>
    /// Default <c>50 ms</c> trades at most one flush-interval of dirty
    /// signal against eliminating the per-Delete storage write from the
    /// shard-root hot path. Set to <c>0</c> to disable coalescing (every
    /// first-call-per-leaf-per-window persists synchronously, matching
    /// pre-U9h-B behaviour).
    /// </para>
    /// </summary>
    public int DirtyLeafFlushIntervalMs { get; set; } = DefaultDirtyLeafFlushIntervalMs;

    /// <summary>
    /// Number of leaf caches each shard root pre-warms when
    /// <see cref="ILattice.WarmUpAsync"/> runs, ranked by a persisted histogram
    /// of the leaves this shard's reads have visited.
    /// Defaults to <see cref="DefaultLeafCachePreWarmCount"/> (<c>8</c>).
    /// <c>0</c> is the kill switch: it disables the whole feature, so no access
    /// is tracked, nothing is persisted, and warm-up primes nothing - exactly the
    /// behaviour of a deployment from before the feature existed.
    /// <para>
    /// When positive, every routed cache read increments the target leaf's visit
    /// count in a bounded in-memory histogram. A coalescing timer
    /// (<see cref="LeafAccessModelFlushIntervalMs"/>) persists a compact
    /// snapshot of the histogram into the shard root's own state, and a clean
    /// deactivation flushes it once more, so the ranking survives a silo
    /// restart. On the next warm-up the shard root ranks its leaves by observed
    /// read frequency - what fraction of reads land on each leaf, rather than
    /// mere recency - and primes that many
    /// <c>LeafCacheGrain</c> activations with a bounded, best-effort fan-out.
    /// Because the shard root is the only caller of the stateless-worker leaf
    /// cache, the primed activations land on the silo that will serve the
    /// subsequent reads.
    /// </para>
    /// <para>
    /// Capped at <see cref="MaxLeafCachePreWarmCount"/>. A failure to prime any
    /// individual leaf is swallowed - pre-warm can never fail warm-up.
    /// </para>
    /// </summary>
    public int LeafCachePreWarmCount { get; set; } = DefaultLeafCachePreWarmCount;

    /// <summary>
    /// Coalescing window (in milliseconds) for persisting the shard root's
    /// leaf-access histogram. A grain timer drains the model at most once
    /// per interval and only when it has changed, so read traffic never pays a
    /// storage write. Clean deactivation always performs a final flush.
    /// <para>
    /// Set to <c>0</c> to persist only on deactivation, which is free under
    /// read load but loses the model entirely on an ungraceful silo kill.
    /// Ignored when <see cref="LeafCachePreWarmCount"/> is <c>0</c>.
    /// </para>
    /// </summary>
    public int LeafAccessModelFlushIntervalMs { get; set; } = DefaultLeafAccessModelFlushIntervalMs;

    /// <summary>
    /// How long a soft-deleted tree is retained before its grains are permanently
    /// purged. During this window the tree is inaccessible (reads and writes throw
    /// <see cref="InvalidOperationException"/>), but its data still exists in storage
    /// and could theoretically be recovered by clearing the <c>IsDeleted</c> flag.
    /// After the duration elapses, a grain reminder triggers a full purge that
    /// walks every shard and clears all leaf and internal node state.
    /// Set to <see cref="TimeSpan.Zero"/> for immediate purge on the next reminder tick.
    /// </summary>
    public TimeSpan SoftDeleteDuration { get; set; } = DefaultSoftDeleteDuration;

    /// <summary>
    /// Minimum time between consecutive delta refreshes from the primary leaf
    /// in the <c>LeafCacheGrain</c>. When set to <see cref="TimeSpan.Zero"/>
    /// (the default), every read triggers a delta refresh - the version-vector
    /// comparison on the primary is cheap but the RPC overhead remains. Setting
    /// a non-zero value (e.g. 100 ms) allows the cache to serve reads from its
    /// local dictionary without contacting the primary, trading freshness for
    /// lower read latency. This option can be changed freely at any time.
    /// </summary>
    public TimeSpan CacheTtl { get; set; } = DefaultCacheTtl;

    /// <summary>
    /// Optional upper bound, in bytes, on the resident value-payload memory a
    /// single <c>LeafCacheGrain</c> activation may hold in its read-through
    /// mirror. <c>null</c> (the default) leaves the mirror unbounded - it
    /// grows to a faithful 1:1 copy of the primary leaf's live entry set,
    /// which is the lowest-latency configuration but scales per-silo memory
    /// linearly with the touched-leaf entry count.
    /// <para>
    /// When set to a positive value, the cache evicts <em>value payloads
    /// only</em> (never whole rows) in least-recently-used order once the sum
    /// of resident <c>byte[]</c> payload lengths would exceed the budget. The
    /// per-row metadata envelope (timestamp, delivery-sequence position,
    /// tombstone / migration flags) is always retained, so eviction cannot
    /// violate the delta-refresh cursor, pending-key, moved-away, or
    /// migrated-entry contracts. A read that lands on an evicted payload
    /// transparently delegates to the primary leaf (one RPC) and is counted as
    /// a cache miss; hot keys stay resident and continue to serve from memory.
    /// Only the value payload is bounded - the retained envelope metadata
    /// (tens of bytes per row) is not counted against this budget.
    /// </para>
    /// <para>
    /// Intended as deploy-time configuration; the budget is re-read on each
    /// cache refresh so a running silo honours option changes, but toggling it
    /// on a warm activation only bounds payloads merged after the change.
    /// </para>
    /// </summary>
    public long? MaxCacheValueBytes { get; set; } = DefaultMaxCacheValueBytes;

    /// <summary>Default value for <see cref="SoftDeleteDuration"/> (72 hours).</summary>
    public static readonly TimeSpan DefaultSoftDeleteDuration = TimeSpan.FromHours(72);

    /// <summary>Default value for <see cref="TombstoneGracePeriod"/> (24 hours).</summary>
    public static readonly TimeSpan DefaultTombstoneGracePeriod = TimeSpan.FromHours(24);

    /// <summary>Default value for <see cref="MinTombstoneRatioForCompaction"/> (<c>0.0</c> - disabled).</summary>
    public const double DefaultMinTombstoneRatioForCompaction = 0.0;

    /// <summary>
    /// Default value for <see cref="MaxLeafEntriesBeforeForcedCompaction"/>
    /// (<c>0</c> - disabled).
    /// <para>
    /// Deliberately left disabled. Arming a size trigger cluster-wide has two
    /// costs that a host is better placed than the library to accept. First,
    /// <c>TombstoneCompactionGrain</c> only opens the compaction
    /// <c>trigger</c> metric-tag scope when this knob or
    /// <see cref="MinTombstoneRatioForCompaction"/> is non-default, so turning
    /// it on by default would start tagging previously untagged per-leaf
    /// instruments and silently break every dashboard filtering on the empty
    /// trigger label. Second, the leaf evaluates the trigger by walking its whole
    /// entry table on every successful foreground commit, which forces a
    /// partially hydrated leaf to materialise in full and works against
    /// <see cref="LeafPartialHydrationEnabled"/>. Nothing is lost by leaving it
    /// off: the reminder-driven compaction pass still reaps tombstones on its
    /// regular cadence, and a host that knows its trees churn can arm the
    /// trigger per tree (as the repository-context host does for its churn
    /// trees).
    /// </para>
    /// </summary>
    public const int DefaultMaxLeafEntriesBeforeForcedCompaction = 0;

    /// <summary>Default value for <see cref="CompactionTriggerCooldown"/> (5 minutes).</summary>
    public static readonly TimeSpan DefaultCompactionTriggerCooldown = TimeSpan.FromMinutes(5);

    /// <summary>Default value for <see cref="CompactionShardTickInterval"/> (500 milliseconds).</summary>
    public static readonly TimeSpan DefaultCompactionShardTickInterval = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Minimum effective value for <see cref="CompactionShardTickInterval"/>
    /// (100 milliseconds). Configured values below this floor are clamped
    /// up by <see cref="Orleans.Lattice.BPlusTree.LatticeOptionsResolver"/> with a one-shot warning
    /// per tree per process. The floor exists so a pathological setting
    /// (e.g. 1 ms) cannot starve the rest of the compactor grain's
    /// scheduler quota by yielding too briefly between shard walks.
    /// </summary>
    public static readonly TimeSpan MinCompactionShardTickInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>Default value for <see cref="CompactionLeafBatchSize"/> (64 leaves).</summary>
    public const int DefaultCompactionLeafBatchSize = 64;

    /// <summary>
    /// Default value for <see cref="DirtyLeafFlushIntervalMs"/> (50 ms).
    /// Coalesces shard-root dirty-leaf marks into one <c>WriteStateAsync</c>
    /// per window, removing the per-Delete storage write from the hot path.
    /// </summary>
    public const int DefaultDirtyLeafFlushIntervalMs = 50;

    /// <summary>
    /// Default value for <see cref="LeafCachePreWarmCount"/> (<c>8</c>).
    /// <para>
    /// Leaf-cache pre-warm is <b>on by default</b>: a cold-start improvement that
    /// has to be switched on heals nothing, because the deployments that most need
    /// it are precisely the ones nobody reconfigures. The value matches the shard
    /// root's own pre-warm fan-out concurrency, so warm-up issues exactly one
    /// bounded wave of priming calls per shard and never queues behind its own
    /// semaphore - the warm-up cost is one round trip regardless of shard count.
    /// It is one eighth of <see cref="MaxLeafCachePreWarmCount"/>, leaving ample
    /// headroom for a deployment with a wider hot set.
    /// </para>
    /// <para>
    /// Set <see cref="LeafCachePreWarmCount"/> to <c>0</c> to restore the previous
    /// behaviour exactly: no access tracking, no persisted model, and a warm-up
    /// that primes nothing.
    /// </para>
    /// </summary>
    public const int DefaultLeafCachePreWarmCount = 8;

    /// <summary>
    /// Hard upper bound on <see cref="LeafCachePreWarmCount"/> (<c>64</c>).
    /// Matches the number of leaves the shard root persists, so a larger
    /// request could never be satisfied from the durable model anyway.
    /// </summary>
    public const int MaxLeafCachePreWarmCount = 64;

    /// <summary>
    /// Default value for <see cref="LeafAccessModelFlushIntervalMs"/>
    /// (<c>30000 ms</c>). One small shard-root state write per 30 seconds of
    /// read activity is negligible next to the read traffic that produced it,
    /// and bounds the model loss from an ungraceful silo kill to one window.
    /// </summary>
    public const int DefaultLeafAccessModelFlushIntervalMs = 30_000;

    /// <summary>
    /// Minimum effective value for <see cref="CompactionLeafBatchSize"/>
    /// (<c>1</c> leaf). Configured values below this floor are clamped up
    /// by <see cref="Orleans.Lattice.BPlusTree.LatticeOptionsResolver"/> with a one-shot warning per
    /// tree per process. A batch size of zero would stall the pass
    /// indefinitely; a batch size of one is the legitimate "yield after
    /// every leaf" extreme.
    /// </summary>
    public const int MinCompactionLeafBatchSize = 1;

    /// <summary>Default value for <see cref="KeysPageSize"/>.</summary>
    public const int DefaultKeysPageSize = 512;

    /// <summary>Default value for <see cref="CacheTtl"/> (zero - refresh on every read).</summary>
    public static readonly TimeSpan DefaultCacheTtl = TimeSpan.Zero;

    /// <summary>Default value for <see cref="MaxCacheValueBytes"/> (<c>null</c> - the read-through cache mirror is unbounded).</summary>
    public static readonly long? DefaultMaxCacheValueBytes = null;

    /// <summary>Default value for <see cref="MaxLiveKeys"/> (<c>null</c> - live-key admission control disabled).</summary>
    public static readonly long? DefaultMaxLiveKeys = null;

    /// <summary>Default value for <see cref="MaxEstimatedBytes"/> (<c>null</c> - byte admission control disabled).</summary>
    public static readonly long? DefaultMaxEstimatedBytes = null;

    /// <summary>Default value for <see cref="AdmissionAdvisoryLiveKeys"/> (<c>null</c> - the live-key advisory dry-run signal is disabled).</summary>
    public static readonly long? DefaultAdmissionAdvisoryLiveKeys = null;

    /// <summary>Default value for <see cref="AdmissionAdvisoryBytes"/> (<c>null</c> - the byte advisory dry-run signal is disabled).</summary>
    public static readonly long? DefaultAdmissionAdvisoryBytes = null;

    /// <summary>
    /// When <c>true</c>, <see cref="ILattice.KeysAsync"/> pre-fetches the next page
    /// from each shard in parallel while the current page is being consumed,
    /// hiding per-shard grain-call latency during ordered scans. Because pre-fetched
    /// pages are held in memory until consumed, callers that abort iteration early
    /// (e.g. <c>Take(n)</c>) pay for pages they never read. Disabled by default.
    /// This option can also be overridden per-call via the <c>prefetch</c> parameter
    /// on <see cref="ILattice.KeysAsync"/>.
    /// </summary>
    public bool PrefetchKeysScan { get; set; } = DefaultPrefetchKeysScan;

    /// <summary>Default value for <see cref="PrefetchKeysScan"/> (<c>false</c>).</summary>
    public const bool DefaultPrefetchKeysScan = false;

    /// <summary>
    /// When <c>true</c>, <see cref="ILattice.EntriesAsync"/> pre-fetches the next page
    /// from each shard in parallel while the current page is being consumed,
    /// hiding per-shard grain-call latency during ordered entry scans. Because
    /// entries carry <c>byte[]</c> values, pre-fetched pages increase in-flight memory
    /// proportionally to <c>shardCount × KeysPageSize × avgValueSize</c>, so this is
    /// gated separately from <see cref="PrefetchKeysScan"/>. Callers that abort
    /// iteration early (e.g. <c>Take(n)</c>) pay for pages they never read.
    /// Disabled by default. This option can also be overridden per-call via the
    /// <c>prefetch</c> parameter on <see cref="ILattice.EntriesAsync"/>.
    /// </summary>
    public bool PrefetchEntriesScan { get; set; } = DefaultPrefetchEntriesScan;

    /// <summary>Default value for <see cref="PrefetchEntriesScan"/> (<c>false</c>).</summary>
    public const bool DefaultPrefetchEntriesScan = false;

    /// <summary>
    /// When <c>true</c>, the autonomic <c>HotShardMonitorGrain</c> periodically
    /// polls each physical shard's hotness counters (<see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetHotnessAsync"/>)
    /// and triggers an online adaptive split when the observed
    /// operations-per-second exceeds <see cref="HotShardOpsPerSecondThreshold"/>.
    /// Splits happen fully online via shadow-writing - no shard is ever taken
    /// offline. Set to <c>false</c> to disable autonomic splitting entirely.
    /// </summary>
    public bool AutoSplitEnabled { get; set; } = DefaultAutoSplitEnabled;

    /// <summary>Default value for <see cref="AutoSplitEnabled"/> (<c>true</c>).</summary>
    public const bool DefaultAutoSplitEnabled = true;

    /// <summary>
    /// Operations-per-second threshold above which a shard is considered hot
    /// and eligible for an autonomic split. Computed as
    /// <c>(reads + writes) / window.TotalSeconds</c> over the period reported
    /// by <see cref="Orleans.Lattice.BPlusTree.ShardHotness.Window"/>. Lower values trigger splits more
    /// aggressively; the default of 200 ops/s is intentionally low so splits
    /// occur well before throughput degrades.
    /// </summary>
    public int HotShardOpsPerSecondThreshold { get; set; } = DefaultHotShardOpsPerSecondThreshold;

    /// <summary>Default value for <see cref="HotShardOpsPerSecondThreshold"/> (200).</summary>
    public const int DefaultHotShardOpsPerSecondThreshold = 200;

    /// <summary>
    /// How often the autonomic monitor polls shard hotness counters.
    /// Shorter intervals detect hot shards faster at a small CPU cost.
    /// </summary>
    public TimeSpan HotShardSampleInterval { get; set; } = DefaultHotShardSampleInterval;

    /// <summary>Default value for <see cref="HotShardSampleInterval"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultHotShardSampleInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Minimum interval between consecutive autonomic splits of the same
    /// physical shard. Prevents thrashing when a single hot virtual slot
    /// dominates traffic (the slot will be split once, then need to wait this
    /// long before the new shard can be split again).
    /// </summary>
    public TimeSpan HotShardSplitCooldown { get; set; } = DefaultHotShardSplitCooldown;

    /// <summary>Default value for <see cref="HotShardSplitCooldown"/> (2 minutes).</summary>
    public static readonly TimeSpan DefaultHotShardSplitCooldown = TimeSpan.FromMinutes(2);

    /// <summary>
    /// How disproportionately loaded the hottest shard must be, relative to the
    /// tree's own median shard load, before any autonomic split is admitted.
    /// <para>
    /// <see cref="HotShardOpsPerSecondThreshold"/> measures how <em>fast</em> a
    /// shard is; this measures the <em>shape</em> of the load. A bulk ingest
    /// streams writes uniformly across the whole key space, so every shard sits
    /// far above the rate threshold while none is disproportionately loaded:
    /// splitting cannot relieve it (each half is equally hot) and the only
    /// durable effect is a permanent multiplication of grain activations. The
    /// monitor therefore computes <c>maxShardRate / medianShardRate</c> across
    /// the tree's physical shards each pass and admits splits only when that
    /// ratio reaches this value. A uniformly loaded tree sits at approximately
    /// <c>1.0</c> and is never a split candidate; a genuinely skewed read
    /// workload sits well above it and still splits exactly as before.
    /// </para>
    /// <para>
    /// The median (rather than the mean) is used because it is robust: one hot
    /// shard among many barely moves it, so the ratio reflects true
    /// concentration rather than being diluted by the shard count.
    /// </para>
    /// <para>
    /// Must be strictly greater than
    /// <see cref="HotShardConsolidationSkewRatio"/>: the gap between the two is
    /// the hysteresis dead band in which neither the split trigger nor the
    /// consolidation trigger fires, which is what stops the two control loops
    /// oscillating against each other. A value at or below <c>1.0</c> disables
    /// the skew gate entirely and restores pure rate-based admission.
    /// </para>
    /// </summary>
    public double HotShardMinSkewRatio { get; set; } = DefaultHotShardMinSkewRatio;

    /// <summary>Default value for <see cref="HotShardMinSkewRatio"/> (1.5).</summary>
    public const double DefaultHotShardMinSkewRatio = 1.5;

    /// <summary>
    /// The load-skew ratio at or below which a tree counts as uniformly loaded,
    /// and is therefore a candidate for shard <em>consolidation</em> rather than
    /// splitting. Measured on exactly the same statistic as
    /// <see cref="HotShardMinSkewRatio"/> (<c>maxShardRate / medianShardRate</c>).
    /// <para>
    /// The split trigger fires at or above <see cref="HotShardMinSkewRatio"/>;
    /// the consolidation trigger fires at or below this value. The interval
    /// between them is a dead band in which neither acts, so a tree that has
    /// just been split cannot immediately qualify for consolidation and a tree
    /// that has just been consolidated cannot immediately qualify for a split.
    /// This value must therefore be strictly less than
    /// <see cref="HotShardMinSkewRatio"/>.
    /// </para>
    /// </summary>
    public double HotShardConsolidationSkewRatio { get; set; } = DefaultHotShardConsolidationSkewRatio;

    /// <summary>Default value for <see cref="HotShardConsolidationSkewRatio"/> (1.15).</summary>
    public const double DefaultHotShardConsolidationSkewRatio = 1.15;

    /// <summary>
    /// Minimum number of live entries a physical shard must hold before it is
    /// eligible for an autonomic split, whatever its operation rate.
    /// <para>
    /// A split relieves a hot shard by moving half of its virtual slots to a
    /// newly allocated physical shard. That can only help when there is enough
    /// data to redistribute: splitting a shard holding a few dozen records
    /// relieves nothing and permanently doubles the tree's activation
    /// footprint. This floor makes an under-occupied shard structurally
    /// ineligible so a pathological hotness signal cannot shatter a small tree.
    /// </para>
    /// <para>
    /// Occupancy is sampled with a single
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.CountAsync()"/> per
    /// candidate, and only for shards that already cleared every cheaper gate,
    /// so a tree under uniform load pays nothing for it. Set to <c>0</c> to
    /// disable the occupancy floor (and its probe) entirely.
    /// </para>
    /// </summary>
    public int HotShardMinShardEntries { get; set; } = DefaultHotShardMinShardEntries;

    /// <summary>Default value for <see cref="HotShardMinShardEntries"/> (1024).</summary>
    public const int DefaultHotShardMinShardEntries = 1024;

    /// <summary>
    /// Absolute ceiling on the number of physical shards a tree may reach
    /// through autonomic splitting. Once the tree's physical shard count
    /// reaches this value the monitor admits no further splits, whatever the
    /// observed load, so a pathological or mis-calibrated hotness signal cannot
    /// run a tree away into thousands of shards.
    /// <para>
    /// The ceiling bounds <em>autonomic</em> growth only: an explicit
    /// <see cref="ILattice.ReshardAsync"/> is an operator decision and is not
    /// gated by it. A tree deliberately configured with more physical shards
    /// than this ceiling simply never splits autonomically; raise the ceiling
    /// to re-enable growth for such a tree. Set to <c>0</c> (or less) for no
    /// ceiling.
    /// </para>
    /// </summary>
    public int MaxPhysicalShardsPerTree { get; set; } = DefaultMaxPhysicalShardsPerTree;

    /// <summary>Default value for <see cref="MaxPhysicalShardsPerTree"/> (256).</summary>
    public const int DefaultMaxPhysicalShardsPerTree = 256;

    /// <summary>
    /// Maximum number of autonomic splits that can be in flight concurrently
    /// for a single tree. The monitor refuses to start a new split while this
    /// many are already active. Defaults to 2 - splits are I/O-bounded by the
    /// drain phase but small enough that two parallel splits typically saturate
    /// neither storage nor the coordinator silo. Set to <c>1</c> for the most
    /// conservative behavior, or higher when many shards are simultaneously
    /// hot and target storage can absorb the extra drain traffic.
    /// </summary>
    public int MaxConcurrentAutoSplits { get; set; } = DefaultMaxConcurrentAutoSplits;

    /// <summary>Default value for <see cref="MaxConcurrentAutoSplits"/> (2).</summary>
    public const int DefaultMaxConcurrentAutoSplits = 2;

    /// <summary>
    /// Optional cluster-wide ceiling on the total number of autonomic shard
    /// splits that may be in flight concurrently across <em>all</em> trees.
    /// <para>
    /// <c>null</c> (the default) disables the cluster gate entirely: each tree's
    /// autonomic monitor enforces only its own <see cref="MaxConcurrentAutoSplits"/>,
    /// which is the current behaviour. In this mode no cluster singleton is
    /// activated and no extra RPC is issued per monitor tick, so the disabled
    /// path is byte-for-byte identical to running without this option and costs
    /// nothing.
    /// </para>
    /// <para>
    /// A positive value opts in to a cluster-wide admission gate that caps the
    /// aggregate number of concurrently draining splits regardless of how many
    /// trees are simultaneously hot. Because <c>HotShardMonitorGrain</c> is keyed
    /// by tree, the per-tree <see cref="MaxConcurrentAutoSplits"/> cannot see
    /// splits happening on other trees; in a multi-tenant or many-tree cluster
    /// the summed drain I/O can saturate the storage provider even though no
    /// single tree exceeds its own cap. The cluster ceiling is enforced
    /// <em>in addition to</em> each tree's <see cref="MaxConcurrentAutoSplits"/>
    /// and can only ever <em>lower</em> the number of splits a tree triggers,
    /// never raise it. Admission is granted through lease-based, time-bounded
    /// permits so a crashed or abandoned split releases its slot within the lease
    /// window rather than wedging splitting cluster-wide.
    /// </para>
    /// <para>
    /// Mirrors the <c>null</c> = disabled, zero-overhead-when-unset idiom used by
    /// <see cref="MaxCacheValueBytes"/>.
    /// </para>
    /// </summary>
    public int? MaxClusterConcurrentAutoSplits { get; set; } = DefaultMaxClusterConcurrentAutoSplits;

    /// <summary>Default value for <see cref="MaxClusterConcurrentAutoSplits"/> (<c>null</c> - the cluster-wide split gate is disabled).</summary>
    public static readonly int? DefaultMaxClusterConcurrentAutoSplits = null;

    /// <summary>
    /// Maximum number of parallel <see cref="Orleans.Lattice.BPlusTree.ITreeShardSplitGrain"/> splits
    /// that an online reshard (<see cref="ILattice.ReshardAsync"/>) may drive
    /// concurrently. Each split drains one physical shard's upper-half
    /// virtual slots into a newly allocated target shard; running several
    /// in parallel shortens total reshard time at the cost of proportionally
    /// higher background drain I/O. Splits dispatched by the reshard
    /// coordinator operate independently of those triggered autonomically
    /// by <c>HotShardMonitorGrain</c> - the two caps compose additively.
    /// </summary>
    public int MaxConcurrentMigrations { get; set; } = DefaultMaxConcurrentMigrations;

    /// <summary>Default value for <see cref="MaxConcurrentMigrations"/> (4).</summary>
    public const int DefaultMaxConcurrentMigrations = 4;

    /// <summary>
    /// Maximum number of parallel per-shard drains that an online snapshot
    /// (<see cref="ILattice.SnapshotAsync"/> in <see cref="SnapshotMode.Online"/>)
    /// may dispatch concurrently. Each drain reads one source shard's leaf
    /// chain and bulk-loads it into the corresponding destination shard while
    /// live writes continue to mirror via the shadow-forwarding primitive.
    /// Higher values shorten total snapshot duration at the cost of
    /// proportionally higher background drain I/O and memory on the
    /// coordinator silo. The snapshot remains crash-safe and idempotent
    /// under any cap - re-running converges via CRDT LWW.
    /// </summary>
    public int MaxConcurrentDrains { get; set; } = DefaultMaxConcurrentDrains;

    /// <summary>Default value for <see cref="MaxConcurrentDrains"/> (4).</summary>
    public const int DefaultMaxConcurrentDrains = 4;

    /// <summary>
    /// Maximum number of parallel per-shard baseline captures that opening a
    /// snapshot-isolated (point-in-time) cursor may dispatch concurrently.
    /// Opening such a cursor freezes a per-shard baseline on every physical
    /// shard root; each capture walks that shard's whole leaf chain and
    /// materialises its rows on the shard root's non-reentrant turn. Fanning
    /// the capture out to every shard at once therefore blocks every shard
    /// root simultaneously, starving replication applies and reads queued on
    /// those same shard roots. Bounding the fan-out keeps all but
    /// <see cref="MaxConcurrentSnapshotCaptures"/> shard roots free to serve
    /// other work while the open proceeds in waves. Lower values reduce the
    /// per-open blast radius at the cost of a longer open; higher values open
    /// faster but block more shard roots at once. The captured baseline and
    /// its point-in-time consistency are identical under any cap - only the
    /// dispatch schedule changes. Must be at least 1; values below 1 are
    /// clamped to 1 at the open site.
    /// </summary>
    public int MaxConcurrentSnapshotCaptures { get; set; } = DefaultMaxConcurrentSnapshotCaptures;

    /// <summary>Default value for <see cref="MaxConcurrentSnapshotCaptures"/> (4).</summary>
    public const int DefaultMaxConcurrentSnapshotCaptures = 4;

    /// <summary>
    /// Whether a snapshot-isolated cursor open (the <c>OpenSnapshot*CursorAsync</c>
    /// family) is shed fast with a retryable <see cref="LatticeSaturatedException"/>
    /// when the tree's per-silo WAL saturation signal reports
    /// <see cref="WalSaturationState.Saturated"/> at the moment of the open,
    /// before the expensive per-shard baseline capture is fanned out.
    /// <para>
    /// A snapshot open freezes and materialises every shard's leaf chain on the
    /// non-reentrant shard roots - heavier than a single write. Admitting one
    /// into an already-saturated tree piles that work onto roots that are
    /// collapsing under write back-pressure, starving replication applies and
    /// reads queued on the same roots, and a client that retries on the resulting
    /// timeout sustains a scan storm. When enabled (the default), the open is
    /// refused at admission: the caller receives a typed, retryable back-pressure
    /// error and the fan-out never starts. Only
    /// <see cref="WalSaturationState.Saturated"/> (the "pause new appends" regime)
    /// sheds; a <see cref="WalSaturationState.Throttled"/> tree is unaffected and
    /// stays browsable, mirroring the atomic-write saga's quiesce gate.
    /// </para>
    /// <para>
    /// Set to <see langword="false"/> to restore the prior behaviour where a
    /// snapshot open always proceeds regardless of the saturation regime. The
    /// signal is a cheap silo-local lookup; the check adds no fan-out.
    /// </para>
    /// </summary>
    public bool ShedSnapshotOpensWhenSaturated { get; set; } = DefaultShedSnapshotOpensWhenSaturated;

    /// <summary>Default value for <see cref="ShedSnapshotOpensWhenSaturated"/> (<see langword="true"/>).</summary>
    public const bool DefaultShedSnapshotOpensWhenSaturated = true;

    /// <summary>
    /// Maximum number of moved-slot entries the split coordinator accumulates
    /// in a single <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/> call to the
    /// target shard during drain. Larger values reduce per-call overhead;
    /// smaller values bound peak memory on the coordinator silo and the size
    /// of the Orleans grain message. The drain phase remains idempotent under
    /// any chunking - re-running converges via CRDT LWW.
    /// </summary>
    public int SplitDrainBatchSize { get; set; } = DefaultSplitDrainBatchSize;

    /// <summary>Default value for <see cref="SplitDrainBatchSize"/> (1024 entries).</summary>
    public const int DefaultSplitDrainBatchSize = 1024;

    /// <summary>
    /// Minimum age of a tree (since the monitor first activated) before
    /// autonomic splits are allowed. Prevents premature splits during
    /// startup bursts before the workload has stabilised.
    /// </summary>
    public TimeSpan AutoSplitMinTreeAge { get; set; } = DefaultAutoSplitMinTreeAge;

    /// <summary>Default value for <see cref="AutoSplitMinTreeAge"/> (60 seconds).</summary>
    public static readonly TimeSpan DefaultAutoSplitMinTreeAge = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maximum number of times a strongly-consistent scan
    /// (<c>ILattice.CountAsync</c>, <c>KeysAsync</c>, <c>EntriesAsync</c>)
    /// will reconcile against newly-discovered shard-map changes before giving
    /// up. Each retry only re-fetches data for the slots that actually moved
    /// during the scan, so the cost is bounded by the number of in-flight
    /// splits, not the size of the tree. Set to <c>1</c> to disable
    /// reconciliation entirely (fall back to eventually-consistent scans).
    /// </summary>
    public int MaxScanRetries { get; set; } = DefaultMaxScanRetries;

    /// <summary>Default value for <see cref="MaxScanRetries"/> (3).</summary>
    public const int DefaultMaxScanRetries = 3;

    /// <summary>
    /// How long an open stateful cursor may remain idle before it is
    /// automatically cleaned up. On every <c>Open</c> / <c>Next</c> /
    /// <c>DeleteRangeStep</c> call, the cursor grain refreshes a grain
    /// reminder set to fire after this interval; if the reminder ever fires
    /// (no activity for the full window) the grain clears its persisted
    /// state, unregisters the reminder, and deactivates. Protects against
    /// leaked cursor state from clients that open a cursor and never call
    /// <c>CloseCursorAsync</c>. Set to <see cref="Timeout.InfiniteTimeSpan"/>
    /// to disable automatic cleanup. Minimum effective interval is
    /// <c>1 minute</c> (Orleans reminder granularity).
    /// </summary>
    public TimeSpan CursorIdleTtl { get; set; } = DefaultCursorIdleTtl;

    /// <summary>Default value for <see cref="CursorIdleTtl"/> (48 hours).</summary>
    public static readonly TimeSpan DefaultCursorIdleTtl = TimeSpan.FromHours(48);

    /// <summary>
    /// How long a completed atomic-write saga retains its persisted
    /// state for idempotent re-invocation by the client. After the retention
    /// window elapses, a grain reminder fires, the saga's state is cleared,
    /// and the coordinator grain deactivates. A client that re-issues a
    /// <c>SetManyAtomicAsync</c> call with the same <c>operationId</c>
    /// within this window will see the original result (success or the
    /// original failure exception); after the window expires, a re-issue
    /// starts a new saga. Set to <see cref="Timeout.InfiniteTimeSpan"/> to
    /// disable automatic cleanup and retain saga state indefinitely.
    /// Minimum effective interval is <c>1 minute</c> (Orleans reminder
    /// granularity).
    /// </summary>
    public TimeSpan AtomicWriteRetention { get; set; } = DefaultAtomicWriteRetention;

    /// <summary>Default value for <see cref="AtomicWriteRetention"/> (48 hours).</summary>
    public static readonly TimeSpan DefaultAtomicWriteRetention = TimeSpan.FromHours(48);

    /// <summary>
    /// The lease duration <see cref="Orleans.Lattice.ILatticeLockGrain"/> grants
    /// when an acquire supplies a non-positive
    /// <see cref="Orleans.Lattice.LockAcquireRequest.LeaseDuration"/> (or a
    /// non-positive duration to
    /// <see cref="Orleans.Lattice.ILatticeLockGrain.TryAcquireAsync"/>), i.e. when
    /// the caller defers to the server default. A holder that neither renews nor
    /// releases before its lease elapses has the lock reclaimed and handed to the
    /// next FIFO waiter, so this value bounds how long a crashed holder can wedge
    /// the lock. Must be positive.
    /// </summary>
    public TimeSpan DefaultLockLeaseDuration { get; set; } = DefaultLockLeaseDurationValue;

    /// <summary>Default value for <see cref="DefaultLockLeaseDuration"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultLockLeaseDurationValue = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The upper bound <see cref="Orleans.Lattice.ILatticeLockGrain"/> clamps every
    /// granted or renewed lease to. A caller cannot pin a lock for longer than this
    /// even by requesting a larger duration; the grant is silently capped so a
    /// misconfigured client cannot hold a contended lock for hours. Must be
    /// positive and at least <see cref="DefaultLockLeaseDuration"/>.
    /// </summary>
    public TimeSpan MaxLockLeaseDuration { get; set; } = MaxLockLeaseDurationValue;

    /// <summary>Default value for <see cref="MaxLockLeaseDuration"/> (5 minutes).</summary>
    public static readonly TimeSpan MaxLockLeaseDurationValue = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Retention window for a terminal atomic-action (saga) coordinator's state
    /// after <see cref="Orleans.Lattice.IAtomicActionGrain.ExecuteAsync"/> reaches a
    /// terminal outcome. A re-issue of the same operation id within this window
    /// returns the memoized outcome (idempotent re-invocation); after it expires
    /// the coordinator clears its persisted state and a re-issue starts a new saga.
    /// Set to <see cref="Timeout.InfiniteTimeSpan"/> to retain saga state
    /// indefinitely. Minimum effective interval is <c>1 minute</c> (Orleans
    /// reminder granularity).
    /// </summary>
    public TimeSpan AtomicActionRetention { get; set; } = DefaultAtomicActionRetention;

    /// <summary>Default value for <see cref="AtomicActionRetention"/> (48 hours).</summary>
    public static readonly TimeSpan DefaultAtomicActionRetention = TimeSpan.FromHours(48);

    /// <summary>
    /// The maximum number of steps an atomic-action plan submitted to
    /// <see cref="Orleans.Lattice.IAtomicActionGrain.ExecuteAsync"/> may contain. A
    /// plan exceeding this bound is rejected before the saga starts so a
    /// pathological plan cannot pin an activation for an unbounded time. Must be
    /// positive.
    /// </summary>
    public int MaxAtomicActionSteps { get; set; } = DefaultMaxAtomicActionSteps;

    /// <summary>Default value for <see cref="MaxAtomicActionSteps"/> (64).</summary>
    public const int DefaultMaxAtomicActionSteps = 64;

    /// <summary>
    /// The maximum size, in bytes, of a single custom step's argument payload in an
    /// atomic-action plan. A step whose payload exceeds this bound is rejected
    /// before the saga starts so a wire- or storage-supplied payload cannot bloat
    /// persisted saga state without bound. Must be positive.
    /// </summary>
    public int MaxAtomicActionArgsBytes { get; set; } = DefaultMaxAtomicActionArgsBytes;

    /// <summary>Default value for <see cref="MaxAtomicActionArgsBytes"/> (32 KiB).</summary>
    public const int DefaultMaxAtomicActionArgsBytes = 32 * 1024;

    /// <summary>
    /// How long a completed saga's commit/abort decision persists in the
    /// per-tree <see cref="Orleans.Lattice.BPlusTree.Grains.TxRegistryGrain"/> as a tombstone after
    /// the saga calls <c>ForgetAsync</c>. Covers the race window where a
    /// concurrent <c>TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync</c>
    /// installs a pending bucket on a destination shard <i>after</i> the
    /// saga's terminal fan-out (and after the late-pickup fetch-loop in
    /// <c>AtomicWriteGrain.BroadcastTerminalsAsync</c>) but <i>before</i>
    /// the saga's <c>ForgetAsync</c> call: with a non-zero retention, the
    /// sweep's post-sweep cleanup pass can still resolve the saga's
    /// outcome via <c>GetStatusAsync</c> and apply the terminal directly,
    /// draining the orphan pending bucket. Default is 60 seconds - long
    /// enough to absorb typical sweep durations while bounding the
    /// registry's persisted footprint. Set to <see cref="TimeSpan.Zero"/>
    /// to disable tombstoning entirely (legacy behaviour: <c>ForgetAsync</c>
    /// removes the decision immediately, restoring the original semantic
    /// from before the tombstone feature shipped). Increase for
    /// environments where sweep completion can exceed 60 seconds (very
    /// large shards or cascading split storms under sustained write load).
    /// </summary>
    public TimeSpan TxDecisionRetention { get; set; } = DefaultTxDecisionRetention;

    /// <summary>Default value for <see cref="TxDecisionRetention"/> (60 seconds).</summary>
    public static readonly TimeSpan DefaultTxDecisionRetention = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Hard cap on how long the per-tree <see cref="Orleans.Lattice.BPlusTree.Grains.TxRegistryGrain"/>
    /// will retain a point-in-time snapshot pin recorded for a
    /// <see cref="LatticeCursorSpec.PointInTime"/> cursor. The cursor grain
    /// refreshes its pin on every step (<c>Next*Async</c> /
    /// <c>DeleteRangeStepAsync</c>); a cursor that misses a refresh window
    /// past this TTL has its pin expired by the registry's own prune pass,
    /// and the next step throws
    /// <see cref="LatticeCursorSnapshotExpiredException"/>. Acts as a
    /// defence-in-depth bound against reminder-service degradation or
    /// <c>CursorIdleTtl = Timeout.InfiniteTimeSpan</c> configurations that
    /// would otherwise leave a forgotten cursor pinning registry decisions
    /// forever. Default 7 days. The minimum effective interval is
    /// <see cref="TxDecisionRetention"/> - a pin shorter than the tombstone
    /// retention is silently floored, because the registry's own
    /// tombstone-prune pass already covers anything shorter.
    /// </summary>
    public TimeSpan MaxCursorSnapshotPinTtl { get; set; } = DefaultMaxCursorSnapshotPinTtl;

    /// <summary>Default value for <see cref="MaxCursorSnapshotPinTtl"/> (7 days).</summary>
    public static readonly TimeSpan DefaultMaxCursorSnapshotPinTtl = TimeSpan.FromDays(7);

    /// <summary>
    /// Absolute footprint cap on the union of saga decisions pinned by all
    /// active <see cref="LatticeCursorSpec.PointInTime"/> cursors against
    /// the per-tree <see cref="Orleans.Lattice.BPlusTree.Grains.TxRegistryGrain"/>. A new
    /// <c>OpenAsync(PointInTime: true)</c> whose snapshot would push the
    /// total pinned-decision footprint over this cap throws
    /// <see cref="LatticeCursorRegistryPinExhaustedException"/> rather than
    /// silently degrading or growing unbounded. <c>Next*Async</c> on an
    /// already-open cursor never throws for pin-exhaustion reasons.
    /// Default 100 000 decisions.
    /// </summary>
    public int MaxPinnedSagaDecisions { get; set; } = DefaultMaxPinnedSagaDecisions;

    /// <summary>Default value for <see cref="MaxPinnedSagaDecisions"/> (100 000).</summary>
    public const int DefaultMaxPinnedSagaDecisions = 100_000;

    /// <summary>
    /// Per-shard cap on the projected WAL-replay record count consulted
    /// at <c>OpenSnapshot*Async</c> time. The open fan-out reads each
    /// touched shard's <c>GetMaterialiserLagAsync</c> and fails fast
    /// with <see cref="LatticeSnapshotReplayBudgetExceededException"/>
    /// when any shard's projected lag exceeds this cap, so a snapshot
    /// cursor cannot be opened against a tree whose materialiser is
    /// far enough behind that replay would dominate the open call.
    /// Operators tune this against the steady-state apply rate and
    /// the materialiser-checkpoint cadence
    /// (<see cref="MaterialiserCheckpointInterval"/>,
    /// <see cref="MaterialiserCheckpointEntries"/>): a healthy
    /// materialiser sits well below the cap; a sustained excursion
    /// surfaces as the open-time fail-fast.
    /// <para>
    /// The cap is the snapshot analogue of
    /// <see cref="MaxLeafReplayEntries"/>, which bounds activation-
    /// time replay on a single leaf. Default 10 000 000 records,
    /// matching the conservative-but-non-degenerate sizing typical
    /// production WAL retention windows admit at the per-shard scale.
    /// </para>
    /// </summary>
    public long MaxSnapshotReplayEntries { get; set; } = DefaultMaxSnapshotReplayEntries;

    /// <summary>Default value for <see cref="MaxSnapshotReplayEntries"/> (10 000 000).</summary>
    public const long DefaultMaxSnapshotReplayEntries = 10_000_000L;

    /// <summary>
    /// Idle-eviction window for transient per-shard snapshot leaf
    /// grains materialised by a zero-observable-writes snapshot
    /// cursor. A snapshot leaf rebuilds the shard's projection on
    /// first read by replaying the captured WAL prefix, then stays
    /// activated for further pages against the same shard. After
    /// this window elapses without activity the grain self-evicts;
    /// the next <c>Next*Async</c> transparently rebuilds it on
    /// demand (the underlying WAL prefix is held alive by the
    /// snapshot's <see cref="MaxCursorSnapshotPinTtl"/> pin, so the
    /// rebuild is always feasible until the cursor is closed or
    /// expires).
    /// <para>
    /// Decoupled from <see cref="CursorIdleTtl"/> because the
    /// snapshot-leaf state is purely a replay-cost cache, not part
    /// of the cursor's correctness boundary - evicting it earlier
    /// only trades a replay re-run for memory. Default 30 minutes.
    /// </para>
    /// </summary>
    public TimeSpan SnapshotLeafIdleTtl { get; set; } = DefaultSnapshotLeafIdleTtl;

    /// <summary>Default value for <see cref="SnapshotLeafIdleTtl"/> (30 minutes).</summary>
    public static readonly TimeSpan DefaultSnapshotLeafIdleTtl = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Leak-guard retention window for the durable per-cursor, per-shard frozen
    /// baselines persisted by a zero-observable-writes snapshot cursor. A
    /// snapshot cursor seeds its baselines into transient snapshot leaves in
    /// memory at open and only flushes them to the durable
    /// <c>ISnapshotBaselineStorageGrain</c> store lazily, once a scan must
    /// survive past its first page (issue #916). The normal lifecycle deletes
    /// each baseline when the cursor closes or its idle TTL expires; this
    /// retention window is the backstop for the abnormal case where neither
    /// fires - a client that crashes or abandons a multi-page continuation token
    /// without closing the cursor.
    /// <para>
    /// Implemented as a sliding self-clear reminder on each baseline row: a
    /// still-active scan slides the window forward (throttled, so the reminder
    /// table is not rewritten on every page), while an abandoned baseline is
    /// reclaimed automatically once the window elapses with no activity. It also
    /// bounds the maximum lifetime of a paused-then-resumed snapshot: a scan
    /// idle longer than this window loses its baseline and must reopen. Default
    /// 6 hours; set to <see cref="Timeout.InfiniteTimeSpan"/> to disable the
    /// leak guard (baselines then rely solely on the close / idle-TTL delete).
    /// </para>
    /// </summary>
    public TimeSpan SnapshotBaselineTtl { get; set; } = DefaultSnapshotBaselineTtl;

    /// <summary>Default value for <see cref="SnapshotBaselineTtl"/> (6 hours).</summary>
    public static readonly TimeSpan DefaultSnapshotBaselineTtl = TimeSpan.FromHours(6);

    /// <summary>
    /// Optional retention window for <see cref="Orleans.Lattice.VersionVector"/>
    /// entries.
    /// <see cref="Orleans.Lattice.VersionVector.PruneOlderThan(long)"/> with
    /// <c>UtcNow - VersionVectorRetention</c>, replica entries whose
    /// wall-clock tick falls before the cutoff are dropped to bound the
    /// vector's memory footprint.
    /// <para>
    /// Defaults to <see cref="Timeout.InfiniteTimeSpan"/> (no pruning) to
    /// preserve wire and state compatibility. Pruning must be applied
    /// consistently across replicas that merge against each other,
    /// otherwise a short-retention replica will keep reinstating entries
    /// from a long-retention peer. Values below
    /// <see cref="DefaultMinVersionVectorRetention"/> are typically
    /// unsafe on networks where clock skew exceeds the window.
    /// </para>
    /// </summary>
    public TimeSpan VersionVectorRetention { get; set; } = DefaultVersionVectorRetention;

    /// <summary>Default value for <see cref="VersionVectorRetention"/> (disabled).</summary>
    public static readonly TimeSpan DefaultVersionVectorRetention = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// Practical lower bound for <see cref="VersionVectorRetention"/> below
    /// which pruning may drop entries that are still causally relevant. Not
    /// enforced - provided as a reference constant.
    /// </summary>
    public static readonly TimeSpan DefaultMinVersionVectorRetention = TimeSpan.FromHours(1);

    /// <summary>
    /// How long an assembled <see cref="TreeDiagnosticReport"/> is held in-memory
    /// by the diagnostics grain before a fresh fan-out is performed. Short
    /// windows deliver near-live data at the cost of extra shard traffic under
    /// dashboard polling; longer windows smooth out diagnostics load.
    /// Shallow and deep reports are cached independently. Set to
    /// <see cref="TimeSpan.Zero"/> to disable caching entirely (every call
    /// fans out). The cache is automatically invalidated whenever the
    /// diagnostics grain observes a new split event via
    /// <c>ILatticeStats.RecordSplitAsync</c>.
    /// </summary>
    public TimeSpan DiagnosticsCacheTtl { get; set; } = DefaultDiagnosticsCacheTtl;

    /// <summary>Default value for <see cref="DiagnosticsCacheTtl"/> (5 seconds).</summary>
    public static readonly TimeSpan DefaultDiagnosticsCacheTtl = TimeSpan.FromSeconds(5);

    /// <summary>
    /// When <c>true</c>, Lattice publishes <see cref="LatticeTreeEvent"/> notifications
    /// on an Orleans stream (namespace <see cref="LatticeEventConstants.StreamNamespace"/>)
    /// covering per-key writes, atomic-write completions, splits, compactions,
    /// snapshots, resizes, reshards, and tree-lifecycle transitions. Consumers
    /// subscribe via <c>LatticeExtensions.SubscribeToEventsAsync</c>; publication
    /// is fire-and-forget and log-and-swallow, so a missing or misconfigured
    /// provider never breaks the write path. Opt in per tree.
    /// </summary>
    public bool PublishEvents { get; set; } = DefaultPublishEvents;
    /// <summary>Default value for <see cref="PublishEvents"/> (<c>false</c>).</summary>
    public const bool DefaultPublishEvents = false;

    /// <summary>
    /// Name of the Orleans stream provider used to publish and subscribe to
    /// <see cref="LatticeTreeEvent"/>. Defaults to <c>"Default"</c>, matching
    /// the conventional name used by <c>siloBuilder.AddMemoryStreams("Default")</c>.
    /// The same name must be used on the client (subscribers) and on every
    /// silo (publishers).
    /// </summary>
    public string EventStreamProviderName { get; set; } = DefaultEventStreamProviderName;

    /// <summary>Default value for <see cref="EventStreamProviderName"/> (<c>"Default"</c>).</summary>
    public const string DefaultEventStreamProviderName = "Default";

    /// <summary>
    /// Soft budget on the number of entries a leaf grain expects to replay
    /// through its projection rebuild seam (<c>ILeafProjection.Apply</c>) at
    /// activation time. Bounds the <i>expected</i> replay cost when a leaf
    /// reactivates after an extended outage and the gap between its persisted
    /// projection checkpoint and the current write-ahead-log head grows large.
    /// <para>
    /// <b>Exceeding this budget is not an error.</b> When the gap is larger
    /// than the budget but the WAL still covers every offset the leaf needs,
    /// the leaf replays anyway - the result is identical, just slower - and
    /// the overrun is reported as a warning plus the
    /// <c>orleans.lattice.leaf.activation_replays_over_budget</c> counter.
    /// Replay cost is bounded on the read side by
    /// <see cref="WalReplayMaxRecordsPerTurn"/> (which yields between turns)
    /// and by <see cref="WalMaterialiserMaxConcurrentReplays"/>.
    /// </para>
    /// <para>
    /// Before issue #1738 an overrun was fatal: it surfaced
    /// <see cref="LeafProjectionStaleException"/> and left the tree
    /// permanently un-activatable even though its data was fully intact. A
    /// cost guardrail must never be more destructive than the cost it guards
    /// against, so the budget is now advisory. Persistent overruns mean the
    /// materialiser is checkpointing too slowly for the write rate - tune
    /// <see cref="MaterialiserCheckpointInterval"/> /
    /// <see cref="MaterialiserCheckpointEntries"/>, or raise this budget.
    /// </para>
    /// </summary>
    public int MaxLeafReplayEntries { get; set; } = DefaultMaxLeafReplayEntries;

    /// <summary>Default value for <see cref="MaxLeafReplayEntries"/> (10 000).</summary>
    public const int DefaultMaxLeafReplayEntries = 10_000;

    /// <summary>
    /// Maximum interval between durable persistences of a leaf grain's
    /// projection-checkpoint offset (the
    /// <c>ILeafProjection.SetCheckpointOffsetAsync</c> seam introduced
    /// for the WAL-as-sole-commit-point promotion). Checkpoint advances
    /// are coalesced in memory and durably written when whichever of
    /// this interval or <see cref="MaterialiserCheckpointEntries"/>
    /// elapses first; a graceful deactivation flushes any pending
    /// advance synchronously.
    /// <para>
    /// Set to <see cref="TimeSpan.Zero"/> for an every-entry checkpoint
    /// (one extra storage write per applied mutation, but zero replay
    /// on restart) when strict RTO budgets warrant the cost.
    /// <see cref="Timeout.InfiniteTimeSpan"/> disables the time-driven
    /// flush so checkpoints persist only when
    /// <see cref="MaterialiserCheckpointEntries"/> is reached or the
    /// grain deactivates.
    /// </para>
    /// <para>
    /// Crash-recovery cost on a worst-case unflushed checkpoint is
    /// bounded by this interval times the steady-state apply rate.
    /// The seam itself ships dormant - the leaf grain still writes
    /// through its existing storage provider on every commit. This
    /// option becomes observable when the WAL-as-sole-commit-point
    /// promotion lands and the activation path begins consulting the
    /// persisted checkpoint.
    /// </para>
    /// </summary>
    public TimeSpan MaterialiserCheckpointInterval { get; set; } = DefaultMaterialiserCheckpointInterval;

    /// <summary>Default value for <see cref="MaterialiserCheckpointInterval"/> (5 seconds).</summary>
    public static readonly TimeSpan DefaultMaterialiserCheckpointInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of pending checkpoint advances (measured as the
    /// difference between the last requested offset and the last
    /// durably-persisted offset) before the leaf grain forces a
    /// durable persist of the projection checkpoint, even if
    /// <see cref="MaterialiserCheckpointInterval"/> has not yet
    /// elapsed. Bounds the worst-case replay cost on crash recovery
    /// when the steady-state apply rate is high enough that the
    /// time-driven flush has not fired.
    /// </summary>
    public int MaterialiserCheckpointEntries { get; set; } = DefaultMaterialiserCheckpointEntries;

    /// <summary>Default value for <see cref="MaterialiserCheckpointEntries"/> (5 000).</summary>
    public const int DefaultMaterialiserCheckpointEntries = 5_000;

    /// <summary>
    /// Age beyond which a leaf grain's persisted projection checkpoint is
    /// considered stale enough to warrant a warning at activation time.
    /// Compared against the wall-clock age of the persisted checkpoint.
    /// <para>
    /// <b>Advisory only, like <see cref="MaxLeafReplayEntries"/>.</b> An old
    /// checkpoint does not imply the WAL has been trimmed; when the log still
    /// covers the needed window the leaf tail-replays and converges normally.
    /// Only a genuine trim past the checkpoint routes to
    /// <see cref="ProjectionRebuildPolicy"/> (issue #1738).
    /// </para>
    /// <para>
    /// <b>Known gap:</b> the activation path currently passes
    /// <see cref="TimeSpan.Zero"/> as the checkpoint age
    /// (<c>BPlusLeafGrain.ReplayWalSinceCheckpointAsync</c>), because the
    /// persisted checkpoint carries no capture timestamp, so this trigger
    /// never fires from activation today. It is retained because the option is
    /// public API and the detector honours it for any caller that does supply
    /// a real age. Tracked in #1738.
    /// </para>
    /// <para>
    /// Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable the
    /// age-based trigger entirely.
    /// </para>
    /// </summary>
    public TimeSpan LeafProjectionRetention { get; set; } = DefaultLeafProjectionRetention;

    /// <summary>Default value for <see cref="LeafProjectionRetention"/> (7 days).</summary>
    public static readonly TimeSpan DefaultLeafProjectionRetention = TimeSpan.FromDays(7);

    /// <summary>
    /// Proximity threshold, expressed as a fraction in <c>[0.0, 1.0]</c>,
    /// at which a leaf's persisted projection checkpoint is considered
    /// close enough to the WAL tail that a snapshot capture should be
    /// proactively scheduled. The fall-off-log detector evaluates
    /// <c>(checkpoint - tail) / (head - tail) &lt;= LeafSnapshotMargin</c>
    /// after the three hard triggers have been ruled out; when the
    /// inequality holds the detector returns the
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/> advisory and
    /// the leaf grain itself captures a snapshot of its cache (at
    /// activation, and periodically thereafter while it stays active -
    /// see <see cref="LeafSnapshotReClassifyEveryNCheckpoints"/>). The
    /// advisory is non-fatal: the leaf's own activation path treats it
    /// as a tail replay, so reactivation behaviour is unchanged.
    /// <para>
    /// Set to <c>0.0</c> to disable the proactive-capture advisory
    /// (the hard fall-off triggers continue to apply). Values closer
    /// to <c>1.0</c> drive more aggressive capture; <c>0.30</c> (the
    /// default) keeps capture confined to leaves whose checkpoint is
    /// within the trailing 30% of the readable WAL window.
    /// </para>
    /// </summary>
    public double LeafSnapshotMargin { get; set; } = DefaultLeafSnapshotMargin;

    /// <summary>Default value for <see cref="LeafSnapshotMargin"/> (<c>0.30</c>).</summary>
    public const double DefaultLeafSnapshotMargin = 0.30;

    /// <summary>
    /// Cadence at which an active leaf grain re-classifies its WAL
    /// gap and, on the
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/> advisory,
    /// drives a proactive snapshot capture. Expressed as the number
    /// of successful checkpoint persists between re-classifications;
    /// the default <c>64</c> means a leaf that has just persisted its
    /// 64th, 128th, 192nd ... checkpoint since activation re-runs the
    /// detector and captures a snapshot if its checkpoint is now
    /// within <see cref="LeafSnapshotMargin"/> of the WAL tail.
    /// <para>
    /// Set to <c>0</c> to disable the periodic re-classification
    /// entirely; only the once-per-activation capture (driven by the
    /// activation-time advisory) will fire. The activation-time
    /// capture itself is not affected by this option.
    /// </para>
    /// </summary>
    public int LeafSnapshotReClassifyEveryNCheckpoints { get; set; } = DefaultLeafSnapshotReClassifyEveryNCheckpoints;

    /// <summary>Default value for <see cref="LeafSnapshotReClassifyEveryNCheckpoints"/> (<c>64</c>).</summary>
    public const int DefaultLeafSnapshotReClassifyEveryNCheckpoints = 64;

    /// <summary>
    /// When <c>true</c> (the default), a leaf snapshot capture encodes its
    /// rows into the compact binary frame
    /// (<c>LeafSnapshotBlob.EncodedRows</c>) instead of persisting them as the
    /// legacy row object graph. The legacy shape costs a serializer property
    /// envelope plus a base64 string for every row under the default JSON
    /// grain-storage serializer, and its decode path allocates a string, a
    /// scratch buffer, and a fresh array per row; the frame carries raw value
    /// bytes and decodes straight into the entry cache.
    /// <para>
    /// This switch controls the <b>write</b> side only. Reading is always
    /// dual: a blob is decoded from whichever encoding it carries, so turning
    /// the switch off does not orphan blobs already written as frames, and
    /// turning it on does not require any migration of blobs already written
    /// as row lists - each is simply re-encoded on its next natural capture.
    /// Both directions are therefore safe to flip on a running deployment.
    /// </para>
    /// <para>
    /// Set to <c>false</c> only to pin the persisted shape to the legacy
    /// encoding, for example while a rollback to a build that predates the
    /// frame is still possible: such a build has no dual-read and would see a
    /// frame-carrying blob as an empty row set.
    /// </para>
    /// </summary>
    public bool LeafSnapshotBinaryEncodingEnabled { get; set; } = DefaultLeafSnapshotBinaryEncodingEnabled;

    /// <summary>Default value for <see cref="LeafSnapshotBinaryEncodingEnabled"/> (<c>true</c>).</summary>
    public const bool DefaultLeafSnapshotBinaryEncodingEnabled = true;

    /// <summary>
    /// When <c>true</c> (the default), a leaf that rehydrates from a binary
    /// snapshot frame brings itself online without decoding the frame, and
    /// materialises entry ranges out of it only as reads actually require them.
    /// A leaf activation therefore costs what the caller reads rather than what
    /// the leaf happens to hold: a point read seeks the frame's index table and
    /// decodes one block, and a ranged scan decodes only the blocks its range
    /// spans.
    /// <para>
    /// Nothing observable changes. The cache reports the whole snapshot's row
    /// count, footprint and live-key count from the moment it attaches, every
    /// key-addressed accessor materialises what it needs before answering, and
    /// every whole-cache walk (digest, canonical hash, capture, split)
    /// materialises the snapshot in full first - so reads, scans, digests and
    /// the canonical hash are identical either way. Snapshot coverage is
    /// likewise unaffected: it is stamped from the loaded blob's offsets, and a
    /// capture materialises every row before it writes, so a partially hydrated
    /// leaf can never claim coverage it does not hold.
    /// </para>
    /// <para>
    /// Set to <c>false</c> to restore the previous behaviour, where a rehydrate
    /// decodes every row of the snapshot into the cache up front. The switch is
    /// read once per rehydrate and touches no persisted shape, so it is safe to
    /// flip in either direction on a running deployment.
    /// </para>
    /// </summary>
    public bool LeafPartialHydrationEnabled { get; set; } = DefaultLeafPartialHydrationEnabled;

    /// <summary>Default value for <see cref="LeafPartialHydrationEnabled"/> (<c>true</c>).</summary>
    public const bool DefaultLeafPartialHydrationEnabled = true;

    /// <summary>
    /// Maximum snapshot payload, in bytes, a single leaf keeps resident while
    /// <see cref="LeafPartialHydrationEnabled"/> is on. Once a leaf's
    /// materialised rows exceed this, the least recently used hydrated ranges
    /// are evicted and re-materialised from the snapshot the next time a read
    /// needs them, so a large tree does not have to be wholly resident to be
    /// queryable.
    /// <para>
    /// Only ranges that are still byte-identical to the snapshot are ever
    /// evicted: a range that has taken a write is pinned for the rest of the
    /// activation, because re-reading it would resurrect the value the write
    /// replaced. Eviction is therefore incapable of losing a mutation, and the
    /// budget is a bound on resident footprint rather than on correctness.
    /// </para>
    /// <para>
    /// Set to <c>0</c> to leave the resident footprint unbounded, keeping
    /// on-demand hydration but never evicting. The default of 1 MiB sits well
    /// above the ordinary leaf shape, so a typical leaf hydrates on demand and
    /// never evicts, and the bound bites only on leaves materially larger than
    /// that.
    /// </para>
    /// </summary>
    public long LeafHydrationResidentBytes { get; set; } = DefaultLeafHydrationResidentBytes;

    /// <summary>Default value for <see cref="LeafHydrationResidentBytes"/> (1 MiB).</summary>
    public const long DefaultLeafHydrationResidentBytes = 1L * 1024 * 1024;

    /// <summary>
    /// Selects the recovery strategy a leaf grain takes when one of
    /// the fall-off-log triggers fires at activation time
    /// (WAL trimmed past checkpoint, replay budget exceeded, projection
    /// older than <see cref="LeafProjectionRetention"/>).
    /// </summary>
    public ProjectionRebuildPolicy ProjectionRebuildPolicy { get; set; } = ProjectionRebuildPolicy.SnapshotThenWal;

    /// <summary>
    /// When <c>true</c> (the default), every leaf mutation incrementally
    /// updates the per-leaf <c>ProjectionHash</c> and publishes a
    /// <c>ChildDigestSnapshot</c> upward so each internal node maintains
    /// a pre-folded <c>SubtreeProjectionHash</c> aggregate. The pre-folded
    /// aggregate is what makes
    /// <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
    /// cost <c>O(shardCount)</c> grain hops regardless of tree size, so it
    /// must be left enabled for any deployment that polls the digest as a
    /// cross-silo drift-detection signal.
    /// <para>
    /// Set to <c>false</c> to skip both halves of the maintenance work on
    /// every mutation - the per-entry XOR fold on the leaf <i>and</i> the
    /// cross-grain hop that updates each ancestor internal node's
    /// aggregate. The trade-off is that
    /// <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
    /// will throw <see cref="InvalidOperationException"/> on every call
    /// against the affected tree: the persisted aggregates are no longer
    /// the source of truth and recomputing them on demand would defeat the
    /// purpose of opting out.
    /// </para>
    /// <para>
    /// Appropriate when the deployment has no cross-silo drift-detection
    /// requirement (single-replica or single-cluster shapes), when an
    /// external mechanism reconciles state independently (e.g. comparing
    /// WAL offsets directly), or when profiling shows the per-mutation
    /// publish chain is on the critical path of an otherwise
    /// digest-indifferent workload. The leaf's existing
    /// <c>ProjectionHash</c> column is preserved verbatim across the
    /// toggle flip, but see the one-way-disable note below: in practice
    /// the value is only safe to re-engage if no writes have landed under
    /// the disabled setting.
    /// </para>
    /// <para>
    /// <b>Disabling is a one-way operation per tree.</b> The first mutation
    /// that lands while maintenance is disabled stamps an irreversible
    /// registry latch (<c>TreeRegistryEntry.ProjectionDigestPermanentlyDisabled</c>)
    /// on the tree's registry entry. Once stamped, the latch supersedes any
    /// later attempt to flip this option, the per-tree override, or the
    /// silo-wide default back to <c>true</c>: every subsequent activation
    /// resolves <see cref="MaintainProjectionDigest"/> as <c>false</c> and
    /// every <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
    /// call against the tree keeps throwing. The latch exists because the
    /// digest is an XOR-fold aggregate: any mutation accepted while
    /// maintenance is off permanently invalidates the persisted aggregate,
    /// and silently re-enabling maintenance would publish a known-stale
    /// digest as if it were authoritative. The only way to re-engage
    /// digest maintenance for a tree that has been latched off is to
    /// rebuild the tree (or its leaf range) from scratch under a fresh
    /// registry entry.
    /// </para>
    /// <para>
    /// Per-tree overrides are honoured via the registry entry's
    /// <c>MaintainProjectionDigest</c> column - operators can opt an
    /// individual tree out (or, while no writes have yet landed under the
    /// disabled setting, back in) without flipping the silo-wide default.
    /// System trees (those whose id begins with <c>_lattice_</c>) are
    /// always resolved as <c>false</c> regardless of configuration,
    /// because they are not replicated and have no drift-detection
    /// consumer.
    /// </para>
    /// <para>
    /// Storage and wire format are unchanged: the persisted
    /// <c>ProjectionHash</c> / <c>SubtreeProjectionHash</c> / per-child
    /// snapshot tables remain in the leaf and internal-node state schemas
    /// regardless of this setting, so trees may move freely between
    /// digest-maintaining silos and digest-quiescent silos without
    /// reformatting.
    /// </para>
    /// </summary>
    public bool MaintainProjectionDigest { get; set; } = DefaultMaintainProjectionDigest;

    /// <summary>Default value for <see cref="MaintainProjectionDigest"/> (<c>true</c>).</summary>
    public const bool DefaultMaintainProjectionDigest = true;

    /// <summary>
    /// Coalescing window (in milliseconds) for leaf-side projection-digest
    /// publishes to the parent internal node. When greater than zero, the
    /// leaf defers the cross-grain <c>OnChildDigestPublishedAsync</c> hop
    /// behind a one-shot grain timer; mutations arriving within the window
    /// share a single publish, collapsing N per-call cross-grain hops into
    /// one. The leaf's running <c>ProjectionHash</c> is persisted state and
    /// digest consumers (replication shippers, replay coordinators) tolerate
    /// staleness within the window because they re-poll periodically; the
    /// digest is not used for read consistency of point queries.
    /// <para>
    /// Defaults to <see cref="DefaultDigestCoalescingWindowMs"/> (<c>5</c>,
    /// the c2-xxviii measured sweet spot at the c2-iii operating point -
    /// a 27% drop in caller-visible <c>SetAsync</c> p50 vs the synchronous
    /// shape with no observable change to digest correctness). Set to
    /// <c>0</c> to restore the historical synchronous-publish shape if a
    /// consumer depends on the read-after-write digest invariant. The
    /// resolver forces the window to <c>0</c> when
    /// <see cref="MaintainProjectionDigest"/> is <c>false</c>.
    /// </para>
    /// </summary>
    public int DigestCoalescingWindowMs { get; set; } = DefaultDigestCoalescingWindowMs;

    /// <summary>Default value for <see cref="DigestCoalescingWindowMs"/> (<c>5</c>, c2-xxviii measured sweet spot).</summary>
    public const int DefaultDigestCoalescingWindowMs = 5;

    /// <summary>
    /// Number of WAL partitions per tree. Each partition is an independent
    /// per-shard append-only log; the foreground commit-log writer hashes
    /// the mutation key modulo this value to pick the partition. Defaults
    /// to <see cref="DefaultWalPartitions"/> (8) - the multi-partition
    /// fan-out shape. Existing trees pin the value in force at first
    /// WAL write into the tree registry, so a future default flip is
    /// non-breaking for already-registered trees.
    /// </summary>
    public int WalPartitions { get; set; } = DefaultWalPartitions;

    /// <summary>Default value for <see cref="WalPartitions"/> (8).</summary>
    public const int DefaultWalPartitions = 8;

    /// <summary>
    /// Number of shard activations the cluster-wide durable leaf-materialiser
    /// pin store is spread across, per tree. The durable pin store backs the
    /// WAL GC's restart-safe trim floor; every active leaf mirrors its
    /// per-partition checkpoint frontier into it. A single per-tree activation
    /// becomes a fan-in hotspot under a burst that activates or splits many
    /// leaves (every leaf birth and checkpoint funnels durable writes through
    /// the one grain). Re-keying the store across
    /// <see cref="WalMaterialiserPinShards"/> activations spreads that load:
    /// each <c>consumerId</c> deterministically maps to one shard, so the
    /// monotonic-max merge stays correct, and the WAL GC fans its read in
    /// across every shard and unions the result. Defaults to
    /// <see cref="DefaultWalMaterialiserPinShards"/> (8). Set to <c>1</c> to
    /// restore the historical single-activation shape. Changing this value is
    /// a durable-store migration: pins written under the previous shard count
    /// are re-seeded on the next leaf activation / checkpoint, and the WAL GC
    /// also dual-reads the legacy single-activation key during the transition
    /// so no trim floor is lost.
    /// </summary>
    public int WalMaterialiserPinShards { get; set; } = DefaultWalMaterialiserPinShards;

    /// <summary>Default value for <see cref="WalMaterialiserPinShards"/> (8).</summary>
    public const int DefaultWalMaterialiserPinShards = 8;

    /// <summary>
    /// Coalescing window, in milliseconds, for durable writes to the
    /// leaf-materialiser pin store. A non-birth pin report (the per-checkpoint
    /// frontier mirror) advances the in-memory pin immediately and schedules a
    /// single <c>WriteStateAsync</c> at most once per window, so a burst of
    /// reports from many leaves collapses to one durable write per shard per
    /// window instead of one write per report. The in-memory snapshot the WAL
    /// GC reads is always current; only the durable restart-backstop is
    /// debounced, and a durable pin that lags the in-memory frontier only ever
    /// retains more WAL (always GC-safe). Birth "block" pin seeds bypass the
    /// window and write through durably, preserving the crash-safety guarantee
    /// that a new leaf's pin is durable before its data becomes reachable.
    /// Defaults to <see cref="DefaultWalMaterialiserPinFlushIntervalMs"/>
    /// (250 ms). Set to <c>0</c> to disable coalescing (every advancing report
    /// persists synchronously, matching the historical shape).
    /// </summary>
    public int WalMaterialiserPinFlushIntervalMs { get; set; } = DefaultWalMaterialiserPinFlushIntervalMs;

    /// <summary>Default value for <see cref="WalMaterialiserPinFlushIntervalMs"/> (250 ms).</summary>
    public const int DefaultWalMaterialiserPinFlushIntervalMs = 250;

    /// <summary>
    /// Maximum number of leaf-materialiser WAL replays a single silo runs
    /// concurrently. The activation hook
    /// (<see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState"/> projection rebuild) replays the WAL
    /// tail to bring a cold leaf online; under a burst that activates or splits
    /// many leaves at once, an unbounded fan-out of those replays can saturate
    /// every silo thread and starve the foreground request path (the wedge
    /// reproduced in issue #1030). A per-silo semaphore caps how many leaf
    /// replays run at once so a reactivation storm degrades into a queue rather
    /// than a thread-pool stampede; replays release the permit as soon as the
    /// tail is drained. Defaults to
    /// <see cref="DefaultWalMaterialiserMaxConcurrentReplays"/> (<c>0</c>),
    /// which resolves to <see cref="Environment.ProcessorCount"/> at runtime.
    /// Set to a positive value to pin the ceiling explicitly.
    /// </summary>
    public int WalMaterialiserMaxConcurrentReplays { get; set; } = DefaultWalMaterialiserMaxConcurrentReplays;

    /// <summary>
    /// Default value for <see cref="WalMaterialiserMaxConcurrentReplays"/>
    /// (<c>0</c>, resolved to <see cref="Environment.ProcessorCount"/>).
    /// </summary>
    public const int DefaultWalMaterialiserMaxConcurrentReplays = 0;

    /// <summary>
    /// Maximum number of WAL records the activation-time leaf replay applies in
    /// one scheduler turn before yielding cooperatively
    /// (<c>await Task.Yield()</c>). A long-tailed WAL would otherwise let a
    /// single leaf replay monopolise its activation turn, blocking the silo
    /// scheduler from interleaving other ready work (foreground reads, health
    /// probes) for the full duration of the replay. Bounding the per-turn record
    /// count keeps a large replay cooperative so the silo stays responsive while
    /// it drains. Defaults to
    /// <see cref="DefaultWalReplayMaxRecordsPerTurn"/> (256). Set to <c>0</c> to
    /// disable the cooperative yield (replay runs to completion without
    /// voluntarily yielding, the historical shape).
    /// </summary>
    public int WalReplayMaxRecordsPerTurn { get; set; } = DefaultWalReplayMaxRecordsPerTurn;

    /// <summary>Default value for <see cref="WalReplayMaxRecordsPerTurn"/> (256).</summary>
    public const int DefaultWalReplayMaxRecordsPerTurn = 256;

    /// <summary>
    /// Maximum number of entries the WAL grain will batch into a single
    /// storage flush. Defaults to <see cref="DefaultWalMaxBatchEntries"/>
    /// (100). Lower values reduce flush latency at the cost of throughput.
    /// </summary>
    public int WalMaxBatchEntries { get; set; } = DefaultWalMaxBatchEntries;

    /// <summary>Default value for <see cref="WalMaxBatchEntries"/> (100).</summary>
    public const int DefaultWalMaxBatchEntries = 100;

    /// <summary>
    /// Maximum byte budget the WAL grain will accumulate into a single
    /// storage flush. Defaults to <see cref="DefaultWalMaxBatchBytes"/>
    /// (4 MiB). Reached whichever-first with <see cref="WalMaxBatchEntries"/>.
    /// <para>
    /// Measured against the <i>exact</i> serialised size of each
    /// captured <see cref="WalRecord"/> under the WAL grain's wire
    /// format - the per-entry encoder
    /// (<c>IWalRecordEncoder</c>) walks every field of the record
    /// through the same Orleans-binary codec that the storage
    /// provider will see, and the bytes it produces are handed
    /// straight to <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>
    /// on flush so the grain pays exactly one encode per append.
    /// Earlier releases approximated the per-entry cost with
    /// <c>key.Length * 2 + value.Length + 128</c>, which under-counted
    /// records carrying a populated
    /// <see cref="WalRecord.VectorClock"/> and over-counted small-key
    /// records with no vector clock; the budget is now an exact
    /// ceiling, suitable for sizing against the Azure Table Storage
    /// 4 MB transactional-batch limit which has zero tolerance for
    /// under-counts.
    /// </para>
    /// </summary>
    public long WalMaxBatchBytes { get; set; } = DefaultWalMaxBatchBytes;

    /// <summary>Default value for <see cref="WalMaxBatchBytes"/> (4 MiB).</summary>
    public const long DefaultWalMaxBatchBytes = 4L * 1024 * 1024;

    /// <summary>
    /// Maximum number of in-flight + pending batches the per-shard WAL
    /// grain will hold before applying back-pressure to new
    /// <c>Append</c> callers. The grain serialises offset assignment
    /// under the grain turn but lets each batch's
    /// <see cref="IWalStorageProvider.AppendBatchAsync"/> call proceed
    /// independently, so up to this many batches can be "in the system"
    /// at once. New appends beyond the cap await the oldest in-flight
    /// flush before being enqueued; this provides natural back-pressure
    /// under sustained burst load without changing the dense-offset
    /// invariant. Defaults to <see cref="DefaultWalMaxPendingBatches"/>
    /// (16) - the measured Azure Tables Standard sweet spot at 4,000
    /// keys/s offered load on a 4-vCPU host (Standard_D4as_v5 in
    /// westus3, June 2026 measurement). The previous default was 8;
    /// the lift to 16 recorded a +57% increase in steady-state silo
    /// throughput at the 4k:5 rung with no reliability regression. Set
    /// to <c>1</c> for the historical single-in-flight shape (strict
    /// ordering against the provider; no pipeline depth).
    /// <para>
    /// <b>Storage-account ceiling.</b> Raising the cap above 16 in
    /// combination with a matching producer-side dispatch knob can
    /// saturate the Azure Tables storage account: the sustained
    /// per-account throughput threshold (~2,500 transactions/sec on
    /// Standard SKU) collapses under <c>WalPartitions * cap</c>
    /// concurrent flushes, surfaces as <c>429</c> throttling with
    /// <c>Retry-After</c> back-off, and lifts per-flush wall time from
    /// ~50 ms to several seconds. The 16 default at the canonical
    /// <c>WalPartitions = 8</c> caches 128 concurrent flushes against
    /// a single storage account, which is at the edge of the per-
    /// account budget. If you need more headroom, increase
    /// <see cref="WalPartitions"/> (fan-out across accounts) before
    /// lifting the per-partition cap further. Raising the cap above
    /// what the storage provider can usefully serve in parallel
    /// degrades latency without improving throughput.
    /// </para>
    /// <para>
    /// Must be at least <c>1</c>; the registered options validator
    /// rejects non-positive values at first-resolve time.
    /// </para>
    /// </summary>
    public int WalMaxPendingBatches { get; set; } = DefaultWalMaxPendingBatches;

    /// <summary>Default value for <see cref="WalMaxPendingBatches"/> (16, measured Azure Tables Standard sweet spot on Standard_D4as_v5).</summary>
    public const int DefaultWalMaxPendingBatches = 16;

    /// <summary>
    /// Hard ceiling on how long a single per-shard WAL flush (the
    /// <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/> call,
    /// and the post-failure tail resync against
    /// <see cref="IWalStorageProvider.GetHighestOffsetAsync"/>) may run
    /// before it is cancelled and surfaced to callers as a
    /// <see cref="TimeoutException"/>. Bounding the flush is what keeps
    /// a provider call that hangs indefinitely (for example against a
    /// partition left half-activated by a placement/reshard race) from
    /// pinning its in-flight slot forever: without a ceiling the slot is
    /// never removed from the in-flight chain, the chain saturates at
    /// <see cref="WalMaxPendingBatches"/>, and every subsequent append
    /// back-pressures behind a flush that will never settle - a
    /// steady-state stall with no fault and no activation recycle. With
    /// the ceiling the hung flush faults cleanly, the existing failure
    /// handler resynchronises the dense-offset tail from the provider,
    /// drains the chain, and callers retry. Defaults to
    /// <see cref="DefaultWalFlushTimeout"/> (15 seconds) - above the
    /// Azure Tables SDK's worst-case legitimate retry envelope under
    /// sustained throttling (~10 seconds: three exponential backoffs
    /// plus the call times), so a healthy flush never trips it, yet
    /// well below the SDK's per-try network timeout so a true hang is
    /// still caught and the wedged shard self-heals promptly. Set to
    /// <see cref="Timeout.InfiniteTimeSpan"/> to disable the ceiling and
    /// restore the historical unbounded-await behaviour; the registered
    /// options validator rejects any other non-positive value at
    /// first-resolve time.
    /// </summary>
    public TimeSpan WalFlushTimeout { get; set; } = DefaultWalFlushTimeout;

    /// <summary>Default value for <see cref="WalFlushTimeout"/> (15 seconds).</summary>
    public static readonly TimeSpan DefaultWalFlushTimeout = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Optional per-tree <see cref="IWalStorageProvider"/> resolver. When
    /// supplied, takes precedence over the DI-registered default for the
    /// matching tree id. <c>null</c> falls back to the singleton default
    /// (<see cref="InMemoryWalStorageProvider"/> unless replaced by the
    /// host).
    /// </summary>
    public Func<string, IWalStorageProvider>? WalStorageProvider { get; set; }

    /// <summary>
    /// Optional wall-clock hard ceiling for WAL retention. When set, the
    /// WAL garbage collector (<see cref="ILatticeWalGc"/>) trims entries
    /// whose <see cref="Orleans.Lattice.HybridLogicalClock.WallClockTicks"/>
    /// is older than <c>now - WalRetention</c> regardless of consumer
    /// cursor position - bounding worst-case disk usage even when a
    /// registered consumer is hopelessly behind. The lagging consumer
    /// then "falls off the log" on its next read, surfacing the gap to
    /// the auto-bootstrap trigger (replication-side concern).
    /// <para>
    /// <see langword="null"/> (the default) disables the ceiling: the GC
    /// predicate is purely <c>min(consumer cursors)</c>, and a lagging
    /// consumer pins the WAL until it catches up. When set, the value
    /// must be strictly greater than <see cref="TimeSpan.Zero"/>.
    /// </para>
    /// </summary>
    public TimeSpan? WalRetention { get; set; }

    /// <summary>
    /// Upper bound on the cadence at which the per-silo core WAL
    /// garbage-collection scheduler (<see cref="ILatticeWalGc"/>) runs a pass
    /// over a registered tree, so a durable-WAL host gets bounded WAL retention
    /// without the replication package and without any caller invoking
    /// <see cref="ILatticeWalGc.RunOnceAsync"/>.
    /// <para>
    /// Defaults to <see cref="DefaultWalGcInterval"/> (1 hour),
    /// <b>enabled</b>: the core library trims the WAL of every registered
    /// tree (replicated or not) at least once an hour, so the WAL can no
    /// longer grow without bound and <see cref="WalRetention"/> is
    /// effective out of the box. This is the <i>quiet-path</i> tick: a tree
    /// whose passes are reclaiming entries is collected far more often, down to
    /// <see cref="WalGcMinInterval"/>, and relaxes back to this interval once it
    /// has nothing left to reclaim. A pass is retention housekeeping, not a
    /// latency-sensitive operation, so the coarse ceiling keeps the idle
    /// storage cost low; a host that needs a tighter disk bound - a high write
    /// rate paired with a small <see cref="WalRetention"/> - can lower it, and
    /// <see cref="TimeSpan.Zero"/> (or any non-positive value) disables
    /// the scheduler entirely to restore the historical caller-driven
    /// behaviour.
    /// </para>
    /// <para>
    /// The scheduler composes with the replication maintenance grain
    /// (which collects replicated trees on its own faster cadence):
    /// <see cref="ILatticeWalGc.RunOnceAsync"/> and the underlying
    /// <see cref="IWalStorageProvider.TrimAsync"/> are idempotent, so a
    /// replicated tree collected by both drivers is trimmed safely. The
    /// pass honours the minimum consumer cursor and the leaf-materialiser
    /// checkpoint floor, so it never over-trims. This is a global knob
    /// read from the default (unnamed) options; per-tree overrides do not
    /// apply.
    /// </para>
    /// </summary>
    public TimeSpan WalGcInterval { get; set; } = DefaultWalGcInterval;

    /// <summary>
    /// Default value for <see cref="WalGcInterval"/> (1 hour, enabled).
    /// <para>
    /// Deliberately unchanged now that this value is the <i>ceiling</i> of the
    /// adaptive band rather than a fixed tick. Responsiveness is governed by
    /// <see cref="WalGcStartupDelay"/> (first pass within 30 seconds of start)
    /// and <see cref="WalGcMinInterval"/> (the floor a reclaiming tree collapses
    /// to), so lowering this ceiling would only make an <i>idle</i> tree poll
    /// more often - paying storage cost for passes that reclaim nothing - while
    /// buying no responsiveness on a tree that has work to do.
    /// </para>
    /// </summary>
    public static readonly TimeSpan DefaultWalGcInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// Upper bound on the randomized delay before the per-silo WAL
    /// garbage-collection scheduler runs its <b>first</b> pass. The scheduler
    /// draws a uniform offset in <c>[WalGcStartupDelay / 2, WalGcStartupDelay)</c>
    /// so the first pass stays clear of the silo's activation window and no two
    /// silos in a rolling restart align their first fan-out.
    /// <para>
    /// Defaults to <see cref="DefaultWalGcStartupDelay"/> (30 seconds), so the
    /// first pass lands 15 to 30 seconds after start. Before this knob existed
    /// the first pass was staggered across <c>[WalGcInterval / 2,
    /// WalGcInterval)</c> - 30 to 60 minutes at the default cadence - so a host
    /// recreated more often than that never trimmed its WAL at all. Decoupling
    /// the startup stagger from the steady-state cadence keeps both properties
    /// (post-activation, de-correlated) while making reclamation reachable on a
    /// short-lived box.
    /// </para>
    /// <para>
    /// The effective window is capped at <see cref="WalGcInterval"/>, so a host
    /// that configures a cadence shorter than this knob is not made to wait
    /// longer than one interval for its first pass. Set
    /// <see cref="TimeSpan.Zero"/> (or any non-positive value) to run the first
    /// pass immediately, which forfeits both the activation-window guard and
    /// cross-silo de-correlation and is intended for single-silo hosts and
    /// tests. This is a global knob read from the default (unnamed) options;
    /// per-tree overrides do not apply, and the value is read once at start.
    /// </para>
    /// </summary>
    public TimeSpan WalGcStartupDelay { get; set; } = DefaultWalGcStartupDelay;

    /// <summary>Default value for <see cref="WalGcStartupDelay"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultWalGcStartupDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Lower bound on the adaptive per-tree WAL garbage-collection cadence: the
    /// interval the scheduler falls back to for a tree whose last pass actually
    /// reclaimed entries, so a fast-growing log is collected promptly instead of
    /// waiting out a fixed <see cref="WalGcInterval"/> tick.
    /// <para>
    /// The scheduler keeps an independent cadence per tree inside the closed band
    /// <c>[WalGcMinInterval, WalGcInterval]</c>. A pass that trims at least one
    /// entry - the observation that the tree had backlog above the trim floor -
    /// snaps that tree back to this floor; a pass that trims nothing (or fails)
    /// doubles the tree's interval, up to <see cref="WalGcInterval"/> as the
    /// quiet-path ceiling. So a busy tree is collected at this cadence, an idle
    /// tree geometrically relaxes to the configured interval and costs nothing,
    /// and neither one can affect the other's schedule.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultWalGcMinInterval"/> (30 seconds). Set
    /// <see cref="TimeSpan.Zero"/> (or any non-positive value) to disable the
    /// adaptive cadence and restore a fixed <see cref="WalGcInterval"/> tick for
    /// every tree; a value above <see cref="WalGcInterval"/> is likewise clamped
    /// to the interval, which has the same effect. This knob does not change what
    /// a pass may reclaim - trim eligibility and the coverage-gated trim floor are
    /// untouched - only how often a pass runs. This is a global knob read from
    /// the default (unnamed) options; per-tree overrides do not apply, and the
    /// value is read once at start.
    /// </para>
    /// </summary>
    public TimeSpan WalGcMinInterval { get; set; } = DefaultWalGcMinInterval;

    /// <summary>Default value for <see cref="WalGcMinInterval"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultWalGcMinInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional advisory per-tree ceiling, in bytes, on retained WAL size.
    /// When set and the byte-accounting core reports that a tree's retained
    /// WAL exceeds this value, the host-scheduled WAL garbage collector
    /// (<see cref="ILatticeWalGc"/>) lowers its effective trim frontier
    /// toward <see cref="WalBytePressureReclaimTarget"/> of the ceiling -
    /// but <b>only within the already-safe frontier</b> (the minimum
    /// consumer cursor intersected with the causal-stable frontier). The
    /// ceiling never blocks the write path and never trims past a live
    /// consumer's cursor: it is guidance that schedules safe trim work, not
    /// a hard quota that rejects mutations. When the bytes cannot be safely
    /// reclaimed (a consumer is lagging), the policy surfaces an advisory
    /// over-threshold signal and leaves the data intact.
    /// <para>
    /// <see langword="null"/> (the default) disables the policy entirely: the
    /// byte-pressure evaluator short-circuits with zero hot-path cost. When
    /// set, the value must be strictly positive.
    /// </para>
    /// <para>
    /// Deliberately left disabled. This is a capacity quota, not a retention
    /// mechanism: a correct value is a fraction of the volume the WAL lives on,
    /// which the library cannot know, and any value the library picked would be
    /// wrong for most deployments in one direction or the other. Enabling it also
    /// costs one <c>GetRetainedByteSizeAsync</c> probe per WAL partition on every
    /// garbage-collection pass - a cost every consumer would pay for a signal
    /// most do not need. Leaving it off does not blind an operator: the
    /// unconditional pass counter still reports every pass and its outcome, and
    /// reclaimed volume is visible through the entries-trimmed counter, so a tree
    /// reporting passes but no retained-byte samples is knowably "not measured"
    /// rather than "no backlog". Set it when the WAL volume has a hard size
    /// budget and the provider accounts bytes.
    /// </para>
    /// </summary>
    public long? WalMaxRetainedBytes { get; set; }

    /// <summary>
    /// Low-water fraction of <see cref="WalMaxRetainedBytes"/> that disarms the
    /// advisory byte-pressure policy, providing hysteresis so a tree hovering
    /// near the ceiling does not trigger a trim on every pass. The policy arms
    /// when retained WAL crosses the full ceiling (high-water) and re-triggers
    /// a byte-pressure trim on each pass until a trim drives retained bytes at
    /// or below <c>WalMaxRetainedBytes x WalBytePressureReclaimTarget</c>
    /// (low-water), at which point it disarms; growth that then stays inside the
    /// <c>(low-water, ceiling]</c> band does not re-trigger until the ceiling is
    /// crossed again. Defaults to <see cref="DefaultWalBytePressureReclaimTarget"/>
    /// (0.8). Must be in the open-closed interval <c>(0, 1]</c>; values outside
    /// that range are clamped at evaluation time. Ignored when
    /// <see cref="WalMaxRetainedBytes"/> is <see langword="null"/>.
    /// </summary>
    public double WalBytePressureReclaimTarget { get; set; } = DefaultWalBytePressureReclaimTarget;

    /// <summary>Default value for <see cref="WalBytePressureReclaimTarget"/> (0.8).</summary>
    public const double DefaultWalBytePressureReclaimTarget = 0.8;

    /// <summary>
    /// How long a <see cref="TreeStorageUsageReport"/> is cached by the
    /// per-tree storage-usage aggregator before the next call re-fans out
    /// across the tree's shards and partitions. Coalesces repeat dashboard
    /// scrapes so the observable-gauge measurement callbacks never fan out
    /// more than once per window. Defaults to
    /// <see cref="DefaultStorageUsageCacheTtl"/> (10 seconds). Set to
    /// <see cref="TimeSpan.Zero"/> to disable caching (every call fans out).
    /// </summary>
    public TimeSpan StorageUsageCacheTtl { get; set; } = DefaultStorageUsageCacheTtl;

    /// <summary>Default value for <see cref="StorageUsageCacheTtl"/> (10 seconds).</summary>
    public static readonly TimeSpan DefaultStorageUsageCacheTtl = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Cadence at which every silo's background storage-usage poller calls
    /// <see cref="ILatticeAdmin.PollWalUsageAsync"/> so the WAL-bytes and
    /// over-threshold storage gauges populate without any caller invoking
    /// <see cref="ILattice.GetStorageUsageAsync"/>. The poll path is
    /// activation-light: it touches only WAL partition grains, so idle trees
    /// stay cold. The poller runs on every silo (no leader election); because
    /// each tree's WAL-only aggregator is a single cluster-wide activation, its
    /// publish lands on its own host silo's sink and the gauges union across
    /// every live sink, so a tree contributes its series once cluster-wide.
    /// The snapshot-bytes, leaf-state-bytes, and total-bytes gauges are not
    /// refreshed by this poll; they populate on demand via
    /// <see cref="ILattice.GetStorageUsageAsync"/> /
    /// <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>, or on the optional
    /// <see cref="StorageUsageDeepPollInterval"/> cadence. This is a global
    /// knob read from the default (unnamed) options; per-tree overrides do not
    /// apply. Defaults to <see cref="DefaultStorageUsagePollInterval"/>
    /// (15 seconds). Set to <see cref="TimeSpan.Zero"/> or a negative value to
    /// disable the WAL poll (the gauges then only populate when the public API
    /// is called).
    /// </summary>
    public TimeSpan StorageUsagePollInterval { get; set; } = DefaultStorageUsagePollInterval;

    /// <summary>Default value for <see cref="StorageUsagePollInterval"/> (15 seconds).</summary>
    public static readonly TimeSpan DefaultStorageUsagePollInterval = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Optional cadence at which the background storage-usage poller also drives
    /// the <i>deep</i> per-tree aggregator so the snapshot-bytes, leaf-state-bytes,
    /// and total-bytes gauges populate without any caller invoking
    /// <see cref="ILattice.GetStorageUsageAsync"/>. The cheap
    /// <see cref="StorageUsagePollInterval"/> path refreshes only the WAL-bytes
    /// surface (it touches only WAL partition grains); this deep path additionally
    /// reads each shard root's incrementally-maintained byte totals (an O(1) read
    /// per shard root that never walks the leaf chain), so it is heavier than the
    /// WAL-only poll and activates each registered tree's shard roots. Defaults to
    /// <see cref="TimeSpan.Zero"/> (disabled), which preserves the activation-light
    /// poll: the deep gauges then populate only on demand via
    /// <see cref="ILattice.GetStorageUsageAsync"/> or the operator-driven
    /// <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>. Set a positive value
    /// (typically a small multiple of <see cref="StorageUsagePollInterval"/>) to
    /// keep the deep gauges live on a dashboard. A value at or below
    /// <see cref="TimeSpan.Zero"/> disables the deep poll. This is a global knob
    /// read from the default (unnamed) options; per-tree overrides do not apply.
    /// The deep poll never invokes the operator-driven force-refresh fan-out that
    /// re-walks every leaf, so it cannot pin idle leaves resident.
    /// </summary>
    public TimeSpan StorageUsageDeepPollInterval { get; set; } = DefaultStorageUsageDeepPollInterval;

    /// <summary>Default value for <see cref="StorageUsageDeepPollInterval"/> (<see cref="TimeSpan.Zero"/>, disabled).</summary>
    public static readonly TimeSpan DefaultStorageUsageDeepPollInterval = TimeSpan.Zero;

    /// <summary>
    /// Maximum number of trees a cluster-wide storage-usage roll-up
    /// (<see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/>,
    /// <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>, and
    /// <see cref="ILatticeAdmin.PollWalUsageAsync"/>) samples concurrently.
    /// <para>
    /// The roll-up is a two-level fan-out and the levels <b>multiply</b>: each
    /// tree sampled concurrently fans out again to its own shard roots and WAL
    /// partitions (bounded by
    /// <see cref="MaxConcurrentStorageUsageSurfaces"/>), so the peak in-flight
    /// grain-call count is this value times that one. Left unbounded, a cluster
    /// of 90 trees at the default 64 shards and 8 WAL partitions dispatches
    /// roughly 6,500 concurrent calls in a single burst that all race one
    /// Orleans response deadline, and the roll-up fails wholesale with response
    /// timeouts rather than merely taking longer. Bounding both levels makes
    /// the roll-up degrade in <i>latency</i> instead.
    /// </para>
    /// Raising it shortens a roll-up on a large, healthy cluster; lowering it
    /// further reduces the burst a roll-up imposes on silos serving live
    /// traffic. The aggregated figures are identical under any bound - only the
    /// dispatch schedule changes, and per-tree result ordering is preserved.
    /// This is a cluster-wide knob read from the default (unnamed) options by
    /// the admin grain that drives the roll-up; per-tree overrides do not
    /// apply, because that grain is not keyed by tree. The inner, genuinely
    /// per-tree half of the same fan-out
    /// (<see cref="MaxConcurrentStorageUsageSurfaces"/>) <i>is</i> per-tree
    /// overridable. Defaults to
    /// <see cref="DefaultMaxConcurrentStorageUsageTrees"/> (8). Must be at least
    /// 1; values below 1 are clamped to 1 at the roll-up site.
    /// </summary>
    public int MaxConcurrentStorageUsageTrees { get; set; } = DefaultMaxConcurrentStorageUsageTrees;

    /// <summary>Default value for <see cref="MaxConcurrentStorageUsageTrees"/> (8).</summary>
    public const int DefaultMaxConcurrentStorageUsageTrees = 8;

    /// <summary>
    /// Maximum number of per-tree storage surfaces - shard roots plus WAL
    /// partitions - that a single tree's storage-usage aggregator
    /// (<see cref="ILattice.GetStorageUsageAsync"/>) queries concurrently. The
    /// bound spans both surface kinds jointly, so a tree never has more than
    /// this many usage reads outstanding regardless of how its shard count and
    /// <see cref="WalPartitions"/> divide.
    /// <para>
    /// This is the inner level of the two-level roll-up fan-out described on
    /// <see cref="MaxConcurrentStorageUsageTrees"/>; the two multiply into the
    /// cluster-wide peak. A wide tree (the default shard count is 64) would
    /// otherwise dispatch every shard-root read at once even for a single-tree
    /// report.
    /// </para>
    /// The report is byte-for-byte identical under any bound - only the
    /// dispatch schedule changes. Resolved per tree through
    /// <c>LatticeOptionsResolver</c>, like
    /// <see cref="MaxConcurrentSnapshotCaptures"/>, so a single very wide tree
    /// can narrow its own fan-out without changing the cluster-wide bound.
    /// Defaults to
    /// <see cref="DefaultMaxConcurrentStorageUsageSurfaces"/> (16). Must be at
    /// least 1; values below 1 are clamped to 1 at the fan-out site.
    /// </summary>
    public int MaxConcurrentStorageUsageSurfaces { get; set; } = DefaultMaxConcurrentStorageUsageSurfaces;

    /// <summary>Default value for <see cref="MaxConcurrentStorageUsageSurfaces"/> (16).</summary>
    public const int DefaultMaxConcurrentStorageUsageSurfaces = 16;

    /// <summary>
    /// Wall-clock budget a cluster-wide storage-usage roll-up
    /// (<see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/>) may spend sampling
    /// trees before it stops dispatching and returns what it has.
    /// <para>
    /// Bounding the fan-out caps the <i>burst</i> a roll-up imposes, but it
    /// cannot cap the <i>total</i> work: a deep refresh re-walks every shard of
    /// every tree, so a large enough catalogue cannot be sampled inside one
    /// Orleans response deadline however gently it is dispatched. Without a
    /// budget the whole call then fails on the deadline and the caller learns
    /// nothing at all. With one, the trees sampled so far report real figures,
    /// the remainder report as not-answered, and
    /// <see cref="ClusterStorageUsageReport.Partial"/> is set - the same
    /// "an honest flagged lower bound beats a silently wrong or absent answer"
    /// rule the per-surface reporting follows.
    /// </para>
    /// Set it comfortably below the response deadline of the transport carrying
    /// the call, so the truncated report can still be returned. This is a
    /// cluster-wide knob read from the default (unnamed) options by the admin
    /// grain that drives the roll-up; per-tree overrides do not apply, because
    /// that grain is not keyed by tree. Defaults to
    /// <see cref="DefaultStorageUsageRollupBudget"/> (20 seconds). A
    /// non-positive value disables the budget, restoring the previous
    /// run-to-completion behaviour.
    /// </summary>
    public TimeSpan StorageUsageRollupBudget { get; set; } = DefaultStorageUsageRollupBudget;

    /// <summary>Default value for <see cref="StorageUsageRollupBudget"/> (20 seconds).</summary>
    public static readonly TimeSpan DefaultStorageUsageRollupBudget = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Hard ceiling on how long a single outbound shard-to-shard write
    /// forward (the shadow-forward and cross-shard migration forwards a
    /// <c>ShardRootGrain</c> issues to a sibling shard while an adaptive
    /// split or online resize is in flight) may run before it is
    /// abandoned and surfaced to the forwarding turn as a
    /// <see cref="TimeoutException"/>. Bounding the forward is what keeps
    /// a forwarded write that targets a shard whose ownership is changing
    /// during the reshard swap phase - where Orleans can reject the
    /// outbound message and leave the caller-side await neither completing
    /// nor faulting - from pinning the foreground write pipeline
    /// indefinitely: without a ceiling the forwarding turn never returns,
    /// the lattice grain's per-shard fan-out saturates at its in-flight
    /// limit, and every subsequent write back-pressures behind a forward
    /// that will never settle - a steady-state stall with no fault and no
    /// activation recycle. With the ceiling the parked forward faults
    /// cleanly; convergence on the destination shard is already guaranteed
    /// by last-writer-wins plus the split coordinator's background drain,
    /// so the foreground write commits locally and the existing
    /// stale-routing retry loop re-runs the operation against refreshed
    /// routing once the swap has settled. Defaults to
    /// <see cref="DefaultShardForwardTimeout"/> (15 seconds) - above the
    /// worst-case legitimate cross-shard RPC envelope under a healthy
    /// split, yet well below the caller-side budget so a true park is
    /// caught and the wedged pipeline self-heals promptly. Set to
    /// <see cref="Timeout.InfiniteTimeSpan"/> to disable the ceiling and
    /// restore the historical unbounded-await behaviour; the registered
    /// options validator rejects any other non-positive value at
    /// first-resolve time.
    /// </summary>
    public TimeSpan ShardForwardTimeout { get; set; } = DefaultShardForwardTimeout;

    /// <summary>Default value for <see cref="ShardForwardTimeout"/> (15 seconds).</summary>
    public static readonly TimeSpan DefaultShardForwardTimeout = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Ceiling on the emptiness probe that reshard initiation runs before
    /// taking its empty-tree fast path.
    /// <para>
    /// <see cref="Orleans.Lattice.ILattice.ReshardAsync(int, System.Threading.CancellationToken)"/>
    /// only needs a boolean - "does this tree hold any live key?" - but the
    /// count that answers it is a strongly-consistent whole-tree fan-out that
    /// restarts whenever the shard map moves under it (see
    /// <see cref="MaxScanRetries"/>). Reshard initiation is precisely when
    /// that map is most likely to be churning: a caller may be writing
    /// concurrently, and a small leaf fan-out splits continuously. Unbounded,
    /// the probe can burn the whole caller-side response budget and time the
    /// reshard out before it has even started.
    /// </para>
    /// <para>
    /// An inconclusive probe is treated as "not empty", which is not merely
    /// the safe direction but the accurate one: the only thing that makes the
    /// probe slow is concurrent split churn, and a tree whose topology is
    /// churning necessarily holds keys. A genuinely empty tree has nothing to
    /// churn, answers well inside the budget, and still takes the fast path.
    /// Defaults to <see cref="DefaultEmptyTreeProbeBudget"/> (10 seconds) -
    /// comfortably above a cold fan-out's activation-retry envelope, yet well
    /// below the caller-side budget. Set to
    /// <see cref="Timeout.InfiniteTimeSpan"/> to restore the historical
    /// unbounded-probe behaviour; the registered options validator rejects any
    /// other non-positive value at first-resolve time.
    /// </para>
    /// </summary>
    public TimeSpan EmptyTreeProbeBudget { get; set; } = DefaultEmptyTreeProbeBudget;

    /// <summary>Default value for <see cref="EmptyTreeProbeBudget"/> (10 seconds).</summary>
    public static readonly TimeSpan DefaultEmptyTreeProbeBudget = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Hard ceiling on how long a <c>ShardRootGrain</c>'s one-time
    /// activation-readiness seed (the cross-grain awaits a brand-new or
    /// freshly-reactivated shard runs the first time it prepares for an
    /// operation: the defensive state re-read, the tree-registry
    /// registration, the deterministic root-leaf initialization, and the
    /// initial shard-state write) may run before it is abandoned and
    /// surfaced to the preparing turn as a <see cref="TimeoutException"/>.
    /// Bounding the seed is what keeps a half-activated shard - one whose
    /// registry or leaf RPC Orleans rejected or parked during a startup
    /// reshard / membership change, leaving the caller-side await neither
    /// completing nor faulting - from holding the non-reentrant
    /// activation gate indefinitely: without a ceiling the seeding turn
    /// never returns, every interleaved read/write on that activation
    /// parks behind the held gate, the lattice grain's per-shard fan-out
    /// saturates at its in-flight limit, and the whole write pipeline
    /// wedges with no fault and no activation recycle until the
    /// caller-side Orleans response deadline (default 3 minutes) expires.
    /// With the ceiling the parked seed faults cleanly, the gate's
    /// <c>finally</c> releases, and the existing transient-exception retry
    /// envelope on every mutation path re-runs the seed against refreshed
    /// routing / registration once the startup reshard has settled. The
    /// seed's cross-grain steps are each idempotent on retry, so
    /// abandoning a parked seed never loses data or double-registers.
    /// Defaults to <see cref="DefaultActivationReadyTimeout"/> (15
    /// seconds) - above the worst-case legitimate first-activation RPC
    /// envelope under a healthy cluster, yet well below the caller-side
    /// budget so a true park is caught and the wedged pipeline self-heals
    /// promptly. Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable
    /// the ceiling and restore the historical unbounded-await behaviour;
    /// the registered options validator rejects any other non-positive
    /// value at first-resolve time.
    /// </summary>
    public TimeSpan ActivationReadyTimeout { get; set; } = DefaultActivationReadyTimeout;

    /// <summary>Default value for <see cref="ActivationReadyTimeout"/> (15 seconds).</summary>
    public static readonly TimeSpan DefaultActivationReadyTimeout = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Maximum time an internal-node digest publish (the upward
    /// <c>ChildDigestSnapshot</c> propagation that a
    /// <c>BPlusInternalGrain</c> issues to its parent after folding a
    /// child's digest) may park before it is abandoned. The publish is a
    /// cross-grain RPC held under the internal node's non-reentrant split
    /// gate while it recurses up the internal-node chain toward the shard
    /// root; a parent that is itself mid-mutation can leave the await
    /// neither completing nor faulting, pinning the gate on this
    /// activation with no ceiling and wedging every subsequent mutating
    /// turn behind it. With the ceiling the parked publish faults cleanly
    /// and the gate is released; the digest is staleness-tolerant, so the
    /// next mutation's dirty-flag publish re-drives convergence and no
    /// data or digest-count accuracy is lost (the exact-count invariant is
    /// preserved because the abandoned publish never partially applied at
    /// the parent). Defaults to
    /// <see cref="DefaultDigestPublishTimeout"/> (15 seconds) - above the
    /// worst-case legitimate per-hop digest-fold RPC envelope, yet well
    /// below the Orleans response timeout so a true park is caught before
    /// the activation wedges. Set to
    /// <see cref="Timeout.InfiniteTimeSpan"/> to disable the ceiling and
    /// restore the historical unbounded-await behaviour; the registered
    /// options validator rejects any other non-positive value at
    /// first-resolve time.
    /// </summary>
    public TimeSpan DigestPublishTimeout { get; set; } = DefaultDigestPublishTimeout;

    /// <summary>Default value for <see cref="DigestPublishTimeout"/> (15 seconds).</summary>
    public static readonly TimeSpan DefaultDigestPublishTimeout = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Maximum time the per-tree WAL writer
    /// (<c>WalCommitLogWriter</c>) will wait on a single outbound
    /// cross-grain <c>IWalShardGrain.AppendBatchAsync</c> /
    /// <c>AppendAsync</c> dispatch before abandoning the await and
    /// surfacing a <see cref="TimeoutException"/> to the caller. The
    /// dispatch is the request-path RPC from the producer-facing
    /// writer into the per-shard WAL grain; it is the outermost
    /// observable seam on the write pipeline and was historically
    /// unbounded on the writer side, so a wedged shard activation would
    /// hold every caller's dispatch parked until the Orleans response
    /// deadline (default 3 minutes) expired - a 180-second blind hang
    /// with no per-shard attribution. Bounding the dispatch converts
    /// that blind hang into a structured fault with per-shard counter
    /// attribution (<see cref="Orleans.Lattice.LatticeMetrics.WalAppendDispatchTimeouts"/>),
    /// so a wedged shard surfaces immediately and the request pipeline
    /// releases its slot rather than back-filling behind the wedge.
    /// This option does <b>not</b> fix the wedge mechanism itself - the
    /// grain-side flush / activation deadlines already bound their own
    /// regions - it bounds the symptom on the writer side and makes
    /// every wedge attributable to a specific
    /// <c>(tree, shard)</c> in O(timeout) instead of O(response
    /// timeout) time. Defaults to
    /// <see cref="DefaultWalAppendDispatchTimeout"/> (30 seconds) -
    /// above the legitimate envelope of a fully-saturated dispatch
    /// (one healthy flush + headroom), yet well below the Orleans
    /// response timeout so a true park is caught and surfaced
    /// promptly. Set to <see cref="Timeout.InfiniteTimeSpan"/> to
    /// disable the ceiling and restore the historical unbounded-await
    /// behaviour; the registered options validator rejects any other
    /// non-positive value at first-resolve time.
    /// </summary>
    public TimeSpan WalAppendDispatchTimeout { get; set; } = DefaultWalAppendDispatchTimeout;

    /// <summary>Default value for <see cref="WalAppendDispatchTimeout"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultWalAppendDispatchTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum time the per-shard WAL grain's <c>FlushAsync</c> may
    /// spend in its preflight region (the synchronous setup and
    /// initial scheduler yield that precede the bounded provider
    /// call) before the flush is abandoned and the slot drains. The
    /// preflight region is normally microseconds - it copies the
    /// already-encoded segments into the parallel arrays the provider
    /// expects and hands them over - but if the activation's grain
    /// scheduler never resumes the post-yield continuation (e.g. a
    /// startup reshard / membership change parked the activation, a
    /// non-cooperative work item is hogging the scheduler, or the
    /// activation is being torn down mid-flush), the slot sits in
    /// <c>_inFlight</c> with no deadline armed - the existing
    /// <see cref="WalFlushTimeout"/> only covers the provider call,
    /// which has not yet been issued - and the chain saturates at
    /// <see cref="WalMaxPendingBatches"/> with no fault and no
    /// activation recycle. With the ceiling the parked preflight
    /// faults cleanly as a <see cref="TimeoutException"/> routed
    /// through the normal failure handler, the slot drains, and the
    /// <see cref="Orleans.Lattice.LatticeMetrics.WalFlushPreflightTimeouts"/>
    /// counter attributes the trip to the affected
    /// <c>(tree, shard)</c>. Defaults to
    /// <see cref="DefaultWalFlushPreflightTimeout"/> (5 seconds) -
    /// orders of magnitude above the legitimate microsecond envelope,
    /// yet small enough that a genuinely stalled scheduler is caught
    /// before the writer-side dispatch deadline
    /// (<see cref="WalAppendDispatchTimeout"/>) trips. Set to
    /// <see cref="Timeout.InfiniteTimeSpan"/> to disable the ceiling
    /// and restore the historical unbounded-await behaviour; the
    /// registered options validator rejects any other non-positive
    /// value at first-resolve time.
    /// </summary>
    public TimeSpan WalFlushPreflightTimeout { get; set; } = DefaultWalFlushPreflightTimeout;

    /// <summary>Default value for <see cref="WalFlushPreflightTimeout"/> (5 seconds).</summary>
    public static readonly TimeSpan DefaultWalFlushPreflightTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Hard ceiling on how long a per-shard WAL grain's
    /// <c>OnDeactivateAsync</c> drain may run before the remaining
    /// in-flight slots are force-faulted and the chain is released so
    /// the activation can finish tearing down. Bounds the host-level
    /// SIGTERM drain so the silo's shutdown accounting (the
    /// benchmark host's <c>FINAL</c> line, an
    /// <see cref="Microsoft.Extensions.Hosting.IHostApplicationLifetime.ApplicationStopping"/>
    /// cancellation source) always settles within bounded time of the
    /// SIGTERM, regardless of whether the underlying storage provider
    /// is healthy.
    /// <para>
    /// Defends against the saturating-storage-account wedge: when the
    /// provider call's await is parked behind an SDK retry loop in
    /// pre-attempt back-off, the existing per-flush
    /// <see cref="WalFlushTimeout"/> may not fire promptly (the SDK
    /// observes cancellation only between attempts, not during
    /// back-off), so a chain with N in-flight slots can hold the
    /// deactivation indefinitely. With this budget the drain cancels
    /// every in-flight flush's cancellation token at entry, awaits the
    /// chain to settle naturally for up to <see cref="WalDrainBudget"/>,
    /// and then force-faults any slot that has not unlinked with a
    /// <see cref="TimeoutException"/>. The faulted slot's ack TCSs are
    /// completed via the normal failure-handler path so callers parked
    /// on <c>AppendAsync</c> / <c>AppendBatchAsync</c> are released
    /// rather than parking through the rest of the host shutdown.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultWalDrainBudget"/>
    /// (75 seconds = <c>5 * <see cref="DefaultWalFlushTimeout"/></c>),
    /// chosen so a healthy chain with cap=16 in-flight flushes has
    /// time to drain naturally (each flush is itself bounded by
    /// <see cref="WalFlushTimeout"/>) while a wedged chain still
    /// surfaces within a bounded window of the SIGTERM. The
    /// <see cref="Orleans.Lattice.LatticeMetrics.WalShardDrainBudgetExpirations"/>
    /// counter attributes a budget-driven force-fault to the affected
    /// <c>(tree, shard)</c>. Set to
    /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to
    /// disable the ceiling and restore the historical unbounded-drain
    /// behaviour; the registered options validator rejects any other
    /// non-positive value at first-resolve time.
    /// </para>
    /// </summary>
    public TimeSpan WalDrainBudget { get; set; } = DefaultWalDrainBudget;

    /// <summary>Default value for <see cref="WalDrainBudget"/> (75 seconds = 5 * <see cref="DefaultWalFlushTimeout"/>).</summary>
    public static readonly TimeSpan DefaultWalDrainBudget = TimeSpan.FromSeconds(75);

    /// <summary>
    /// Cadence at which the silo-scoped sampler that backs
    /// <see cref="Orleans.Lattice.IWalSaturationSignal"/> and
    /// <see cref="Orleans.Lattice.IWalSaturationObserver"/> recomputes
    /// the per-tree saturation state from the writer-side admission
    /// gate and the recent dispatch-timeout-trip rate. A shorter
    /// interval lowers the worst-case transition latency observers
    /// see (the bound is one sample interval beyond the underlying
    /// signal crossing the threshold) at the cost of slightly more
    /// timer-driven sampler work. Defaults to
    /// <see cref="DefaultWalSaturationSampleInterval"/> (200 ms),
    /// chosen so subscribers transition well within the one-second
    /// bound the public surface promises while keeping the sampler
    /// at a negligible CPU footprint on an idle silo. Set to
    /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to
    /// disable the sampler entirely (the signal stays
    /// <see cref="Orleans.Lattice.WalSaturationState.Healthy"/>
    /// forever); the registered options validator rejects any other
    /// non-positive value at first-resolve time.
    /// </summary>
    public TimeSpan WalSaturationSampleInterval { get; set; } = DefaultWalSaturationSampleInterval;

    /// <summary>Default value for <see cref="WalSaturationSampleInterval"/> (200 ms).</summary>
    public static readonly TimeSpan DefaultWalSaturationSampleInterval = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Per-partition admission-depth ratio (in <c>[0.0, 1.0]</c>) at or
    /// above which the saturation signal raises a tree to
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>. The
    /// ratio is computed as
    /// <c>in_flight / <see cref="WalMaxPendingBatches"/></c> on each
    /// partition; the tree's state is the worst-case across its
    /// partitions. Below the ratio the tree stays
    /// <see cref="Orleans.Lattice.WalSaturationState.Healthy"/>; at or
    /// above the ratio it advances to
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>; at
    /// the cap with a non-empty wait queue (or when the dispatch-timeout
    /// rate crosses
    /// <see cref="WalSaturationDispatchTimeoutThreshold"/>) it advances
    /// to <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>.
    /// Defaults to <see cref="DefaultWalSaturationThrottledRatio"/>
    /// (0.75) - far enough above steady-state pipeline depth that
    /// healthy bursts do not flap the state, while still leaving a
    /// 25%-of-cap headroom for callers to slow down before the cap
    /// pins. Must be in the inclusive range <c>[0.0, 1.0]</c>.
    /// </summary>
    public double WalSaturationThrottledRatio { get; set; } = DefaultWalSaturationThrottledRatio;

    /// <summary>Default value for <see cref="WalSaturationThrottledRatio"/> (0.75).</summary>
    public const double DefaultWalSaturationThrottledRatio = 0.75;

    /// <summary>
    /// Minimum number of
    /// <c>orleans.lattice.wal.append_dispatch.timeouts</c> trips
    /// observed within a single
    /// <see cref="WalSaturationSampleInterval"/> sample window that
    /// raises a tree to
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// regardless of admission-semaphore depth. Captures the
    /// dispatch-deadline failure-tail of the saturation regime
    /// (parked dispatches abandoned because a downstream shard wedged)
    /// in addition to the admission-depth fast signal. Defaults to
    /// <see cref="DefaultWalSaturationDispatchTimeoutThreshold"/>
    /// (1), so even a single dispatch-timeout trip in a sample window
    /// flags the affected tree as saturated; raise it on dashboards
    /// where occasional single trips are expected without operator
    /// concern. Must be greater than or equal to 1.
    /// </summary>
    public int WalSaturationDispatchTimeoutThreshold { get; set; } = DefaultWalSaturationDispatchTimeoutThreshold;

    /// <summary>Default value for <see cref="WalSaturationDispatchTimeoutThreshold"/> (1).</summary>
    public const int DefaultWalSaturationDispatchTimeoutThreshold = 1;

    /// <summary>
    /// Minimum number of provider-side commit failures (any
    /// <see cref="System.Exception"/> surfaced from a downstream
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.IWalShardGrain.AppendAsync(Orleans.Lattice.WalRecord, System.Threading.CancellationToken)"/>
    /// / <see cref="Orleans.Lattice.BPlusTree.Grains.IWalShardGrain.AppendBatchAsync(System.Collections.Generic.IReadOnlyList{Orleans.Lattice.WalRecord}, System.Threading.CancellationToken)"/>
    /// dispatch other than the writer-side
    /// <see cref="System.TimeoutException"/> already captured by
    /// <see cref="WalSaturationDispatchTimeoutThreshold"/>) observed
    /// within a single <see cref="WalSaturationSampleInterval"/>
    /// sample window that raises a tree to
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// regardless of admission-semaphore depth and dispatch-timeout
    /// trips. Captures the third saturation regime the writer side
    /// cannot otherwise surface: a downstream storage provider whose
    /// commit calls return quickly (so neither the admission depth nor
    /// the dispatch deadline ever crosses the threshold) but
    /// terminally fail at a high rate, e.g. an Azure Tables single-
    /// account 409-Conflict burst where the SDK retry races a server-
    /// side-already-committed transaction. Without this input, a
    /// caller saw the failure tail (a <c>SetAsync</c> /
    /// <c>SetManyAsync</c> faulted) but the per-tree saturation signal
    /// stayed <see cref="Orleans.Lattice.WalSaturationState.Healthy"/>
    /// and any back-pressure consumer (the bench TCP reader, an
    /// upstream load balancer) had no leading-edge surface to slow
    /// down before the leak became visible at the operator level.
    /// Defaults to <see cref="DefaultWalSaturationProviderFailureRateThreshold"/>
    /// (1), so even a single provider failure in a sample window
    /// flags the affected tree as saturated; raise it on dashboards
    /// where occasional single failures are expected without operator
    /// concern. Set to <c>0</c> to disable the trigger entirely
    /// (matches the <c>InfiniteTimeSpan</c> sentinel on the other
    /// saturation options); the registered options validator rejects
    /// any other non-negative value at first-resolve time.
    /// </summary>
    public int WalSaturationProviderFailureRateThreshold { get; set; } = DefaultWalSaturationProviderFailureRateThreshold;

    /// <summary>Default value for <see cref="WalSaturationProviderFailureRateThreshold"/> (1).</summary>
    public const int DefaultWalSaturationProviderFailureRateThreshold = 1;

    /// <summary>
    /// Window after the most-recently observed
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// transition during which the saturation classifier holds a tree
    /// at or above <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>
    /// regardless of the current sampler tick's per-partition depth
    /// observation. Defends against the bursty per-partition WAL
    /// drain pattern where one partition fills to cap, drains
    /// entirely in the next tick, and the next partition then fills:
    /// without the window, the per-tick <c>max(depth_ratio)</c> across
    /// partitions oscillates between ~1.0 and ~0.0 inside a single
    /// sampler period and the classifier flaps
    /// <see cref="Orleans.Lattice.WalSaturationState.Healthy"/> &lt;-&gt;
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/> at
    /// the sampler cadence, leaving the
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>
    /// advisory regime effectively unobservable. With the window the
    /// classifier upgrades a transient
    /// <see cref="Orleans.Lattice.WalSaturationState.Healthy"/>
    /// classification to
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/> for
    /// the duration of the window after a
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// observation, so callers see the regime persist as the natural
    /// lead-up and fall-back state around saturation episodes.
    /// <para>
    /// Does not affect the
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// detection itself - that still fires on the current tick's
    /// at-cap condition, so the
    /// <see cref="Orleans.Lattice.WalSaturationState.Healthy"/>
    /// -&gt;
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// transition latency remains bounded by one
    /// <see cref="WalSaturationSampleInterval"/>. Does not affect
    /// the recovery path either: once the window expires AND the
    /// current tick observes no saturation pressure, the tree drops
    /// to <see cref="Orleans.Lattice.WalSaturationState.Healthy"/>
    /// and any pending
    /// <see cref="Orleans.Lattice.IWalSaturationSignal.WaitForHealthyAsync(string, System.Threading.CancellationToken)"/>
    /// completes - so the public saturation-signal contract that
    /// recovery latency is bounded by one sample interval after the
    /// underlying signal clears still holds; the window only delays
    /// it by the configured value.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultWalSaturationRecoveryWindow"/>
    /// (1 second) - long enough to span the typical multi-partition
    /// burst cycle at <see cref="WalPartitions"/> = 8 and
    /// <see cref="WalMaxPendingBatches"/> = 16 (one partition
    /// completing its cycle within several sampler ticks of the
    /// previous), short enough that a genuinely-recovered tree
    /// surfaces back to <see cref="Orleans.Lattice.WalSaturationState.Healthy"/>
    /// within one second of the underlying signal clearing. Set to
    /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to
    /// hold a tree at <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>
    /// forever after the first
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// observation - useful for tests that want a sticky
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>
    /// floor without arming a wall-clock dependency. Set to
    /// <see cref="System.TimeSpan.Zero"/> to disable the window
    /// entirely and restore the pre-fix classifier behaviour where
    /// the per-tick depth observation drives the regime directly;
    /// the registered options validator rejects any other negative
    /// value at first-resolve time.
    /// </para>
    /// </summary>
    public TimeSpan WalSaturationRecoveryWindow { get; set; } = DefaultWalSaturationRecoveryWindow;

    /// <summary>Default value for <see cref="WalSaturationRecoveryWindow"/> (1 second).</summary>
    public static readonly TimeSpan DefaultWalSaturationRecoveryWindow = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Optional per-flush latency threshold that, when crossed for
    /// <see cref="WalSaturationFlushLatencySampleWindows"/> consecutive
    /// sampler ticks, escalates the affected tree to
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// regardless of admission-semaphore depth, dispatch-timeout count,
    /// or provider-failure count. Defaults to <c>null</c> (disabled);
    /// when set, the classifier gains a fourth Saturated input on top of
    /// the existing depth-ratio, dispatch-timeout, and provider-failure
    /// branches.
    /// <para>
    /// <b>Why a flush-latency input.</b> The original three classifier
    /// inputs leave a workload-shape blind spot on small-batch
    /// workloads (e.g. single-entry <c>SetAsync</c>): the per-call batch
    /// entries are 1, so the per-partition WAL admission semaphore
    /// never fills to the
    /// <see cref="WalSaturationThrottledRatio"/> cap and the depth-
    /// ratio path never trips; the writer-side
    /// <see cref="WalAppendDispatchTimeout"/> takes longer to expire
    /// than the wedge takes to form; and terminal provider failures
    /// happen late in the regime (after the SDK has exhausted internal
    /// retries). Sustained slow flushes are the leading-edge signal on
    /// these workloads. When the per-shard
    /// <c>orleans.lattice.wal.append.provider.duration</c> observation
    /// crosses this threshold the writer increments a per-(tree, shard)
    /// trip counter; the sampler reads the per-window delta and applies
    /// the consecutive-window check described on
    /// <see cref="WalSaturationFlushLatencySampleWindows"/>.
    /// </para>
    /// <para>
    /// <b>Why opt-in and consecutive-window-gated.</b> The default
    /// classifier-input set (admission depth, dispatch-timeout count,
    /// provider-failure count) already closes the canonical large-batch
    /// regime via the indirect <c>HasParkedCallers</c> signal that the
    /// writer-side admission gate produces under saturation. The
    /// flush-latency input is a belt-and-braces leading-edge surface
    /// for workload shapes where the indirect signal is too thin; it
    /// stays disabled by default so a single flush-latency spike on a
    /// healthy host cannot flip the regime. The consecutive-window
    /// requirement defends against single-shot flush-latency outliers
    /// (a GC pause, a transient transport hiccup) so only sustained
    /// slow-flush regimes escalate.
    /// </para>
    /// <para>
    /// <b>Sizing.</b> Pick a threshold well above the steady-state
    /// p99 of <c>orleans.lattice.wal.append.provider.duration</c> on the
    /// healthy host so routine latency variance never trips it. A
    /// typical Azure Tables operating point sits at sub-100 ms p99 on a
    /// well-provisioned account; a threshold of 500 ms - 1 s with
    /// <see cref="WalSaturationFlushLatencySampleWindows"/> = 3 yields a
    /// 600 ms - 3 s detection latency at the default
    /// <see cref="WalSaturationSampleInterval"/> (200 ms) - faster than
    /// the writer-side <see cref="WalAppendDispatchTimeout"/> default
    /// (30 s) and still inside the
    /// <see cref="WalAdmissionSaturationWaitBudget"/> default (5 s),
    /// so the new input arms the admission gate's saturation refusal
    /// before the dispatch deadline would otherwise surface as a
    /// timeout.
    /// </para>
    /// <para>
    /// Set to <c>null</c> (the default) to disable the input entirely;
    /// the classifier observes its historical three-input behaviour
    /// exactly. The registered options validator rejects any
    /// non-positive value when the option is set.
    /// </para>
    /// </summary>
    public TimeSpan? WalSaturationFlushLatencyThreshold { get; set; }

    /// <summary>
    /// Number of consecutive sampler ticks during which the per-(tree,
    /// shard) <c>orleans.lattice.wal.append.provider.duration</c>
    /// observation must exceed
    /// <see cref="WalSaturationFlushLatencyThreshold"/> before the
    /// classifier escalates the tree to
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/> via
    /// the flush-latency branch. Defaults to
    /// <see cref="DefaultWalSaturationFlushLatencySampleWindows"/> (3),
    /// which at the default <see cref="WalSaturationSampleInterval"/>
    /// (200 ms) means the trip must persist for 600 ms before the
    /// regime escalates - long enough to suppress single-shot
    /// flush-latency spikes (GC pauses, transient transport hiccups)
    /// while still leading the writer-side
    /// <see cref="WalAppendDispatchTimeout"/> by an order of magnitude.
    /// <para>
    /// Has no effect when
    /// <see cref="WalSaturationFlushLatencyThreshold"/> is <c>null</c>
    /// (the flush-latency input is disabled entirely). When the
    /// threshold is set, the classifier maintains a per-tree counter
    /// that increments on every sampler tick whose per-window flush-
    /// latency trip delta is non-zero, and resets to zero on every
    /// tick whose delta is zero. The Saturated escalation fires when
    /// the counter reaches this value. Must be greater than or equal
    /// to 1.
    /// </para>
    /// </summary>
    public int WalSaturationFlushLatencySampleWindows { get; set; } = DefaultWalSaturationFlushLatencySampleWindows;

    /// <summary>Default value for <see cref="WalSaturationFlushLatencySampleWindows"/> (3).</summary>
    public const int DefaultWalSaturationFlushLatencySampleWindows = 3;

    /// <summary>
    /// WAL saturation input that escalates a tree to
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/> when its
    /// leaf-materialiser drain frontier falls more than this far behind the
    /// WAL head - the direct "the materialiser is not keeping up with the
    /// write rate" surface that the indirect admission-depth and flush-latency
    /// inputs only approximate. The WAL GC measures the lag on each pass as
    /// <c>walHead.WallClockTicks - materialiserFrontier.WallClockTicks</c>
    /// (clamped at zero): the age of the oldest WAL entry the slowest durable
    /// leaf-materialiser checkpoint has not yet drained. Because the measure is
    /// head-relative rather than wall-clock-relative it reads zero on an
    /// idle-but-caught-up tree (the frontier reaches the head), so a quiescent
    /// tree never trips. The GC records the standing lag as a per-tree level
    /// that the saturation sampler re-reads every tick; once the level stays
    /// above this threshold for
    /// <see cref="WalSaturationMaterialiserLagSampleWindows"/> consecutive
    /// sampler windows the tree is held at Throttled.
    /// <para>
    /// This is the back-pressure surface that protects a downstream
    /// replication receiver. When a write burst outruns the materialiser drain
    /// the resulting Throttled state flows automatically through
    /// <c>IWalSaturationSignal</c> to the replication receiver flow-control
    /// policy (which drip-feeds and pauses the upstream sender) and to the
    /// writer admission path (which rides its natural bounded-semaphore
    /// back-pressure) - so a modest burst slows the producers rather than
    /// pegging every silo before the drain catches up. Throttled is a pure
    /// back-off: unlike the acute dispatch-timeout, provider-failure, and
    /// flush-latency inputs the drain-lag input never escalates to Saturated
    /// and so never trips the admission gate's
    /// <c>LatticeSaturatedException</c> fast-fail - a sustained drain lag
    /// slows callers, it does not fault them.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultWalSaturationMaterialiserLagThreshold"/>
    /// (30 seconds); set to <c>null</c> to disable the input entirely (the
    /// classifier then ignores drain lag and the GC skips the WAL-head read). A
    /// block pin (a never-checkpointed leaf, which disables the cursor trim
    /// branch) is not treated as lag and never trips this input. The registered
    /// options validator rejects a non-positive value when the option is set.
    /// The level observation refreshes at the WAL GC cadence (the replication
    /// maintenance interval for replicated trees, <see cref="WalGcInterval"/>
    /// otherwise), so the input engages for trees whose GC runs frequently
    /// enough to keep the observation fresh.
    /// </para>
    /// </summary>
    public TimeSpan? WalSaturationMaterialiserLagThreshold { get; set; } = DefaultWalSaturationMaterialiserLagThreshold;

    /// <summary>Default value for <see cref="WalSaturationMaterialiserLagThreshold"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultWalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Number of consecutive saturation-sampler windows during which a tree's
    /// leaf-materialiser drain lag must stay above
    /// <see cref="WalSaturationMaterialiserLagThreshold"/> before the
    /// saturation classifier holds the tree at
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/> via the
    /// drain-lag branch. Defaults to
    /// <see cref="DefaultWalSaturationMaterialiserLagSampleWindows"/> (3),
    /// mirroring the flush-latency input so a single sampler tick cannot flip
    /// the regime. Has no effect when
    /// <see cref="WalSaturationMaterialiserLagThreshold"/> is <c>null</c>. Must
    /// be greater than or equal to 1.
    /// </summary>
    public int WalSaturationMaterialiserLagSampleWindows { get; set; } = DefaultWalSaturationMaterialiserLagSampleWindows;

    /// <summary>Default value for <see cref="WalSaturationMaterialiserLagSampleWindows"/> (3).</summary>
    public const int DefaultWalSaturationMaterialiserLagSampleWindows = 3;

    /// <summary>
    /// Wall-clock budget the WAL writer admission gate
    /// (<c>PartitionTracker.AcquireAsync</c>) spends parked on
    /// <see cref="Orleans.Lattice.IWalSaturationSignal.WaitForHealthyAsync(string, System.Threading.CancellationToken)"/>
    /// before refusing a dispatch with
    /// <see cref="Orleans.Lattice.LatticeSaturatedException"/> when
    /// the per-tree saturation signal reports
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>.
    /// Closes the pre-FX consumer-coverage gap where the writer
    /// admission semaphore was signal-blind: under the storage-account
    /// 409-Conflict regime the saturation classifier raised
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// many times before the first observable failure, but every new
    /// dispatch still admitted into the semaphore and parked at the
    /// cap, taking the full
    /// <see cref="WalAppendDispatchTimeout"/> to surface as
    /// <see cref="System.TimeoutException"/> instead of the configured
    /// shorter budget.
    /// <para>
    /// <b>Mechanics.</b> Before each
    /// <see cref="System.Threading.SemaphoreSlim.WaitAsync(System.Threading.CancellationToken)"/>
    /// on the admission semaphore, the tracker calls
    /// <see cref="Orleans.Lattice.IWalSaturationSignal.GetCurrentState(string)"/>.
    /// On <see cref="Orleans.Lattice.WalSaturationState.Healthy"/> /
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>
    /// the check is a single concurrent-dictionary lookup and the
    /// caller proceeds directly into the semaphore (no allocation,
    /// no extra await). On <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// the tracker awaits
    /// <see cref="Orleans.Lattice.IWalSaturationSignal.WaitForHealthyAsync(string, System.Threading.CancellationToken)"/>
    /// bounded by this budget; if the signal recovers within the
    /// budget the caller proceeds into the semaphore as normal, if
    /// the budget expires with the tree still
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// the tracker throws
    /// <see cref="Orleans.Lattice.LatticeSaturatedException"/> with
    /// the originating tree id, so the caller can detect the
    /// saturation regime via a single <see langword="is"/> check
    /// instead of waiting out the full
    /// <see cref="WalAppendDispatchTimeout"/>.
    /// </para>
    /// <para>
    /// <b>Sizing rule.</b> The budget should be shorter than
    /// <see cref="WalAppendDispatchTimeout"/> (so the saturation
    /// refusal wins over the dispatch timeout) and longer than one
    /// <see cref="WalSaturationSampleInterval"/> (so a transient
    /// classifier flap does not surface as a refusal). The default
    /// (<see cref="DefaultWalAdmissionSaturationWaitBudget"/>,
    /// 5 seconds) leaves
    /// <see cref="WalAppendDispatchTimeout"/>'s 30-second default
    /// as a strict outer bound and gives the storage account a
    /// realistic recovery window for the canonical 409-Conflict
    /// burst (typical recovery 1-3 seconds once offered load drops).
    /// </para>
    /// <para>
    /// Set to <see cref="System.TimeSpan.Zero"/> to disable the
    /// admission-gate saturation check entirely (the historical
    /// pre-admission-gate behaviour; the gate falls through directly to
    /// <see cref="System.Threading.SemaphoreSlim.WaitAsync(System.Threading.CancellationToken)"/>
    /// regardless of the saturation signal). Set to
    /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to
    /// wait forever on
    /// <see cref="Orleans.Lattice.IWalSaturationSignal.WaitForHealthyAsync(string, System.Threading.CancellationToken)"/>;
    /// the registered options validator rejects any other negative
    /// value at first-resolve time.
    /// </para>
    /// </summary>
    public TimeSpan WalAdmissionSaturationWaitBudget { get; set; } = DefaultWalAdmissionSaturationWaitBudget;

    /// <summary>Default value for <see cref="WalAdmissionSaturationWaitBudget"/> (5 seconds).</summary>
    public static readonly TimeSpan DefaultWalAdmissionSaturationWaitBudget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Per-append pacing delay the WAL writer applies on the local admission
    /// path while the per-tree saturation signal reports
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>. This is what
    /// gives the drain-lag (and any other Throttled-mapped) back-pressure input
    /// teeth on the single-silo local-write path, where there is no remote
    /// replication sender to drip-feed and the
    /// <see cref="WalAdmissionSaturationWaitBudget"/> gate (Saturated-only)
    /// never engages. Before each dispatch admits into the per-partition
    /// admission semaphore the writer reads the signal once; on
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/> it awaits a
    /// single bounded <see cref="System.Threading.Tasks.Task.Delay(System.TimeSpan, System.Threading.CancellationToken)"/>
    /// of this duration, pacing the local producer so the materialiser drain can
    /// catch up. It is a pure back-off: it never throws, and it never escalates
    /// to <see cref="Orleans.Lattice.LatticeSaturatedException"/> - a Throttled
    /// tree slows callers, it does not fault them.
    /// <para>
    /// <b>Fast paths.</b> No-op when no saturation signal is registered
    /// (single-node / unit-test writers). No-op when the signal reports
    /// <see cref="Orleans.Lattice.WalSaturationState.Healthy"/> (a single
    /// concurrent-dictionary lookup, no await). No-op when set to
    /// <see cref="System.TimeSpan.Zero"/> (the operator opted out of local
    /// pacing). On <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>
    /// the separate admission gate already governs the dispatch, so the pace is
    /// skipped to avoid double-charging the caller.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultWalThrottledAdmissionPace"/> (25
    /// milliseconds), enabled out of the box so the local write path obeys the
    /// signal by default. The registered options validator rejects a negative
    /// value at first-resolve time.
    /// </para>
    /// </summary>
    public TimeSpan WalThrottledAdmissionPace { get; set; } = DefaultWalThrottledAdmissionPace;

    /// <summary>Default value for <see cref="WalThrottledAdmissionPace"/> (25 milliseconds).</summary>
    public static readonly TimeSpan DefaultWalThrottledAdmissionPace = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Optional caller-controlled retry policy applied at the boundary
    /// of every public <see cref="ILattice"/> mutating call. When
    /// <c>null</c> (the default), the library preserves today's
    /// throw-and-revert contract: a failed grain write surfaces
    /// verbatim to the caller and the grain's in-memory state is
    /// reverted to match disk. When set, the policy re-runs the
    /// caller's mutation under the same ambient
    /// <see cref="LatticeIdempotencyContext"/> scope - which the
    /// caller must have entered explicitly - for the policy's
    /// budget; on exhaustion the original failure is surfaced
    /// verbatim. Retry is therefore strictly opt-in at two layers
    /// (policy registration + idempotency-key scope) so the library's
    /// default behaviour is bit-identical to the pre-feature shape.
    /// </summary>
    public ILatticeRetryPolicy? RetryPolicy { get; set; }

    /// <summary>
    /// Maximum number of donor entries the online shard-consolidation
    /// coordinator accumulates in a single
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>
    /// call to the survivor shard during drain. Larger values reduce per-call
    /// overhead; smaller values bound peak memory on the coordinator silo and
    /// the size of the Orleans grain message. The drain is idempotent under
    /// any chunking - re-running converges via CRDT LWW - so this is purely a
    /// cost knob and never a correctness input.
    /// </summary>
    public int ConsolidationDrainBatchSize { get; set; } = DefaultConsolidationDrainBatchSize;

    /// <summary>Default value for <see cref="ConsolidationDrainBatchSize"/> (1024 entries).</summary>
    public const int DefaultConsolidationDrainBatchSize = 1024;

    /// <summary>
    /// Maximum number of donor leaves the online shard-consolidation
    /// coordinator visits in a single background pass before persisting its
    /// resume cursor and yielding.
    /// <para>
    /// This is what turns consolidating a thousand-leaf donor from one
    /// unbounded stall into steady background work: each pass does a bounded
    /// amount of drain, records where it got to, and lets the next timer tick
    /// continue. It also bounds the blast radius of an interruption, because a
    /// crash resumes from the persisted cursor instead of restarting the
    /// sweep. Set higher to consolidate faster at the cost of longer
    /// individual turns on the donor's leaves.
    /// </para>
    /// </summary>
    public int ConsolidationDrainLeavesPerPass { get; set; } = DefaultConsolidationDrainLeavesPerPass;

    /// <summary>Default value for <see cref="ConsolidationDrainLeavesPerPass"/> (16 leaves).</summary>
    public const int DefaultConsolidationDrainLeavesPerPass = 16;

    /// <summary>
    /// Maximum number of online shard consolidations a driver may run
    /// concurrently against a single tree.
    /// <para>
    /// Each consolidation drains a whole donor shard into its neighbour, so
    /// running many at once turns a background repair into a foreground load
    /// spike on exactly the busy deployment consolidation exists to heal. The
    /// conservative default of 1 makes healing a steady trickle; a driver that
    /// has measured headroom can raise it. Consolidations of overlapping
    /// donor/survivor pairs are refused regardless of this value.
    /// </para>
    /// <para>
    /// <b>Set to <c>0</c> to admit no consolidation at all.</b> That is the
    /// supported way to switch automated shard healing off without removing
    /// the driver, and it is a legal value rather than a rejected one. A
    /// negative value is rejected at startup by
    /// <c>LatticeOptionsValidator</c>. This cap is enforced by the healing
    /// driver that schedules folds, not by the consolidation coordinator
    /// itself, which owns only the correctness of a single fold.
    /// </para>
    /// </summary>
    public int MaxConcurrentShardConsolidations { get; set; } = DefaultMaxConcurrentShardConsolidations;

    /// <summary>Default value for <see cref="MaxConcurrentShardConsolidations"/> (1).</summary>
    public const int DefaultMaxConcurrentShardConsolidations = 1;

    /// <summary>
    /// Whether the per-tree automatic over-split healing orchestrator runs.
    /// <b>This is the kill switch for automatic shard healing</b>, on by
    /// default so an existing deployment whose trees were shattered by an
    /// over-eager splitter repairs itself with no operator action.
    /// <para>
    /// Set to <c>false</c> to stop healing specifically, without disabling
    /// adaptive splitting and without reverting the image. Turning it off
    /// leaves any in-flight fold to finish on its own coordinator - a fold is
    /// resumable and idempotent, so nothing is stranded - and simply stops new
    /// folds being admitted. Turning it back on resumes healing from whatever
    /// shape the tree is in.
    /// </para>
    /// <para>
    /// The related <see cref="MaxConcurrentShardConsolidations"/> set to
    /// <c>0</c> also admits nothing, but keeps the observer running so the
    /// tree's healing backlog is still published. Use this switch to stop the
    /// mechanism, and that one to pause admission while still watching.
    /// </para>
    /// </summary>
    public bool ShardHealingEnabled { get; set; } = DefaultShardHealingEnabled;

    /// <summary>Default value for <see cref="ShardHealingEnabled"/> (<c>true</c>).</summary>
    public const bool DefaultShardHealingEnabled = true;

    /// <summary>
    /// How often the healing orchestrator observes a tree's shape and decides
    /// whether to consolidate.
    /// <para>
    /// This is the cadence that spreads healing out: at most one fold is
    /// admitted per sweep, so the interval - not a burst - sets the pace at
    /// which an over-split tree comes back down. A tree damaged into a
    /// thousand physical shards heals as a steady background trickle rather
    /// than a stampede of concurrent drains. Shorten it to heal faster on a
    /// quiet box; raise <see cref="MaxConcurrentShardConsolidations"/> instead
    /// when the box has measured headroom for parallel folds.
    /// </para>
    /// <para>
    /// Must be strictly positive; use <see cref="ShardHealingEnabled"/> to
    /// switch healing off.
    /// </para>
    /// </summary>
    public TimeSpan ShardHealingInterval { get; set; } = DefaultShardHealingInterval;

    /// <summary>Default value for <see cref="ShardHealingInterval"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultShardHealingInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long the healing orchestrator waits after observing an adaptive
    /// split on a tree before it will consolidate that tree again.
    /// <para>
    /// This is the <em>time-domain</em> half of the hysteresis between the two
    /// control loops. Their skew triggers are already disjoint - splitting at
    /// or above <see cref="HotShardMinSkewRatio"/>, healing at or below
    /// <see cref="HotShardConsolidationSkewRatio"/> - so they cannot both fire
    /// on one sample. This window additionally stops a tree whose skew wanders
    /// across the dead band from alternating between the two over successive
    /// samples: after a split, healing stands off until the tree's shape has
    /// had time to settle at its new shard count.
    /// </para>
    /// <para>
    /// <c>TimeSpan.Zero</c> is legal and disables the window, leaving only the
    /// skew dead band. A negative value is rejected at startup.
    /// </para>
    /// </summary>
    public TimeSpan ShardHealingCooldown { get; set; } = DefaultShardHealingCooldown;

    /// <summary>Default value for <see cref="ShardHealingCooldown"/> (5 minutes).</summary>
    public static readonly TimeSpan DefaultShardHealingCooldown = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The tree's <em>median</em> shard rate, in operations per second, at or
    /// above which automatic healing yields to foreground traffic and admits
    /// no new fold. This is the backpressure signal that keeps healing a
    /// thousand-leaf tree invisible to a user issuing queries.
    /// <para>
    /// The median rather than the sum, deliberately: a summed tree rate scales
    /// with the shard count, so a tree shattered into a thousand near-idle
    /// shards would look like the busiest tree on the box and would never
    /// heal - precisely inverting the intent. The median measures whether the
    /// <em>typical</em> shard is busy, which is what "the tree is serving
    /// foreground traffic" actually means, and it is the same robust statistic
    /// the skew ratio is built from.
    /// </para>
    /// <para>
    /// The default matches <see cref="DefaultHotShardOpsPerSecondThreshold"/>,
    /// so healing yields at exactly the load at which a shard would be
    /// considered hot. <c>0</c> is legal and disables backpressure entirely,
    /// healing regardless of load; a negative or <c>NaN</c> value is rejected
    /// at startup.
    /// </para>
    /// </summary>
    public double ShardHealingBackpressureOpsPerSecond { get; set; } = DefaultShardHealingBackpressureOpsPerSecond;

    /// <summary>Default value for <see cref="ShardHealingBackpressureOpsPerSecond"/> (200 ops/s, matching <see cref="DefaultHotShardOpsPerSecondThreshold"/>).</summary>
    public const double DefaultShardHealingBackpressureOpsPerSecond = DefaultHotShardOpsPerSecondThreshold;

    /// <summary>
    /// The name of the Orleans grain storage provider used by Lattice grains.
    /// Used internally by <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// and exposed for advanced scenarios where callers register storage directly.
    /// </summary>
    public const string StorageProviderName = "lattice";
}
