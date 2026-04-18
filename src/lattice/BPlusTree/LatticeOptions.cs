namespace Orleans.Lattice;

/// <summary>
/// Configuration options for a Lattice tree. Register a named instance to
/// override settings for a specific tree:
/// <code>
/// siloBuilder.Services.Configure&lt;LatticeOptions&gt;("my-tree", o => o.MaxLeafKeys = 256);
/// </code>
/// The unnamed (default) instance applies to all trees that do not have a
/// named override.
/// </summary>
public sealed class LatticeOptions
{
    /// <summary>Maximum number of keys per leaf node before a split is triggered.</summary>
    public int MaxLeafKeys { get; set; } = DefaultMaxLeafKeys;

    /// <summary>Maximum number of children per internal node before a split is triggered.</summary>
    public int MaxInternalChildren { get; set; } = DefaultMaxInternalChildren;

    /// <summary>Number of independent shards the key space is divided into.</summary>
    public int ShardCount { get; set; } = DefaultShardCount;

    /// <summary>
    /// Size of the virtual shard space used for key routing. Keys are hashed
    /// into one of <see cref="VirtualShardCount"/> virtual slots, and a per-tree
    /// <c>ShardMap</c> collapses those virtual slots onto the
    /// <see cref="ShardCount"/> physical shards. This indirection enables future
    /// adaptive shard splitting (F-011) without rehashing existing keys.
    /// <para>
    /// Must be greater than or equal to <see cref="ShardCount"/>, and must be an
    /// integer multiple of <see cref="ShardCount"/> so that the default identity
    /// map preserves the legacy <c>hash % shardCount</c> routing.
    /// </para>
    /// </summary>
    public int VirtualShardCount { get; set; } = DefaultVirtualShardCount;

    /// <summary>Number of keys per page returned by <see cref="IShardRootGrain.GetSortedKeysBatchAsync"/>.</summary>
    public int KeysPageSize { get; set; } = DefaultKeysPageSize;

    /// <summary>
    /// How long a tombstone must exist before it is eligible for compaction.
    /// A grain reminder fires at this interval; tombstones older than this
    /// grace period are permanently removed. Set to <see cref="Timeout.InfiniteTimeSpan"/>
    /// to disable compaction entirely. Per-tree overrides follow the same
    /// named-options pattern as other properties.
    /// </summary>
    public TimeSpan TombstoneGracePeriod { get; set; } = DefaultTombstoneGracePeriod;

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
    /// (the default), every read triggers a delta refresh — the version-vector
    /// comparison on the primary is cheap but the RPC overhead remains. Setting
    /// a non-zero value (e.g. 100 ms) allows the cache to serve reads from its
    /// local dictionary without contacting the primary, trading freshness for
    /// lower read latency. This option can be changed freely at any time.
    /// </summary>
    public TimeSpan CacheTtl { get; set; } = DefaultCacheTtl;

    /// <summary>Default value for <see cref="SoftDeleteDuration"/> (72 hours).</summary>
    public static readonly TimeSpan DefaultSoftDeleteDuration = TimeSpan.FromHours(72);

    /// <summary>Default value for <see cref="TombstoneGracePeriod"/> (24 hours).</summary>
    public static readonly TimeSpan DefaultTombstoneGracePeriod = TimeSpan.FromHours(24);

    /// <summary>Default value for <see cref="MaxLeafKeys"/>.</summary>
    public const int DefaultMaxLeafKeys = 128;

    /// <summary>Default value for <see cref="MaxInternalChildren"/>.</summary>
    public const int DefaultMaxInternalChildren = 128;

    /// <summary>Default value for <see cref="ShardCount"/>.</summary>
    public const int DefaultShardCount = 64;

    /// <summary>Default value for <see cref="VirtualShardCount"/> (4096).</summary>
    public const int DefaultVirtualShardCount = 4096;

    /// <summary>Default value for <see cref="KeysPageSize"/>.</summary>
    public const int DefaultKeysPageSize = 512;

    /// <summary>Default value for <see cref="CacheTtl"/> (zero — refresh on every read).</summary>
    public static readonly TimeSpan DefaultCacheTtl = TimeSpan.Zero;

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
    /// When <c>true</c>, the autonomic <c>HotShardMonitorGrain</c> periodically
    /// polls each physical shard's hotness counters (<see cref="IShardRootGrain.GetHotnessAsync"/>)
    /// and triggers an online adaptive split (F-011) when the observed
    /// operations-per-second exceeds <see cref="HotShardOpsPerSecondThreshold"/>.
    /// Splits happen fully online via shadow-writing — no shard is ever taken offline.
    /// Set to <c>false</c> to disable autonomic splitting entirely; manual
    /// splits via <c>ITreeShardSplitGrain</c> remain available for tooling.
    /// </summary>
    public bool AutoSplitEnabled { get; set; } = DefaultAutoSplitEnabled;

    /// <summary>Default value for <see cref="AutoSplitEnabled"/> (<c>true</c>).</summary>
    public const bool DefaultAutoSplitEnabled = true;

    /// <summary>
    /// Operations-per-second threshold above which a shard is considered hot
    /// and eligible for an autonomic split. Computed as
    /// <c>(reads + writes) / window.TotalSeconds</c> over the period reported
    /// by <see cref="ShardHotness.Window"/>. Lower values trigger splits more
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
    /// Maximum number of autonomic splits that can be in flight concurrently
    /// for a single tree. The monitor refuses to start a new split while this
    /// many are already active. Defaults to 1 to keep storage I/O bounded.
    /// </summary>
    public int MaxConcurrentAutoSplits { get; set; } = DefaultMaxConcurrentAutoSplits;

    /// <summary>Default value for <see cref="MaxConcurrentAutoSplits"/> (1).</summary>
    public const int DefaultMaxConcurrentAutoSplits = 1;

    /// <summary>
    /// Minimum age of a tree (since the monitor first activated) before
    /// autonomic splits are allowed. Prevents premature splits during
    /// startup bursts before the workload has stabilised.
    /// </summary>
    public TimeSpan AutoSplitMinTreeAge { get; set; } = DefaultAutoSplitMinTreeAge;

    /// <summary>Default value for <see cref="AutoSplitMinTreeAge"/> (60 seconds).</summary>
    public static readonly TimeSpan DefaultAutoSplitMinTreeAge = TimeSpan.FromSeconds(60);

    /// <summary>
    /// The name of the Orleans grain storage provider used by Lattice grains.
    /// Used internally by <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// and exposed for advanced scenarios where callers register storage directly.
    /// </summary>
    public const string StorageProviderName = "lattice";
}
