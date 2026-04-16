namespace Orleans.Lattice.BPlusTree;

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

    /// <summary>Default value for <see cref="KeysPageSize"/>.</summary>
    public const int DefaultKeysPageSize = 512;

    /// <summary>Default value for <see cref="CacheTtl"/> (zero — refresh on every read).</summary>
    public static readonly TimeSpan DefaultCacheTtl = TimeSpan.Zero;

    /// <summary>
    /// The name of the Orleans grain storage provider used by Lattice grains.
    /// Used internally by <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// and exposed for advanced scenarios where callers register storage directly.
    /// </summary>
    public const string StorageProviderName = "lattice";
}
