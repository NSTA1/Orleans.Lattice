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
    /// adaptive shard splitting without rehashing existing keys.
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
    /// and triggers an online adaptive split when the observed
    /// operations-per-second exceeds <see cref="HotShardOpsPerSecondThreshold"/>.
    /// Splits happen fully online via shadow-writing — no shard is ever taken
    /// offline. Set to <c>false</c> to disable autonomic splitting entirely.
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
    /// many are already active. Defaults to 2 — splits are I/O-bounded by the
    /// drain phase but small enough that two parallel splits typically saturate
    /// neither storage nor the coordinator silo. Set to <c>1</c> for the most
    /// conservative behavior, or higher when many shards are simultaneously
    /// hot and target storage can absorb the extra drain traffic.
    /// </summary>
    public int MaxConcurrentAutoSplits { get; set; } = DefaultMaxConcurrentAutoSplits;

    /// <summary>Default value for <see cref="MaxConcurrentAutoSplits"/> (2).</summary>
    public const int DefaultMaxConcurrentAutoSplits = 2;

    /// <summary>
    /// Maximum number of moved-slot entries the split coordinator accumulates
    /// in a single <see cref="IShardRootGrain.MergeManyAsync"/> call to the
    /// target shard during drain. Larger values reduce per-call overhead;
    /// smaller values bound peak memory on the coordinator silo and the size
    /// of the Orleans grain message. The drain phase remains idempotent under
    /// any chunking — re-running converges via CRDT LWW.
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
    /// Optional retention window for <see cref="Primitives.VersionVector"/>
    /// entries (FX-003). When a merge pipeline calls
    /// <see cref="Primitives.VersionVector.PruneOlderThan(long)"/> with
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
    /// enforced — provided as a reference constant.
    /// </summary>
    public static readonly TimeSpan DefaultMinVersionVectorRetention = TimeSpan.FromHours(1);

    /// <summary>
    /// The name of the Orleans grain storage provider used by Lattice grains.
    /// Used internally by <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// and exposed for advanced scenarios where callers register storage directly.
    /// </summary>
    public const string StorageProviderName = "lattice";
}
