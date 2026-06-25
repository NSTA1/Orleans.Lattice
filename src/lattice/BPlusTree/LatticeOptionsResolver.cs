using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Resolves the effective <see cref="LatticeOptions"/> for a given tree by
/// merging the non-structural configuration from
/// <see cref="IOptionsMonitor{LatticeOptions}"/> with the registry-pinned
/// structural fields (<c>MaxLeafKeys</c>, <c>MaxInternalChildren</c>,
/// <c>ShardCount</c>) stored on <see cref="State.TreeRegistryEntry"/>.
/// <para>
/// Every grain that needs structural sizing goes through this resolver; the
/// registry is the single source of truth. A user tree without a complete
/// pin is an invariant violation and causes <see cref="InvalidOperationException"/>.
/// System trees (IDs beginning with <see cref="LatticeConstants.SystemTreePrefix"/>)
/// resolve to the canonical defaults in <see cref="LatticeConstants"/>
/// without consulting the registry, to avoid circular bootstrap. System
/// trees additionally resolve
/// <see cref="LatticeOptions.MaintainProjectionDigest"/> to <c>false</c>
/// unconditionally because system trees are silo-internal metadata that
/// is never replicated across clusters and so the cross-silo drift
/// canary the digest exists to support is not applicable.
/// </para>
/// <para>
/// The per-tree
/// <see cref="State.TreeRegistryEntry.MaintainProjectionDigest"/> override,
/// when present, takes priority over the silo-wide configured value;
/// the per-tree
/// <see cref="State.TreeRegistryEntry.ProjectionDigestPermanentlyDisabled"/>
/// latch, when <c>true</c>, supersedes both and forces <c>false</c>
/// regardless of any other configured value. The latch reflects that
/// the tree has already accepted writes while maintenance was disabled,
/// so re-enabling would expose a stale aggregate through the public
/// digest API - the resolver enforces the one-way semantics rather than
/// pushing the check to every leaf grain.
/// </para>
/// </summary>
internal sealed class LatticeOptionsResolver(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeOptionsResolver>? logger = null,
    IWalStorageProviderCatalog? walProviderCatalog = null)
{
    private readonly ILogger _logger = (ILogger?)logger ?? NullLogger.Instance;

    /// <summary>
    /// Per-tree cache of the registry-pinned <see cref="LatticeOptions.WalPartitions"/>
    /// value. The pin is established at first <see cref="ILatticeRegistry.RegisterAsync"/>
    /// from the silo's then-current <see cref="LatticeOptions.WalPartitions"/> and is
    /// documented as tree-immutable thereafter (see <see cref="State.TreeRegistryEntry.WalPartitions"/>),
    /// so process-local memoisation is safe even though other resolved fields
    /// remain dynamic via <see cref="IOptionsMonitor{TOptions}"/>.
    /// <para>
    /// The cache exists to elide the per-call <see cref="ILatticeRegistry.GetEntryAsync"/>
    /// grain RPC on the foreground WAL commit path. Without it, every
    /// <see cref="Grains.WalCommitLogWriter.AppendAsync"/> serialises through
    /// one turn on the cluster-singleton registry activation before the writer
    /// can fan its append across <c>WalPartitions</c> WAL shards, collapsing
    /// the realised throughput by an order of magnitude on multi-partition
    /// configurations (the WAL-hot-path registry-RPC attribution).
    /// </para>
    /// <para>
    /// Cache is per-resolver-instance (not static) so each silo's activation
    /// owns its own memoisation; tests that instantiate the resolver directly
    /// get a clean cache per fixture without needing a reset hook.
    /// </para>
    /// </summary>
    private readonly ConcurrentDictionary<string, int> _walPartitionsCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Trees for which a "configured = true but latched-disabled" warning
    /// has already been logged. Re-resolving the same tree must not spam
    /// the log on every grain activation; the warning is informational
    /// and the latch semantics are unconditional.
    /// </summary>
    private static readonly ConcurrentDictionary<string, byte> WarnedLatchedTrees = new(StringComparer.Ordinal);

    /// <summary>
    /// Trees for which a "configured CompactionShardTickInterval below floor"
    /// warning has already been logged. Re-resolving the same tree must not
    /// spam the log on every grain activation; the warning is informational
    /// and the clamp is unconditional.
    /// </summary>
    private static readonly ConcurrentDictionary<string, byte> WarnedClampedTickIntervalTrees = new(StringComparer.Ordinal);

    /// <summary>
    /// Trees for which a "configured CompactionLeafBatchSize below floor"
    /// warning has already been logged. Re-resolving the same tree must not
    /// spam the log on every grain activation; the warning is informational
    /// and the clamp is unconditional.
    /// </summary>
    private static readonly ConcurrentDictionary<string, byte> WarnedClampedLeafBatchSizeTrees = new(StringComparer.Ordinal);

    /// <summary>
    /// Fast-path resolver for the WAL <see cref="LatticeOptions.WalPartitions"/>
    /// pin only. Returns the cached value when present; on a miss, performs the
    /// one-shot registry lookup, caches the resulting pin, and returns it.
    /// <para>
    /// Intended for hot-path producers (the foreground commit-log writer)
    /// that need the partition count to route an append and nothing else.
    /// Callers that need the full <see cref="ResolvedLatticeOptions"/> record
    /// (admin grains, activation-time materialiser) continue to use
    /// <see cref="ResolveAsync"/>; that method also populates this cache as a
    /// side effect so a tree that has been touched by any caller becomes
    /// fast-path-eligible for subsequent writer calls.
    /// </para>
    /// <para>
    /// System trees (IDs beginning with <see cref="LatticeConstants.SystemTreePrefix"/>)
    /// resolve synchronously to <see cref="LatticeConstants.DefaultSystemTreeWalPartitions"/>
    /// without touching the registry or the cache - matching the
    /// <see cref="ResolveAsync"/> branch and avoiding bootstrap cycles for the
    /// registry tree's own routing.
    /// </para>
    /// </summary>
    public ValueTask<int> GetWalPartitionsAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            return new ValueTask<int>(LatticeConstants.DefaultSystemTreeWalPartitions);
        }
        if (_walPartitionsCache.TryGetValue(treeId, out var cached))
        {
            return new ValueTask<int>(cached);
        }
        return new ValueTask<int>(LoadWalPartitionsSlowAsync(treeId));
    }

    private async Task<int> LoadWalPartitionsSlowAsync(string treeId)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = await registry.GetEntryAsync(treeId).ConfigureAwait(false);
        var baseOptions = optionsMonitor.Get(treeId);
        var partitions = entry?.WalPartitions ?? baseOptions.WalPartitions;
        // First writer wins the cache slot; if a racing ResolveAsync
        // populates it concurrently with a structurally identical value
        // (the pin is tree-immutable, so it MUST be identical) the
        // GetOrAdd here is a no-op and we return the racing winner's
        // value. Using TryAdd avoids overwriting a value installed by
        // ResolveAsync that may have run the lazy-register seam.
        _walPartitionsCache.TryAdd(treeId, partitions);
        return _walPartitionsCache.TryGetValue(treeId, out var afterRace) ? afterRace : partitions;
    }

    /// <summary>
    /// Test-only seam to drop a tree's cached
    /// <see cref="LatticeOptions.WalPartitions"/> pin so a subsequent
    /// <see cref="GetWalPartitionsAsync"/> or <see cref="ResolveAsync"/>
    /// call re-hits the registry. Production code does not need this -
    /// the pin is tree-immutable for the lifetime of the tree by design.
    /// </summary>
    internal void InvalidateWalPartitionsCacheForTests(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        _walPartitionsCache.TryRemove(treeId, out _);
    }

    /// <summary>
    /// Reads the durable WAL placement pin for <paramref name="treeId"/> fresh
    /// from the registry (no caching - the pin is read once per WAL shard
    /// activation and once per GC tick, never on the append hot path). System
    /// trees resolve synchronously to the default pin without touching the
    /// registry, avoiding the registry-tree bootstrap cycle.
    /// </summary>
    public ValueTask<State.WalPlacementPin> GetWalPlacementSnapshotAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            return new ValueTask<State.WalPlacementPin>(State.WalPlacementPin.Create());
        }
        return new ValueTask<State.WalPlacementPin>(LoadWalPlacementAsync(treeId));
    }

    private async Task<State.WalPlacementPin> LoadWalPlacementAsync(string treeId)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.GetWalPlacementAsync(treeId).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the <see cref="IWalStorageProvider"/> backing
    /// <paramref name="partition"/> of <paramref name="treeId"/> against the
    /// supplied placement <paramref name="pin"/>, <b>failing closed</b> if the
    /// pinned catalog key cannot be resolved on this silo.
    /// <para>
    /// For the default key (<see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>)
    /// the legacy per-tree <see cref="LatticeOptions.WalStorageProvider"/>
    /// resolver still wins when configured, exactly preserving pre-placement
    /// behaviour; otherwise the catalog's baseline provider is used. For any
    /// other key the catalog is the sole source and a missing key throws
    /// <see cref="LatticeWalProviderMissingException"/> rather than silently
    /// re-routing the partition's log to the baseline.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree whose WAL partition is being placed.</param>
    /// <param name="pin">The placement pin to resolve against.</param>
    /// <param name="partition">The WAL partition index.</param>
    /// <returns>The resolved provider and the catalog key it was resolved under.</returns>
    public (IWalStorageProvider Provider, string ProviderKey) ResolveWalProvider(
        string treeId, State.WalPlacementPin pin, int partition)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(pin);
        if (walProviderCatalog is null)
        {
            throw new InvalidOperationException(
                "WAL placement resolution requires an IWalStorageProviderCatalog; none was supplied to LatticeOptionsResolver.");
        }

        var key = pin.ResolveKey(partition);
        if (string.Equals(key, IWalStorageProviderCatalog.DefaultProviderKey, StringComparison.Ordinal))
        {
            // Backward-compatible default-key path: the per-tree legacy resolver
            // delegate wins when configured, otherwise the catalog baseline.
            var legacy = optionsMonitor.Get(treeId).WalStorageProvider?.Invoke(treeId);
            if (legacy is not null)
            {
                return (legacy, key);
            }
        }

        if (!walProviderCatalog.TryGet(key, out var provider))
        {
            throw new LatticeWalProviderMissingException(treeId, partition, key);
        }
        return (provider, key);
    }

    /// <summary>
    /// Reads the placement pin fresh and resolves the provider backing
    /// <paramref name="partition"/> of <paramref name="treeId"/>, returning the
    /// observed placement version alongside the provider so the caller can fence
    /// against a concurrent placement change. Fails closed per
    /// <see cref="ResolveWalProvider"/>.
    /// </summary>
    public async Task<(IWalStorageProvider Provider, long PlacementVersion, string ProviderKey)> GetWalProviderAsync(
        string treeId, int partition)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var pin = await GetWalPlacementSnapshotAsync(treeId).ConfigureAwait(false);
        var (provider, key) = ResolveWalProvider(treeId, pin, partition);
        return (provider, pin.Version, key);
    }

    /// <summary>Resolves the effective options for <paramref name="treeId"/>.</summary>
    public async Task<ResolvedLatticeOptions> ResolveAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var baseOptions = optionsMonitor.Get(treeId);

        int mlk, mic, sc;
        int walPartitions;
        bool maintainDigest;
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            mlk = LatticeConstants.DefaultMaxLeafKeys;
            mic = LatticeConstants.DefaultMaxInternalChildren;
            sc = LatticeConstants.DefaultShardCount;
            // System trees pin their WAL fan-out at the dedicated
            // constant (1) regardless of the silo-wide default. System
            // trees are silo-internal metadata with low key cardinality
            // and low write churn; fanning their WAL out across
            // multiple partition grains multiplies activation cost for
            // zero throughput win. The registry tree in particular
            // cannot consult the registry to resolve its own pin
            // without a bootstrap cycle - this branch therefore never
            // touches the registry.
            walPartitions = LatticeConstants.DefaultSystemTreeWalPartitions;
            // System trees are silo-internal metadata that is never
            // replicated across clusters. The digest is a cross-silo
            // drift canary; for system trees it has no consumer, so
            // we unconditionally take the trimmed mutation path to
            // avoid paying maintenance cost for a feature that does
            // not apply.
            maintainDigest = false;
        }
        else
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var entry = await registry.GetEntryAsync(treeId);
#if LATTICE_DIAG
            // DIAG-PATH1: record every resolve so we can see when entry transitions to defaults.
            try
            {
                Orleans.Lattice.BPlusTree.Grains.DiagSink.Write(
                    $"resolve-pre treeId={treeId} entry={(entry is null ? "null" : $"{{mlk={entry.MaxLeafKeys},mic={entry.MaxInternalChildren},sc={entry.ShardCount}}}")}");
            }
            catch { }
#endif
            if (entry is null ||
                entry.MaxLeafKeys is null ||
                entry.MaxInternalChildren is null ||
                entry.ShardCount is null)
            {
                // Lazy first-use seeding: every user tree must have a
                // structural pin, but callers should not have to register
                // explicitly for simple scenarios. RegisterAsync is
                // idempotent and fills nulls with LatticeConstants defaults.
                await registry.RegisterAsync(treeId, entry);
                entry = await registry.GetEntryAsync(treeId) ?? entry;
#if LATTICE_DIAG
                try
                {
                    Orleans.Lattice.BPlusTree.Grains.DiagSink.Write(
                        $"resolve-post-register treeId={treeId} entry={(entry is null ? "null" : $"{{mlk={entry.MaxLeafKeys},mic={entry.MaxInternalChildren},sc={entry.ShardCount}}}")}");
                }
                catch { }
#endif
            }
            mlk = entry?.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys;
            mic = entry?.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren;
            sc = entry?.ShardCount ?? LatticeConstants.DefaultShardCount;
            // WAL partition pin precedence:
            //   1. Registry pin (entry.WalPartitions): tree-immutable,
            //      stamped at first RegisterAsync from the silo's
            //      then-current LatticeOptions.WalPartitions.
            //   2. Live IOptionsMonitor<LatticeOptions> value: fallback
            //      for legacy registry rows persisted before the
            //      WalPartitions slot was added. These rows resolve to
            //      the live value once; the next RegisterAsync stamps
            //      the pin and subsequent resolves read from it.
            // The pin is required because the foreground commit-log
            // writer hashes each mutation key modulo this value to
            // route the write to a WAL partition grain - flipping the
            // value after the tree has accepted writes would silently
            // re-route new writes into grains that the activation-time
            // materialiser is not configured to read from.
            walPartitions = entry?.WalPartitions ?? baseOptions.WalPartitions;
            // Populate the hot-path WalPartitions cache as a side effect
            // of any full ResolveAsync. The pin is tree-immutable, so
            // repeating the assignment under racing resolvers is harmless
            // (TryAdd: first writer wins). This keeps the cache warm for
            // every tree that has been touched by any caller, not just by
            // the writer's GetWalPartitionsAsync fast path.
            _walPartitionsCache.TryAdd(treeId, walPartitions);

            // Effective MaintainProjectionDigest precedence:
            //   1. Latch (ProjectionDigestPermanentlyDisabled == true) forces false.
            //   2. Per-tree override (entry.MaintainProjectionDigest) wins over silo default.
            //   3. Silo-wide LatticeOptions.MaintainProjectionDigest is the fallback.
            var latched = entry?.ProjectionDigestPermanentlyDisabled == true;
            var configured = entry?.MaintainProjectionDigest ?? baseOptions.MaintainProjectionDigest;
            if (latched && configured)
            {
                // One-shot warning per tree per process: configuration
                // says re-enable, but the tree has accepted writes while
                // disabled so the persisted aggregate is stale. The
                // latch wins; the digest API stays unavailable until
                // the operator rebuilds the tree (rewrite every key or
                // take a snapshot-based reseed).
                if (WarnedLatchedTrees.TryAdd(treeId, 0))
                {
                    _logger.LogWarning(
                        "Tree {TreeId} has the projection-digest latch set (ProjectionDigestPermanentlyDisabled=true) " +
                        "but the configured MaintainProjectionDigest is true. The latch overrides the configuration " +
                        "because mutations landed while maintenance was disabled and the persisted aggregate is stale. " +
                        "Re-enable requires a snapshot-based rebuild that rewrites every entry.",
                        treeId);
                }
            }
            maintainDigest = !latched && configured;
        }

        // Compaction shard-tick interval: clamp configured values below
        // the documented floor up to the floor with a one-shot warning
        // per tree per process. The floor exists so a pathological
        // setting cannot starve the rest of the compactor grain's
        // scheduler quota by yielding too briefly between shard walks.
        var configuredTickInterval = baseOptions.CompactionShardTickInterval;
        var effectiveTickInterval = configuredTickInterval;
        if (configuredTickInterval < LatticeOptions.MinCompactionShardTickInterval)
        {
            effectiveTickInterval = LatticeOptions.MinCompactionShardTickInterval;
            if (WarnedClampedTickIntervalTrees.TryAdd(treeId, 0))
            {
                _logger.LogWarning(
                    "Tree {TreeId} has CompactionShardTickInterval={Configured} configured below the {Floor} floor; " +
                    "clamping to the floor. The floor protects scheduler fairness; lower it only if you have a measured reason.",
                    treeId, configuredTickInterval, LatticeOptions.MinCompactionShardTickInterval);
            }
        }

        // Compaction leaf-batch size: clamp configured values below the
        // documented floor (1) up to the floor with a one-shot warning per
        // tree per process. A batch size of zero would stall the leaf walk
        // indefinitely; a batch size of one is the legitimate "yield after
        // every leaf" extreme.
        var configuredLeafBatchSize = baseOptions.CompactionLeafBatchSize;
        var effectiveLeafBatchSize = configuredLeafBatchSize;
        if (configuredLeafBatchSize < LatticeOptions.MinCompactionLeafBatchSize)
        {
            effectiveLeafBatchSize = LatticeOptions.MinCompactionLeafBatchSize;
            if (WarnedClampedLeafBatchSizeTrees.TryAdd(treeId, 0))
            {
                _logger.LogWarning(
                    "Tree {TreeId} has CompactionLeafBatchSize={Configured} configured below the {Floor} floor; " +
                    "clamping to the floor. A batch size of zero would stall the leaf walk indefinitely.",
                    treeId, configuredLeafBatchSize, LatticeOptions.MinCompactionLeafBatchSize);
            }
        }

        return new ResolvedLatticeOptions
        {
            MaxLeafKeys = mlk,
            MaxInternalChildren = mic,
            ShardCount = sc,
            KeysPageSize = baseOptions.KeysPageSize,
            TombstoneGracePeriod = baseOptions.TombstoneGracePeriod,
            SoftDeleteDuration = baseOptions.SoftDeleteDuration,
            CacheTtl = baseOptions.CacheTtl,
            PrefetchKeysScan = baseOptions.PrefetchKeysScan,
            AutoSplitEnabled = baseOptions.AutoSplitEnabled,
            HotShardOpsPerSecondThreshold = baseOptions.HotShardOpsPerSecondThreshold,
            HotShardSampleInterval = baseOptions.HotShardSampleInterval,
            HotShardSplitCooldown = baseOptions.HotShardSplitCooldown,
            MaxConcurrentAutoSplits = baseOptions.MaxConcurrentAutoSplits,
            MaxConcurrentMigrations = baseOptions.MaxConcurrentMigrations,
            MaxConcurrentDrains = baseOptions.MaxConcurrentDrains,
            SplitDrainBatchSize = baseOptions.SplitDrainBatchSize,
            AutoSplitMinTreeAge = baseOptions.AutoSplitMinTreeAge,
            MaxScanRetries = baseOptions.MaxScanRetries,
            CursorIdleTtl = baseOptions.CursorIdleTtl,
            AtomicWriteRetention = baseOptions.AtomicWriteRetention,
            VersionVectorRetention = baseOptions.VersionVectorRetention,
            DiagnosticsCacheTtl = baseOptions.DiagnosticsCacheTtl,
            StorageUsageCacheTtl = baseOptions.StorageUsageCacheTtl,
            StorageUsagePollInterval = baseOptions.StorageUsagePollInterval,
            StorageUsageDeepPollInterval = baseOptions.StorageUsageDeepPollInterval,
            WalGcInterval = baseOptions.WalGcInterval,
            ShardForwardTimeout = baseOptions.ShardForwardTimeout,
            ActivationReadyTimeout = baseOptions.ActivationReadyTimeout,
            DigestPublishTimeout = baseOptions.DigestPublishTimeout,
            WalAppendDispatchTimeout = baseOptions.WalAppendDispatchTimeout,
            WalFlushPreflightTimeout = baseOptions.WalFlushPreflightTimeout,
            WalDrainBudget = baseOptions.WalDrainBudget,
            WalAdmissionSaturationWaitBudget = baseOptions.WalAdmissionSaturationWaitBudget,
            WalMaxRetainedBytes = baseOptions.WalMaxRetainedBytes,
            WalBytePressureReclaimTarget = baseOptions.WalBytePressureReclaimTarget,
            MaterialiserCheckpointInterval = baseOptions.MaterialiserCheckpointInterval,
            MaterialiserCheckpointEntries = baseOptions.MaterialiserCheckpointEntries,
            LeafProjectionRetention = baseOptions.LeafProjectionRetention,
            ProjectionRebuildPolicy = baseOptions.ProjectionRebuildPolicy,
            MaxLeafReplayEntries = baseOptions.MaxLeafReplayEntries,
            LeafSnapshotMargin = baseOptions.LeafSnapshotMargin,
            LeafSnapshotReClassifyEveryNCheckpoints = baseOptions.LeafSnapshotReClassifyEveryNCheckpoints,
            MinTombstoneRatioForCompaction = baseOptions.MinTombstoneRatioForCompaction,
            MaxLeafEntriesBeforeForcedCompaction = baseOptions.MaxLeafEntriesBeforeForcedCompaction,
            CompactionTriggerCooldown = baseOptions.CompactionTriggerCooldown,
            CompactionShardTickInterval = effectiveTickInterval,
            CompactionLeafBatchSize = effectiveLeafBatchSize,
            DirtyLeafFlushIntervalMs = baseOptions.DirtyLeafFlushIntervalMs,
            MaintainProjectionDigest = maintainDigest,
            // c2-xxix bugfix: the resolver previously dropped this
            // field, so every leaf grain observed the
            // ResolvedLatticeOptions default (0 = synchronous publish)
            // regardless of what the operator or bench configured. The
            // c2-xxviii coalescing path therefore never fired on Azure
            // - the apparent win in the c2-xxviii memo (digest p50
            // 13ms -> 0.00ms) was misattribution. The leaf clamps
            // this to 0 anyway when MaintainProjectionDigest resolves
            // to false. See LatticeOptionsResolverPropagationGuardTests
            // for the regression gate.
            DigestCoalescingWindowMs = baseOptions.DigestCoalescingWindowMs,
            // WalPartitions sourced from the per-tree pin (registry
            // entry) for user trees and from LatticeConstants for
            // system trees. The pin is established at first
            // RegisterAsync from the silo's then-current
            // LatticeOptions.WalPartitions; once stamped it is
            // tree-immutable so the foreground commit-log writer and
            // the activation-time materialiser always agree on the
            // partition fan-out shape regardless of what the silo's
            // live IOptionsMonitor<LatticeOptions> value is.
            WalPartitions = walPartitions,
        };
    }

    /// <summary>
    /// Test-only seam to reset the per-process "configured = true but
    /// latched-disabled" warning memo so multiple test cases can each
    /// observe the warning behaviour independently.
    /// </summary>
    internal static void ResetWarnedLatchedTreesForTests() => WarnedLatchedTrees.Clear();

    /// <summary>
    /// Test-only seam to reset the per-process "configured tick interval
    /// below floor" warning memo so multiple test cases can each observe
    /// the warning behaviour independently.
    /// </summary>
    internal static void ResetWarnedClampedTickIntervalTreesForTests() =>
        WarnedClampedTickIntervalTrees.Clear();

    /// <summary>
    /// Test-only seam to reset the per-process "configured leaf batch size
    /// below floor" warning memo so multiple test cases can each observe
    /// the warning behaviour independently.
    /// </summary>
    internal static void ResetWarnedClampedLeafBatchSizeTreesForTests() =>
        WarnedClampedLeafBatchSizeTrees.Clear();
}
