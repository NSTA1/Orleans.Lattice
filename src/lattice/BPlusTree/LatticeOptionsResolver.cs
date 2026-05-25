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
    ILogger<LatticeOptionsResolver>? logger = null)
{
    private readonly ILogger _logger = (ILogger?)logger ?? NullLogger.Instance;

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

    /// <summary>Resolves the effective options for <paramref name="treeId"/>.</summary>
    public async Task<ResolvedLatticeOptions> ResolveAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var baseOptions = optionsMonitor.Get(treeId);

        int mlk, mic, sc;
        bool maintainDigest;
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            mlk = LatticeConstants.DefaultMaxLeafKeys;
            mic = LatticeConstants.DefaultMaxInternalChildren;
            sc = LatticeConstants.DefaultShardCount;
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
