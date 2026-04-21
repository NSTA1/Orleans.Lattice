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
/// without consulting the registry, to avoid circular bootstrap.
/// </para>
/// </summary>
internal sealed class LatticeOptionsResolver(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor)
{
    /// <summary>Resolves the effective options for <paramref name="treeId"/>.</summary>
    public async Task<ResolvedLatticeOptions> ResolveAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var baseOptions = optionsMonitor.Get(treeId);

        int mlk, mic, sc;
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            mlk = LatticeConstants.DefaultMaxLeafKeys;
            mic = LatticeConstants.DefaultMaxInternalChildren;
            sc = LatticeConstants.DefaultShardCount;
        }
        else
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var entry = await registry.GetEntryAsync(treeId);
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
            }
            mlk = entry?.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys;
            mic = entry?.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren;
            sc = entry?.ShardCount ?? LatticeConstants.DefaultShardCount;
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
        };
    }
}
