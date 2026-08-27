using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The two catalog-page projections issue #1686 compares, expressed over the
/// same already-filtered page of tree ids and the same counting grain surface so
/// the only difference between them is the call shape.
/// </summary>
/// <remarks>
/// Both take a page that has <em>already</em> passed every filter (tenancy,
/// system-tree, page token, and the per-entry visibility probe). That ordering
/// is the correctness constraint the change is built around: the visibility
/// filter thins the candidate set, so batching ahead of it would both over-fetch
/// and read entries the per-entry path would have dropped.
/// </remarks>
internal static class CatalogPageProjections
{
    // Hoisted so the shared mapping allocates nothing per entry beyond the
    // records themselves; the shipped query reads these off IOptionsMonitor.
    private static readonly int DefaultWalPartitions = new LatticeOptions().WalPartitions;
    private static readonly TimeSpan DefaultSoftDeleteDuration = new LatticeOptions().SoftDeleteDuration;

    /// <summary>
    /// Reproduces the pre-#1686 projection verbatim: for each surviving id, one
    /// registry read and one deletion probe, each awaited before the next id is
    /// touched. A default 100-entry page costs 200 sequential grain round-trips.
    /// </summary>
    public static async Task<List<TreeCatalogEntry>> PerEntryAsync(
        CatalogGrainSurface surface,
        List<string> pageIds)
    {
        var entries = new List<TreeCatalogEntry>(pageIds.Count);
        foreach (var id in pageIds)
        {
            var entry = await surface.ReadEntryAsync(id).ConfigureAwait(false);
            var deleted = await surface.Deletion(id).IsDeletedAsync().ConfigureAwait(false);
            entries.Add(Map(id, entry, deleted));
        }

        return entries;
    }

    /// <summary>
    /// Reproduces the shipped projection: one batched registry multi-get for the
    /// whole page plus one bounded concurrent fan-out of the deletion probes, so
    /// a default 100-entry page costs 1 + 100 round-trips in two waves.
    /// </summary>
    public static async Task<List<TreeCatalogEntry>> BatchedAsync(
        CatalogGrainSurface surface,
        List<string> pageIds)
    {
        var entries = new List<TreeCatalogEntry>(pageIds.Count);
        if (pageIds.Count == 0)
        {
            return entries;
        }

        var registryWave = surface.ReadEntriesAsync(pageIds);

        var deletionProbes = new Task<bool>[pageIds.Count];
        for (var i = 0; i < pageIds.Count; i++)
        {
            deletionProbes[i] = surface.Deletion(pageIds[i]).IsDeletedAsync();
        }

        var deletionWave = Task.WhenAll(deletionProbes);

        await Task.WhenAll(registryWave, deletionWave).ConfigureAwait(false);
        var byTreeId = await registryWave.ConfigureAwait(false);
        var deleted = await deletionWave.ConfigureAwait(false);

        for (var i = 0; i < pageIds.Count; i++)
        {
            var id = pageIds[i];
            entries.Add(Map(id, byTreeId.TryGetValue(id, out var found) ? found : null, deleted[i]));
        }

        return entries;
    }

    /// <summary>
    /// The projection both arms share. Mirrors the shipped
    /// <c>LatticeStateQuery.MapCatalogEntry</c> so the per-entry mapping cost is
    /// identical on both sides and the measured delta is the call shape alone.
    /// </summary>
    private static TreeCatalogEntry Map(string treeId, TreeRegistryEntry? entry, bool deleted)
    {
        var physicalTreeId = entry?.PhysicalTreeId;
        var shardCount = entry?.ShardCount ?? LatticeConstants.DefaultShardCount;

        return new TreeCatalogEntry
        {
            TreeId = treeId,
            IsAlias = physicalTreeId is not null,
            PhysicalTreeId = physicalTreeId,
            Lifecycle = deleted ? TreeLifecycleState.SoftDeleted : TreeLifecycleState.Active,
            ShardCount = shardCount,
            RestoreShadowOfTreeId = entry?.RestoreShadowOfTreeId,
            Config = new TreeConfigSummary
            {
                ShardCount = shardCount,
                VirtualShardCount = LatticeConstants.DefaultVirtualShardCount,
                MaxLeafKeys = entry?.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = entry?.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren,
                WalPartitions = entry?.WalPartitions ?? DefaultWalPartitions,
                SoftDeleteDuration = DefaultSoftDeleteDuration,
            },
        };
    }
}
