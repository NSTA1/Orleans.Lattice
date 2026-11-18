using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Resolves the trees an all-trees backup-set schedule captures: every live user
/// tree in one tenant's namespace. Reads the tree registry by prefix (a bounded
/// range scan) and checks restore shadows and soft deletes in bounded batches,
/// so a large estate never has every registry entry in memory at once.
/// </summary>
internal static class BackupSetMembership
{
    /// <summary>How many tree ids one registry-entry or deletion probe batch covers.</summary>
    internal const int ProbeBatchSize = 64;

    /// <summary>
    /// Returns a whole-tree scope for every tree an all-trees schedule captures,
    /// in ordinal tree-id order: every registered tree under
    /// <paramref name="tenantTreePrefix"/> (or, for the default tenant, every tree
    /// that is not tenant-scoped) that is not a system, materialised-view,
    /// tag-index or system-data tree, not a restore shadow, not soft-deleted, and
    /// not under one of <paramref name="excludedTreePrefixes"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="tenantTreePrefix">The tenant's tree-id prefix, or <c>null</c> for the default tenant.</param>
    /// <param name="excludedTreePrefixes">Effective tree-id prefixes to leave out.</param>
    /// <param name="cancellationToken">Cancels between batches.</param>
    /// <returns>The member scopes.</returns>
    public static async Task<IReadOnlyList<BackupScopeSelector>> ResolveAllTreesAsync(
        IGrainFactory grainFactory,
        string? tenantTreePrefix,
        IReadOnlyList<string> excludedTreePrefixes,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(excludedTreePrefixes);

        var registry = grainFactory.GetLatticeRegistry();
        IReadOnlyList<string> ids;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            ids = await registry.GetAllTreeIdsAsync(tenantTreePrefix).ConfigureAwait(false);
        }

        var candidates = new List<string>();
        foreach (var id in ids)
        {
            if (IsCandidate(id, tenantTreePrefix, excludedTreePrefixes))
            {
                candidates.Add(id);
            }
        }

        var scopes = new List<BackupScopeSelector>(candidates.Count);
        for (var offset = 0; offset < candidates.Count; offset += ProbeBatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var batch = candidates.GetRange(offset, Math.Min(ProbeBatchSize, candidates.Count - offset));
            Dictionary<string, BPlusTree.State.TreeRegistryEntry> entries;
            bool[] deleted;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                entries = await registry.GetEntriesAsync(batch).ConfigureAwait(false);
                var probes = new Task<bool>[batch.Count];
                for (var i = 0; i < batch.Count; i++)
                {
                    probes[i] = grainFactory.GetGrain<ITreeDeletionGrain>(batch[i]).IsDeletedAsync();
                }

                deleted = await Task.WhenAll(probes).ConfigureAwait(false);
            }

            for (var i = 0; i < batch.Count; i++)
            {
                if (deleted[i])
                {
                    continue;
                }

                if (entries.TryGetValue(batch[i], out var entry) && entry.RestoreShadowOfTreeId is not null)
                {
                    continue;
                }

                scopes.Add(BackupScopeSelector.WholeTree(batch[i]));
            }
        }

        scopes.Sort(static (a, b) => string.CompareOrdinal(a.TreeId, b.TreeId));
        return scopes;
    }

    /// <summary>
    /// Whether <paramref name="treeId"/> is an all-trees candidate by name alone:
    /// inside the tenant's namespace, not a reserved tree of any kind, and not
    /// excluded.
    /// </summary>
    /// <param name="treeId">The tree id.</param>
    /// <param name="tenantTreePrefix">The tenant's tree-id prefix, or <c>null</c> for the default tenant.</param>
    /// <param name="excludedTreePrefixes">Effective tree-id prefixes to leave out.</param>
    /// <returns><see langword="true"/> when the tree is a candidate.</returns>
    internal static bool IsCandidate(string treeId, string? tenantTreePrefix, IReadOnlyList<string> excludedTreePrefixes)
    {
        if (tenantTreePrefix is null)
        {
            if (LatticeTenantTrees.IsTenantScoped(treeId))
            {
                return false;
            }
        }
        else if (!treeId.StartsWith(tenantTreePrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var local = LatticeTenantTrees.LocalName(treeId.AsSpan());
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            || local.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            || treeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
            || local.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
            || local.StartsWith(LatticeConstants.TagIndexTreePrefix, StringComparison.Ordinal)
            || LatticeViewTrees.IsViewTree(treeId))
        {
            return false;
        }

        foreach (var prefix in excludedTreePrefixes)
        {
            if (treeId.StartsWith(prefix, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
}
