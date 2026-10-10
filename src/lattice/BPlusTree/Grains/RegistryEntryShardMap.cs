using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Derives a tree's effective routing <see cref="ShardMap"/> from a registry
/// entry the caller has already read, so a coordinator that needs the physical
/// tree id and the shard map pays one <c>GetEntryAsync</c> instead of
/// <c>ResolveAsync</c> + an options resolve + <c>GetShardMapAsync</c>.
/// <para>
/// <c>ILatticeRegistry.ResolveAsync(id)</c> is <c>GetEntryAsync(id)?.PhysicalTreeId ?? id</c>
/// and <c>GetShardMapAsync(id)</c> is <c>GetEntryAsync(id)?.ShardMap</c>, so the
/// entry already carries both. The default map needs the tree's shard count; an
/// entry with a pinned <see cref="TreeRegistryEntry.ShardCount"/> carries the
/// exact value <see cref="LatticeOptionsResolver.ResolveAsync"/> would return for
/// it, so only an absent or unpinned entry falls back to the options resolve,
/// which keeps its lazy-seeding behaviour. An absent entry is re-read after that
/// seed, exactly as the separate-call shape did.
/// </para>
/// </summary>
internal static class RegistryEntryShardMap
{
    /// <summary>
    /// The routing map for <paramref name="treeId"/> given its already-read
    /// <paramref name="entry"/>: the persisted map when present, otherwise the
    /// shared default identity map at the tree's resolved shard count.
    /// </summary>
    public static async Task<ShardMap> ResolveAsync(
        ILatticeRegistry registry,
        LatticeOptionsResolver optionsResolver,
        string treeId,
        TreeRegistryEntry? entry)
    {
        if (entry?.ShardMap is { } persisted)
        {
            return persisted;
        }

        if (entry?.ShardCount is int pinned
            && !treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            return ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, pinned);
        }

        var resolved = await optionsResolver.ResolveAsync(treeId);
        var map = entry is null ? await registry.GetShardMapAsync(treeId) : null;
        return map ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
    }
}
