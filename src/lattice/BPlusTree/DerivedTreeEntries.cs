using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Builds the registry entry a physical copy derived from a source tree is
/// registered with, so the copy lays keys out exactly as the source does and
/// keeps the source's structural pins.
/// <para>
/// A copy registered with library defaults routes by the default map for the
/// default shard count, and an alias cutover then carries that map onto the
/// logical tree (#4250), silently resetting a resharded tree's topology and
/// discarding its sizing pins. A snapshot's (and so a resize's) destination has
/// inherited the source's map and split allocation mark since #3880; a schema
/// remediation's destination does too (#4379), and additionally keeps the
/// source's leaf sizing, WAL partition count and the runtime configuration
/// overrides a resize keeps on the logical row at its swap.
/// </para>
/// </summary>
internal static class DerivedTreeEntries
{
    /// <summary>
    /// The entry for a copy of a tree routed by <paramref name="sourceMap"/>: the
    /// map (copied, at its own version) and split allocation mark, so every slot
    /// routes to the same physical index on both trees and a later split of the
    /// copy allocates above every index the copy populated, plus the given
    /// structural pins. A <see langword="null"/> map leaves the copy on the
    /// default map for <paramref name="shardCount"/>; a <see langword="null"/>
    /// pin is seeded with its default at registration.
    /// </summary>
    public static TreeRegistryEntry ForCopy(
        ShardMap? sourceMap,
        int? sourceNextShardIndex,
        int? shardCount,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        string? derivedFrom = null) => new()
        {
            MaxLeafKeys = maxLeafKeys,
            MaxInternalChildren = maxInternalChildren,
            ShardCount = shardCount,
            DerivedFrom = derivedFrom,
            ShardMap = sourceMap is null
                ? null
                : new ShardMap { Slots = (int[])sourceMap.Slots.Clone(), Version = sourceMap.Version },
            NextShardIndex = sourceNextShardIndex,
        };

    /// <summary>
    /// Copies the per-tree runtime configuration overrides of
    /// <paramref name="source"/> onto <paramref name="entry"/>: event publishing,
    /// projection digest maintenance and its permanent-disable latch, history
    /// retention, and the cache-value and WAL retention ceilings. These are the
    /// registry-persisted overrides a resize keeps on the logical row at its swap
    /// (<c>TreeResizeGrain.SwapAliasAsync</c>); the structural pins, the routing
    /// map, the alias and the WAL placement are not overrides and are left as
    /// they are.
    /// </summary>
    public static TreeRegistryEntry WithConfigurationOverridesOf(this TreeRegistryEntry entry, TreeRegistryEntry? source)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return source is null
            ? entry
            : entry with
            {
                PublishEvents = source.PublishEvents,
                MaintainProjectionDigest = source.MaintainProjectionDigest,
                ProjectionDigestPermanentlyDisabled = source.ProjectionDigestPermanentlyDisabled,
                HistoryRetentionMode = source.HistoryRetentionMode,
                HistoryRetentionWindowTicks = source.HistoryRetentionWindowTicks,
                MaxCacheValueBytes = source.MaxCacheValueBytes,
                WalMaxRetainedBytes = source.WalMaxRetainedBytes,
            };
    }

    /// <summary>
    /// The entry for a copy of <paramref name="logicalTreeId"/> that is to replace
    /// it behind an alias and inherit everything but its values. Routing - the map
    /// and split allocation mark - is read from the logical tree, under which
    /// splits, folds and reshards write it: the map by a forced routing read (the
    /// stateless routing workers may cache one from before a reshard, #4206), so it
    /// is the effective map even when none is persisted, and with it the tree's
    /// virtual slot count. The shard-count pin comes from the logical row too; the
    /// leaf sizing and WAL partition count from the physical tree that routing
    /// resolves to, whose leaves and WAL run with them, falling back to the
    /// logical row. The runtime overrides come from the logical row (see
    /// <see cref="WithConfigurationOverridesOf"/>).
    /// </summary>
    public static async Task<TreeRegistryEntry> InheritingAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        string? derivedFrom,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();

        // Routing first: the entry read after it carries a split mark at least as
        // new as the map, so the copy never allocates an index the map uses.
        var routing = await grainFactory.GetGrain<ILattice>(logicalTreeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken);
        var registry = grainFactory.GetLatticeRegistry();
        var logical = await registry.GetEntryAsync(logicalTreeId);
        var physical = string.IsNullOrEmpty(routing.PhysicalTreeId)
            || string.Equals(routing.PhysicalTreeId, logicalTreeId, StringComparison.Ordinal)
                ? logical
                : await registry.GetEntryAsync(routing.PhysicalTreeId) ?? logical;

        var copy = ForCopy(
            routing.Map,
            logical?.NextShardIndex,
            logical?.ShardCount ?? physical?.ShardCount,
            physical?.MaxLeafKeys ?? logical?.MaxLeafKeys,
            physical?.MaxInternalChildren ?? logical?.MaxInternalChildren,
            derivedFrom);
        return (copy with { WalPartitions = physical?.WalPartitions ?? logical?.WalPartitions })
            .WithConfigurationOverridesOf(logical);
    }
}
