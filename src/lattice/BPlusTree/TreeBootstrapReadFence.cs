using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The tree-level side of the receiver bootstrap read fence (issue #4526): the
/// operations a snapshot bootstrap coordinator uses to arm and lift the fence on
/// every shard of the copy a tree routes to, and to learn whether a migration or
/// resize would move the tree off the shards it fenced.
/// <para>
/// <b>Interlock.</b> The fence is a per-shard flag (see
/// <c>ShardRootGrain.BootstrapReadFence.cs</c>). The bootstrap arms it on every
/// routed shard, and only then reads each shard's migration record and the
/// tree's resize coordinator (<see cref="FindBlockerAsync"/>). The other side
/// publishes first and reads second: a shard refuses to open a split or
/// consolidation record while armed, on the same activation the fence is written
/// on; a resize persists its intent before reading the shards' fences
/// (<see cref="FindFencedShardAsync"/>); an undo publishes a running marker
/// before reading them. Whichever of two racing starts reads second sees the
/// first, so no interleaving lets a migration, resize or undo run under a drain.
/// </para>
/// </summary>
internal static class TreeBootstrapReadFence
{
    /// <summary>
    /// The copy a tree routes to and its routed shard indices: the set a
    /// bootstrap arms and lifts.
    /// </summary>
    /// <param name="PhysicalTreeId">The physical tree the logical tree resolves to.</param>
    /// <param name="ShardIndices">The sorted, distinct routed shard indices of that copy.</param>
    internal readonly record struct Shards(string PhysicalTreeId, int[] ShardIndices);

    /// <summary>
    /// Resolves the copy <paramref name="logicalTreeId"/> routes to and its routed
    /// shard indices, computed as a resize computes the shards it copies.
    /// </summary>
    internal static async Task<Shards> ResolveAsync(
        IGrainFactory grainFactory, LatticeOptionsResolver optionsResolver, string logicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(optionsResolver);
        ArgumentNullException.ThrowIfNull(logicalTreeId);

        var registry = grainFactory.GetLatticeRegistry();
        // One registry read yields both the routed copy and its shard map
        // (ResolveAsync is GetEntryAsync(id)?.PhysicalTreeId ?? id), so the
        // pair is also a consistent snapshot rather than two separate reads.
        var entry = await registry.GetEntryAsync(logicalTreeId);
        var physical = entry?.PhysicalTreeId ?? logicalTreeId;
        var shardCount = (await optionsResolver.ResolveAsync(physical)).ShardCount;
        return new Shards(physical, RoutedShardIndices.Resolve(shardCount, entry?.ShardMap));
    }

    /// <summary>Arms or lifts the fence on every shard of <paramref name="shards"/>.</summary>
    internal static Task SetAsync(IGrainFactory grainFactory, Shards shards, bool fenced)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(shards.ShardIndices);

        var calls = new Task[shards.ShardIndices.Length];
        for (var i = 0; i < calls.Length; i++)
        {
            calls[i] = grainFactory
                .GetGrain<IShardRootGrain>($"{shards.PhysicalTreeId}/{shards.ShardIndices[i]}")
                .SetBootstrapReadFenceAsync(fenced);
        }

        return Task.WhenAll(calls);
    }

    /// <summary>
    /// Why a bootstrap must not drain into <paramref name="shards"/> yet, or
    /// <see langword="null"/> when nothing holds it: a shard with an open split
    /// or consolidation record, or a resize in flight or an undo pending or
    /// running on <paramref name="logicalTreeId"/>. Call only after arming the
    /// fence on every shard. Fails closed: a probe that throws is a hold.
    /// </summary>
    internal static async Task<string?> FindBlockerAsync(IGrainFactory grainFactory, string logicalTreeId, Shards shards)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);

        try
        {
            if (await ShardMigrationResizeInterlock.FindMigratingShardAsync(
                    grainFactory, shards.PhysicalTreeId, shards.ShardIndices) is { } migrating)
            {
                return $"a shard split or consolidation is in progress on shard {migrating}";
            }

            // A resize in flight, or an undo pending or running, moves the tree
            // onto a copy the fence does not cover: the same unsettled-resize
            // test that holds an adaptive split. A completed resize that is
            // merely undoable does not hold the drain; its undo is refused
            // while the fence is up instead.
            if (await grainFactory.GetGrain<ITreeResizeGrain>(logicalTreeId).HoldsShardSplitsAsync())
            {
                return "a resize or a resize undo is in progress";
            }
        }
        catch (Exception ex)
        {
            return $"whether a migration or resize is in progress could not be established ({ex.GetType().Name})";
        }

        return null;
    }

    /// <summary>
    /// The first of <paramref name="shardIndices"/> on
    /// <paramref name="physicalTreeId"/> whose bootstrap read fence is armed, or
    /// <see langword="null"/> when none is. A probe fault propagates, so a caller
    /// that must fail closed treats it as fenced.
    /// </summary>
    internal static async Task<int?> FindFencedShardAsync(
        IGrainFactory grainFactory, string physicalTreeId, IReadOnlyList<int> shardIndices)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(physicalTreeId);
        ArgumentNullException.ThrowIfNull(shardIndices);

        var probes = new Task<bool>[shardIndices.Count];
        for (var i = 0; i < shardIndices.Count; i++)
        {
            probes[i] = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndices[i]}").IsBootstrapReadFencedAsync();
        }

        await Task.WhenAll(probes);
        for (var i = 0; i < probes.Length; i++)
        {
            if (probes[i].Result) return shardIndices[i];
        }

        return null;
    }
}
