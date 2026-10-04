using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// TEST-ONLY seam for issue #4452. After a resize completes, the copy it
/// replaced keeps mirroring into the resized copy through the soft-delete window
/// (<see cref="LatticeOptions.SoftDeleteDuration"/>), and the resize holds every
/// split, consolidation and reshard of the tree until the purge clears that
/// copy's shadow-forward state (<see cref="ITreeResizeGrain.HoldsShardMigrationsAsync"/>).
/// A test that resizes and then changes the tree's topology clears that state
/// here, exactly as the purge would, instead of waiting out the window. It uses
/// only the existing internal shard-root surface; there is no production
/// equivalent, and production code must never call it.
/// </summary>
internal static class ResizeMigrationHoldSeam
{
    /// <summary>
    /// Ends <paramref name="treeId"/>'s most recent completed resize's hold on
    /// shard migrations, if it holds them, and throws when it cannot.
    /// <para>
    /// The replaced copy is not guessed from what a caller happened to observe -
    /// a resize's own timer can swap before a driver's first look - but taken
    /// from the resize coordinator: of the tree's own id and every copy
    /// registered under <c>{treeId}/resized/</c>, the one
    /// <see cref="ITreeResizeGrain.ReferencesPhysicalTreeAsync"/> names that the
    /// tree no longer resolves to. The resize shadow-forwarded the union of
    /// <c>0</c> to the pinned shard count and every index the map routed to (a
    /// shrink leaves retired donors in the first set but not the second), so the
    /// whole contiguous range up to the highest of those is cleared.
    /// <paramref name="previousPhysicalTreeIds"/> only adds candidates.
    /// </para>
    /// </summary>
    public static async Task ReleaseAsync(
        IGrainFactory grainFactory, string treeId, IEnumerable<string>? previousPhysicalTreeIds = null)
    {
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(treeId);
        if (!await resize.HoldsShardMigrationsAsync()) return;
        if (!await resize.IsIdleAsync())
            throw new InvalidOperationException($"The resize of '{treeId}' is still in flight; only a completed resize's hold can be released.");

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resized = await registry.ResolveAsync(treeId);
        var operationId = resized[(resized.LastIndexOf('/') + 1)..];

        var candidates = new List<string> { treeId };
        candidates.AddRange(await registry.GetAllTreeIdsAsync($"{treeId}/resized/"));
        candidates.AddRange(previousPhysicalTreeIds ?? []);

        string? replaced = null;
        foreach (var candidate in candidates.Distinct())
        {
            if (!string.Equals(candidate, resized, StringComparison.Ordinal)
                && await resize.ReferencesPhysicalTreeAsync(candidate))
            {
                replaced = candidate;
                break;
            }
        }

        if (replaced is null)
            throw new InvalidOperationException($"Could not identify the copy the resize of '{treeId}' replaced.");

        var highest = -1;
        foreach (var index in await TopologyDrivers.PhysicalShardsAsync(grainFactory, treeId))
        {
            highest = Math.Max(highest, index);
        }
        foreach (var id in new[] { treeId, replaced })
        {
            if (await registry.GetEntryAsync(id) is { ShardCount: { } pinned })
            {
                highest = Math.Max(highest, pinned - 1);
            }
        }
        highest = Math.Max(highest, LatticeConstants.DefaultShardCount - 1);

        for (var index = 0; index <= highest; index++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{replaced}/{index}").ClearShadowForwardAsync(operationId);
        }

        if (await resize.HoldsShardMigrationsAsync())
            throw new InvalidOperationException($"The resize of '{treeId}' still holds shard migrations after its replaced copy '{replaced}' stopped mirroring.");
    }
}
