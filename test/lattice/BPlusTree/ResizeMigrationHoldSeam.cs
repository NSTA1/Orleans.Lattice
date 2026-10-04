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
    /// shard migrations, if it holds them. The replaced copy is
    /// <paramref name="treeId"/> itself after a first resize, or one of
    /// <paramref name="previousPhysicalTreeIds"/> (the copy the tree resolved to
    /// before a later resize). Throws when the hold cannot be released.
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

        string? replaced = null;
        foreach (var candidate in (previousPhysicalTreeIds ?? []).Append(treeId).Distinct())
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

        foreach (var index in await TopologyDrivers.PhysicalShardsAsync(grainFactory, treeId))
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{replaced}/{index}").ClearShadowForwardAsync(operationId);
        }

        if (await resize.HoldsShardMigrationsAsync())
            throw new InvalidOperationException($"The resize of '{treeId}' still holds shard migrations after its replaced copy stopped mirroring.");
    }
}
