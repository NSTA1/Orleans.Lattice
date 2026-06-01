using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILatticeAdmin"/> implementation. A single activation
/// per cluster keyed by <see cref="LatticeConstants.AdminGrainKey"/>. Reduces
/// cluster-wide administrative queries across every registered tree reported
/// by the tree registry.
/// </summary>
internal sealed class LatticeAdminGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    ILogger<LatticeAdminGrain> logger) : ILatticeAdmin, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task<ClusterStorageUsageReport> GetTotalStorageUsageAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var treeIds = await registry.GetAllTreeIdsAsync();
        cancellationToken.ThrowIfCancellationRequested();

        var tasks = new Task<TreeStorageUsageReport>[treeIds.Count];
        for (var i = 0; i < treeIds.Count; i++)
        {
            tasks[i] = GetTreeUsageAsync(treeIds[i], cancellationToken);
        }

        var reports = await Task.WhenAll(tasks);
        cancellationToken.ThrowIfCancellationRequested();

        // Tree ids come back sorted from the registry; preserve that order.
        var sorted = reports.ToImmutableArray();

        long walRetained = 0;
        long snapshot = 0;
        long leafState = 0;
        long total = 0;
        var partial = false;
        foreach (var report in sorted)
        {
            walRetained += report.WalRetainedBytes;
            snapshot += report.SnapshotBytes;
            leafState += report.LeafStateBytes;
            total += report.TotalBytes;
            partial |= report.Partial;
        }

        return new ClusterStorageUsageReport
        {
            TreeCount = sorted.Length,
            WalRetainedBytes = walRetained,
            SnapshotBytes = snapshot,
            LeafStateBytes = leafState,
            TotalBytes = total,
            Partial = partial,
            Trees = sorted,
            SampledAt = DateTimeOffset.UtcNow,
        };
    }

    private async Task<TreeStorageUsageReport> GetTreeUsageAsync(string treeId, CancellationToken cancellationToken)
    {
        try
        {
            var lattice = grainFactory.GetGrain<ILattice>(treeId);
            return await lattice.GetStorageUsageAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cluster storage-usage roll-up failed for tree {TreeId}", treeId);
            // A failing tree contributes a partial zero rather than aborting
            // the whole cluster roll-up.
            return new TreeStorageUsageReport
            {
                TreeId = treeId,
                Partial = true,
                SampledAt = DateTimeOffset.UtcNow,
            };
        }
    }
}
