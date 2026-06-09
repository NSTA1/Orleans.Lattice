using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILatticeAdmin"/> implementation. A single activation
/// per cluster keyed by <see cref="LatticeConstants.AdminGrainKey"/>. Reduces
/// cluster-wide administrative queries across every registered tree reported
/// by the tree registry.
/// </summary>
internal sealed partial class LatticeAdminGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    ILogger<LatticeAdminGrain> logger,
    LatticeOptionsResolver? optionsResolver = null,
    IWalStorageProviderCatalog? walProviderCatalog = null,
    IWalRecordEncoder? walRecordEncoder = null) : ILatticeAdmin, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public Task<ClusterStorageUsageReport> GetTotalStorageUsageAsync(CancellationToken cancellationToken = default)
        => BuildRollupAsync(forceRefresh: false, cancellationToken);

    /// <inheritdoc />
    public Task<ClusterStorageUsageReport> RefreshStorageUsageAsync(CancellationToken cancellationToken = default)
        => BuildRollupAsync(forceRefresh: true, cancellationToken);

    /// <inheritdoc />
    public async Task PollWalUsageAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var treeIds = await registry.GetAllTreeIdsAsync();
        cancellationToken.ThrowIfCancellationRequested();

        if (treeIds.Count == 0) return;

        var tasks = new Task[treeIds.Count];
        for (var i = 0; i < treeIds.Count; i++)
        {
            tasks[i] = PollWalUsageForTreeAsync(treeIds[i], cancellationToken);
        }
        await Task.WhenAll(tasks);
    }

    private async Task PollWalUsageForTreeAsync(string treeId, CancellationToken cancellationToken)
    {
        try
        {
            var wal = grainFactory.GetGrain<ILatticeWalUsage>(treeId);
            await wal.GetWalUsageAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            // A failing tree must not abort the cluster-wide WAL poll;
            // the next tick retries. The metrics sink's staleness horizon
            // covers a few missed polls before a series expires.
            logger.LogDebug(ex, "WAL-usage poll failed for tree {TreeId}", treeId);
        }
    }

    private async Task<ClusterStorageUsageReport> BuildRollupAsync(bool forceRefresh, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var treeIds = await registry.GetAllTreeIdsAsync();
        cancellationToken.ThrowIfCancellationRequested();

        var tasks = new Task<TreeStorageUsageReport>[treeIds.Count];
        for (var i = 0; i < treeIds.Count; i++)
        {
            tasks[i] = GetTreeUsageAsync(treeIds[i], forceRefresh, cancellationToken);
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

    private async Task<TreeStorageUsageReport> GetTreeUsageAsync(string treeId, bool forceRefresh, CancellationToken cancellationToken)
    {
        try
        {
            var usage = grainFactory.GetGrain<ILatticeStorageUsage>(treeId);
            return await usage.GetReportAsync(forceRefresh, cancellationToken);
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
