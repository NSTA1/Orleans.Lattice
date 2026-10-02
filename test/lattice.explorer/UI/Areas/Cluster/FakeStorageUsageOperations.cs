using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Operations;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>A scripted <see cref="ILatticeStorageUsageOperations"/>: a start registers a queued re-measure a test then drives by hand.</summary>
internal sealed class FakeStorageUsageOperations() : FakeOperations(StorageUsageRefreshOperation.Kind), ILatticeStorageUsageOperations
{
    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartStorageUsageRefreshAsync(string? operationId = null, CancellationToken cancellationToken = default) =>
        StartAsync(nameof(StartStorageUsageRefreshAsync), operationId);

    /// <summary>Finishes refresh <paramref name="operationId"/> as succeeded with <paramref name="summary"/>.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="summary">The cluster totals.</param>
    public void Succeed(string operationId, ClusterStorageUsageSummary summary) =>
        Succeed(operationId, StorageUsageRefreshResults.ToResultMap(summary));
}
