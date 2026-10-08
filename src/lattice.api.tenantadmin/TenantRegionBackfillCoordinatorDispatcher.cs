using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed class TenantRegionBackfillCoordinatorDispatcher(
    ITenantRegistry registry,
    IGrainFactory grainFactory,
    IOptions<ClusterOptions> clusterOptions)
{
    private readonly string _localRegionId = string.IsNullOrEmpty(clusterOptions.Value.ClusterId)
        ? "default"
        : clusterOptions.Value.ClusterId;

    internal async Task EnsureRunningAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await grainFactory.GetGrain<ITenantRegionBackfillCoordinatorGrain>(tenant.Value)
            .EnsureRunningAsync().ConfigureAwait(false);
    }

    internal async Task ReconcileOnceAsync(CancellationToken cancellationToken)
    {
        await foreach (var tenant in registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            var status = tenant.GetRegionStatus(_localRegionId);
            if (status is TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling)
            {
                await EnsureRunningAsync(tenant.Id, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
