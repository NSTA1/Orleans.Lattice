using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed class TenantRegionBackfillCoordinatorDispatcher(
    ITenantRegistry registry,
    IGrainFactory grainFactory,
    TenantLocalRegion localRegion)
{
    internal async Task EnsureRunningAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await grainFactory.GetGrain<ITenantRegionBackfillCoordinatorGrain>(tenant.Value)
            .EnsureRunningAsync().ConfigureAwait(false);
    }

    internal async Task ReconcileOnceAsync(CancellationToken cancellationToken)
    {
        var localRegionId = localRegion.RegionId;
        await foreach (var tenant in registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            var status = tenant.GetRegionStatus(localRegionId);
            if (status is TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling)
            {
                await EnsureRunningAsync(tenant.Id, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
