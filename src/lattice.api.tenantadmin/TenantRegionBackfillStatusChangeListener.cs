using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed class TenantRegionBackfillStatusChangeListener(
    TenantRegionBackfillCoordinatorDispatcher dispatcher,
    TenantLocalRegion localRegion)
    : ITenantRegionStatusChangeListener
{
    public Task OnRegionStatusChangedAsync(
        TenantRegionStatusChange change,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(change.RegionId, localRegion.RegionId, StringComparison.Ordinal)
            || change.CurrentStatus is not (TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling))
        {
            return Task.CompletedTask;
        }

        return dispatcher.EnsureRunningAsync(change.Tenant, cancellationToken);
    }
}
