using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed class TenantRegionBackfillStatusChangeListener(
    TenantRegionBackfillCoordinatorDispatcher dispatcher,
    Microsoft.Extensions.Options.IOptions<Orleans.Configuration.ClusterOptions> clusterOptions)
    : ITenantRegionStatusChangeListener
{
    private readonly string _localRegionId = string.IsNullOrEmpty(clusterOptions.Value.ClusterId)
        ? "default"
        : clusterOptions.Value.ClusterId;

    public Task OnRegionStatusChangedAsync(
        TenantRegionStatusChange change,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(change.RegionId, _localRegionId, StringComparison.Ordinal)
            || change.CurrentStatus is not (TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling))
        {
            return Task.CompletedTask;
        }

        return dispatcher.EnsureRunningAsync(change.Tenant, cancellationToken);
    }
}
