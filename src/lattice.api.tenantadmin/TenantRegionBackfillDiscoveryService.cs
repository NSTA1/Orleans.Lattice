using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed class TenantRegionBackfillDiscoveryService(
    TenantRegionBackfillCoordinatorDispatcher dispatcher,
    ILogger<TenantRegionBackfillDiscoveryService> logger) : BackgroundService
{
    private static readonly TimeSpan ReconcileInterval = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await dispatcher.ReconcileOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Tenant-region backfill discovery failed; it will retry.");
            }

            await Task.Delay(ReconcileInterval, stoppingToken).ConfigureAwait(false);
        }
    }
}
