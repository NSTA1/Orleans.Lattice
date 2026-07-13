using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Starts the cluster-wide backup-health monitor once the silo is dispatch-ready.
/// The Orleans silo is itself an <see cref="IHostedService"/>, so a grain call made
/// from a hosted service's start can race ahead of the silo becoming ready; this
/// <see cref="BackgroundService"/> therefore calls
/// <see cref="ILatticeBackupHealthMonitorGrain.EnsureStartedAsync"/> on a tracked
/// task with a short retry-with-backoff loop until it succeeds or the host stops.
/// The monitor grain itself is gated on a durable sink and on
/// <see cref="LatticeBackupHealthOptions.Enabled"/>, so this activation is a cheap
/// no-op registration on a non-durable-sink deployment.
/// </summary>
internal sealed class BackupHealthMonitorActivationService(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeBackupHealthOptions> optionsMonitor,
    ILogger<BackupHealthMonitorActivationService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!optionsMonitor.CurrentValue.Enabled)
        {
            logger.LogInformation("Backup health monitoring is disabled; skipping monitor activation.");
            return;
        }

        var delay = InitialRetryDelay;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await grainFactory
                    .GetGrain<ILatticeBackupHealthMonitorGrain>(BackupHealthMonitorKey.Value)
                    .EnsureStartedAsync()
                    .ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogDebug(ex, "Backup health monitor not yet startable; retrying in {Delay}.", delay);
                try
                {
                    await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                delay = delay < MaxRetryDelay ? TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks)) : MaxRetryDelay;
            }
        }
    }
}
