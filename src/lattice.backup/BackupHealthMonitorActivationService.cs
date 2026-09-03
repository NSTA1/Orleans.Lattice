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
            catch (Exception ex)
            {
                // Deliberately NOT `catch (...) when (!stoppingToken.IsCancellationRequested)`.
                //
                // An exception filter is evaluated when the exception is thrown, so a
                // shutdown that lands between the grain call failing and the filter
                // running made the filter false and left the exception UNCAUGHT. It then
                // escaped ExecuteAsync and faulted the BackgroundService task - and
                // because BackgroundServiceExceptionBehavior defaults to StopHost, a
                // benign "silo not ready" during shutdown could take the host down with
                // it. The window is small but real: it turned up as an intermittent
                // failure of the cancellation test on a loaded CI runner.
                //
                // Catch unconditionally and decide afterwards. Once cancellation has been
                // requested the call was going to be abandoned anyway, so whatever it
                // threw on the way out is not a fault - it is just the shape of stopping.
                if (stoppingToken.IsCancellationRequested)
                {
                    return;
                }

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
