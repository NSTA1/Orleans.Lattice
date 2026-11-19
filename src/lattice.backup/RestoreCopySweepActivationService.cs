using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Arms the estate-wide restore-copy sweep (issue #6103) once the silo is
/// dispatch-ready, retrying with a bounded back-off until it succeeds or the host
/// stops, as <see cref="BackupHealthMonitorActivationService"/> does for the health
/// monitor. The sweep grain keeps an existing reminder's schedule, so every silo
/// calling this at start-up never postpones a sweep.
/// </summary>
internal sealed class RestoreCopySweepActivationService(
    IGrainFactory grainFactory,
    ILogger<RestoreCopySweepActivationService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delay = InitialRetryDelay;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await grainFactory.GetGrain<IRestoreCopySweepGrain>(IRestoreCopySweepGrain.SingletonKey)
                    .EnsureStartedAsync()
                    .ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                // Caught unconditionally, as in BackupHealthMonitorActivationService:
                // a filter could let a fault raised during shutdown escape and stop the host.
                if (stoppingToken.IsCancellationRequested)
                {
                    return;
                }

                logger.LogDebug(ex, "Restore-copy sweep not yet startable; retrying in {Delay}.", delay);
                try
                {
                    await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks));
            }
        }
    }
}
