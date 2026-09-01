using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The startup sweep that arms an approximate-index build coordinator for every
/// registered repository, and re-sweeps periodically so a repository added later
/// is picked up too.
/// <para>
/// <b>This is the part that actually fixes cold start.</b> Scheduling the build on
/// a durable coordinator makes it crash-safe, but a coordinator nobody arms is
/// still a build nobody starts - which was the original defect in a different
/// costume. The sweep removes the last dependence on traffic: a restored volume
/// with no client at all converges to a serving index, and the first query that
/// does arrive finds one already built rather than being the thing that triggers
/// the build and then paying for it.
/// </para>
/// <para>
/// Arming is idempotent, so re-sweeping costs a reminder re-registration per
/// repository. The Orleans silo is itself a hosted service, so a grain call from a
/// hosted service's start can race ahead of the silo becoming dispatch-ready; the
/// sweep therefore retries with backoff until it gets through or the host stops.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexSweepService(
    RepoContextStore store,
    RepoContextAnnIndexScheduler scheduler,
    RepoContextIndexingOptions options,
    ILogger<RepoContextAnnIndexSweepService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The floor on the re-sweep cadence. The sweep follows the reconcile interval
    /// so it stays in step with the pass that produces the vectors it schedules an
    /// index over, but a host that makes the reconcile near-continuous must not
    /// turn this into a hot loop of grain calls.
    /// </summary>
    private static readonly TimeSpan MinimumSweepInterval = TimeSpan.FromMinutes(1);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!scheduler.CanSchedule)
        {
            logger.LogInformation(
                "Repository-context approximate-index build scheduling is off (switch disabled, exact retrieval "
                + "configured, or no embedding provider bound); no build coordinator will be armed.");
            return;
        }

        var interval = options.ReconcileInterval > MinimumSweepInterval
            ? options.ReconcileInterval
            : MinimumSweepInterval;

        var delay = InitialRetryDelay;
        while (!stoppingToken.IsCancellationRequested)
        {
            var armed = await TrySweepAsync(stoppingToken).ConfigureAwait(false);

            // A failed sweep backs off and retries promptly, because until it gets
            // through nothing is scheduled at all. A successful one waits a full
            // interval, since re-arming a coordinator that is already running buys
            // nothing.
            var wait = armed ? interval : delay;
            if (!armed)
            {
                delay = delay < MaxRetryDelay
                    ? TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks))
                    : MaxRetryDelay;
            }
            else
            {
                delay = InitialRetryDelay;
            }

            try
            {
                await Task.Delay(wait, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Runs one sweep, returning whether it completed. A fault is logged and
    /// reported as an incomplete sweep so the caller retries rather than settling
    /// into the long cadence with nothing scheduled.
    /// </summary>
    private async Task<bool> TrySweepAsync(CancellationToken stoppingToken)
    {
        try
        {
            var repos = await store.ListReposAsync(stoppingToken).ConfigureAwait(false);
            foreach (var repo in repos.Repos)
            {
                stoppingToken.ThrowIfCancellationRequested();
                await scheduler.TryArmAsync(repo.RepoId, stoppingToken).ConfigureAwait(false);
            }

            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return true;
        }
        catch (Exception ex)
        {
            logger.LogDebug(
                ex,
                "Repository-context approximate-index sweep could not arm the build coordinators yet; retrying.");
            return false;
        }
    }
}
