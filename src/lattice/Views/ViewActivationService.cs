using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Views;

/// <summary>
/// <see cref="IHostedService"/> that registers every startup-declared view in
/// the <see cref="IViewCatalog"/> and activates its
/// <see cref="IViewMaintainerGrain"/> on silo startup. Orleans' single-activation
/// guarantee makes the activation calls cluster-singleton even though every silo
/// runs this service.
/// <para>
/// Mirrors <c>ReplicationDriverActivationService</c>: activation is retried with
/// exponential backoff because hosted-service start can race ahead of the silo
/// becoming dispatch-ready. The loop exits when every maintainer has activated or
/// the host is shutting down.
/// </para>
/// </summary>
internal sealed class ViewActivationService(
    IServiceProvider services,
    IReadOnlyList<StartupViewRegistration> registrations,
    IViewCatalog catalog,
    IGrainFactory grainFactory,
    ILogger<ViewActivationService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (registrations.Count == 0)
        {
            return;
        }

        // Register every declared view up front so a maintainer that activates
        // (or a runtime query) can resolve its source tree id and projection.
        var pending = new List<string>();
        foreach (var registration in registrations)
        {
            catalog.Register(registration.Resolve(services));
            pending.Add(registration.ViewName);
        }

        var delay = InitialRetryDelay;
        var pass = 0;
        while (pending.Count > 0)
        {
            stoppingToken.ThrowIfCancellationRequested();
            pass++;
            var anySuccess = false;
            for (var i = pending.Count - 1; i >= 0; i--)
            {
                stoppingToken.ThrowIfCancellationRequested();
                var viewName = pending[i];
                try
                {
                    await grainFactory.GetGrain<IViewMaintainerGrain>(viewName)
                        .EnsureActiveAsync(stoppingToken).ConfigureAwait(false);
                    pending.RemoveAt(i);
                    anySuccess = true;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex,
                        "View maintainer activation failed for '{ViewName}' on pass {Pass}; will retry.",
                        viewName, pass);
                }
            }

            if (pending.Count == 0)
            {
                return;
            }

            if (anySuccess)
            {
                delay = InitialRetryDelay;
            }

            try
            {
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }

            var nextTicks = Math.Min(MaxRetryDelay.Ticks, delay.Ticks * 2);
            delay = TimeSpan.FromTicks(nextTicks);
        }
    }
}
