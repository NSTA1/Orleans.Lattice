using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Views;

/// <summary>
/// <see cref="IHostedService"/> that registers every startup-declared view and
/// re-hydrates every durably-registered runtime view (created previously through
/// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>)
/// into the <see cref="IViewCatalog"/>,
/// then activates each one's <see cref="IViewMaintainerGrain"/> on silo startup.
/// Orleans' single-activation guarantee makes the activation calls
/// cluster-singleton even though every silo runs this service.
/// <para>
/// Re-hydrating runtime views gives them the same restart-durability that startup
/// views get: their maintainer resumes from the durable checkpoint without the
/// application having to re-call
/// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>.
/// A runtime view's projection is resolved from the silo service provider by its
/// persisted concrete type; a startup declaration of the same name wins on
/// conflict (the runtime record is skipped).
/// </para>
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
    RuntimeViewProjectionProviderCatalog runtimeProviders,
    IGrainFactory grainFactory,
    ILogger<ViewActivationService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Register every declared view up front so a maintainer that activates
        // (or a runtime query) can resolve its source tree id and projection.
        var pending = new List<string>();
        var startupNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var registration in registrations)
        {
            catalog.Register(registration.Resolve(services));
            startupNames.Add(registration.ViewName);
            pending.Add(registration.ViewName);
        }

        // Re-hydration of durably-registered runtime views runs inside the same
        // retry loop because reading the registry grain can also race the silo
        // becoming dispatch-ready. It is attempted until it succeeds once.
        var runtimeHydrated = false;

        var delay = InitialRetryDelay;
        var pass = 0;
        while (true)
        {
            stoppingToken.ThrowIfCancellationRequested();
            pass++;
            var anySuccess = false;

            if (!runtimeHydrated)
            {
                try
                {
                    await HydrateRuntimeViewsAsync(startupNames, pending, stoppingToken);
                    runtimeHydrated = true;
                    anySuccess = true;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex,
                        "Re-hydrating durable runtime views failed on pass {Pass}; will retry.", pass);
                }
            }

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

            if (runtimeHydrated && pending.Count == 0)
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

    /// <summary>
    /// Reads the durable runtime-view registry and re-registers each runtime view
    /// (resolving its projection from the service provider by the persisted type),
    /// skipping any name a startup declaration owns. Newly-registered runtime view
    /// names are appended to <paramref name="pending"/> for activation.
    /// </summary>
    private async Task HydrateRuntimeViewsAsync(
        HashSet<string> startupNames,
        List<string> pending,
        CancellationToken cancellationToken)
    {
        var registry = grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
        var records = await registry.ListAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        foreach (var record in records)
        {
            // A startup declaration of the same name is authoritative and already
            // registered above; the durable runtime record is skipped.
            if (startupNames.Contains(record.ViewName))
            {
                continue;
            }

            var registration = RuntimeViewRehydrator.Resolve(record, services, runtimeProviders, logger);
            if (registration is null)
            {
                continue;
            }

            catalog.Register(registration);
            if (!pending.Contains(record.ViewName))
            {
                pending.Add(record.ViewName);
            }
        }
    }
}
