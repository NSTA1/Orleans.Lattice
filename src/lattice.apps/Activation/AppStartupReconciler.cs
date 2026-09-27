using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Re-reconciles every enabled app in the background once the silo starts, so an app's trees and
/// rules re-converge with the in-image manifests after a deploy. It never affects silo startup:
/// the work runs off the start path, the registry read retries with backoff until the silo can
/// serve it, and every per-app failure - an invalid or over-ceiling manifest included - is
/// recorded against that app by the pipeline and logged, never thrown.
/// </summary>
internal sealed class AppStartupReconciler(
    IAppRegistry registry,
    IAppActivationPipeline pipeline,
    IOptionsMonitor<LatticeAppsOptions> options,
    ILogger<AppStartupReconciler> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            if (!options.CurrentValue.ReconcileOnStartup)
            {
                return;
            }

            await ReconcileEnabledAppsAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Deliberately unconditional: a fault here must never stop the host.
            if (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(ex, "The startup reconcile of enabled apps faulted; apps keep their last applied state.");
            }
        }
    }

    /// <summary>Reconciles every enabled app once and returns each run's outcome.</summary>
    internal async Task<IReadOnlyList<AppActivationOutcome>> ReconcileEnabledAppsAsync(CancellationToken cancellationToken)
    {
        var enabled = await ListEnabledAsync(cancellationToken).ConfigureAwait(false);
        var outcomes = new List<AppActivationOutcome>(enabled.Count);
        foreach (var record in enabled)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                AppActivationOutcome outcome;
                using (LatticeSystemOrigin.Enter())
                {
                    outcome = await pipeline.ReconcileAsync(record.Tenant, record.Slug, cancellationToken).ConfigureAwait(false);
                }

                outcomes.Add(outcome);
                if (!outcome.Succeeded)
                {
                    logger.LogWarning(
                        "Startup reconcile of app {Tenant}/{Slug} failed with {Failure}.",
                        record.Tenant.Value, record.Slug.Value, outcome.Failure);
                }
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Startup reconcile of app {Tenant}/{Slug} could not run.", record.Tenant.Value, record.Slug.Value);
            }
        }

        return outcomes;
    }

    private async Task<List<AppRegistryRecord>> ListEnabledAsync(CancellationToken cancellationToken)
    {
        var settings = options.CurrentValue;
        var delay = settings.StartupRetryDelay;
        while (true)
        {
            try
            {
                var enabled = new List<AppRegistryRecord>();
                await foreach (var record in registry.ListAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (record.State == AppRegistryLifecycleState.Enabled)
                    {
                        enabled.Add(record);
                    }
                }

                return enabled;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogDebug(ex, "App registry not yet readable; retrying the startup reconcile in {Delay}.", delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, settings.StartupRetryMaxDelay.Ticks));
            }
        }
    }
}
