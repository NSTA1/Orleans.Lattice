using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The silo-hosted background service that publishes the per-tenant observability
/// gauges. On start (when <see cref="TenantObservabilityOptions.PublishGauges"/>
/// is set) it registers the observable gauges once, publishes an initial snapshot,
/// and then republishes on a <see cref="TimeProvider"/>-driven timer at
/// <see cref="TenantObservabilityOptions.PublishInterval"/>. Every publish samples
/// the warm usage index and the durable overage billing seam off the metric scrape
/// path, so a scrape only ever reads pre-built measurement arrays.
/// </summary>
/// <remarks>
/// Each publish cycle is wrapped so a transient failure (for example the overage
/// grains not yet being reachable at start-up) is logged and retried on the next
/// tick rather than tearing down the silo, mirroring the tenant rate-limiter's
/// lease loop. The gauges are created only from here, so a cluster with tenancy
/// disabled - which never registers this service - publishes no tenancy meter at
/// all.
/// </remarks>
internal sealed class TenantObservabilityPublisher : IHostedService
{
    private readonly TenantObservabilitySource _source;
    private readonly TimeProvider _timeProvider;
    private readonly IOptionsMonitor<TenantObservabilityOptions> _options;
    private readonly ILogger<TenantObservabilityPublisher> _logger;

    private readonly CancellationTokenSource _stopping = new();
    private Task? _loop;

    /// <summary>Initializes the publisher over its snapshot source and schedule inputs.</summary>
    /// <param name="source">The snapshot source composing usage and overage. Must not be <c>null</c>.</param>
    /// <param name="timeProvider">The timestamp source backing the publish timer. Must not be <c>null</c>.</param>
    /// <param name="options">The observability options monitor. Must not be <c>null</c>.</param>
    /// <param name="logger">The logger for publish-cycle failures. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantObservabilityPublisher(
        TenantObservabilitySource source,
        TimeProvider timeProvider,
        IOptionsMonitor<TenantObservabilityOptions> options,
        ILogger<TenantObservabilityPublisher> logger)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _source = source;
        _timeProvider = timeProvider;
        _options = options;
        _logger = logger;
    }

    /// <summary>The background publish loop, exposed so a test can await its completion after <see cref="StopAsync"/>.</summary>
    internal Task? Loop => _loop;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.CurrentValue.PublishGauges)
        {
            return Task.CompletedTask;
        }

        TenantObservabilityGaugeRegistry.EnsureRegistered();

        // Launch fire-and-forget: the first cycle may need the cluster to be ready,
        // so awaiting it here could stall silo start-up.
        _loop = RunLoopAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);

        if (_loop is { } loop)
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }
    }

    /// <summary>
    /// Samples the sources once and publishes the per-tenant gauge snapshot.
    /// Exposed so a test can drive a deterministic publish without waiting on the
    /// timer. Ensures the gauges are registered so a test that only calls this
    /// still observes series.
    /// </summary>
    /// <param name="cancellationToken">Cancels the sample.</param>
    internal async Task PublishOnceAsync(CancellationToken cancellationToken = default)
    {
        TenantObservabilityGaugeRegistry.EnsureRegistered();
        var tenants = await _source.SnapshotAllAsync(cancellationToken).ConfigureAwait(false);
        TenantObservabilityGaugeRegistry.Publish(TenantObservabilityGaugeSnapshot.Build(tenants));
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        await PublishCycleSafelyAsync(cancellationToken).ConfigureAwait(false);

        var interval = _options.CurrentValue.PublishInterval;
        if (interval <= TimeSpan.Zero)
        {
            interval = TenantObservabilityOptions.DefaultPublishInterval;
        }

        using var timer = new PeriodicTimer(interval, _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            await PublishCycleSafelyAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task PublishCycleSafelyAsync(CancellationToken cancellationToken)
    {
        try
        {
            await PublishOnceAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Tenant observability publish cycle failed; the previously published gauge snapshot remains in effect until the next tick.");
        }
    }
}
