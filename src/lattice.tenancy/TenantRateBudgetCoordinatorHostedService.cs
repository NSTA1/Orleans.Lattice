using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Drives the <see cref="TenantRateBudgetCoordinator"/> on a periodic lease
/// cadence as a silo-hosted background service. It runs one bootstrap cycle at
/// start (naturally static-even, since there is no demand yet) and then a cycle
/// each lease interval, all off the per-op hot path. Each cycle is wrapped so a
/// transient failure (for example the management grain not yet being reachable at
/// start-up) is logged and retried on the next tick rather than tearing down the
/// silo.
/// </summary>
internal sealed class TenantRateBudgetCoordinatorHostedService : IHostedService
{
    private readonly TenantRateBudgetCoordinator _coordinator;
    private readonly TimeProvider _timeProvider;
    private readonly IOptionsMonitor<LatticeTenantRateLimiterOptions> _options;
    private readonly ILogger<TenantRateBudgetCoordinatorHostedService> _logger;

    private readonly CancellationTokenSource _stopping = new();
    private Task? _loop;

    /// <summary>Initializes the hosted service over the coordinator and its schedule inputs.</summary>
    /// <param name="coordinator">The budget coordinator to drive. Must not be <c>null</c>.</param>
    /// <param name="timeProvider">The timestamp source backing the lease timer. Must not be <c>null</c>.</param>
    /// <param name="options">The limiter options monitor (for the lease interval). Must not be <c>null</c>.</param>
    /// <param name="logger">The logger for cycle failures. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantRateBudgetCoordinatorHostedService(
        TenantRateBudgetCoordinator coordinator,
        TimeProvider timeProvider,
        IOptionsMonitor<LatticeTenantRateLimiterOptions> options,
        ILogger<TenantRateBudgetCoordinatorHostedService> logger)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _coordinator = coordinator;
        _timeProvider = timeProvider;
        _options = options;
        _logger = logger;
    }

    /// <summary>The background lease loop, exposed so a test can await its completion after <see cref="StopAsync"/>.</summary>
    internal Task? Loop => _loop;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
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

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        await RunCycleSafelyAsync(cancellationToken).ConfigureAwait(false);

        var interval = _options.CurrentValue.LeaseInterval;
        if (interval <= TimeSpan.Zero)
        {
            interval = LatticeTenantRateLimiterOptions.DefaultLeaseInterval;
        }

        using var timer = new PeriodicTimer(interval, _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            await RunCycleSafelyAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RunCycleSafelyAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _coordinator.RunLeaseCycleAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Tenant rate-limiter budget lease cycle failed; buckets keep their previous apportionment until the next cycle.");
        }
    }
}
