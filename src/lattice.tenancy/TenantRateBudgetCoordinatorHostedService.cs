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
/// start-up) is logged and retried on a later tick rather than tearing down the
/// silo.
/// </summary>
/// <remarks>
/// Two bounds keep a slow cluster from turning the loop into a livelock. Each cycle
/// runs under <see cref="LatticeTenantRateLimiterOptions.LeaseCycleTimeout"/>
/// (clamped to at most one lease interval), so a stalled registry read is cancelled
/// rather than left to occupy the loop; and consecutive failures double the
/// effective interval up to
/// <see cref="LatticeTenantRateLimiterOptions.MaxLeaseBackoff"/>, resetting on the
/// first success, so an unhealthy cluster is probed at a decaying rate instead of
/// being hammered every tick.
/// </remarks>
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
        var interval = ResolveLeaseInterval();
        var succeeded = await RunCycleSafelyAsync(interval, cancellationToken).ConfigureAwait(false);
        var consecutiveFailures = succeeded ? 0 : 1;

        using var timer = new PeriodicTimer(NextPeriod(interval, consecutiveFailures), _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            // Re-read each tick so a live options change is honoured, and so the
            // period returns to the configured interval as soon as a cycle succeeds.
            interval = ResolveLeaseInterval();
            succeeded = await RunCycleSafelyAsync(interval, cancellationToken).ConfigureAwait(false);
            consecutiveFailures = succeeded ? 0 : Math.Min(consecutiveFailures + 1, MaxBackoffShift);
            timer.Period = NextPeriod(interval, consecutiveFailures);
        }
    }

    /// <summary>
    /// Caps the doubling exponent so the shift can never overflow, independently of
    /// the <see cref="LatticeTenantRateLimiterOptions.MaxLeaseBackoff"/> clamp.
    /// </summary>
    private const int MaxBackoffShift = 16;

    private TimeSpan ResolveLeaseInterval()
    {
        var interval = _options.CurrentValue.LeaseInterval;
        return interval > TimeSpan.Zero ? interval : LatticeTenantRateLimiterOptions.DefaultLeaseInterval;
    }

    /// <summary>
    /// The effective tick period: the configured interval, doubled once per
    /// consecutive failure and clamped to the configured backoff ceiling. A ceiling
    /// below the interval simply disables backoff.
    /// </summary>
    internal TimeSpan NextPeriod(TimeSpan interval, int consecutiveFailures)
    {
        if (consecutiveFailures <= 0)
        {
            return interval;
        }

        var ceiling = _options.CurrentValue.MaxLeaseBackoff;
        if (ceiling <= TimeSpan.Zero)
        {
            ceiling = LatticeTenantRateLimiterOptions.DefaultMaxLeaseBackoff;
        }

        if (ceiling <= interval)
        {
            return interval;
        }

        var multiplier = 1L << Math.Min(consecutiveFailures, MaxBackoffShift);
        var backedOff = interval.Ticks >= ceiling.Ticks / multiplier
            ? ceiling
            : TimeSpan.FromTicks(interval.Ticks * multiplier);

        return backedOff > ceiling ? ceiling : backedOff;
    }

    /// <summary>
    /// The bound on one cycle: the configured timeout, falling back to the default
    /// when non-positive and clamped to at most one lease interval so a single cycle
    /// can never consume more than one tick's worth of wall clock.
    /// </summary>
    internal TimeSpan ResolveCycleTimeout(TimeSpan interval)
    {
        var timeout = _options.CurrentValue.LeaseCycleTimeout;
        if (timeout <= TimeSpan.Zero)
        {
            timeout = LatticeTenantRateLimiterOptions.DefaultLeaseCycleTimeout;
        }

        return timeout > interval ? interval : timeout;
    }

    private async Task<bool> RunCycleSafelyAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        var timeout = ResolveCycleTimeout(interval);
        // Two sources: a TimeProvider-driven deadline (so a test clock can drive it)
        // linked with the shutdown token, so either can cancel the cycle.
        using var deadline = new CancellationTokenSource(timeout, _timeProvider);
        using var cycle = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);

        try
        {
            await _coordinator.RunLeaseCycleAsync(cycle.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Tenant rate-limiter budget lease cycle exceeded its {Timeout} bound and was cancelled; "
                + "buckets keep their previous apportionment and the next cycle backs off.",
                timeout);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Tenant rate-limiter budget lease cycle failed; buckets keep their previous apportionment until the next cycle.");
            return false;
        }
    }
}
