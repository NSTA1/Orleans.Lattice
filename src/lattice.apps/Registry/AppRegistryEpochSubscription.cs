using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The silo-hosted half of the app-registry currency protocol (#5713, a port of the tenant registry's
/// #4030 subscription). It keeps this silo's <see cref="CompiledAppRegistrySnapshotMaintainer"/> leased
/// by, and subscribed to, the cluster-wide <see cref="IAppRegistryEpochGrain"/>, and applies every epoch
/// the grain pushes. While the lease is live, an evaluation trusts the snapshot without a grain call.
/// </summary>
/// <remarks>
/// <para>
/// The lease loop is launched fire-and-forget from <see cref="StartAsync"/> and retries a failure on a
/// short bounded cadence, so silo start-up never blocks on a cluster round-trip. Until the first lease
/// lands, and whenever renewal keeps failing past the lease, the maintainer reads the epoch on every
/// evaluation as it did before (#5595), and fails closed when it cannot. Losing this service therefore
/// costs grain calls, never revocation.
/// </para>
/// <para>
/// The lease is renewed every third of its duration, so one lost renewal does not cost the silo its fast
/// path. The deadline is measured from the moment each request was sent.
/// </para>
/// </remarks>
internal sealed class AppRegistryEpochSubscription : IHostedService, IAppRegistryEpochObserver
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IGrainFactory? _grainFactory;
    private readonly SiloAddress? _siloAddress;
    private readonly CompiledAppRegistrySnapshotMaintainer _maintainer;
    private readonly TimeProvider _time;
    private readonly ILogger<AppRegistryEpochSubscription> _logger;
    private readonly CancellationTokenSource _stopping = new();

    private IAppRegistryEpochObserver? _reference;
    private Task _leaseLoop = Task.CompletedTask;

    /// <summary>Initializes the subscription.</summary>
    /// <param name="grainFactory">The silo's grain factory, or null on a host with no cluster.</param>
    /// <param name="localSilo">This silo's details, whose address identifies it to the epoch grain, or null on a host that is not a silo.</param>
    /// <param name="maintainer">This silo's compiled app-registry snapshot maintainer.</param>
    /// <param name="timeProvider">The clock the lease and retry delays are measured on.</param>
    /// <param name="logger">The logger for renewal failures.</param>
    /// <exception cref="ArgumentNullException"><paramref name="maintainer"/>, <paramref name="timeProvider"/> or <paramref name="logger"/> is <c>null</c>.</exception>
    /// <remarks>Without a grain factory or a local silo the subscription never leases, and evaluations read the epoch.</remarks>
    public AppRegistryEpochSubscription(
        IGrainFactory? grainFactory,
        ILocalSiloDetails? localSilo,
        CompiledAppRegistrySnapshotMaintainer maintainer,
        TimeProvider timeProvider,
        ILogger<AppRegistryEpochSubscription> logger)
    {
        ArgumentNullException.ThrowIfNull(maintainer);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _siloAddress = localSilo?.SiloAddress;
        _maintainer = maintainer;
        _time = timeProvider;
        _logger = logger;
    }

    /// <summary>The lease-renewal loop, exposed so a test can await it after <see cref="StopAsync"/>.</summary>
    internal Task LeaseLoop => _leaseLoop;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_maintainer.HasClusterEpoch || _grainFactory is not { } grainFactory || _siloAddress is not { } silo)
        {
            return Task.CompletedTask;
        }

        _reference = grainFactory.CreateObjectReference<IAppRegistryEpochObserver>(this);
        _leaseLoop = LeaseLoopAsync(grainFactory, silo, _reference, _stopping.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        try
        {
            await _leaseLoop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }

        if (_reference is { } reference && _grainFactory is { } grainFactory)
        {
            grainFactory.DeleteObjectReference<IAppRegistryEpochObserver>(reference);
            _reference = null;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Marks the snapshot out of date and schedules its rebuild synchronously, so the completed task is a
    /// true acknowledgement to the epoch grain.
    /// </remarks>
    public Task OnEpochAdvancedAsync(AppRegistryEpoch epoch)
    {
        _maintainer.ObserveEpoch(epoch);
        return Task.CompletedTask;
    }

    private async Task LeaseLoopAsync(
        IGrainFactory grainFactory,
        SiloAddress silo,
        IAppRegistryEpochObserver reference,
        CancellationToken cancellationToken)
    {
        var grain = grainFactory.GetGrain<IAppRegistryEpochGrain>(IAppRegistryEpochGrain.GrainKey);
        while (!cancellationToken.IsCancellationRequested)
        {
            var delay = MaxRetryDelay;
            var requestedAt = _time.GetTimestamp();
            try
            {
                var lease = await grain.LeaseAsync(reference, silo).WaitAsync(cancellationToken).ConfigureAwait(false);
                _maintainer.ApplyLease(lease, requestedAt);
                delay = lease.Duration / 3;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    ex,
                    "Renewing the app-registry snapshot lease failed; retrying. Grant evaluations read the cluster epoch while the lease is lapsed.");
            }

            try
            {
                await Task.Delay(delay, _time, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
