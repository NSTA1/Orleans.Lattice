using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

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
/// <para>
/// It also watches cluster membership (#6216). A silo newly declared dead may have committed a registry
/// write and stopped before publishing it, which leaves the epoch unchanged, so the snapshot is invalidated
/// (<see cref="CompiledAppRegistrySnapshotMaintainer.InvalidateClusterView"/>) and rebuilt. Membership can
/// also declare a live silo dead, and that silo can still commit a write before it learns of its death, after
/// the one invalidation the declaration triggered. So each newly-dead silo opens a false-death horizon - the
/// membership table refresh plus the probe timeout, never shorter than one lease - during which this silo
/// re-invalidates the snapshot every lease, until an invalidation lands at or after the horizon's end. One
/// loop serves every death. The guarantee assumes a silo falsely declared dead stops within the horizon.
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
    private readonly IClusterMembershipService? _membership;
    private readonly TimeSpan _reinvalidationInterval;
    private readonly TimeSpan _falseDeathHorizon;
    private readonly CancellationTokenSource _stopping = new();

    private IAppRegistryEpochObserver? _reference;
    private Task _leaseLoop = Task.CompletedTask;
    private Task _membershipLoop = Task.CompletedTask;

    // The false-death horizon: the timestamp (on _time) until which the snapshot is
    // re-invalidated, and the single re-invalidation loop (0 idle, 1 running).
    private long _horizonEnd;
    private int _horizonLoopState;
    private Task _horizonLoop = Task.CompletedTask;

    /// <summary>Initializes the subscription.</summary>
    /// <param name="grainFactory">The silo's grain factory, or null on a host with no cluster.</param>
    /// <param name="localSilo">This silo's details, whose address identifies it to the epoch grain, or null on a host that is not a silo.</param>
    /// <param name="maintainer">This silo's compiled app-registry snapshot maintainer.</param>
    /// <param name="timeProvider">The clock the lease and retry delays are measured on.</param>
    /// <param name="logger">The logger for renewal failures.</param>
    /// <param name="membership">Cluster membership, watched for silos declared dead; null on a host that is not a silo.</param>
    /// <param name="membershipOptions">Orleans cluster membership options the false-death horizon is derived from; Orleans' defaults when null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="maintainer"/>, <paramref name="timeProvider"/> or <paramref name="logger"/> is <c>null</c>.</exception>
    /// <remarks>Without a grain factory or a local silo the subscription never leases, and evaluations read the epoch.</remarks>
    public AppRegistryEpochSubscription(
        IGrainFactory? grainFactory,
        ILocalSiloDetails? localSilo,
        CompiledAppRegistrySnapshotMaintainer maintainer,
        TimeProvider timeProvider,
        ILogger<AppRegistryEpochSubscription> logger,
        IClusterMembershipService? membership = null,
        IOptions<ClusterMembershipOptions>? membershipOptions = null)
    {
        ArgumentNullException.ThrowIfNull(maintainer);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _siloAddress = localSilo?.SiloAddress;
        _maintainer = maintainer;
        _time = timeProvider;
        _logger = logger;
        _membership = membership;
        _reinvalidationInterval = AppRegistryEpochLedger<IAppRegistryEpochObserver>.DefaultLeaseDuration;
        _falseDeathHorizon = EffectiveFalseDeathHorizon(membershipOptions?.Value ?? new ClusterMembershipOptions(), _reinvalidationInterval);
    }

    /// <summary>The membership-watch loop, exposed so a test can await it after <see cref="StopAsync"/>.</summary>
    internal Task MembershipLoop => _membershipLoop;

    /// <summary>The false-death re-invalidation loop, exposed so a test can await it.</summary>
    internal Task HorizonLoop => Volatile.Read(ref _horizonLoop);

    /// <summary>
    /// The false-death horizon: the membership table refresh plus the probe timeout, never shorter than
    /// <paramref name="interval"/>.
    /// </summary>
    /// <param name="membership">The Orleans cluster membership options.</param>
    /// <param name="interval">The re-invalidation interval.</param>
    /// <returns>How long a newly-dead silo keeps this silo re-invalidating.</returns>
    internal static TimeSpan EffectiveFalseDeathHorizon(ClusterMembershipOptions membership, TimeSpan interval)
    {
        ArgumentNullException.ThrowIfNull(membership);
        var horizon = membership.TableRefreshTimeout + membership.ProbeTimeout;
        return horizon < interval ? interval : horizon;
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
        if (_membership is { } membership)
        {
            _membershipLoop = WatchMembershipAsync(membership, _stopping.Token);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        foreach (var loop in (Task[])[_leaseLoop, _membershipLoop, HorizonLoop])
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

    private async Task WatchMembershipAsync(IClusterMembershipService membership, CancellationToken cancellationToken)
    {
        HashSet<SiloAddress>? dead = null;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await foreach (var snapshot in membership.MembershipUpdates.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    var newlyDead = false;
                    var current = new HashSet<SiloAddress>();
                    foreach (var member in snapshot.Members.Values)
                    {
                        if (member.Status == SiloStatus.Dead)
                        {
                            current.Add(member.SiloAddress);
                            newlyDead |= dead is not null && !dead.Contains(member.SiloAddress);
                        }
                    }

                    // The first snapshot is the baseline: silos already dead before this
                    // silo started cannot have a write this silo's first build missed.
                    dead = current;
                    if (newlyDead)
                    {
                        _maintainer.InvalidateClusterView();
                        OpenFalseDeathHorizon();
                    }
                }

                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // A silo that missed membership updates may have missed a death, so treat the gap as one.
                _logger.LogDebug(ex, "Watching cluster membership for the app-registry snapshot failed; retrying.");
                _maintainer.InvalidateClusterView();
                OpenFalseDeathHorizon();
            }

            try
            {
                await Task.Delay(MaxRetryDelay, _time, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Extends the false-death horizon to now plus its length and starts the re-invalidation loop when it
    /// is idle.
    /// </summary>
    private void OpenFalseDeathHorizon()
    {
        var end = AppRegistryEpochTimestamps.Add(_time, _time.GetTimestamp(), _falseDeathHorizon);
        var current = Interlocked.Read(ref _horizonEnd);
        while (end > current)
        {
            var observed = Interlocked.CompareExchange(ref _horizonEnd, end, current);
            if (observed == current)
            {
                break;
            }

            current = observed;
        }

        if (Interlocked.CompareExchange(ref _horizonLoopState, 1, 0) == 0)
        {
            Volatile.Write(ref _horizonLoop, ReinvalidateUntilHorizonAsync(_stopping.Token));
        }
    }

    /// <summary>
    /// Re-invalidates the snapshot each interval until one invalidation lands at or after the horizon's
    /// end, so a write committed inside the horizon is followed by an invalidation within one interval.
    /// </summary>
    private async Task ReinvalidateUntilHorizonAsync(CancellationToken cancellationToken)
    {
        // The next delay is armed before each invalidation, so the cadence never drifts by its cost.
        var next = Task.Delay(_reinvalidationInterval, _time, cancellationToken);
        while (true)
        {
            try
            {
                await next.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Volatile.Write(ref _horizonLoopState, 0);
                return;
            }

            if (_time.GetTimestamp() < Interlocked.Read(ref _horizonEnd))
            {
                next = Task.Delay(_reinvalidationInterval, _time, cancellationToken);
                _maintainer.InvalidateClusterView();
                continue;
            }

            _maintainer.InvalidateClusterView();

            // Go idle, then re-check so a death recorded after the read above is not left without a loop.
            Volatile.Write(ref _horizonLoopState, 0);
            if (_time.GetTimestamp() >= Interlocked.Read(ref _horizonEnd)
                || Interlocked.CompareExchange(ref _horizonLoopState, 1, 0) != 0)
            {
                return;
            }

            next = Task.Delay(_reinvalidationInterval, _time, cancellationToken);
        }
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
