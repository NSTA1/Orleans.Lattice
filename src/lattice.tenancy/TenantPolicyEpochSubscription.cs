using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The silo-hosted half of the cross-silo tenant-policy currency protocol (issue
/// #4030, #4051, #4052). It keeps every per-silo tenant-registry snapshot (each
/// <see cref="ITenantEpochSubscriber"/>: the compiled tenant-policy, residency and
/// placement snapshots) leased by, and subscribed to, the cluster-wide
/// <see cref="ITenantPolicyEpochGrain"/>; applies every epoch the grain pushes to
/// each of them; treats any silo declared dead by cluster membership as a possible
/// unpublished registry write; and warms every snapshot at start-up so a cold silo
/// does not report registered tenants as unregistered.
/// </summary>
/// <remarks>
/// <para>
/// Every loop is launched fire-and-forget from <see cref="StartAsync"/> and retries
/// a failure on a short bounded cadence, so silo start-up never blocks on a cluster
/// round-trip. Until the first lease lands, and whenever renewal keeps failing past
/// the lease, the snapshot is simply not authoritative: consumers confirm against
/// the registry or deny. Losing this service therefore fails closed.
/// </para>
/// <para>
/// The lease is renewed every third of its duration, so one lost renewal does not
/// cost the silo its authority. The deadline is measured from the moment each
/// request was sent (see <see cref="CompiledTenantPolicySnapshotMaintainer.ApplyLease"/>).
/// </para>
/// </remarks>
internal sealed class TenantPolicyEpochSubscription : IHostedService, ITenantPolicyEpochObserver
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IGrainFactory _grainFactory;
    private readonly ITenantEpochSubscriber[] _snapshots;
    private readonly IClusterMembershipService _membership;
    private readonly SiloAddress _siloAddress;
    private readonly TimeProvider _time;
    private readonly TimeSpan _leaseDuration;
    private readonly ILogger<TenantPolicyEpochSubscription> _logger;
    private readonly CancellationTokenSource _stopping = new();

    private ITenantPolicyEpochObserver? _reference;
    private Task _leaseLoop = Task.CompletedTask;
    private Task _membershipLoop = Task.CompletedTask;
    private Task _warmup = Task.CompletedTask;

    /// <summary>Initializes the subscription.</summary>
    /// <param name="grainFactory">The silo's grain factory.</param>
    /// <param name="snapshots">This silo's tenant-registry snapshots kept current by the epoch.</param>
    /// <param name="membership">Cluster membership, watched for silos declared dead.</param>
    /// <param name="localSilo">This silo's details, whose address identifies it to the epoch grain.</param>
    /// <param name="timeProvider">The clock the lease and retry delays are measured on.</param>
    /// <param name="options">The tenancy options carrying the lease duration.</param>
    /// <param name="logger">The logger for renewal and warm-up failures.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantPolicyEpochSubscription(
        IGrainFactory grainFactory,
        IEnumerable<ITenantEpochSubscriber> snapshots,
        IClusterMembershipService membership,
        ILocalSiloDetails localSilo,
        TimeProvider timeProvider,
        IOptions<LatticeTenancyOptions> options,
        ILogger<TenantPolicyEpochSubscription> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(snapshots);
        ArgumentNullException.ThrowIfNull(membership);
        ArgumentNullException.ThrowIfNull(localSilo);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _snapshots = [.. snapshots];
        _membership = membership;
        _siloAddress = localSilo.SiloAddress;
        _time = timeProvider;
        _leaseDuration = options.Value.PolicySnapshotLeaseDuration;
        _logger = logger;
    }

    /// <summary>The lease-renewal loop, exposed so a test can await it after <see cref="StopAsync"/>.</summary>
    internal Task LeaseLoop => _leaseLoop;

    /// <summary>The membership-watch loop, exposed so a test can await it after <see cref="StopAsync"/>.</summary>
    internal Task MembershipLoop => _membershipLoop;

    /// <summary>The start-up warm-up, exposed so a test can await it.</summary>
    internal Task Warmup => _warmup;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _reference = _grainFactory.CreateObjectReference<ITenantPolicyEpochObserver>(this);
        var stopping = _stopping.Token;
        _warmup = WarmAsync(stopping);
        _leaseLoop = LeaseLoopAsync(_reference, stopping);
        _membershipLoop = WatchMembershipAsync(stopping);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        foreach (var loop in (Task[])[_warmup, _leaseLoop, _membershipLoop])
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

        if (_reference is { } reference)
        {
            _grainFactory.DeleteObjectReference<ITenantPolicyEpochObserver>(reference);
            _reference = null;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Marks every snapshot out of date and schedules its rebuild synchronously, so
    /// the completed task is a true acknowledgement to the epoch grain.
    /// </remarks>
    public Task OnEpochAdvancedAsync(TenantPolicyEpoch epoch)
    {
        foreach (var snapshot in _snapshots)
        {
            snapshot.ObserveEpoch(epoch);
        }

        return Task.CompletedTask;
    }

    private async Task WarmAsync(CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromMilliseconds(250);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                foreach (var snapshot in _snapshots)
                {
                    await snapshot.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
                }

                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Tenant-policy snapshot warm-up failed; retrying.");
            }

            if (!await DelayAsync(delay, cancellationToken).ConfigureAwait(false))
            {
                return;
            }
        }
    }

    private async Task LeaseLoopAsync(ITenantPolicyEpochObserver reference, CancellationToken cancellationToken)
    {
        var retryDelay = _leaseDuration / 10 > MaxRetryDelay ? MaxRetryDelay : _leaseDuration / 10;
        var grain = _grainFactory.GetGrain<ITenantPolicyEpochGrain>(ITenantPolicyEpochGrain.Key);
        while (!cancellationToken.IsCancellationRequested)
        {
            var delay = retryDelay;
            var requestedAt = _time.GetTimestamp();
            try
            {
                var lease = await grain.LeaseAsync(reference, _siloAddress).WaitAsync(cancellationToken).ConfigureAwait(false);
                foreach (var snapshot in _snapshots)
                {
                    snapshot.ApplyLease(lease, requestedAt);
                }

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
                    "Renewing the tenant-policy snapshot lease failed; retrying. Cross-tenant crossings are confirmed against the registry while the lease is lapsed.");
            }

            if (!await DelayAsync(delay, cancellationToken).ConfigureAwait(false))
            {
                return;
            }
        }
    }

    private async Task WatchMembershipAsync(CancellationToken cancellationToken)
    {
        HashSet<SiloAddress>? dead = null;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await foreach (var snapshot in _membership.MembershipUpdates.WithCancellation(cancellationToken).ConfigureAwait(false))
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
                        InvalidateAll();
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
                // A silo that missed membership updates may have missed a death, so
                // treat the gap as one.
                _logger.LogDebug(ex, "Watching cluster membership for the tenant-policy snapshot failed; retrying.");
                InvalidateAll();
            }

            if (!await DelayAsync(MaxRetryDelay, cancellationToken).ConfigureAwait(false))
            {
                return;
            }
        }
    }

    private void InvalidateAll()
    {
        foreach (var snapshot in _snapshots)
        {
            snapshot.InvalidateClusterView();
        }
    }

    private async Task<bool> DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, _time, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
