namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The cross-silo currency state one per-silo tenant-registry snapshot keeps
/// (issues #4030, #4051, #4052): the latest cluster <see cref="TenantPolicyEpoch"/>
/// the silo has observed, a generation counter bumped whenever the snapshot may be
/// out of date, and the silo's lease from the tenant-policy epoch grain. A snapshot
/// built for the current generation, while the lease is live, reflects every
/// committed tenant-registry write; anything else is not authoritative.
/// </summary>
/// <remarks>
/// <para>
/// Composed by <see cref="CompiledTenantPolicySnapshotMaintainer"/>,
/// <see cref="TenantResidencySnapshotMaintainer"/> and
/// <see cref="TenantPlacementSnapshotMaintainer"/>, each of which records the
/// generation its current snapshot was built for (read from
/// <see cref="Generation"/> before the scan starts) and asks
/// <see cref="IsCurrent"/> on its hot path.
/// </para>
/// <para>
/// On the system clock the lease is measured on <see cref="Environment.TickCount64"/>,
/// which is far cheaper to read than a high-resolution timestamp; its coarseness
/// is absorbed by expiring the lease <see cref="CoarseClockAllowanceMilliseconds"/>
/// early, so it can only lapse early, never late. Any other
/// <see cref="TimeProvider"/> (a test's fake clock) is read precisely.
/// </para>
/// </remarks>
internal sealed class TenantSnapshotCurrency
{
    /// <summary>
    /// How much earlier than its true deadline a lease measured on the coarse
    /// system tick count lapses: two tick periods (the Windows tick is about
    /// 15.6ms), covering the tick lag on both the reading that sets the deadline
    /// and the reading that checks it.
    /// </summary>
    internal const long CoarseClockAllowanceMilliseconds = 32;

    private readonly TimeProvider _time;
    private readonly bool _systemClock;
    private readonly Lock _gate = new();
    private readonly TaskCompletionSource _leaseEstablished = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private TenantPolicyEpoch _knownEpoch;
    private long _generation;

    // The instant until which the lease is live, on the lease clock (see
    // LeaseClockNow); zero until the first lease.
    private long _leaseDeadline;

    /// <summary>Initializes the currency state on <paramref name="timeProvider"/>.</summary>
    /// <param name="timeProvider">The clock the lease is measured on.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is <c>null</c>.</exception>
    public TenantSnapshotCurrency(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _time = timeProvider;
        _systemClock = ReferenceEquals(timeProvider, TimeProvider.System);
    }

    /// <summary>The clock the lease is measured on.</summary>
    public TimeProvider Time => _time;

    /// <summary>
    /// The current generation. A snapshot built for an older generation is out of
    /// date. Read it before scanning the registry and record it with the result.
    /// </summary>
    public long Generation => Volatile.Read(ref _generation);

    /// <summary>Completes when the first lease has been applied.</summary>
    public Task LeaseEstablished => _leaseEstablished.Task;

    /// <summary>
    /// <c>true</c> when a snapshot built for <paramref name="builtForGeneration"/> is
    /// current: no change has been observed since it was built, and the lease is
    /// live. A handful of field reads and one clock read; allocates nothing.
    /// </summary>
    /// <param name="builtForGeneration">The generation the snapshot was built for.</param>
    /// <returns><c>true</c> when the snapshot can be trusted.</returns>
    public bool IsCurrent(long builtForGeneration) =>
        builtForGeneration == Volatile.Read(ref _generation)
        && LeaseClockNow() < Volatile.Read(ref _leaseDeadline);

    /// <summary>
    /// Records an observed cluster epoch. Returns <c>true</c> (and bumps the
    /// generation) when it supersedes the latest epoch observed, in which case the
    /// caller must rebuild; an equal or older epoch is ignored.
    /// </summary>
    /// <param name="epoch">The observed cluster epoch.</param>
    /// <returns><c>true</c> when the snapshot is now out of date.</returns>
    public bool Observe(TenantPolicyEpoch epoch)
    {
        lock (_gate)
        {
            if (!epoch.Supersedes(_knownEpoch))
            {
                return false;
            }

            _knownEpoch = epoch;
            Interlocked.Increment(ref _generation);
            return true;
        }
    }

    /// <summary>
    /// Applies a lease: observes its epoch and extends the lease to
    /// <paramref name="requestedAt"/> plus its duration, never shortening an
    /// existing deadline. Returns <c>true</c> when the lease's epoch made the
    /// snapshot out of date.
    /// </summary>
    /// <param name="lease">The granted lease.</param>
    /// <param name="requestedAt">The <see cref="TimeProvider"/> timestamp taken before the lease was requested.</param>
    /// <returns><c>true</c> when the caller must rebuild.</returns>
    public bool ApplyLease(TenantPolicyEpochLease lease, long requestedAt)
    {
        var superseded = Observe(lease.Epoch);

        long deadline;
        if (_systemClock)
        {
            // Re-express "requestedAt + duration" on the tick-count clock: subtract
            // the (rounded-up) time since the request from the current tick, add the
            // duration, and take off the coarse-clock allowance so the lease can
            // only lapse early.
            var sinceRequest = (long)Math.Ceiling(_time.GetElapsedTime(requestedAt).TotalMilliseconds);
            deadline = Environment.TickCount64 - sinceRequest
                + (long)lease.Duration.TotalMilliseconds
                - CoarseClockAllowanceMilliseconds;
        }
        else
        {
            deadline = TenantPolicyTimestamps.Add(_time, requestedAt, lease.Duration);
        }

        var current = Volatile.Read(ref _leaseDeadline);
        while (deadline > current)
        {
            var observed = Interlocked.CompareExchange(ref _leaseDeadline, deadline, current);
            if (observed == current)
            {
                break;
            }

            current = observed;
        }

        _leaseEstablished.TrySetResult();
        return superseded;
    }

    /// <summary>Marks every snapshot built so far as out of date, without a new epoch.</summary>
    public void Invalidate() => Interlocked.Increment(ref _generation);

    /// <summary>
    /// The current instant on the lease clock: <see cref="Environment.TickCount64"/>
    /// (milliseconds) for the system clock, otherwise the provider's timestamp.
    /// </summary>
    private long LeaseClockNow() => _systemClock ? Environment.TickCount64 : _time.GetTimestamp();
}
