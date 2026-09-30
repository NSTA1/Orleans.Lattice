namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The host-independent state machine behind <see cref="ITenantPolicyEpochGrain"/>:
/// the current <see cref="TenantPolicyEpoch"/>, the lease table of subscribed
/// silos, and the advance protocol that guarantees no silo keeps answering from a
/// snapshot that predates a committed tenant-registry write. Kept free of Orleans
/// so the protocol is unit-testable against a fake clock.
/// </summary>
/// <typeparam name="TSubscriber">The subscriber identity (a silo's observer reference in production).</typeparam>
/// <remarks>
/// <para>
/// A silo's snapshot is authoritative only while it holds a live lease (see
/// <see cref="CompiledTenantPolicySnapshotMaintainer.IsSnapshotAuthoritative"/>).
/// <see cref="AdvanceAsync"/> bumps the version, pushes it to every leased
/// subscriber and waits for each acknowledgement. A subscriber that does not
/// acknowledge within <see cref="AckTimeout"/> is waited out: the advance does not
/// complete until that subscriber's recorded lease deadline, plus a clock-rate
/// margin, has passed, because the silo's own deadline was measured from before
/// the grant and so has expired by then. Either way, when the advance completes
/// no silo can still hold authority over a pre-advance snapshot.
/// </para>
/// <para>
/// A fresh ledger (a new activation of the grain, with a new
/// <see cref="TenantPolicyEpoch.Incarnation"/>) starts with an empty lease table,
/// yet silos may still hold unexpired leases granted by the previous incarnation.
/// An advance that pushed only to the (empty) new table would return while those
/// silos stayed authoritative on a stale snapshot. So every advance also waits out
/// a grace period of one lease plus the margin from the ledger's creation, which
/// covers any lease the previous incarnation could have granted - unless the host
/// establishes sooner that every silo that could hold such a lease has since leased
/// from this incarnation (and so has already been told its snapshot is out of date),
/// and calls <see cref="ReleaseGrace"/>.
/// </para>
/// <para>
/// The lease table and the current epoch are guarded by a lock, so leases can be
/// granted, and further advances started, while an advance is waiting.
/// </para>
/// </remarks>
internal sealed class TenantPolicyEpochLedger<TSubscriber>
    where TSubscriber : notnull
{
    /// <summary>The longest duration a timer-backed wait accepts (<c>0xFFFFFFFE</c> milliseconds).</summary>
    private static readonly TimeSpan MaxTimerDuration = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly object _gate = new();
    private readonly Dictionary<TSubscriber, long> _leases = [];
    private readonly TimeProvider _time;
    private readonly long _graceUntil;
    private readonly TaskCompletionSource _graceReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TenantPolicyEpoch _current;

    /// <summary>Initializes a ledger for a fresh incarnation.</summary>
    /// <param name="leaseDuration">The lease granted to each subscriber. Must be strictly positive.</param>
    /// <param name="time">The clock leases and waits are measured on.</param>
    /// <param name="incarnation">The identity of this incarnation.</param>
    /// <exception cref="ArgumentNullException"><paramref name="time"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="leaseDuration"/> is not strictly positive.</exception>
    public TenantPolicyEpochLedger(TimeSpan leaseDuration, TimeProvider time, Guid incarnation)
    {
        ArgumentNullException.ThrowIfNull(time);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(leaseDuration, TimeSpan.Zero);

        _time = time;
        LeaseDuration = leaseDuration;
        AckTimeout = leaseDuration / 5;
        Margin = leaseDuration / 10;
        _current = new TenantPolicyEpoch(incarnation, 0);
        _graceUntil = Add(time.GetTimestamp(), leaseDuration + Margin);
    }

    /// <summary>The current epoch.</summary>
    public TenantPolicyEpoch Current
    {
        get
        {
            lock (_gate)
            {
                return _current;
            }
        }
    }

    /// <summary>The lease granted to each subscriber.</summary>
    public TimeSpan LeaseDuration { get; }

    /// <summary>How long an advance waits for a subscriber's acknowledgement before waiting out its lease instead.</summary>
    public TimeSpan AckTimeout { get; }

    /// <summary>The clock-rate allowance added to every lease the ledger waits out.</summary>
    public TimeSpan Margin { get; }

    /// <summary><c>true</c> once the fresh-incarnation grace has been released early by <see cref="ReleaseGrace"/>.</summary>
    public bool IsGraceReleased => _graceReleased.Task.IsCompleted;

    /// <summary>
    /// Ends the fresh-incarnation grace early. Call only once every silo that could
    /// hold a lease granted by a previous incarnation has leased from this one, so no
    /// such lease can still make a silo authoritative. Idempotent.
    /// </summary>
    public void ReleaseGrace() => _graceReleased.TrySetResult();

    /// <summary>The number of subscribers currently in the lease table, expired or not.</summary>
    public int SubscriberCount
    {
        get
        {
            lock (_gate)
            {
                return _leases.Count;
            }
        }
    }

    /// <summary>
    /// Grants or renews <paramref name="subscriber"/>'s lease. A renewal never
    /// shortens a recorded deadline.
    /// </summary>
    /// <param name="subscriber">The subscriber requesting the lease.</param>
    /// <returns>The current epoch and the granted duration.</returns>
    public TenantPolicyEpochLease Lease(TSubscriber subscriber)
    {
        ArgumentNullException.ThrowIfNull(subscriber);

        var deadline = Add(_time.GetTimestamp(), LeaseDuration);
        lock (_gate)
        {
            if (!_leases.TryGetValue(subscriber, out var existing) || deadline > existing)
            {
                _leases[subscriber] = deadline;
            }

            return new TenantPolicyEpochLease(_current, LeaseDuration);
        }
    }

    /// <summary>
    /// Advances the epoch and completes once every leased subscriber has
    /// acknowledged it or had its lease expire, and once the fresh-incarnation
    /// grace period has elapsed.
    /// </summary>
    /// <param name="notify">Pushes the new epoch to one subscriber; its completion is the acknowledgement.</param>
    /// <param name="cancellationToken">Cancels the wait (the epoch has already advanced).</param>
    /// <returns>The advanced epoch.</returns>
    public async Task<TenantPolicyEpoch> AdvanceAsync(
        Func<TSubscriber, TenantPolicyEpoch, Task> notify,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(notify);

        var now = _time.GetTimestamp();
        TenantPolicyEpoch epoch;
        List<KeyValuePair<TSubscriber, long>> live;
        lock (_gate)
        {
            _current = _current with { Version = _current.Version + 1 };
            epoch = _current;

            live = new List<KeyValuePair<TSubscriber, long>>(_leases.Count);
            List<TSubscriber>? expired = null;
            foreach (var lease in _leases)
            {
                if (lease.Value > now)
                {
                    live.Add(lease);
                }
                else
                {
                    (expired ??= []).Add(lease.Key);
                }
            }

            if (expired is not null)
            {
                foreach (var subscriber in expired)
                {
                    _leases.Remove(subscriber);
                }
            }
        }

        var pending = new List<Task>(live.Count + 1);
        foreach (var lease in live)
        {
            pending.Add(DeliverAsync(lease.Key, lease.Value, epoch, notify, cancellationToken));
        }

        if (now < _graceUntil && !_graceReleased.Task.IsCompleted)
        {
            pending.Add(WaitOutGraceAsync(cancellationToken));
        }

        await Task.WhenAll(pending).ConfigureAwait(false);
        return epoch;
    }

    private async Task DeliverAsync(
        TSubscriber subscriber,
        long deadline,
        TenantPolicyEpoch epoch,
        Func<TSubscriber, TenantPolicyEpoch, Task> notify,
        CancellationToken cancellationToken)
    {
        try
        {
            await notify(subscriber, epoch).WaitAsync(AckTimeout, _time, cancellationToken).ConfigureAwait(false);
            return;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // Not acknowledged (unreachable, faulted, or timed out): the silo may
            // still believe its old snapshot is current, so wait until its lease has
            // certainly expired instead.
        }

        await WaitUntilAsync(Add(deadline, Margin), cancellationToken).ConfigureAwait(false);

        lock (_gate)
        {
            // Drop it only if it has not renewed since (a renewal after the advance
            // already carried the new epoch to it).
            if (_leases.TryGetValue(subscriber, out var current) && current == deadline)
            {
                _leases.Remove(subscriber);
            }
        }
    }

    private async Task WaitOutGraceAsync(CancellationToken cancellationToken)
    {
        using var expiry = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var grace = WaitUntilAsync(_graceUntil, expiry.Token);
        try
        {
            await Task.WhenAny(grace, _graceReleased.Task).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
        finally
        {
            await expiry.CancelAsync().ConfigureAwait(false);
        }
    }

    private Task WaitUntilAsync(long timestamp, CancellationToken cancellationToken)
    {
        var now = _time.GetTimestamp();
        if (timestamp <= now)
        {
            return Task.CompletedTask;
        }

        var remaining = _time.GetElapsedTime(now, timestamp);
        return Task.Delay(remaining > MaxTimerDuration ? MaxTimerDuration : remaining, _time, cancellationToken);
    }

    private long Add(long timestamp, TimeSpan duration) => TenantPolicyTimestamps.Add(_time, timestamp, duration);
}
