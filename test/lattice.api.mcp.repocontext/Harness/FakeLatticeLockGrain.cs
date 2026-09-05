namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A deterministic, in-process stand-in for <see cref="ILatticeLockGrain"/>.
/// <para>
/// It reproduces exactly the properties the claim surface leans on: mutual
/// exclusion, a strictly increasing fencing token that is never reused, a stale
/// renew that faults with <see cref="LatticeLockConflictException"/>, and a stale
/// release that is a silent no-op. It reproduces none of the FIFO queuing, because
/// the claim surface only ever observes the queue through
/// <see cref="LockStatus.QueueDepth"/>.
/// </para>
/// <para>
/// There is no timer and no clock-driven expiry. A test that needs a lapsed lease
/// calls <see cref="ExpireLease"/>, so lease-expiry coverage asserts on a stated
/// transition rather than on elapsed wall-clock time and can never flake.
/// </para>
/// </summary>
internal sealed class FakeLatticeLockGrain : ILatticeLockGrain
{
    private static readonly TimeSpan DefaultLease = TimeSpan.FromSeconds(30);

    private long _nextToken;
    private long _currentToken;
    private bool _held;
    private DateTimeOffset _expiresAt;
    private TimeSpan _leaseDuration;

    /// <summary>The number of waiters this lock reports through <see cref="GetStatusAsync"/>.</summary>
    public int QueueDepth { get; set; }

    /// <summary>
    /// Whether a contended <see cref="AcquireAsync"/> faults with
    /// <see cref="TimeoutException"/> rather than waiting. Always <see langword="true"/>
    /// in the unit lane: a queued acquire that completes from a later turn cannot be
    /// modelled without a scheduler, and the surface treats a timeout as the
    /// contended outcome anyway.
    /// </summary>
    public bool TimesOutWhenContended => true;

    /// <summary>Whether the lock is currently held.</summary>
    public bool IsHeld => _held;

    /// <summary>The fencing token of the most recent grant.</summary>
    public long CurrentFencingToken => _currentToken;

    /// <summary>
    /// Drops the current grant as the lock's own expiry reclaim would, without
    /// waiting for a lease to elapse. The next grant still mints a strictly higher
    /// token, which is what fences the lapsed holder out.
    /// </summary>
    public void ExpireLease() => _held = false;

    /// <inheritdoc />
    public Task<LockLease> AcquireAsync(LockAcquireRequest request)
        => _held
            ? Task.FromException<LockLease>(
                new TimeoutException($"The lock was not granted within {request.MaxWait}."))
            : Task.FromResult(Grant(request.LeaseDuration));

    /// <inheritdoc />
    public Task<LockLease?> TryAcquireAsync(TimeSpan leaseDuration)
        => Task.FromResult<LockLease?>(_held ? null : Grant(leaseDuration));

    /// <inheritdoc />
    public Task<LockLease> RenewAsync(LockToken token, TimeSpan leaseDuration)
    {
        if (!_held || token.FencingToken != _currentToken)
        {
            return Task.FromException<LockLease>(new LatticeLockConflictException(
                $"Fencing token {token.FencingToken} is stale; the current token is {_currentToken}."));
        }

        _leaseDuration = Normalise(leaseDuration);
        _expiresAt = DateTimeOffset.UtcNow.Add(_leaseDuration);
        return Task.FromResult(new LockLease(new LockToken(_currentToken), _expiresAt, _leaseDuration));
    }

    /// <inheritdoc />
    public Task ReleaseAsync(LockToken token)
    {
        if (_held && token.FencingToken == _currentToken)
        {
            _held = false;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<LockStatus> GetStatusAsync()
        => Task.FromResult(new LockStatus(
            _held, _currentToken, _held ? _expiresAt : null, QueueDepth));

    private LockLease Grant(TimeSpan leaseDuration)
    {
        _currentToken = ++_nextToken;
        _held = true;
        _leaseDuration = Normalise(leaseDuration);
        _expiresAt = DateTimeOffset.UtcNow.Add(_leaseDuration);
        return new LockLease(new LockToken(_currentToken), _expiresAt, _leaseDuration);
    }

    private static TimeSpan Normalise(TimeSpan requested)
        => requested > TimeSpan.Zero ? requested : DefaultLease;
}
