namespace Orleans.Lattice.Testing;

/// <summary>
/// The one shared hand-driven <see cref="TimeProvider"/>: a clock that only
/// moves when a test moves it, and that fires the timers scheduled against it
/// when it does.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> Nine private copies of this idea had accumulated
/// across seven test projects under four different names (<c>FakeTimeProvider</c>,
/// <c>ManualTimeProvider</c>, <c>ManualDelayTimeProvider</c>,
/// <c>StubTimeProvider</c>), and they had drifted in exactly the way
/// <see cref="TestPoll"/> documents for its own predecessors - most override
/// <see cref="GetUtcNow"/> alone. That omission is not cosmetic: several BCL
/// primitives a test would naturally reach for schedule through
/// <see cref="CreateTimer"/> rather than reading the clock, so against a
/// clock-only fake they never fire at all. A fixture built on one does not fail
/// with a wrong time - it HANGS, and then reads as a flaky or slow test rather
/// than as a harness that cannot express what it is asserting.
/// </para>
/// <para>
/// The two that matter most here are
/// <see cref="CancellationTokenSource(TimeSpan, TimeProvider)"/> and
/// <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>, which is
/// what any test of a wall-clock budget needs.
/// </para>
/// <para>
/// <b>The timestamp overrides are load-bearing, not tidiness.</b>
/// <see cref="TimeProvider.GetTimestamp"/> defaults to
/// <see cref="System.Diagnostics.Stopwatch"/>, so a provider that overrode only
/// <see cref="GetUtcNow"/> would still measure elapsed time against the REAL
/// clock. Production code that budgets with
/// <see cref="TimeProvider.GetElapsedTime(long)"/> - the allocation-free way to
/// do it, and therefore the preferred one here - would then be entirely
/// unaffected by <see cref="Advance"/>, and the resulting test would pass or
/// fail on how long the machine actually took. Both are overridden below so that
/// every reading of time this type can be asked for moves together.
/// </para>
/// </remarks>
public sealed class ManualTimeProvider : TimeProvider
{
    private readonly object _gate = new();
    private readonly List<ManualTimer> _timers = [];
    private DateTimeOffset _utcNow;

    /// <summary>
    /// Creates a clock parked at <paramref name="start"/>, defaulting to a fixed
    /// point well clear of <see cref="DateTimeOffset.MinValue"/> so that a test
    /// which subtracts from it cannot underflow.
    /// </summary>
    /// <param name="start">The instant the clock starts at.</param>
    public ManualTimeProvider(DateTimeOffset? start = null)
        => _utcNow = start ?? new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Always UTC, so a test cannot read differently on a machine in another zone.</summary>
    public override TimeZoneInfo LocalTimeZone => TimeZoneInfo.Utc;

    /// <summary>Ticks per second, matching the units <see cref="GetTimestamp"/> returns.</summary>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate)
        {
            return _utcNow;
        }
    }

    /// <summary>
    /// The same instant as <see cref="GetUtcNow"/>, expressed in ticks, so that
    /// <see cref="TimeProvider.GetElapsedTime(long)"/> measures against this
    /// clock rather than against the machine's.
    /// </summary>
    /// <returns>The current instant in ticks.</returns>
    public override long GetTimestamp()
    {
        lock (_gate)
        {
            return _utcNow.UtcTicks;
        }
    }

    /// <summary>
    /// Moves the clock forward and fires every timer that has come due.
    /// </summary>
    /// <param name="delta">How far to move. Must not be negative.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="delta"/> is negative. Time running backwards is never what
    /// a test means, and silently allowing it would let a fixture assert against
    /// a state no deployment can reach.
    /// </exception>
    public void Advance(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);

        ManualTimer[] pending;
        DateTimeOffset now;
        lock (_gate)
        {
            _utcNow += delta;
            now = _utcNow;
            pending = [.. _timers];
        }

        // Fired outside the lock, and over a snapshot: a callback may dispose its
        // own timer or schedule another, and both take this lock.
        foreach (var timer in pending)
        {
            timer.Fire(now);
        }
    }

    /// <inheritdoc />
    public override ITimer CreateTimer(
        TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);

        var timer = new ManualTimer(this, callback, state);
        lock (_gate)
        {
            _timers.Add(timer);
        }

        timer.Change(dueTime, period);
        return timer;
    }

    private void Remove(ManualTimer timer)
    {
        lock (_gate)
        {
            _timers.Remove(timer);
        }
    }

    /// <summary>
    /// A timer that fires only when <see cref="Advance"/> carries the clock past
    /// its due time.
    /// </summary>
    private sealed class ManualTimer(ManualTimeProvider clock, TimerCallback callback, object? state)
        : ITimer
    {
        private readonly object _gate = new();
        private DateTimeOffset? _dueAt;
        private TimeSpan _period = Timeout.InfiniteTimeSpan;
        private bool _disposed;

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return false;
                }

                _dueAt = dueTime == Timeout.InfiniteTimeSpan ? null : clock.GetUtcNow() + dueTime;
                _period = period;
                return true;
            }
        }

        public void Fire(DateTimeOffset now)
        {
            lock (_gate)
            {
                if (_disposed || _dueAt is not { } due || now < due)
                {
                    return;
                }

                // Rearmed (or disarmed) BEFORE the callback runs, so a callback
                // that calls Change sets the next due time rather than having it
                // overwritten on return.
                _dueAt = _period == Timeout.InfiniteTimeSpan ? null : now + _period;
            }

            // Outside the lock: a callback commonly disposes its own timer.
            callback(state);
        }

        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
                _dueAt = null;
            }

            clock.Remove(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
