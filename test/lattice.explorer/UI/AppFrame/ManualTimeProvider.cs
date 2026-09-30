namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// A <see cref="TimeProvider"/> whose clock moves only when a test calls <see cref="Advance"/>.
/// Timers fire synchronously inside <see cref="Advance"/>, so nothing in a test waits on
/// real time.
/// </summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private readonly List<ManualTimer> _timers = [];
    private long _ticks = TimeSpan.TicksPerDay;

    /// <inheritdoc />
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <summary>The number of timers created and not disposed.</summary>
    public int ActiveTimers => _timers.Count(timer => !timer.Disposed);

    /// <inheritdoc />
    public override long GetTimestamp() => _ticks;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => new(_ticks, TimeSpan.Zero);

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ManualTimer(this, callback, state, dueTime);
        _timers.Add(timer);
        return timer;
    }

    /// <summary>Moves the clock forward and fires every timer that falls due.</summary>
    /// <param name="by">How far to move.</param>
    public void Advance(TimeSpan by)
    {
        _ticks += by.Ticks;
        foreach (var timer in _timers.ToArray())
        {
            if (!timer.Disposed && timer.DueAt is { } due && due <= _ticks)
            {
                timer.DueAt = null;
                timer.Callback(timer.State);
            }
        }
    }

    private sealed class ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state, TimeSpan dueTime) : ITimer
    {
        public TimerCallback Callback { get; } = callback;

        public object? State { get; } = state;

        public long? DueAt { get; set; } = dueTime == Timeout.InfiniteTimeSpan ? null : owner._ticks + dueTime.Ticks;

        public bool Disposed { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            DueAt = dueTime == Timeout.InfiniteTimeSpan ? null : owner._ticks + dueTime.Ticks;
            return !Disposed;
        }

        public void Dispose() => Disposed = true;

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
