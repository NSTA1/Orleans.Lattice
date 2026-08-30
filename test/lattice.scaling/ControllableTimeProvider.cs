namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// A <see cref="TimeProvider"/> whose timers never fire on their own: a test
/// fires them explicitly. This makes the periodic sampling loop in
/// <see cref="LatticeScalingSignal"/> testable without a sleep or a wall-clock
/// race - the tick happens exactly when the test says it does.
/// </summary>
internal sealed class ControllableTimeProvider(DateTimeOffset start) : TimeProvider
{
    private readonly List<ManualTimer> _timers = new();
    private readonly TaskCompletionSource _timerCreated =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The current UTC instant this provider reports.</summary>
    public DateTimeOffset Now { get; set; } = start;

    /// <summary>Completes as soon as the first timer has been created.</summary>
    public Task TimerCreated => _timerCreated.Task;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => Now;

    /// <summary>Advances the reported clock by <paramref name="delta"/>.</summary>
    public void Advance(TimeSpan delta) => Now += delta;

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ManualTimer(callback, state);
        lock (_timers)
        {
            _timers.Add(timer);
        }

        _timerCreated.TrySetResult();
        return timer;
    }

    /// <summary>Invokes the callback of every timer created so far.</summary>
    public void FireAll()
    {
        ManualTimer[] snapshot;
        lock (_timers)
        {
            snapshot = _timers.ToArray();
        }

        foreach (var timer in snapshot)
        {
            timer.Fire();
        }
    }

    private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
    {
        private bool _disposed;

        public void Fire()
        {
            if (!_disposed)
            {
                callback(state);
            }
        }

        public bool Change(TimeSpan dueTime, TimeSpan period) => !_disposed;

        public void Dispose() => _disposed = true;

        public ValueTask DisposeAsync()
        {
            _disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
