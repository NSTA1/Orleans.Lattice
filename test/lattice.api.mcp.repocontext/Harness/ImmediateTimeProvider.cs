namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A <see cref="TimeProvider"/> whose timers fire as soon as they are created, so a
/// test can drive a background service's delay-and-retry loop to completion without
/// waiting out its real cadence. Wall-clock reads still come from the system clock.
/// </summary>
internal sealed class ImmediateTimeProvider : TimeProvider
{
    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        return new ImmediateTimer(callback, state);
    }

    /// <summary>A timer that has already elapsed by the time it is handed back.</summary>
    private sealed class ImmediateTimer : ITimer
    {
        public ImmediateTimer(TimerCallback callback, object? state) => callback(state);

        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
