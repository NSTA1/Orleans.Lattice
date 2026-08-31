using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// A fully virtual <see cref="TimeProvider"/> for testing scheduled background
/// work without any dependence on the wall clock.
/// <para>
/// Time only moves when a test calls <see cref="Advance"/>, and a timer only
/// fires when virtual time reaches its due point, so the ordering of every
/// scheduled callback is decided by the test rather than by thread-pool
/// latency. <c>Task.Delay(delay, provider, token)</c> schedules its completion
/// through this provider, so a component under test that delays through an
/// injected <see cref="TimeProvider"/> is driven entirely from here.
/// </para>
/// <para>
/// A scheduled due time is recorded from <b>both</b> <see cref="CreateTimer"/>
/// and <see cref="ITimer.Change"/>: <c>Task.Delay</c> creates its timer
/// <i>disarmed</i> (an infinite due time) and arms it with a separate
/// <c>Change</c> call, so a provider that only watched <c>CreateTimer</c> would
/// see no delays at all.
/// </para>
/// <para>
/// <see cref="NextTimerAsync"/> closes the one remaining race: after a timer
/// fires, the awaiting continuation resumes on the thread pool, so a test must
/// know when the component has come back round and armed its next delay. Arm it
/// <i>before</i> advancing and await it afterwards, and the wait is on a logical
/// event ("the component scheduled its next delay") rather than on an elapsed
/// duration.
/// </para>
/// </summary>
internal sealed class VirtualTimeProvider : TimeProvider
{
    private readonly object _gate = new();
    private readonly List<VirtualTimer> _timers = [];
    private readonly List<TimeSpan> _scheduled = [];
    private DateTimeOffset _now;
    private TaskCompletionSource _nextTimer = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Creates a provider whose virtual clock starts at <paramref name="start"/>.</summary>
    /// <param name="start">The initial virtual time.</param>
    public VirtualTimeProvider(DateTimeOffset start) => _now = start;

    /// <summary>Creates a provider whose virtual clock starts at a fixed, arbitrary instant.</summary>
    public VirtualTimeProvider()
        : this(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
    {
    }

    /// <summary>
    /// Every finite due time armed on this provider, in arming order. For a
    /// component that delays through <c>Task.Delay(..., TimeProvider, ...)</c>
    /// this is the exact sequence of waits it asked for.
    /// </summary>
    public IReadOnlyList<TimeSpan> ScheduledDelays
    {
        get { lock (_gate) { return _scheduled.ToArray(); } }
    }

    /// <summary>The number of finite due times armed so far.</summary>
    public int TimersCreated
    {
        get { lock (_gate) { return _scheduled.Count; } }
    }

    /// <summary>The most recently armed due time.</summary>
    public TimeSpan LastScheduledDelay
    {
        get { lock (_gate) { return _scheduled[^1]; } }
    }

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate) { return _now; }
    }

    /// <summary>
    /// Arms and returns a task that completes when the next finite due time is
    /// armed. Call it before <see cref="Advance"/> so the completion cannot be
    /// missed, then await it to know the component has parked again.
    /// </summary>
    /// <returns>A task completing when the next delay is scheduled.</returns>
    public Task NextTimerAsync()
    {
        lock (_gate)
        {
            _nextTimer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _nextTimer.Task;
        }
    }

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);

        var timer = new VirtualTimer(callback, state, this);
        lock (_gate)
        {
            _timers.Add(timer);
        }

        timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>
    /// Advances virtual time by <paramref name="by"/> and fires every timer that
    /// has come due. Advancing by <see cref="TimeSpan.Zero"/> fires anything
    /// already due without moving the clock.
    /// </summary>
    /// <param name="by">How far to advance the virtual clock.</param>
    public void Advance(TimeSpan by)
    {
        List<VirtualTimer> due = [];
        lock (_gate)
        {
            _now += by;
            foreach (var timer in _timers)
            {
                if (timer.IsDue(_now))
                {
                    due.Add(timer);
                }
            }
        }

        foreach (var timer in due)
        {
            timer.Fire();
        }
    }

    private void OnArmed(TimeSpan dueTime)
    {
        TaskCompletionSource signal;
        lock (_gate)
        {
            _scheduled.Add(dueTime);
            signal = _nextTimer;
        }

        signal.TrySetResult();
    }

    private void Remove(VirtualTimer timer)
    {
        lock (_gate) { _timers.Remove(timer); }
    }

    private sealed class VirtualTimer(TimerCallback callback, object? state, VirtualTimeProvider owner) : ITimer
    {
        private readonly object _timerGate = new();
        private TimerCallback? _callback = callback;
        private DateTimeOffset? _dueAt;

        /// <summary>Whether the timer is armed and its due point has been reached.</summary>
        public bool IsDue(DateTimeOffset now)
        {
            lock (_timerGate) { return _dueAt is { } due && due <= now; }
        }

        public void Fire()
        {
            TimerCallback? callbackToRun;
            lock (_timerGate)
            {
                if (_dueAt is null)
                {
                    return;
                }

                // One-shot: disarm before invoking so a re-entrant Advance from
                // the callback cannot fire the same due point twice.
                _dueAt = null;
                callbackToRun = _callback;
            }

            callbackToRun?.Invoke(state);
        }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            if (dueTime == Timeout.InfiniteTimeSpan)
            {
                lock (_timerGate) { _dueAt = null; }
                return true;
            }

            // Read the clock before taking the timer lock: the owner's lock is
            // always acquired before a timer's, and inverting that here would
            // deadlock against a concurrent Advance.
            var armAt = owner.GetUtcNow() + dueTime;
            lock (_timerGate) { _dueAt = armAt; }
            owner.OnArmed(dueTime);
            return true;
        }

        [SuppressMessage("Usage", "CA1816", Justification = "Test double; there is no finalizer to suppress.")]
        public void Dispose()
        {
            lock (_timerGate)
            {
                _dueAt = null;
                _callback = null;
            }

            owner.Remove(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
