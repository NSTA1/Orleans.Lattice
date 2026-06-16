using NSubstitute;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public
/// <see cref="WalSaturationSignalExtensions.ApplyBackPressureAsync(IWalSaturationSignal, string, TimeSpan, System.Threading.CancellationToken)"/>
/// helper. Pins the canonical per-call back-pressure response shape
/// so consumers (the bench TCP reader, future ingest paths, third-
/// party consumers) all see the same Healthy/Throttled/Saturated
/// behaviour: no-op / delay / park-until-Healthy.
/// </summary>
[TestFixture]
public class WalSaturationSignalExtensionsTests
{
    private const string TreeId = "tree-bp";

    [Test]
    public async Task ApplyBackPressureAsync_returns_immediately_on_Healthy()
    {
        // Healthy fast-path must be the synchronous no-op path: it returns
        // an already-completed task (one dictionary lookup, no delay, no
        // park). Asserting synchronous completion is deterministic, unlike
        // a wall-clock upper bound that trips under parallel CI load.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Healthy);

        var task = signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(500));

        Assert.That(task.IsCompletedSuccessfully, Is.True,
            "Healthy fast-path must return an already-completed task without applying the Throttled delay");
        await task;
        await signal.DidNotReceive().WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBackPressureAsync_delays_on_Throttled()
    {
        // Throttled must apply the configured per-call delay. The canonical
        // use case is per-line back-pressure on a TCP reader; the delay must
        // actually happen so the producer's TCP window can shrink. Asserted
        // deterministically through an injected time provider: the call
        // schedules a delay for exactly the configured duration and the
        // returned task does not complete until that timer fires - no
        // wall-clock measurement, so no parallel-load timing flakiness.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);
        var time = new ManualDelayTimeProvider();

        var task = signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(50), time);

        Assert.That(task.IsCompleted, Is.False,
            "Throttled response must await a real delay rather than completing synchronously");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMilliseconds(50)),
            "Throttled response must schedule the configured per-call delay (the canonical back-pressure mechanism)");

        time.FireDueTimers();
        await task;
        await signal.DidNotReceive().WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public void ApplyBackPressureAsync_skips_delay_on_Throttled_when_delay_is_Zero()
    {
        // Zero delay disables the Throttled response (equivalent to the
        // historical scheduler-yield pattern). Useful for operators that want
        // to opt out of the per-call delay. The Zero branch is the
        // synchronous no-op path, so assert completion deterministically.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);

        var task = signal.ApplyBackPressureAsync(TreeId, TimeSpan.Zero);

        Assert.That(task.IsCompletedSuccessfully, Is.True,
            "Zero throttled delay must disable the Throttled branch (no-op fast path)");
    }

    [Test]
    public async Task ApplyBackPressureAsync_parks_on_Saturated()
    {
        // Saturated must call WaitForHealthyAsync to park the caller
        // until recovery. The signal substitute observes the call.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(50));

        await signal.Received(1).WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBackPressureAsync_uses_DefaultThrottledDelay_when_no_duration_supplied()
    {
        // The convenience overload uses
        // WalSaturationSignalExtensions.DefaultThrottledDelay (1 ms).
        // Default must actually delay so consumers that adopt the
        // convenience overload get meaningful back-pressure
        // out-of-the-box (this is the fix for "the bench's Throttled
        // response was too soft because it rolled its own"). Asserted
        // deterministically through an injected time provider: the
        // convenience overload schedules a delay for exactly
        // DefaultThrottledDelay rather than completing synchronously -
        // no wall-clock measurement, so no parallel-load timing flakiness.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);
        var time = new ManualDelayTimeProvider();

        var task = signal.ApplyBackPressureAsync(TreeId, time);

        Assert.That(task.IsCompleted, Is.False,
            "default convenience overload must apply a real delay, not complete synchronously");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(WalSaturationSignalExtensions.DefaultThrottledDelay),
            "default convenience overload must schedule the DefaultThrottledDelay (1 ms)");
        Assert.That(WalSaturationSignalExtensions.DefaultThrottledDelay,
            Is.EqualTo(TimeSpan.FromMilliseconds(1)),
            "DefaultThrottledDelay must stay at 1 ms - the documented value the bench's per-line cost is sized against");

        time.FireDueTimers();
        await task;
    }

    [Test]
    public void ApplyBackPressureAsync_throws_on_null_signal()
    {
        IWalSaturationSignal? nullSignal = null;
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await nullSignal!.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(1)));
    }

    [Test]
    public void ApplyBackPressureAsync_throws_on_null_treeId()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await signal.ApplyBackPressureAsync(null!, TimeSpan.FromMilliseconds(1)));
    }

    [Test]
    public void ApplyBackPressureAsync_throws_on_negative_delay()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        // The signal substitute must return a non-Saturated state so
        // the validation runs against the throttledDelay argument
        // (not against the underlying WaitForHealthyAsync semantics).
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Healthy);
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(-1)));
    }

    [Test]
    public void ApplyBackPressureAsync_propagates_cancellation_on_Saturated()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var token = (CancellationToken)callInfo[1];
                return Task.Delay(Timeout.Infinite, token);
            });

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(50), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ApplyBackPressureAsync_propagates_cancellation_on_Throttled()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        // Pre-cancelled token must propagate immediately through
        // Task.Delay -> OperationCanceledException (or the more
        // specific TaskCanceledException subclass).
        Assert.That(
            async () => await signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(500), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// Controllable <see cref="TimeProvider"/> for asserting the Throttled
    /// delay deterministically. <c>Task.Delay(delay, provider, ct)</c> schedules
    /// its completion through <see cref="CreateTimer"/>; this provider records
    /// the requested due time and never fires a timer on its own, so a delay
    /// stays pending until the test calls <see cref="FireDueTimers"/> - removing
    /// the wall-clock dependency that made these tests flaky under parallel load.
    /// </summary>
    private sealed class ManualDelayTimeProvider : TimeProvider
    {
        private readonly List<ManualTimer> _timers = new();

        /// <summary>The due time of the most recently scheduled timer.</summary>
        public TimeSpan? LastScheduledDelay { get; private set; }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(callback, state, this);
            lock (_timers)
            {
                _timers.Add(timer);
            }
            if (dueTime != Timeout.InfiniteTimeSpan)
            {
                LastScheduledDelay = dueTime;
            }
            return timer;
        }

        /// <summary>Fires every pending timer, completing any awaited delays.</summary>
        public void FireDueTimers()
        {
            ManualTimer[] pending;
            lock (_timers)
            {
                pending = _timers.FindAll(static t => !t.Fired).ToArray();
            }
            foreach (var timer in pending)
            {
                timer.Fire();
            }
        }

        private void Remove(ManualTimer timer)
        {
            lock (_timers)
            {
                _timers.Remove(timer);
            }
        }

        private sealed class ManualTimer(TimerCallback callback, object? state, ManualDelayTimeProvider owner) : ITimer
        {
            private TimerCallback? _callback = callback;

            public bool Fired { get; private set; }

            public void Fire()
            {
                if (Fired)
                {
                    return;
                }
                Fired = true;
                _callback?.Invoke(state);
            }

            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            public void Dispose()
            {
                Fired = true;
                _callback = null;
                owner.Remove(this);
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}