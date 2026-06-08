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
        // Healthy fast-path must be synchronous (no allocation, one
        // dictionary lookup). Assert via wall-clock that the call
        // returned in well under the configured Throttled delay.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Healthy);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(500));
        sw.Stop();

        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(100)),
            "Healthy fast-path must return immediately without applying the Throttled delay");
        await signal.DidNotReceive().WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBackPressureAsync_delays_on_Throttled()
    {
        // Throttled must apply the configured per-call delay. The
        // canonical use case is per-line back-pressure on a TCP
        // reader; the delay must actually happen so the producer's
        // TCP window can shrink.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await signal.ApplyBackPressureAsync(TreeId, TimeSpan.FromMilliseconds(50));
        sw.Stop();

        Assert.That(sw.Elapsed, Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(40)),
            "Throttled response must apply the configured per-call delay (the canonical back-pressure mechanism)");
        await signal.DidNotReceive().WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBackPressureAsync_skips_delay_on_Throttled_when_delay_is_Zero()
    {
        // Zero delay disables the Throttled response (equivalent to
        // the historical scheduler-yield pattern). Useful for
        // operators that want to opt out of the per-call delay.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await signal.ApplyBackPressureAsync(TreeId, TimeSpan.Zero);
        sw.Stop();

        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(100)),
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
        // response was too soft because it rolled its own").
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await signal.ApplyBackPressureAsync(TreeId);
        sw.Stop();

        // The default is 1 ms; with CI scheduler variance, assert >= 0.5ms
        // (proves the delay actually happened, not just a yield).
        Assert.That(sw.Elapsed, Is.GreaterThanOrEqualTo(TimeSpan.FromMicroseconds(500)),
            "default convenience overload must apply the DefaultThrottledDelay (1 ms)");
        Assert.That(WalSaturationSignalExtensions.DefaultThrottledDelay,
            Is.EqualTo(TimeSpan.FromMilliseconds(1)),
            "DefaultThrottledDelay must stay at 1 ms - the documented value the bench's per-line cost is sized against");
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
}