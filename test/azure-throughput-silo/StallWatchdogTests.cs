using VehicleFleetSimulator.AzureThroughput.Silo;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the dual-arm wedge-detection contract of
/// <see cref="StallWatchdog"/>: the in-flight arm (the historic
/// "writtenTotal frozen with in-flight pinned" gate, now generalised
/// to any non-zero in-flight count) and the provider-saturation arm
/// (a new gate that fires when a sustained per-sample failure delta
/// accumulates without writtenTotal advancement; covers the wedge
/// phenotype where the chain drained to zero because every batch
/// faulted but the silo never settled to FINAL). The watchdog must
/// NOT fire on a healthy steady-state (writtenTotal advancing) or a
/// clean drained idle (in-flight = 0, no failures); it MUST fire on
/// either wedge phenotype.
/// </summary>
[TestFixture]
public class StallWatchdogTests
{
    /// <summary>
    /// Constructs a watchdog with stable, test-overridable snapshot
    /// functions so the assertion drives the snapshot values directly
    /// rather than racing the ambient process clock. Default budget /
    /// poll values are tight so the tests terminate inside a few
    /// hundred milliseconds.
    /// </summary>
    private static StallWatchdog Create(
        Func<long> writtenTotal,
        Func<long> inFlight,
        Func<long>? failedTotal = null,
        long failedDeltaThreshold = 0L,
        TimeSpan? stallWindow = null,
        TimeSpan? pollInterval = null)
        => new(
            writtenTotalSnapshot: writtenTotal,
            inFlightSnapshot: inFlight,
            failedTotalSnapshot: failedTotal ?? (() => 0L),
            failedDeltaThreshold: failedDeltaThreshold,
            stallWindow: stallWindow ?? TimeSpan.FromMilliseconds(50),
            pollInterval: pollInterval ?? TimeSpan.FromMilliseconds(10));

    [Test]
    public void Ctor_null_writtenTotal_throws()
    {
        Assert.That(
            () => new StallWatchdog(
                writtenTotalSnapshot: null!,
                inFlightSnapshot: () => 0L,
                failedTotalSnapshot: () => 0L,
                failedDeltaThreshold: 0L,
                stallWindow: TimeSpan.FromSeconds(1),
                pollInterval: TimeSpan.FromMilliseconds(100)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_inFlight_throws()
    {
        Assert.That(
            () => new StallWatchdog(
                writtenTotalSnapshot: () => 0L,
                inFlightSnapshot: null!,
                failedTotalSnapshot: () => 0L,
                failedDeltaThreshold: 0L,
                stallWindow: TimeSpan.FromSeconds(1),
                pollInterval: TimeSpan.FromMilliseconds(100)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_failedTotal_throws()
    {
        Assert.That(
            () => new StallWatchdog(
                writtenTotalSnapshot: () => 0L,
                inFlightSnapshot: () => 0L,
                failedTotalSnapshot: null!,
                failedDeltaThreshold: 0L,
                stallWindow: TimeSpan.FromSeconds(1),
                pollInterval: TimeSpan.FromMilliseconds(100)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_negative_failedDeltaThreshold_throws()
    {
        Assert.That(
            () => new StallWatchdog(
                writtenTotalSnapshot: () => 0L,
                inFlightSnapshot: () => 0L,
                failedTotalSnapshot: () => 0L,
                failedDeltaThreshold: -1L,
                stallWindow: TimeSpan.FromSeconds(1),
                pollInterval: TimeSpan.FromMilliseconds(100)),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    /// <summary>
    /// Counts how many poll iterations the watchdog has actually
    /// completed, by wrapping one of its snapshot functions.
    /// <see cref="StallWatchdog.RunAsync"/> reads
    /// <c>inFlightSnapshot</c> exactly once per loop iteration (and
    /// never during pre-loop priming), so wrapping that function yields
    /// an exact iteration counter.
    /// </summary>
    private sealed class PollCounter
    {
        private int _polls;

        public int Polls => Volatile.Read(ref _polls);

        public Func<long> Counting(Func<long> snapshot) => () =>
        {
            Interlocked.Increment(ref _polls);
            return snapshot();
        };
    }

    /// <summary>
    /// Waits until the watchdog has completed at least
    /// <paramref name="polls"/> poll iterations.
    /// <para>
    /// This is the sound anchor for a "must NOT fire" assertion.
    /// <see cref="StallWatchdog.RunAsync"/> awaits <c>pollInterval</c>
    /// and then samples every snapshot exactly once per iteration, and
    /// <see cref="Task.Delay(TimeSpan, CancellationToken)"/> never
    /// returns early, so N observed polls proves at least
    /// N * pollInterval elapsed <em>inside the watchdog's own loop</em>
    /// - the gate genuinely had N opportunities to arm and fire. A bare
    /// <c>Task.Delay</c> in the test proves only that wall-clock passed
    /// in the test; on a starved CI worker it can elapse while the
    /// watchdog loop has barely run, so the negative assertion would
    /// pass without ever exercising the gate it exists to cover.
    /// </para>
    /// </summary>
    private static async Task WaitForPollsAsync(PollCounter counter, int polls, int timeoutMs = 30_000)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while (counter.Polls < polls)
        {
            if (stopwatch.ElapsedMilliseconds > timeoutMs)
            {
                Assert.Fail(
                    $"Timed out after {timeoutMs} ms waiting for {polls} watchdog poll iterations; observed {counter.Polls}.");
            }

            await Task.Delay(5);
        }
    }

    /// <summary>
    /// Poll count that gives a negative ("must not fire") assertion its
    /// evidence: with the fixture's default 10 ms poll interval and
    /// 50 ms stall window, 20 polls means the watchdog looped for at
    /// least four consecutive stall windows without firing.
    /// </summary>
    private const int PollsCoveringSeveralStallWindows = 20;

    [Test]
    public async Task RunAsync_progressing_writtenTotal_does_not_fire()
    {
        // Healthy: writtenTotal advances every sample. The watchdog
        // must not fire even with non-zero in-flight.
        //
        // Progress is driven from the snapshot callback (the watchdog
        // reads it once per poll) rather than a background pump on its
        // own timer, so no timer race can starve a poll of progress and
        // let the in-flight arm accumulate a stall window.
        var written = 0L;
        var polls = new PollCounter();
        var watchdog = Create(
            writtenTotal: () => Interlocked.Increment(ref written),
            inFlight: polls.Counting(() => 5L));

        using var cts = new CancellationTokenSource();
        var run = watchdog.RunAsync(cts.Token);

        // Let the watchdog loop across several stall windows so a
        // broken gate would have had ample opportunity to fire.
        await WaitForPollsAsync(polls, PollsCoveringSeveralStallWindows);

        // Non-firing IS directly observable via HasFiredForTesting (the
        // sibling positive tests below assert the same flag is true), so
        // assert it rather than settling for "RunAsync returned without
        // throwing" - which this test's name promises but which holds
        // even when the watchdog has fired.
        Assert.That(watchdog.HasFiredForTesting, Is.False,
            "healthy operation: must not fire while writtenTotal is advancing on every sample");

        cts.Cancel();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(run.IsCompletedSuccessfully, Is.True,
            "RunAsync must return cleanly on cancellation rather than surfacing the OperationCanceledException");
    }

    [Test]
    public async Task RunAsync_drained_idle_with_zero_failures_does_not_arm()
    {
        // Clean idle: writtenTotal frozen, inFlight = 0, no failures.
        // The watchdog must NOT arm (the old gate fired when
        // inFlight >= cap; this proves the dual-arm gate keeps the
        // drained-idle non-firing).
        var polls = new PollCounter();
        var watchdog = Create(
            writtenTotal: () => 1_000L,
            inFlight: polls.Counting(() => 0L),
            failedTotal: () => 0L,
            failedDeltaThreshold: 100L);

        using var cts = new CancellationTokenSource();
        var run = watchdog.RunAsync(cts.Token);

        // Loop across several stall windows: with writtenTotal frozen,
        // an over-eager gate would fire here.
        await WaitForPollsAsync(polls, PollsCoveringSeveralStallWindows);

        Assert.That(watchdog.HasFiredForTesting, Is.False,
            "drained idle: must not arm when writtenTotal is frozen but in-flight is zero and no failures are accruing");

        cts.Cancel();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(run.IsCompletedSuccessfully, Is.True,
            "RunAsync must return cleanly on cancellation rather than surfacing the OperationCanceledException");
    }

    [Test]
    public void Ctor_does_not_throw_on_zero_failedDeltaThreshold_disabling_provider_arm()
    {
        // Passing 0 with a constant failedTotal disables the
        // provider-saturation arm (the documented opt-out). Construction
        // must succeed and the watchdog must coexist with the in-flight
        // arm.
        Assert.DoesNotThrow(() => Create(
            writtenTotal: () => 0L,
            inFlight: () => 0L,
            failedTotal: () => 0L,
            failedDeltaThreshold: 0L));
    }

    [Test]
    public async Task RunAsync_fires_on_inflight_arm_when_writtenTotal_frozen_with_nonzero_inflight()
    {
        // Acceptance contract: the watchdog FIRES when writtenTotal is
        // frozen and inFlight is non-zero (regardless of whether it's
        // at the configured cap). The historic gate required
        // inFlight >= cap; the new gate fires on any non-zero inflight,
        // catching the "inFlight=N<cap parked" phenotype that
        // saturating-account wedges produce.
        const long frozenWritten = 1_000L;
        const long pinnedInFlight = 5L; // deliberately below any cap; old gate would have missed this
        var watchdog = Create(
            writtenTotal: () => frozenWritten,
            inFlight: () => pinnedInFlight,
            failedTotal: () => 0L,
            failedDeltaThreshold: 100L,
            stallWindow: TimeSpan.FromMilliseconds(50),
            pollInterval: TimeSpan.FromMilliseconds(10));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var run = watchdog.RunAsync(cts.Token);
        // Poll for the fire flag to flip. Bounded by the cancellation
        // timeout above so a regression fails the test in ~2s.
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (!watchdog.HasFiredForTesting && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        Assert.That(watchdog.HasFiredForTesting, Is.True,
            "in-flight arm: watchdog must fire when writtenTotal is frozen and inFlight > 0, even when inFlight is below the cap");

        cts.Cancel();
        try { await run.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* shutdown */ }
    }

    [Test]
    public async Task RunAsync_fires_on_failure_arm_when_writtenTotal_frozen_with_sustained_failure_rate()
    {
        // Acceptance contract: the watchdog FIRES when writtenTotal is
        // frozen AND the per-sample failure delta exceeds the threshold,
        // even with inFlight = 0 (the "chain drained because every
        // batch faulted, but FINAL never emits" wedge phenotype).
        //
        // The failure stream is driven by the snapshot callback itself
        // rather than a background pump on its own timer. The watchdog
        // reads the failed total exactly once per poll, so every poll
        // observes a fresh delta above the threshold and the armed
        // window is never reset by a poll that happened to land between
        // two pump ticks. Racing two independent equal-period timers
        // made this assertion intermittently fail (an unarmed poll
        // resets lastProgressAt, so the watchdog needed another five
        // consecutive armed polls inside the budget).
        long failedTotal = 0L;
        const long failureIncrement = 200L; // above the 100 threshold
        var watchdog = Create(
            writtenTotal: () => 1_000L,
            inFlight: () => 0L,
            failedTotal: () => Interlocked.Add(ref failedTotal, failureIncrement),
            failedDeltaThreshold: 100L,
            stallWindow: TimeSpan.FromMilliseconds(50),
            pollInterval: TimeSpan.FromMilliseconds(10));

        // The cancellation budget is deliberately well clear of the
        // assertion deadline below so the two no longer race.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var run = watchdog.RunAsync(cts.Token);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!watchdog.HasFiredForTesting && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        Assert.That(watchdog.HasFiredForTesting, Is.True,
            "failure arm: watchdog must fire when writtenTotal is frozen and failedDelta sustained >= threshold, even with inFlight = 0");

        cts.Cancel();
        try { await run.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* shutdown */ }
    }

    [Test]
    public async Task RunAsync_does_not_fire_on_failure_arm_when_failedDelta_stays_below_threshold()
    {
        // Single-straggler late-in-run failure must NOT trip the
        // provider-saturation arm; the threshold guards against
        // false positives.
        long failedTotal = 0L;
        var polls = new PollCounter();
        var watchdog = Create(
            writtenTotal: () => 1_000L,
            inFlight: polls.Counting(() => 0L),
            failedTotal: () => Interlocked.Read(ref failedTotal),
            failedDeltaThreshold: 100L,
            stallWindow: TimeSpan.FromMilliseconds(50),
            pollInterval: TimeSpan.FromMilliseconds(10));

        // The cancellation budget is a safety net only; it is kept well
        // clear of the wait below so the two cannot race on a slow
        // worker (a token that fired first would stop the poll loop and
        // strand the wait).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var run = watchdog.RunAsync(cts.Token);

        // One straggler failure of 5 entries; far below the 100
        // threshold. The watchdog must not fire.
        Interlocked.Add(ref failedTotal, 5L);

        await WaitForPollsAsync(polls, PollsCoveringSeveralStallWindows);
        Assert.That(watchdog.HasFiredForTesting, Is.False,
            "failure arm: must not fire on a single below-threshold straggler");

        cts.Cancel();
        try { await run.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* shutdown */ }
    }

    [Test]
    public async Task RunAsync_does_not_fire_when_writtenTotal_advances_even_with_inflight_pinned()
    {
        // Healthy operation: writtenTotal increments every sample,
        // inFlight is pinned. The watchdog must NOT fire because
        // progress resets lastProgressAt on every sample.
        //
        // Progress is driven from the snapshot callback (the watchdog
        // reads it once per poll) rather than a background pump, so no
        // timer race can starve a poll of progress and let the
        // in-flight arm accumulate a stall window.
        long written = 0L;
        var polls = new PollCounter();
        var watchdog = Create(
            writtenTotal: () => Interlocked.Increment(ref written),
            inFlight: polls.Counting(() => 16L), // pinned at "cap"
            failedTotal: () => 0L,
            failedDeltaThreshold: 100L,
            stallWindow: TimeSpan.FromMilliseconds(50),
            pollInterval: TimeSpan.FromMilliseconds(10));

        // The cancellation budget is a safety net only; it is kept well
        // clear of the wait below so the two cannot race on a slow
        // worker (a token that fired first would stop the poll loop and
        // strand the wait).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var run = watchdog.RunAsync(cts.Token);

        await WaitForPollsAsync(polls, PollsCoveringSeveralStallWindows);
        Assert.That(watchdog.HasFiredForTesting, Is.False,
            "healthy operation: must not fire while writtenTotal is advancing");

        cts.Cancel();
        try { await run.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* shutdown */ }
    }

    [Test]
    public async Task RunAsync_does_not_fire_within_stallWindow_giving_transient_backpressure_a_chance()
    {
        // The stallWindow is a debounce: even with an armed gate,
        // the watchdog must not fire until the gate has stayed
        // armed for the full window. A brief stall that recovers
        // inside the window must not fire.
        long written = 0L;
        long inFlight = 5L;
        var watchdog = Create(
            writtenTotal: () => Interlocked.Read(ref written),
            inFlight: () => Interlocked.Read(ref inFlight),
            failedTotal: () => 0L,
            failedDeltaThreshold: 100L,
            stallWindow: TimeSpan.FromMilliseconds(500),
            pollInterval: TimeSpan.FromMilliseconds(20));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var run = watchdog.RunAsync(cts.Token);

        // Let the gate arm briefly (frozen writtenTotal, non-zero
        // inFlight), then recover inside the stallWindow by
        // advancing writtenTotal.
        await Task.Delay(150);
        Assert.That(watchdog.HasFiredForTesting, Is.False, "watchdog must not fire before the stallWindow elapses");

        // Recover: bump writtenTotal so the next sample sees progress.
        Interlocked.Increment(ref written);
        await Task.Delay(100);

        // Even after the recovery, the watchdog must not have fired
        // because the brief stall was inside the stallWindow.
        Assert.That(watchdog.HasFiredForTesting, Is.False,
            "stallWindow debounce: watchdog must not fire on a transient stall that recovers inside the window");

        cts.Cancel();
        try { await run.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* shutdown */ }
    }
}
