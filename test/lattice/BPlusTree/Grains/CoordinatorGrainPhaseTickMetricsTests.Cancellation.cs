using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <see cref="CoordinatorGrain{TSelf}.PhaseTickToken"/> - the phase
/// tick's own cancellation token - and for the teardown arm of the tick's fault
/// handling.
/// <para>
/// <b>The defect.</b> Orleans hands the grain-timer callback a cancellation token
/// and cancels it when the timer is disposed or the grain deactivates. The base
/// class received that token and discarded it one line later, so every coordinator
/// in the repository ran its phase work with no way to be abandoned. On the
/// repository-context acceptance rig a single build step was measured at 23
/// minutes holding a non-reentrant grain turn, and nothing could shorten it
/// (issue #3130, item 2).
/// </para>
/// <para>
/// <b>Why publishing the token needs its own teardown arm.</b> Before a real token
/// reached the step, no tick could raise a cancellation, so the unfiltered catch
/// counted every exception as a discarded tick and was complete. Once a real token
/// flows, an orderly shutdown raises <see cref="OperationCanceledException"/> on
/// the tick path - and counting that would put a failure on
/// <c>CoordinatorPhaseTickFailures</c> at every shutdown and escalate to an error
/// claiming the phase machine has stopped advancing. The fix would manufacture the
/// exact signal the counter exists to detect, so the arm is tested here beside the
/// token that makes it reachable.
/// </para>
/// <para>
/// Every test drives the real captured timer callback with a token it owns, so the
/// production path under test is the one that runs in a silo. None of them reach
/// past the public seam.
/// </para>
/// </summary>
public partial class CoordinatorGrainPhaseTickMetricsTests
{
    /// <summary>
    /// A coordinator that records the token its step observed, and can be told to
    /// raise a cancellation from inside the step - which is what a real step does
    /// when the work it awaited honoured the token.
    /// </summary>
    private sealed class TokenObservingCoordinator(
        IGrainContext context,
        IReminderRegistry reminderRegistry,
        ILogger<TestCoordinator> logger)
        : TestCoordinator(context, reminderRegistry, logger)
    {
        /// <summary>The token published to the step, captured during the step.</summary>
        public CancellationToken Observed { get; private set; }

        /// <summary>How many times the step ran.</summary>
        public int Steps { get; private set; }

        /// <summary>
        /// When set, the step throws <see cref="OperationCanceledException"/> -
        /// standing in for awaited work that honoured the token.
        /// </summary>
        public bool ThrowCancellation { get; set; }

        /// <summary>
        /// The token as read from OUTSIDE a tick, which is what proves it is
        /// withdrawn rather than left published.
        /// </summary>
        public CancellationToken TokenOutsideTick => PhaseTickToken;

        protected internal override Task ProcessNextPhaseAsync()
        {
            Observed = PhaseTickToken;
            Steps++;

            if (ThrowCancellation)
            {
                throw new OperationCanceledException(PhaseTickToken);
            }

            return base.ProcessNextPhaseAsync();
        }
    }

    private static (Harness Harness, TokenObservingCoordinator Grain) CreateObserving()
    {
        TokenObservingCoordinator? grain = null;
        var harness = Create(factory: (ctx, reminders, logger) =>
            grain = new TokenObservingCoordinator(ctx, reminders, logger));
        return (harness, grain!);
    }

    // --------------------------------------------------------------- publication

    [Test]
    public async Task The_tick_publishes_its_own_cancellation_token_to_the_step()
    {
        // The load-bearing assertion for issue #3130 item 2. Equality against the
        // exact token the timer supplied is what makes this a claim about the wiring
        // rather than about a token that merely happens to be cancellable.
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();

        h.Grain.ArmPhaseTimer();
        await CapturedTick(h.Timers)(cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(grain.Steps, Is.EqualTo(1), "The step must have run.");
            Assert.That(grain.Observed, Is.EqualTo(cts.Token),
                "The step must observe the timer's own token, not a substitute.");
            Assert.That(grain.Observed.CanBeCanceled, Is.True,
                "A token that cannot be cancelled is CancellationToken.None wearing a "
                + "different name, and abandons nothing.");
        });
    }

    [Test]
    public async Task A_cancellation_of_the_ticks_token_is_visible_inside_the_step()
    {
        // Equality alone would still pass if the published token were a detached
        // copy. Cancelling the source and reading the captured token proves the
        // step holds a live handle on the teardown signal.
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();

        h.Grain.ArmPhaseTimer();
        await CapturedTick(h.Timers)(cts.Token);

        Assert.That(grain.Observed.IsCancellationRequested, Is.False);

        await cts.CancelAsync();

        Assert.That(grain.Observed.IsCancellationRequested, Is.True,
            "Cancelling the timer's source must be observable through the token the "
            + "step was handed.");
    }

    [Test]
    public async Task The_published_token_is_withdrawn_when_the_tick_returns()
    {
        // A token left published outlives its tick. After a teardown it is
        // permanently cancelled, so the next non-timer caller - a reminder handler,
        // or a test - would abandon its work instantly for a shutdown long over.
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();

        h.Grain.ArmPhaseTimer();
        await CapturedTick(h.Timers)(cts.Token);

        Assert.That(grain.TokenOutsideTick.CanBeCanceled, Is.False,
            "Outside a tick the token must read CancellationToken.None.");
    }

    [Test]
    public async Task The_published_token_is_withdrawn_when_the_tick_throws()
    {
        // The withdrawal is in a finally for this case specifically: a faulting
        // tick is exactly when a stale published token would survive.
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();

        h.Grain.ArmPhaseTimer();
        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        await CapturedTick(h.Timers)(cts.Token);

        Assert.That(grain.TokenOutsideTick.CanBeCanceled, Is.False,
            "A faulting tick must still withdraw its token.");
    }

    // ------------------------------------------------------------------ teardown

    [Test]
    public async Task A_teardown_cancellation_is_not_counted_as_a_phase_tick_failure()
    {
        // The arm that keeps an orderly shutdown from reading as a wedge.
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            grain.ThrowCancellation = true;
            await CapturedTick(h.Timers)(cts.Token);
        });

        Assert.That(ForTree(measurements, GrainKey).Sum(m => m.Value), Is.EqualTo(0),
            "A tick cancelled by its own teardown is the pump obeying, not failing.");
    }

    [Test]
    public async Task A_teardown_cancellation_is_not_logged_as_a_discarded_tick()
    {
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        h.Grain.ArmPhaseTimer();
        grain.ThrowCancellation = true;
        await CapturedTick(h.Timers)(cts.Token);

        Assert.That(
            h.Logger.Lines.Where(l => l.Level is LogLevel.Warning or LogLevel.Error),
            Is.Empty,
            "A shutdown must not log that this tick's work was discarded.");
    }

    [Test]
    public async Task A_cancellation_raised_without_a_teardown_is_still_counted()
    {
        // THE HONESTY TEST. The teardown arm is filtered on the token actually
        // being cancelled, so a cancellation that did NOT come from the teardown -
        // an inner deadline that expired, most often - is a genuine fault and must
        // still be counted. Widen the filter to catch every OperationCanceledException
        // and this goes red, which is what stops the arm becoming a blanket amnesty
        // for a whole exception type.
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            grain.ThrowCancellation = true;

            // Deliberately NOT cancelled: the token is live throughout.
            await CapturedTick(h.Timers)(cts.Token);
        });

        Assert.That(ForTree(measurements, GrainKey).Sum(m => m.Value), Is.EqualTo(1),
            "A cancellation raised while the tick's token is live is a fault, not a "
            + "teardown, and must be counted like any other.");
    }

    [Test]
    public async Task A_teardown_cancellation_does_not_reset_the_consecutive_failure_run()
    {
        // The swallowed tick neither succeeded nor failed, so it must leave the run
        // alone. Resetting it would let a shutdown erase the evidence of a
        // coordinator that had been failing, and the escalation - which only fires
        // at three in a row - is the observable that distinguishes the two.
        var (h, grain) = CreateObserving();
        using var cts = new CancellationTokenSource();

        h.Grain.ArmPhaseTimer();
        var tick = CapturedTick(h.Timers);

        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        await tick(cts.Token);
        await tick(cts.Token);

        // A teardown-cancelled tick in the middle of the run.
        h.Grain.PhaseThrow = null;
        grain.ThrowCancellation = true;
        using var teardown = new CancellationTokenSource();
        await teardown.CancelAsync();
        await tick(teardown.Token);

        grain.ThrowCancellation = false;
        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        await tick(cts.Token);

        Assert.That(
            h.Logger.Lines.Any(l => l.Level == LogLevel.Error),
            Is.True,
            "The third genuine failure must still escalate: the cancelled tick in the "
            + "middle must not have reset the run to zero.");
    }

    [Test]
    public async Task A_successful_tick_after_a_teardown_cancellation_still_resets_the_run()
    {
        // The complement of the test above, so "does not reset" cannot be satisfied
        // by a run counter that never resets at all.
        var (h, _) = CreateObserving();
        using var cts = new CancellationTokenSource();

        h.Grain.ArmPhaseTimer();
        var tick = CapturedTick(h.Timers);

        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        await tick(cts.Token);
        await tick(cts.Token);

        h.Grain.PhaseThrow = null;
        await tick(cts.Token);

        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        await tick(cts.Token);

        Assert.That(
            h.Logger.Lines.Any(l => l.Level == LogLevel.Error),
            Is.False,
            "A successful tick clears the run, so the next failure is the first of a "
            + "new one and must not escalate.");
    }
}
