using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the negative case added for issue #2397: a drain the host ABANDONS when
/// <c>HostOptions.ShutdownTimeout</c> expires.
/// </summary>
/// <remarks>
/// The completion signal from issue #2389 is not sufficient on its own, and
/// <see cref="HostShutdownTimeoutBehaviourTests"/> is the observation that says why:
/// the generic host raises <c>ApplicationStopped</c> whether the drain finished or
/// was abandoned at the budget. So the pre-#2397 signal emitted "drain complete in
/// 90.0s" for a drain that did not complete - a confident false positive, which is a
/// worse failure than the silence the issue describes. These tests pin the two ways
/// the overrun is caught: an alarm that fires at the instant the budget expires, and
/// a clock comparison in the completion path that cannot be lost to a race.
/// </remarks>
public sealed partial class RepoContextDrainSignalTests
{
    /// <summary>
    /// Records severity alongside the message, because the whole point of the
    /// abandoned case is that it must not be reported at the same level as success.
    /// </summary>
    private sealed class LevelRecordingLogger : ILogger<RepoContextDrainSignal>
    {
        private readonly List<(LogLevel Level, string Message)> _lines = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Lines
        {
            get { lock (_lines) { return _lines.ToArray(); } }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add((logLevel, formatter(state, exception)));
            }
        }
    }

    /// <summary>An alarm that never fires, standing in for a budget that has not yet expired.</summary>
    private static Task NeverFires(TimeSpan budget, CancellationToken cancellationToken)
        => Task.Delay(Timeout.Infinite, cancellationToken);

    [Test]
    public void An_overrun_is_reported_at_the_moment_the_budget_expires_not_when_the_drain_finishes()
    {
        // The load-bearing property. A comparison made in CompleteDrain cannot fire
        // in the case where the drain never completes at all, so the report has to
        // happen while the drain is still in flight and the process is still alive.
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            alarm: static (_, _) => Task.CompletedTask);

        signal.BeginDrain();

        Assert.That(
            () => signal.HasOverrunBudget,
            Is.True.After(2000, 10),
            "the alarm must report the overrun without waiting for a completion that may never arrive");

        var lines = logger.Lines;

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasCompleted, Is.False, "the drain has NOT completed when the overrun is reported");
            Assert.That(
                lines.Any(line => line.Level == LogLevel.Error && line.Message.Contains("ABANDONED", StringComparison.Ordinal)),
                Is.True,
                "an abandoned drain must be reported at Error, not buried at Information alongside success");
            Assert.That(
                lines.Any(line => line.Message.Contains(
                    RepoContextShutdownBudget.StopGracePeriodKey, StringComparison.Ordinal)),
                Is.True,
                "the line must name the knob that actually binds - since #2402 the budget is derived from the "
                + "declared container grant, so an operator told to raise RepoContextHostBuilder.ShutdownBudget "
                + "would edit a default that a deployment declaring a grant never reads");
            Assert.That(
                lines.Any(line => line.Message.Contains(
                    "Raising only the declaration buys no drain time and silences this line",
                    StringComparison.Ordinal)),
                Is.True,
                "the line must warn against the half of the remedy that reintroduces the silent kill of #2389: "
                + "a declaration above the real grace period arms the alarm for an instant the process never "
                + "reaches");
        });
    }

    [Test]
    public void A_completion_after_an_overrun_is_reported_as_a_failure_rather_than_as_success()
    {
        // The regression this fixture exists to prevent. Before #2397 this exact
        // sequence emitted "drain complete in 95.0s" at Information.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(95)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            clock.Next,
            alarm: static (_, _) => Task.CompletedTask);

        signal.BeginDrain();
        Assert.That(() => signal.HasOverrunBudget, Is.True.After(2000, 10));

        signal.CompleteDrain();

        var completion = logger.Lines.Last();

        Assert.Multiple(() =>
        {
            Assert.That(completion.Level, Is.EqualTo(LogLevel.Error));
            Assert.That(
                completion.Message,
                Does.Contain("did NOT complete"),
                "ApplicationStopped fires for an abandoned drain too, so the line must contradict the callback that raised it");
            Assert.That(completion.Message, Does.Contain("95.0"));
        });
    }

    [Test]
    public void A_drain_that_reaches_the_budget_is_reported_as_abandoned_even_if_the_alarm_never_fired()
    {
        // Belt and braces. Timer resolution, or a thread pool saturated by the very
        // teardown being measured, can delay the alarm past the completion callback.
        // An overrun the clock can see must never be reported as a clean drain
        // because of that race.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(90)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(logger, Budget, clock.Next, NeverFires);

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasOverrunBudget, Is.True, "reaching the budget IS the overrun, not the boundary before it");
            Assert.That(logger.Lines.Last().Level, Is.EqualTo(LogLevel.Error));
            Assert.That(logger.Lines.Last().Message, Does.Contain("did NOT complete"));
        });
    }

    [Test]
    public void The_overrun_is_reported_once_even_when_the_alarm_and_the_clock_both_observe_it()
    {
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(120)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            clock.Next,
            alarm: static (_, _) => Task.CompletedTask);

        signal.BeginDrain();
        Assert.That(() => signal.HasOverrunBudget, Is.True.After(2000, 10));
        signal.CompleteDrain();
        signal.CompleteDrain();

        Assert.That(
            logger.Lines.Count(line => line.Message.Contains("ABANDONED", StringComparison.Ordinal)),
            Is.EqualTo(1),
            "the alarm latches, so a late completion cannot re-report the same overrun");
    }

    [Test]
    public void A_drain_that_consumes_most_of_the_budget_completes_but_warns_about_the_headroom()
    {
        // The 67.2s-against-90s drain recorded on issue #2397. It completed, so it is
        // not the abandoned case, but reporting it identically to a one-second drain
        // is what let the headroom erode unobserved in the first place.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(67.2)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(logger, Budget, clock.Next, NeverFires);

        signal.BeginDrain();
        signal.CompleteDrain();

        var completion = logger.Lines.Last();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasCompleted, Is.True);
            Assert.That(signal.HasOverrunBudget, Is.False, "a drain inside the budget is not an overrun");
            Assert.That(
                completion.Level,
                Is.EqualTo(LogLevel.Warning),
                "the measurement that motivated #2397 must not report at the same level as an unremarkable drain");
            Assert.That(completion.Message, Does.Contain("74.7"), "the operator needs the fraction, not just the duration");
        });
    }

    [Test]
    public void A_drain_with_real_headroom_is_reported_as_an_unqualified_success()
    {
        // The positive control for the warning above: without it, a fixture asserting
        // "Warning" would pass just as happily against a signal that warned on EVERY
        // completion, which would be noise rather than a signal.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(33.9)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(logger, Budget, clock.Next, NeverFires);

        signal.BeginDrain();
        signal.CompleteDrain();

        var completion = logger.Lines.Last();

        Assert.Multiple(() =>
        {
            Assert.That(completion.Level, Is.EqualTo(LogLevel.Information));
            Assert.That(completion.Message, Does.Contain("drain complete"));
            Assert.That(completion.Message, Does.Contain("37.7"));
        });
    }

    [Test]
    public void A_drain_that_finishes_inside_the_budget_disarms_the_alarm()
    {
        // Without this the alarm would fire minutes after a clean shutdown, in a
        // process that may still be running under a test host, and report an overrun
        // that did not happen.
        var observed = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(5)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            clock.Next,
            (budget, cancellationToken) =>
            {
                observed.TrySetResult(cancellationToken);
                return Task.Delay(Timeout.Infinite, cancellationToken);
            });

        signal.BeginDrain();
        Assert.That(observed.Task.Wait(TimeSpan.FromSeconds(5)), Is.True, "the alarm must be armed by BeginDrain");

        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => observed.Task.Result.IsCancellationRequested,
                Is.True.After(2000, 10),
                "a completed drain must disarm the alarm");
            Assert.That(signal.HasOverrunBudget, Is.False);
        });
    }

    [Test]
    public void Disposing_a_signal_disarms_an_alarm_that_is_still_armed()
    {
        var observed = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var signal = new RepoContextDrainSignal(
            new LevelRecordingLogger(),
            Budget,
            alarm: (budget, cancellationToken) =>
            {
                observed.TrySetResult(cancellationToken);
                return Task.Delay(Timeout.Infinite, cancellationToken);
            });

        signal.BeginDrain();
        Assert.That(observed.Task.Wait(TimeSpan.FromSeconds(5)), Is.True);

        signal.Dispose();
        signal.Dispose();

        Assert.That(
            () => observed.Task.Result.IsCancellationRequested,
            Is.True.After(2000, 10),
            "disposal must not leave a timer able to report an overrun for a signal nobody is using");
    }

    [Test]
    public void A_faulting_alarm_reports_no_overrun_and_does_not_disturb_the_shutdown()
    {
        // The alarm is diagnostics. If it throws, the correct outcome is a missing
        // diagnostic, never a shutdown that fails because its instrumentation did.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(5)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            clock.Next,
            alarm: static (_, _) => Task.FromException(new InvalidOperationException("alarm broke")));

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasCompleted, Is.True);
            Assert.That(signal.HasOverrunBudget, Is.False);
            Assert.That(
                logger.Lines.Any(line => line.Level == LogLevel.Error),
                Is.False,
                "a broken alarm must not be reported as an abandoned drain");
        });
    }

    [Test]
    public void The_headroom_warning_fraction_leaves_room_to_act_before_the_budget_is_reached()
    {
        // A guard on the constant itself. Set at or above 1.0 it could never fire
        // before the overrun it is meant to lead; set near zero it would fire on
        // every drain and mean nothing.
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextDrainSignal.HeadroomWarningFraction, Is.LessThan(1.0));
            Assert.That(RepoContextDrainSignal.HeadroomWarningFraction, Is.GreaterThan(0.5));
            Assert.That(
                67.2 / 90.0,
                Is.GreaterThanOrEqualTo(RepoContextDrainSignal.HeadroomWarningFraction),
                "the drain measured on #2397 is the one case known to be concerning; a threshold that misses it is decorative");
        });
    }

    [Test]
    public void A_signal_with_no_budget_neither_arms_an_alarm_nor_reports_an_overrun()
    {
        // TimeSpan.Zero means "no budget configured", not "a budget of zero that is
        // instantly exceeded". Reporting every drain as abandoned would be worse
        // than the silence #2397 is fixing.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(600)));
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            TimeSpan.Zero,
            clock.Next,
            alarm: static (_, _) => Task.CompletedTask);

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasOverrunBudget, Is.False);
            Assert.That(logger.Lines.Any(line => line.Level == LogLevel.Error), Is.False);
            Assert.That(logger.Lines.Last().Message, Does.Contain("drain complete"));
        });
    }
}
