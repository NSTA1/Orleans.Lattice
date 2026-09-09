using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers issue #2401: an abandoned drain must report itself through the process
/// exit code, not only through the log line added by #2397 and #2399.
/// </summary>
/// <remarks>
/// <para>
/// The gap these pin is a layering one rather than a behavioural one. The overrun
/// was already detected, already latched, and already logged at <c>Error</c>; what
/// was missing is that the only consumer which acts <b>automatically</b> - the
/// orchestrator - does not read logs. It reads the exit code, and a process exiting
/// <c>0</c> tells it the container stopped cleanly. So the loudest possible log line
/// still degraded to "a human who already suspects a problem may find evidence of
/// it", which is a materially weaker guarantee than the one the drain signal is
/// supposed to provide.
/// </para>
/// <para>
/// Note what is deliberately NOT asserted here: that the container restarts, or
/// fails to restart, because of the code. The sample compose file runs under
/// <c>restart: unless-stopped</c>, which restarts on any exit code, so a test
/// claiming a restart consequence would be asserting something the deployment does
/// not do. What the code changes is what is recorded - <c>Exited (70)</c> rather
/// than <c>Exited (0)</c>, and a Kubernetes termination reason of <c>Error</c>
/// rather than <c>Completed</c> - and that is what these tests and the operator docs
/// claim.
/// </para>
/// </remarks>
public sealed partial class RepoContextDrainSignalTests
{
    /// <summary>
    /// Captures the codes reported to the process, so an overrun can be driven
    /// without the NUnit host's own exit code being mutated by the test.
    /// </summary>
    private sealed class RecordingExitCodeReporter
    {
        private readonly List<int> _codes = new();

        public IReadOnlyList<int> Codes
        {
            get { lock (_codes) { return _codes.ToArray(); } }
        }

        public void Report(int code)
        {
            lock (_codes)
            {
                _codes.Add(code);
            }
        }
    }

    [Test]
    public void The_process_exit_code_is_reported_when_the_alarm_latches_the_overrun()
    {
        // The path that matters most, because it is the one that still works when
        // the drain never finishes at all: the exit code is assigned while the
        // process is alive rather than in a completion callback that may not run.
        var reporter = new RecordingExitCodeReporter();
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            alarm: static (_, _) => Task.CompletedTask,
            reportExitCode: reporter.Report);

        signal.BeginDrain();

        Assert.That(
            () => signal.HasOverrunBudget,
            Is.True.After(2000, 10),
            "the alarm must latch the overrun before the exit code can be asserted");

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasCompleted, Is.False, "the drain has NOT completed when the exit code is reported");
            Assert.That(
                reporter.Codes,
                Is.EqualTo(new[] { RepoContextExitCode.DrainAbandoned }),
                "an abandoned drain must report the abandoned-drain code exactly once");
        });
    }

    [Test]
    public void The_process_exit_code_is_reported_when_the_completion_clock_latches_an_overrun_the_alarm_missed()
    {
        // The belt-and-braces path. The alarm can lose the race - timer resolution,
        // or a thread pool saturated by the very teardown being measured - and an
        // overrun the clock can see must reach the exit code by that route too,
        // otherwise the loud log line and the reported exit code disagree.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(95)));
        var reporter = new RecordingExitCodeReporter();
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            clock.Next,
            NeverFires,
            reporter.Report);

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasOverrunBudget, Is.True, "95s against a 90s budget is an overrun");
            Assert.That(
                reporter.Codes,
                Is.EqualTo(new[] { RepoContextExitCode.DrainAbandoned }),
                "the completion clock must report the exit code the alarm did not");
        });
    }

    [Test]
    public void The_process_exit_code_is_reported_exactly_once_when_the_alarm_and_the_completion_both_see_the_overrun()
    {
        // Both latch sites run in this sequence. Reporting twice would be harmless
        // to the process, which keeps only the last value, but it would mean the
        // latch is not actually single-fire - and the same defect in the log path
        // is what #2397 had to fix.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(120)));
        var reporter = new RecordingExitCodeReporter();
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            clock.Next,
            static (_, _) => Task.CompletedTask,
            reporter.Report);

        signal.BeginDrain();

        Assert.That(
            () => signal.HasOverrunBudget,
            Is.True.After(2000, 10),
            "the alarm must have latched first for this to be the racing case");

        signal.CompleteDrain();

        Assert.That(
            reporter.Codes,
            Is.EqualTo(new[] { RepoContextExitCode.DrainAbandoned }),
            "the second latch site must not report a second time");
    }

    [Test]
    public void A_drain_that_finishes_inside_the_budget_reports_no_process_exit_code_at_all()
    {
        // The discriminating half. A reporter fired unconditionally would still pass
        // every assertion above while making every clean stop look abandoned, which
        // is the failure that would actually reach production: an operator learns to
        // ignore a signal that is always on far faster than one that is never on.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(37.7)));
        var reporter = new RecordingExitCodeReporter();
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            clock.Next,
            NeverFires,
            reporter.Report);

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasOverrunBudget, Is.False, "37.7s is comfortably inside a 90s budget");
            Assert.That(
                reporter.Codes,
                Is.Empty,
                "a drain that finished must leave the process reporting success");
        });
    }

    [Test]
    public void A_signal_given_no_reporter_leaves_the_process_exit_code_untouched()
    {
        // Pins the fail-safe default, and it guards this very suite. Several
        // fixtures here drive a deliberate overrun; were the default the real
        // Environment.ExitCode setter, the NUnit host would exit non-zero and the
        // whole run would be reported failed with every test passing.
        var before = Environment.ExitCode;
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            alarm: static (_, _) => Task.CompletedTask);

        signal.BeginDrain();

        Assert.That(
            () => signal.HasOverrunBudget,
            Is.True.After(2000, 10),
            "the overrun must actually latch, otherwise this asserts nothing");

        Assert.Multiple(() =>
        {
            Assert.That(signal.ReportsProcessExitCode, Is.False, "no reporter was supplied");
            Assert.That(
                Environment.ExitCode,
                Is.EqualTo(before),
                "the default must not reach the real process exit code");
        });
    }

    [Test]
    public void A_signal_given_a_reporter_says_so_which_is_how_the_hosts_wiring_is_asserted()
    {
        using var wired = new RepoContextDrainSignal(
            NullLogger<RepoContextDrainSignal>.Instance,
            Budget,
            reportExitCode: static _ => { });
        using var unwired = new RepoContextDrainSignal(NullLogger<RepoContextDrainSignal>.Instance, Budget);

        Assert.Multiple(() =>
        {
            Assert.That(wired.ReportsProcessExitCode, Is.True);
            Assert.That(
                unwired.ReportsProcessExitCode,
                Is.False,
                "the property must discriminate, otherwise the host wiring test it backs is vacuous");
        });
    }

    [Test]
    public void A_reporter_that_throws_does_not_stop_the_overrun_being_logged()
    {
        // Diagnostics must never convert a shutdown that is merely late into a
        // crash. Losing the exit code costs one signal; throwing here would abandon
        // the remainder of the stop sequence, which is strictly worse than the
        // condition being reported.
        var logger = new LevelRecordingLogger();
        using var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            alarm: static (_, _) => Task.CompletedTask,
            reportExitCode: static _ => throw new InvalidOperationException("exit code reporting failed"));

        signal.BeginDrain();

        Assert.That(
            () => signal.HasOverrunBudget,
            Is.True.After(2000, 10),
            "a throwing reporter must not prevent the overrun latching");

        Assert.That(
            logger.Lines.Any(line =>
                line.Level == LogLevel.Error && line.Message.Contains("ABANDONED", StringComparison.Ordinal)),
            Is.True,
            "the log line must survive a reporter that faults");
    }
}
