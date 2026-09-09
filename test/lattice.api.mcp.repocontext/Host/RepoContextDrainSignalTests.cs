using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the observable drain signal added for issue #2389: the measured drain
/// duration, the idempotence of both transitions, and the one property the whole
/// mechanism exists for - that the completion line is emitted if and only if the
/// drain actually finished, so its ABSENCE in a container log is evidence the
/// container was killed mid-drain.
/// </summary>
[TestFixture]
public sealed partial class RepoContextDrainSignalTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(90);

    /// <summary>Captures emitted log lines so the signal's output can be asserted.</summary>
    private sealed class RecordingLogger : ILogger<RepoContextDrainSignal>
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Lines.Add(formatter(state, exception));
    }

    /// <summary>
    /// A controllable monotonic clock. The signal measures with
    /// <see cref="Stopwatch.GetElapsedTime(long, long)"/>, so the fake hands back
    /// stopwatch ticks and the assertions are exact rather than timing-dependent.
    /// </summary>
    private sealed class FakeClock(params long[] readings)
    {
        private int _index;

        public long Next() => readings[Math.Min(_index++, readings.Length - 1)];
    }

    private static long TicksFor(TimeSpan span)
        => (long)(span.TotalSeconds * Stopwatch.Frequency);

    [Test]
    public void A_new_signal_has_not_begun_or_completed_a_drain()
    {
        var signal = new RepoContextDrainSignal(NullLogger<RepoContextDrainSignal>.Instance, Budget);

        Assert.Multiple(() =>
        {
            Assert.That(signal.IsDraining, Is.False);
            Assert.That(signal.HasCompleted, Is.False);
            Assert.That(signal.Elapsed, Is.Null);
        });
    }

    [Test]
    public void Beginning_a_drain_latches_the_draining_state_and_announces_the_budget()
    {
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(logger, Budget);

        signal.BeginDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.IsDraining, Is.True);
            Assert.That(signal.HasCompleted, Is.False, "a started drain has not finished");
            Assert.That(signal.Elapsed, Is.Null, "no duration is available until the drain completes");
            Assert.That(logger.Lines, Has.Count.EqualTo(1));
            Assert.That(logger.Lines[0], Does.Contain("drain started"));
            Assert.That(logger.Lines[0], Does.Contain("90s"), "the announced budget must be the enforced one");
        });
    }

    [Test]
    public void Completing_a_drain_reports_the_measured_duration()
    {
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(33.9)));
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(logger, Budget, clock.Next);

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasCompleted, Is.True);
            Assert.That(
                signal.Elapsed!.Value.TotalSeconds,
                Is.EqualTo(33.9).Within(0.05),
                "the reported duration is the whole window, which is the number a stop_grace_period is derived from");
            Assert.That(logger.Lines, Has.Count.EqualTo(2));
            Assert.That(logger.Lines[1], Does.Contain("drain complete"));
            Assert.That(logger.Lines[1], Does.Contain("33.9"));
        });
    }

    [Test]
    public void Completing_without_beginning_reports_nothing()
    {
        // The guard that keeps the signal honest. Without it a stray
        // ApplicationStopped callback in a process that never began to drain would
        // emit a duration measured from an unset start - a fabricated number in the
        // exact log line an operator is being told to derive a budget from.
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(logger, Budget);

        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasCompleted, Is.False);
            Assert.That(signal.Elapsed, Is.Null);
            Assert.That(logger.Lines, Is.Empty);
        });
    }

    [Test]
    public void Both_transitions_are_idempotent_so_a_duplicate_callback_cannot_contradict_the_first()
    {
        var clock = new FakeClock(
            0,
            TicksFor(TimeSpan.FromSeconds(12)),
            TicksFor(TimeSpan.FromSeconds(900)));
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(logger, Budget, clock.Next);

        signal.BeginDrain();
        signal.BeginDrain();
        signal.CompleteDrain();
        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(
                signal.Elapsed!.Value.TotalSeconds,
                Is.EqualTo(12).Within(0.05),
                "the second completion must not overwrite the measured duration");
            Assert.That(logger.Lines, Has.Count.EqualTo(2), "exactly one start line and one completion line");
        });
    }

    [Test]
    public void A_drain_killed_before_it_finishes_never_emits_the_completion_line()
    {
        // The property the whole signal exists for, asserted directly. Under the
        // pre-#2389 configuration this was every teardown: SIGTERM arrived, the
        // drain began, and SIGKILL landed 10 seconds later, so the process died
        // between these two calls. An operator reading `docker logs` sees the start
        // line and no completion line, which is the discriminator the exit code
        // could not provide because the next start overwrites it.
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(logger, Budget);

        signal.BeginDrain();

        Assert.Multiple(() =>
        {
            Assert.That(logger.Lines, Has.Count.EqualTo(1));
            Assert.That(
                logger.Lines.Any(line => line.Contains("drain complete", StringComparison.Ordinal)),
                Is.False,
                "an unfinished drain must not claim completion");
            Assert.That(signal.HasCompleted, Is.False);
        });
    }

    /// <summary>
    /// A minimal host lifetime. The framework's own <c>ApplicationLifetime</c> is
    /// internal, and a real host would drag a whole silo into a unit test for the
    /// sake of two cancellation tokens.
    /// </summary>
    private sealed class FakeLifetime : IHostApplicationLifetime, IDisposable
    {
        private readonly CancellationTokenSource _started = new();
        private readonly CancellationTokenSource _stopping = new();
        private readonly CancellationTokenSource _stopped = new();

        public CancellationToken ApplicationStarted => _started.Token;

        public CancellationToken ApplicationStopping => _stopping.Token;

        public CancellationToken ApplicationStopped => _stopped.Token;

        public void StopApplication() => _stopping.Cancel();

        public void NotifyStopped() => _stopped.Cancel();

        public void Dispose()
        {
            _started.Dispose();
            _stopping.Dispose();
            _stopped.Dispose();
        }
    }

    [Test]
    public void Binding_to_a_lifetime_measures_the_whole_stop_sequence()
    {
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(41.5)));
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(logger, Budget, clock.Next);
        using var lifetime = new FakeLifetime();

        signal.Bind(lifetime);

        lifetime.StopApplication();
        Assert.That(signal.IsDraining, Is.True, "ApplicationStopping must start the clock");
        Assert.That(signal.HasCompleted, Is.False, "the drain is not complete while services are still stopping");

        lifetime.NotifyStopped();

        Assert.Multiple(() =>
        {
            Assert.That(signal.HasCompleted, Is.True, "ApplicationStopped must stop the clock");
            Assert.That(signal.Elapsed!.Value.TotalSeconds, Is.EqualTo(41.5).Within(0.05));
        });
    }

    [Test]
    public void Binding_rejects_a_null_lifetime()
    {
        var signal = new RepoContextDrainSignal(NullLogger<RepoContextDrainSignal>.Instance, Budget);

        Assert.Throws<ArgumentNullException>(() => signal.Bind(null!));
    }

    [Test]
    public void The_constructor_rejects_a_null_logger()
    {
        Assert.Throws<ArgumentNullException>(() => new RepoContextDrainSignal(null!, Budget));
    }
}
