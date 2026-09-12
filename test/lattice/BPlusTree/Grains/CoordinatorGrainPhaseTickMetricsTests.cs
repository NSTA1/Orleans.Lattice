using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the phase-tick failure instrumentation on
/// <see cref="CoordinatorGrain{TSelf}"/>.
/// <para>
/// Before this instrumentation existed, a phase step that threw was logged at
/// warning and swallowed, and nothing anywhere counted it. On the acceptance
/// rig a coordinator swallowed thirty-five ticks across eight and a half hours
/// while every exported series sat still, so telemetry reported a healthy
/// deployment throwing work away.
/// </para>
/// <para>
/// Every assertion here drives a real tick and reads a real measurement off a
/// <see cref="MeterListener"/>. None of them assert on a tag set alone, which
/// would be satisfied by the zero-prime and stay green with the counting arm
/// deleted.
/// </para>
/// </summary>
[TestFixture]
public class CoordinatorGrainPhaseTickMetricsTests
{
    private const string GrainKey = "tree-a";
    private const string CompositeGrainKey = "tree-b/7";

    /// <summary>
    /// Derived coordinator leaving every virtual member at its base default, so
    /// the base class's own tagging and counting is the behaviour under test.
    /// </summary>
    private class TestCoordinator(
        IGrainContext context,
        IReminderRegistry reminderRegistry,
        ILogger<TestCoordinator> logger)
        : CoordinatorGrain<TestCoordinator>(context, reminderRegistry, logger)
    {
        public const string ReminderName = "phase-tick-metrics-keepalive";

        public Exception? PhaseThrow { get; set; }

        protected override string KeepaliveReminderName => ReminderName;
        protected override bool InProgress => true;

        protected internal override Task ProcessNextPhaseAsync()
        {
            if (PhaseThrow is not null) throw PhaseThrow;
            return Task.CompletedTask;
        }

        public void ArmPhaseTimer() => StartPhaseTimer();
    }

    /// <summary>
    /// A coordinator whose grain key is composite (<c>tree/shard</c>), standing
    /// in for <c>TreeShardSplitGrain</c> and friends. It overrides
    /// <c>MetricsTreeId</c> exactly as they do.
    /// </summary>
    private sealed class CompositeKeyCoordinator(
        IGrainContext context,
        IReminderRegistry reminderRegistry,
        ILogger<TestCoordinator> logger)
        : TestCoordinator(context, reminderRegistry, logger)
    {
        protected override string MetricsTreeId =>
            Context.GrainId.Key.ToString()!.Split('/')[0];
    }

    /// <summary>
    /// Records every level and formatted message, so the escalation arm is
    /// observable. A null logger would leave it dark while the test passed.
    /// </summary>
    private sealed class CapturingLogger : ILogger<TestCoordinator>
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = [];

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Lines.Add((logLevel, formatter(state, exception)));

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }

    private sealed record Harness(
        TestCoordinator Grain,
        ITimerRegistry Timers,
        CapturingLogger Logger);

    private static Harness Create(string key = GrainKey, bool composite = false)
    {
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());

        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("phase-tick-metrics-coordinator", key));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var reminders = Substitute.For<IReminderRegistry>();
        var logger = new CapturingLogger();
        TestCoordinator grain = composite
            ? new CompositeKeyCoordinator(context, reminders, logger)
            : new TestCoordinator(context, reminders, logger);

        return new Harness(grain, timerRegistry, logger);
    }

    /// <summary>
    /// The phase tick the coordinator handed the timer registry. Firing it
    /// directly makes the tick deterministic with no sleeping - and, crucially,
    /// makes the failure path a real thrown exception rather than a simulation.
    /// </summary>
    private static Func<CancellationToken, Task> CapturedTick(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    private sealed record Measurement(long Value, Dictionary<string, string?> Tags);

    /// <summary>
    /// Runs <paramref name="body"/> with a listener enabled for exactly the
    /// phase-tick failure counter, and returns every measurement it recorded.
    /// The instrument is passed by reference to
    /// <see cref="MeterListening.StartForInstrument"/>, so the owning type
    /// initialiser has necessarily completed before the listener exists.
    /// </summary>
    private static async Task<List<Measurement>> RecordAsync(Func<Task> body)
    {
        var measurements = new List<Measurement>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.CoordinatorPhaseTickFailures,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var copy = new Dictionary<string, string?>(StringComparer.Ordinal);
                foreach (var tag in tags)
                {
                    copy[tag.Key] = tag.Value?.ToString();
                }

                lock (measurements)
                {
                    measurements.Add(new Measurement(value, copy));
                }
            }));

        await body();
        listener.RecordObservableInstruments();

        lock (measurements)
        {
            return [.. measurements];
        }
    }

    private static List<Measurement> ForTree(List<Measurement> all, string tree) =>
        [.. all.Where(m => m.Tags.TryGetValue(LatticeMetrics.TagTree, out var t) && t == tree)];

    // ------------------------------------------------------------------ counting

    [Test]
    public async Task A_phase_tick_that_throws_advances_the_failure_counter()
    {
        // The load-bearing test. It drives a tick that genuinely throws and
        // asserts the counter moved - not that a label exists, which the
        // zero-prime alone would satisfy with the counting arm deleted.
        var h = Create();

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
            await CapturedTick(h.Timers)(CancellationToken.None);
        });

        var mine = ForTree(measurements, GrainKey);
        Assert.That(mine.Sum(m => m.Value), Is.EqualTo(1),
            "One swallowed phase tick must show up as exactly one counted failure.");
    }

    [Test]
    public async Task Every_swallowed_tick_is_counted_not_just_the_first()
    {
        // Thirty-five failures reported as one would be almost as misleading as
        // thirty-five reported as none.
        var h = Create();

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
            var tick = CapturedTick(h.Timers);
            for (var i = 0; i < 5; i++)
            {
                await tick(CancellationToken.None);
            }
        });

        Assert.That(ForTree(measurements, GrainKey).Sum(m => m.Value), Is.EqualTo(5));
    }

    [Test]
    public async Task A_phase_tick_that_succeeds_does_not_advance_the_failure_counter()
    {
        // The negative control. Without it, an arm that counted unconditionally
        // would pass every other test in this fixture.
        var h = Create();

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            await CapturedTick(h.Timers)(CancellationToken.None);
        });

        var mine = ForTree(measurements, GrainKey);
        Assert.That(mine.Sum(m => m.Value), Is.Zero,
            "A tick that returned normally is not a discarded-work event.");
    }

    [Test]
    public async Task The_failure_carries_the_coordinator_kind_and_the_tree_it_serves()
    {
        var h = Create();

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
            await CapturedTick(h.Timers)(CancellationToken.None);
        });

        var failure = ForTree(measurements, GrainKey).Single(m => m.Value == 1);
        Assert.Multiple(() =>
        {
            Assert.That(failure.Tags[LatticeMetrics.TagKind], Is.EqualTo(TestCoordinator.ReminderName),
                "The kind tag is what separates a stalled snapshot coordinator from a stalled resize.");
            Assert.That(failure.Tags[LatticeMetrics.TagTree], Is.EqualTo(GrainKey));
        });
    }

    [Test]
    public async Task A_composite_key_coordinator_tags_the_subject_alone()
    {
        // A coordinator keyed 'tree/shard' that tagged the raw key would emit a
        // fresh series per shard, which no dashboard can group by tree.
        var h = Create(CompositeGrainKey, composite: true);

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
            await CapturedTick(h.Timers)(CancellationToken.None);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ForTree(measurements, "tree-b").Sum(m => m.Value), Is.EqualTo(1));
            Assert.That(ForTree(measurements, CompositeGrainKey), Is.Empty,
                "The raw composite key must never reach the tag.");
        });
    }

    // ------------------------------------------------------------- zero-priming

    [Test]
    public async Task Arming_the_phase_timer_primes_the_failure_series_at_zero()
    {
        // Without this, a coordinator that has never failed exports nothing at
        // all, and an operator cannot tell 'no work discarded' from 'instrument
        // never wired' - which is the exact defect this issue fixes.
        var h = Create();

        var measurements = await RecordAsync(() =>
        {
            h.Grain.ArmPhaseTimer();
            return Task.CompletedTask;
        });

        var mine = ForTree(measurements, GrainKey);
        Assert.Multiple(() =>
        {
            Assert.That(mine, Is.Not.Empty,
                "A coordinator that armed its pump must export a series before it can fail.");
            Assert.That(mine.Sum(m => m.Value), Is.Zero);
            Assert.That(mine[0].Tags[LatticeMetrics.TagKind], Is.EqualTo(TestCoordinator.ReminderName),
                "The primed series must carry the same tags the failure will, or it primes a different series.");
        });
    }

    [Test]
    public async Task The_primed_series_carries_the_same_tags_a_later_failure_does()
    {
        // A prime whose tag set differs from the failure's is worse than no
        // prime: it exports a permanent zero beside a series that appears from
        // nowhere on first failure.
        var h = Create();

        var measurements = await RecordAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
            await CapturedTick(h.Timers)(CancellationToken.None);
        });

        var mine = ForTree(measurements, GrainKey);
        var prime = mine.First(m => m.Value == 0);
        var failure = mine.First(m => m.Value == 1);

        Assert.That(failure.Tags, Is.EqualTo(prime.Tags).AsCollection);
    }

    [Test]
    public async Task Re_arming_an_already_running_timer_does_not_re_prime()
    {
        var h = Create();

        var measurements = await RecordAsync(() =>
        {
            h.Grain.ArmPhaseTimer();
            h.Grain.ArmPhaseTimer();
            h.Grain.ArmPhaseTimer();
            return Task.CompletedTask;
        });

        Assert.That(ForTree(measurements, GrainKey), Has.Count.EqualTo(1),
            "The guard that keeps one timer must keep one prime.");
    }

    // ------------------------------------------------------------- log severity

    [Test]
    public async Task An_isolated_failure_is_logged_at_warning()
    {
        var h = Create();
        h.Grain.ArmPhaseTimer();
        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");

        await CapturedTick(h.Timers)(CancellationToken.None);

        Assert.That(h.Logger.Lines.Select(l => l.Level), Is.EqualTo(new[] { LogLevel.Warning }).AsCollection);
    }

    [Test]
    public async Task A_run_of_consecutive_failures_escalates_to_error()
    {
        // A single swallowed tick is a transient the pump absorbs. A run of them
        // is a phase loop that has stopped advancing, and nothing else in the
        // system reports that.
        var h = Create();
        h.Grain.ArmPhaseTimer();
        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        var tick = CapturedTick(h.Timers);

        for (var i = 0; i < 4; i++)
        {
            await tick(CancellationToken.None);
        }

        Assert.That(
            h.Logger.Lines.Select(l => l.Level),
            Is.EqualTo(new[] { LogLevel.Warning, LogLevel.Warning, LogLevel.Error, LogLevel.Error }).AsCollection);
    }

    [Test]
    public async Task A_successful_tick_resets_the_escalation_run()
    {
        // Two failures an hour apart, either side of hundreds of healthy ticks,
        // are not a stalled loop and must not be reported as one.
        var h = Create();
        h.Grain.ArmPhaseTimer();
        var tick = CapturedTick(h.Timers);

        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        await tick(CancellationToken.None);
        await tick(CancellationToken.None);

        h.Grain.PhaseThrow = null;
        await tick(CancellationToken.None);

        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        await tick(CancellationToken.None);
        await tick(CancellationToken.None);

        Assert.That(
            h.Logger.Lines.Select(l => l.Level),
            Is.EqualTo(new[] { LogLevel.Warning, LogLevel.Warning, LogLevel.Warning, LogLevel.Warning }).AsCollection,
            "The run counter must restart from the first tick that advanced the phase machine.");
    }
}
