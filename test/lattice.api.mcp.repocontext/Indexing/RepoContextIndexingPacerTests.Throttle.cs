using NSubstitute;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Pins the pacer's handling of a vector-tree throttle its own rate cannot move, and
/// of a baseline that must not be dragged down by a small or backed-off batch (issue
/// #3456). Before the fix a tree held <see cref="WalSaturationState.Throttled"/> by a
/// frozen materialiser drain-lag minimum doubled the delay on every batch and held it
/// at <see cref="RepoContextIndexingOptions.PacingMaxBatchDelay"/> for the life of the
/// process, however clean and fast the batches ran.
/// </summary>
public sealed partial class RepoContextIndexingPacerTests
{
    private static readonly TimeSpan CleanLatency = TimeSpan.FromMilliseconds(100);

    /// <summary>A signal whose states are read live from <paramref name="states"/> on every call.</summary>
    private static IWalSaturationSignal LiveSignal(Dictionary<string, WalSaturationState> states)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>())
            .Returns(call => states.TryGetValue(call.Arg<string>(), out var state) ? state : WalSaturationState.Healthy);
        return signal;
    }

    /// <summary>
    /// Runs clean batches, each followed by <paramref name="gap"/> of virtual time
    /// standing in for the pacer's own inter-batch delay, until
    /// <paramref name="span"/> has elapsed, and returns the delay after each one.
    /// </summary>
    private static List<long> RunCleanBatches(
        ManualTimeProvider clock, RepoContextIndexingPacer pacer, TimeSpan span, TimeSpan gap)
    {
        var delays = new List<long>();
        var start = clock.GetUtcNow();
        while (clock.GetUtcNow() - start < span)
        {
            Batch(clock, pacer, succeeded: true, CleanLatency);
            delays.Add(pacer.Snapshot().BatchDelayMilliseconds);
            clock.Advance(gap);
        }

        return delays;
    }

    [Test]
    public void RecordBatch_throttle_that_outlasts_the_influence_window_at_the_ceiling_stops_counting()
    {
        // The regression: a membership tree held Throttled by materialiser_drain_lag
        // that nothing indexing does can clear, with every batch running clean.
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled));

        var delays = RunCleanBatches(
            clock,
            pacer,
            RepoContextIndexingPacer.ThrottleInfluenceWindow + TimeSpan.FromMinutes(2),
            TimeSpan.FromSeconds(5));
        var snapshot = pacer.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(delays, Does.Contain(5000L), "The pacer still backs off a fresh throttle to the ceiling first.");
            Assert.That(delays[^1], Is.Zero, "A throttle that ignores the backoff must not latch the delay at the ceiling.");
            Assert.That(snapshot.State, Is.EqualTo(RepoIndexPaceState.Pacing));
            Assert.That(snapshot.Reason, Does.Contain(RepoContextTrees.VectorMembership).And.Contain("advisory"));
        });
    }

    [Test]
    public void RecordBatch_throttle_still_counts_until_the_influence_window_elapses_at_the_ceiling()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled));

        // 250 -> 5000 ms takes five batches; the probe starts on the first batch seen
        // at the ceiling, so just short of the window after that the delay must hold.
        var delays = RunCleanBatches(
            clock,
            pacer,
            RepoContextIndexingPacer.ThrottleInfluenceWindow,
            TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(delays.Take(5), Is.EqualTo(new long[] { 250, 500, 1000, 2000, 4000 }));
            Assert.That(delays.Skip(5), Is.All.EqualTo(5000L), "Inside the window the throttle still counts as congestion.");
            Assert.That(pacer.Snapshot().Reason, Does.Contain("is throttled"));
        });
    }

    [Test]
    public void RecordBatch_throttle_below_the_ceiling_does_not_start_the_influence_probe()
    {
        // A ceiling above the reach of the time run means the delay is never at the
        // ceiling, so however long the throttle holds it keeps counting.
        var clock = new ManualTimeProvider();
        var pacer = Create(
            clock,
            Options(maxDelay: TimeSpan.FromHours(1)),
            Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled));

        for (var i = 0; i < 30; i++)
        {
            Batch(clock, pacer, succeeded: true, CleanLatency);
            clock.Advance(TimeSpan.FromSeconds(10));
        }

        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.GreaterThan(0));
    }

    [Test]
    public void RecordBatch_throttle_that_clears_while_backing_off_never_becomes_advisory()
    {
        // The throttle indexing does influence: it clears once the drain backs off.
        var states = new Dictionary<string, WalSaturationState>
        {
            [RepoContextTrees.VectorMembership] = WalSaturationState.Throttled,
        };
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: LiveSignal(states));

        RunCleanBatches(clock, pacer, TimeSpan.FromSeconds(40), TimeSpan.FromSeconds(5));
        states[RepoContextTrees.VectorMembership] = WalSaturationState.Healthy;
        RunCleanBatches(clock, pacer, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(1));
        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero);

        // It comes back: it must count straight away, not inherit any earlier probe.
        states[RepoContextTrees.VectorMembership] = WalSaturationState.Throttled;
        Batch(clock, pacer, succeeded: true, CleanLatency);

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain("is throttled"));
        });
    }

    [Test]
    public void RecordBatch_advisory_throttle_that_clears_and_returns_is_probed_afresh()
    {
        var states = new Dictionary<string, WalSaturationState>
        {
            [RepoContextTrees.VectorMembership] = WalSaturationState.Throttled,
        };
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: LiveSignal(states));
        RunCleanBatches(clock, pacer, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(5));
        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "precondition: the throttle became advisory");

        states[RepoContextTrees.VectorMembership] = WalSaturationState.Healthy;
        Batch(clock, pacer, succeeded: true, CleanLatency);
        states[RepoContextTrees.VectorMembership] = WalSaturationState.Throttled;
        Batch(clock, pacer, succeeded: true, CleanLatency);

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain("is throttled").And.Not.Contain("advisory"));
        });
    }

    [Test]
    public void RecordBatch_advisory_throttle_that_worsens_counts_again()
    {
        var states = new Dictionary<string, WalSaturationState>
        {
            [RepoContextTrees.VectorMembership] = WalSaturationState.Throttled,
        };
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: LiveSignal(states));
        RunCleanBatches(clock, pacer, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(5));
        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "precondition: the throttle became advisory");

        states[RepoContextTrees.VectorMembership] = WalSaturationState.Saturated;
        Batch(clock, pacer, succeeded: true, CleanLatency);

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain(RepoContextTrees.VectorMembership));
        });
    }

    [Test]
    public void RecordBatch_advisory_throttle_on_one_tree_does_not_hide_a_throttle_on_another()
    {
        var states = new Dictionary<string, WalSaturationState>
        {
            [RepoContextTrees.VectorMembership] = WalSaturationState.Throttled,
        };
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: LiveSignal(states));
        RunCleanBatches(clock, pacer, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(5));

        states[RepoContextTrees.VectorPayload] = WalSaturationState.Throttled;
        Batch(clock, pacer, succeeded: true, CleanLatency);

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain(RepoContextTrees.VectorPayload));
        });
    }

    [Test]
    public void RecordBatch_advisory_throttle_still_lets_other_congestion_back_off()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled));
        RunCleanBatches(clock, pacer, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(5));

        Batch(clock, pacer, succeeded: false);

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain("failed"));
        });
    }

    [Test]
    public async Task PaceAsync_advisory_throttle_survives_an_idle_gap()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled));
        RunCleanBatches(clock, pacer, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(5));

        clock.Advance(RepoContextIndexingPacer.IdleAfter + TimeSpan.FromSeconds(1));
        var startedAt = await pacer.PaceAsync(CancellationToken.None);
        var pacing = pacer.Snapshot();
        clock.Advance(CleanLatency);
        pacer.RecordBatch(startedAt, succeeded: true);

        Assert.Multiple(() =>
        {
            Assert.That(pacing.State, Is.EqualTo(RepoIndexPaceState.Pacing));
            Assert.That(pacing.Reason, Does.Contain("advisory"), "PaceAsync reports the advisory throttle too.");
            Assert.That(
                pacer.Snapshot().BatchDelayMilliseconds,
                Is.Zero,
                "A throttle already shown not to respond is not re-probed just because the pacer went idle.");
        });
    }

    [Test]
    public void LargestBatchUnits_tracks_the_largest_batch_recorded()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        Assert.That(pacer.LargestBatchUnits, Is.Zero);

        BatchOf(clock, pacer, units: 32, CleanLatency);
        BatchOf(clock, pacer, units: 5, CleanLatency);

        Assert.That(pacer.LargestBatchUnits, Is.EqualTo(32));
    }

    [Test]
    public void RecordBatch_non_positive_units_throws()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);

        Assert.Multiple(() =>
        {
            Assert.That(() => pacer.RecordBatch(clock.GetTimestamp(), true, units: 0), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => pacer.RecordBatch(clock.GetTimestamp(), true, units: -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void RecordBatch_latency_is_judged_per_passage()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);

        BatchOf(clock, pacer, units: 32, TimeSpan.FromMilliseconds(1000));
        BatchOf(clock, pacer, units: 64, TimeSpan.FromMilliseconds(2000));
        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "Twice the passages in twice the time is the same rate.");

        BatchOf(clock, pacer, units: 64, TimeSpan.FromMilliseconds(6000));

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain("per-passage baseline"));
        });
    }

    [Test]
    public void RecordBatch_small_remainder_batch_neither_lowers_the_baseline_nor_counts_as_congestion()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);

        BatchOf(clock, pacer, units: 64, TimeSpan.FromMilliseconds(6400));

        // A 2-passage tail: 900 ms is 450 ms each, over 2.5x the 100 ms baseline, but
        // it is mostly fixed overhead. Nor may its size drag the baseline down.
        BatchOf(clock, pacer, units: 2, TimeSpan.FromMilliseconds(900));
        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "A remainder batch is not judged.");

        BatchOf(clock, pacer, units: 2, TimeSpan.FromMilliseconds(20));
        BatchOf(clock, pacer, units: 64, TimeSpan.FromMilliseconds(7000));

        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "A fast tail must not lower the bar for full batches.");
    }

    [Test]
    public void RecordBatch_batch_run_under_a_backoff_delay_does_not_lower_the_baseline()
    {
        // The overnight ratchet: batches run while the drain holds back are fast
        // because the host is quiet, and a baseline learned from them reads the first
        // full-rate batch as congestion.
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(1000));
        Batch(clock, pacer, succeeded: false);
        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250), "precondition: backing off");

        for (var i = 0; i < 10 && pacer.Snapshot().BatchDelayMilliseconds > 0; i++)
        {
            Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(300));
        }

        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "precondition: recovered to the full rate");

        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(1000));

        Assert.That(
            pacer.Snapshot().BatchDelayMilliseconds,
            Is.Zero,
            "A batch at the rate the baseline was learned at must not read as congestion.");
    }

    [Test]
    public void RecordBatch_batch_at_the_full_rate_still_lowers_the_baseline()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(1000));
        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(200));

        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(600));

        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250), "The baseline fell to 200 ms.");
    }

    private static void BatchOf(ManualTimeProvider clock, RepoContextIndexingPacer pacer, int units, TimeSpan latency)
    {
        var startedAt = clock.GetTimestamp();
        clock.Advance(latency);
        pacer.RecordBatch(startedAt, succeeded: true, units);
    }
}
