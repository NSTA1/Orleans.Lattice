using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for <see cref="RepoContextIndexingPacer"/> (issue #3447): the delay
/// controller (a congested batch doubles the delay up to the ceiling, a clean one
/// walks it back to zero), each congestion cause, the bounded saturation wait and
/// foreground yield, the duty-cycle rest, the idle reset, the background-deferral
/// predicate, and the <c>index_status</c> snapshot. Every wait runs on the shared
/// <see cref="ManualTimeProvider"/>, so each bound is asserted in virtual time.
/// </summary>
[TestFixture]
public sealed partial class RepoContextIndexingPacerTests
{
    private static RepoContextIndexingOptions Options(
        bool pacing = true,
        TimeSpan? slice = null,
        TimeSpan? rest = null,
        TimeSpan? maxDelay = null) => new()
        {
            Pacing = pacing,
            PacingSliceDuration = slice ?? TimeSpan.FromMinutes(10),
            PacingSliceRest = rest ?? TimeSpan.FromSeconds(5),
            PacingMaxBatchDelay = maxDelay ?? TimeSpan.FromSeconds(5),
        };

    private static RepoContextIndexingPacer Create(
        ManualTimeProvider clock,
        RepoContextIndexingOptions? options = null,
        IWalSaturationSignal? saturation = null,
        Func<double>? memoryLoad = null)
        => new(
            options ?? Options(),
            clock,
            NullLogger<RepoContextIndexingPacer>.Instance,
            saturation,
            memoryLoad ?? (() => 0.1));

    private static IWalSaturationSignal Signal(string? tree = null, WalSaturationState state = WalSaturationState.Healthy)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Healthy);
        if (tree is not null)
        {
            signal.GetCurrentState(tree).Returns(state);
        }

        return signal;
    }

    /// <summary>Records one batch that took <paramref name="latency"/> of virtual time.</summary>
    private static void Batch(ManualTimeProvider clock, RepoContextIndexingPacer pacer, bool succeeded, TimeSpan latency = default)
    {
        var startedAt = clock.GetTimestamp();
        clock.Advance(latency);
        pacer.RecordBatch(startedAt, succeeded);
    }

    /// <summary>
    /// Advances virtual time one <paramref name="step"/> per poll until
    /// <paramref name="task"/> completes, and returns the virtual time that took.
    /// Stops advancing at <paramref name="limit"/>, so a wait that never ends fails
    /// at the barrier rather than running the clock on forever.
    /// </summary>
    private static async Task<TimeSpan> DriveToCompletionAsync(
        ManualTimeProvider clock, Task task, TimeSpan step, TimeSpan limit, string because)
    {
        var start = clock.GetUtcNow();
        await TestPoll.UntilAsync(
            () =>
            {
                if (task.IsCompleted)
                {
                    return true;
                }

                if (clock.GetUtcNow() - start < limit)
                {
                    clock.Advance(step);
                }

                return task.IsCompleted;
            },
            because);
        return clock.GetUtcNow() - start;
    }

    /// <summary>
    /// Advances virtual time by <paramref name="span"/>, one <paramref name="step"/>
    /// per poll, so a wait that re-arms a timer on every step can observe each one.
    /// For the negative half of a bound: the caller asserts the task is still pending.
    /// </summary>
    private static async Task AdvanceStepwiseAsync(ManualTimeProvider clock, TimeSpan step, TimeSpan span)
    {
        var start = clock.GetUtcNow();
        await TestPoll.TryUntilAsync(() =>
        {
            if (clock.GetUtcNow() - start >= span)
            {
                return true;
            }

            clock.Advance(step);
            return false;
        });
    }

    [Test]
    public void Constructor_null_argument_throws()
    {
        var clock = new ManualTimeProvider();
        var logger = NullLogger<RepoContextIndexingPacer>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextIndexingPacer(null!, clock, logger), Throws.ArgumentNullException);
            Assert.That(() => new RepoContextIndexingPacer(Options(), null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new RepoContextIndexingPacer(Options(), clock, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task PaceAsync_pacing_disabled_never_waits_backs_off_or_defers()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, Options(pacing: false), Signal(RepoContextTrees.VectorPayload, WalSaturationState.Saturated));
        using var lease = pacer.EnterForeground();

        Batch(clock, pacer, succeeded: false);
        var pace = pacer.PaceAsync(CancellationToken.None);
        var snapshot = pacer.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Enabled, Is.False);
            Assert.That(pace.IsCompleted, Is.True, "A disabled pacer must not wait, even on a saturated tree.");
            Assert.That(pacer.ShouldDeferBackground(out var reason), Is.False);
            Assert.That(reason, Is.Empty);
            Assert.That(snapshot.State, Is.EqualTo(RepoIndexPaceState.Disabled));
            Assert.That(snapshot.Reason, Does.Contain(RepoContextIndexingOptions.PacingKey));
            Assert.That(snapshot.BatchDelayMilliseconds, Is.Zero, "A failure must not raise a disabled pacer's delay.");
            Assert.That(snapshot.ForegroundRequests, Is.EqualTo(1));
        });
        await pace;
    }

    [Test]
    public void Snapshot_before_any_batch_is_idle()
    {
        var pacer = Create(new ManualTimeProvider());
        var snapshot = pacer.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Enabled, Is.True);
            Assert.That(snapshot.State, Is.EqualTo(RepoIndexPaceState.Idle));
            Assert.That(snapshot.BatchDelayMilliseconds, Is.Zero);
            Assert.That(snapshot.Since, Is.Null);
            Assert.That(snapshot.ForegroundRequests, Is.Zero);
        });
    }

    [Test]
    public void RecordBatch_failed_batch_doubles_the_delay_up_to_the_ceiling()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, Options(maxDelay: TimeSpan.FromSeconds(1)));
        var observed = new List<long>();

        for (var i = 0; i < 5; i++)
        {
            Batch(clock, pacer, succeeded: false);
            observed.Add(pacer.Snapshot().BatchDelayMilliseconds);
        }

        var snapshot = pacer.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.EqualTo(new long[] { 250, 500, 1000, 1000, 1000 }));
            Assert.That(snapshot.State, Is.EqualTo(RepoIndexPaceState.Backoff));
            Assert.That(snapshot.Reason, Does.Contain("failed"));
            Assert.That(snapshot.Since, Is.Not.Null);
        });
    }

    [Test]
    public void RecordBatch_clean_batches_walk_the_delay_back_to_zero()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        for (var i = 0; i < 5; i++)
        {
            Batch(clock, pacer, succeeded: false);
        }

        var delays = new List<long> { pacer.Snapshot().BatchDelayMilliseconds };
        for (var i = 0; i < 50 && delays[^1] > 0; i++)
        {
            Batch(clock, pacer, succeeded: true);
            delays.Add(pacer.Snapshot().BatchDelayMilliseconds);
        }

        Assert.Multiple(() =>
        {
            // 250 doubled four times, under the 5 s ceiling.
            Assert.That(delays[0], Is.EqualTo(4000));
            Assert.That(delays[1], Is.EqualTo(3000), "A clean batch takes a quarter off a large delay.");
            Assert.That(delays, Is.Ordered.Descending, "Every clean batch shortens the delay.");
            Assert.That(delays[^1], Is.Zero, "Enough clean batches return the pacer to the full rate.");
            Assert.That(pacer.Snapshot().State, Is.EqualTo(RepoIndexPaceState.Pacing));
        });
    }

    [Test]
    public void RecordBatch_latency_over_the_baseline_ratio_counts_as_congestion()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);

        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(100));
        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "The first batch only sets the baseline.");

        // 400 ms is over both 2.5 x the 100 ms baseline and the 250 ms floor.
        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(400));

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain("100 ms baseline"));
        });
    }

    [Test]
    public void RecordBatch_latency_under_the_ratio_is_not_congestion()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);

        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(200));
        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(400));

        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero, "Twice the baseline is under the 2.5 x ratio.");
    }

    [Test]
    public void RecordBatch_latency_under_the_congestion_floor_is_not_congestion()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);

        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(40));
        Batch(clock, pacer, succeeded: true, TimeSpan.FromMilliseconds(200));

        Assert.That(
            pacer.Snapshot().BatchDelayMilliseconds,
            Is.Zero,
            "Five times a tiny baseline is still jitter while it is under the floor.");
    }

    [Test]
    public void RecordBatch_gc_at_its_high_load_threshold_counts_as_congestion()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, memoryLoad: () => 1.0);

        Batch(clock, pacer, succeeded: true);

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain("memory load"));
        });
    }

    [Test]
    public void RecordBatch_gc_under_its_high_load_threshold_is_not_congestion()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, memoryLoad: () => 0.99);

        Batch(clock, pacer, succeeded: true);

        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero);
    }

    [Test]
    public void RecordBatch_throttled_vector_tree_counts_as_congestion()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled));

        Batch(clock, pacer, succeeded: true);

        Assert.Multiple(() =>
        {
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.EqualTo(250));
            Assert.That(pacer.Snapshot().Reason, Does.Contain(RepoContextTrees.VectorMembership));
        });
    }

    [Test]
    public void RecordBatch_throttled_tree_outside_the_vector_plane_is_ignored()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal("some-other-tree", WalSaturationState.Throttled));

        Batch(clock, pacer, succeeded: true);

        Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero);
    }
}
