using System.Diagnostics;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the storage-pressure half of the WAL replay permit controller
/// (issue #5877): what counts as pressure, how far and how fast the ceiling
/// walks, and what never moves it.
/// </summary>
[TestFixture]
public class ReplayStoragePressureGovernorTests
{
    private const long Window = 100;

    /// <summary>Builds cumulative tallies the way the process-wide recorder accumulates them.</summary>
    private sealed class Tallies
    {
        private readonly long[] _buckets = new long[ReplayStoragePressure.LatencyBuckets];
        private long _reads;
        private long _retries;

        public ReplayStoragePressureSample Add(int reads, int latencyMs, int retries = 0)
        {
            _buckets[ReplayStoragePressure.BucketOf(latencyMs * Stopwatch.Frequency / 1000)] += reads;
            _reads += reads;
            _retries += retries;
            return Sample;
        }

        public ReplayStoragePressureSample Sample => new(_reads, _retries, (long[])_buckets.Clone());
    }

    private static (ReplayStoragePressureGovernor Governor, Tallies Tallies, long Now) Started()
    {
        var governor = new ReplayStoragePressureGovernor(Window);
        var tallies = new Tallies();
        Assert.That(governor.Observe(0, tallies.Sample, 32), Is.Null, "the first observation only opens a window");
        return (governor, tallies, 0);
    }

    private static void SeedBaseline(ReplayStoragePressureGovernor governor, Tallies tallies, ref long now, int latencyMs)
    {
        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(20, latencyMs), 32), Is.Null);
    }

    [Test]
    public void Throttle_retries_on_one_percent_of_a_full_window_withhold_a_quarter_of_the_circulating_permits()
    {
        var (governor, tallies, now) = Started();

        var step = governor.Observe(now + Window, tallies.Add(100, 4, retries: 1), 32);

        Assert.That(step, Is.EqualTo(new ReplayStoragePressureStep(-1, ReplayStoragePressureReason.Throttle)));
        Assert.That(governor.Target, Is.EqualTo(8));
    }

    [Test]
    public void Slice_p99_at_twice_an_unpressured_baseline_withholds_with_the_latency_reason()
    {
        var (governor, tallies, now) = Started();
        SeedBaseline(governor, tallies, ref now, 4);

        var step = governor.Observe(now + Window, tallies.Add(20, 16), 32);

        Assert.That(step, Is.EqualTo(new ReplayStoragePressureStep(-1, ReplayStoragePressureReason.Latency)));
        Assert.That(governor.Target, Is.EqualTo(8));
    }

    [Test]
    public void A_window_short_of_the_minimum_samples_is_not_judged_pressured()
    {
        var (governor, tallies, now) = Started();
        SeedBaseline(governor, tallies, ref now, 4);

        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(49, 4, retries: 49), 32), Is.Null,
            "49 reads is below the throttle-ratio minimum of 50, however many retried");
        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(19, 1000), 32), Is.Null,
            "19 reads is below the p99 minimum of 20, however slow");
        Assert.That(governor.Target, Is.Zero);
    }

    [Test]
    public void Latency_is_not_judged_until_the_baseline_is_seeded()
    {
        var (governor, tallies, now) = Started();

        Assert.That(governor.Observe(now + Window, tallies.Add(20, 1000), 32), Is.Null,
            "a deployment that starts slow has nothing to be slow against");
        Assert.That(governor.Target, Is.Zero);
    }

    [Test]
    public void The_baseline_is_taken_only_from_unpressured_windows()
    {
        var (governor, tallies, now) = Started();

        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(100, 1000, retries: 100), 32)?.Reason,
            Is.EqualTo(ReplayStoragePressureReason.Throttle));
        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(20, 8), 32), Is.Null);

        // Seeded at 8 ms, not at the throttled window's 1000 ms: 16 ms is pressure.
        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(20, 16), 32)?.Reason,
            Is.EqualTo(ReplayStoragePressureReason.Latency));
    }

    [Test]
    public void A_sparse_window_never_steps_down()
    {
        var (governor, tallies, now) = Started();
        SeedBaseline(governor, tallies, ref now, 4);

        foreach (var reads in new[] { 0, 1, 10, 19 })
        {
            now += Window;
            Assert.That(governor.Observe(now, tallies.Add(reads, 5000, retries: reads), 32), Is.Null,
                $"a window of {reads} reads cannot carry a verdict");
        }

        Assert.That(governor.Target, Is.Zero);
    }

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(4)]
    [TestCase(32)]
    public void Sustained_pressure_never_withholds_the_last_permit(int ceiling)
    {
        var governor = new ReplayStoragePressureGovernor(Window);
        var tallies = new Tallies();
        governor.Observe(0, tallies.Sample, ceiling);

        for (var window = 1; window <= 40; window++)
        {
            governor.Observe(window * Window, tallies.Add(100, 4, retries: 100), ceiling);
            Assert.That(governor.Target, Is.InRange(0, ceiling - 1));
        }

        Assert.That(governor.Target, Is.EqualTo(ceiling - 1), "pressure walks down to the floor of one");
    }

    [Test]
    public void Recovery_never_returns_more_than_was_withheld()
    {
        var (governor, tallies, now) = Started();
        now += Window;
        governor.Observe(now, tallies.Add(100, 4, retries: 100), 32);

        for (var window = 0; window < 100; window++)
        {
            now += Window;
            governor.Observe(now, tallies.Add(20, 4), 32);
            Assert.That(governor.Target, Is.GreaterThanOrEqualTo(0));
        }

        Assert.That(governor.Target, Is.Zero, "the governor can only lower concurrency, never raise it past the ceiling");
    }

    [Test]
    public void Steps_down_fast_and_back_up_one_permit_per_three_clean_windows()
    {
        var (governor, tallies, now) = Started();
        now += Window;
        governor.Observe(now, tallies.Add(100, 4, retries: 100), 32);
        Assert.That(governor.Target, Is.EqualTo(8), "one pressured window withholds a quarter");

        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(100, 4, retries: 100), 32), Is.Null,
            "the window after a step is a cooldown, so a step is never re-judged on its own aftermath");
        now += Window;
        governor.Observe(now, tallies.Add(100, 4, retries: 100), 32);
        Assert.That(governor.Target, Is.EqualTo(14), "a quarter of the 24 still circulating, rounded up");

        var steps = new List<ReplayStoragePressureStep?>();
        for (var window = 0; window < 6; window++)
        {
            now += Window;
            steps.Add(governor.Observe(now, tallies.Add(20, 4), 32));
        }

        Assert.That(steps, Is.EqualTo(new ReplayStoragePressureStep?[]
        {
            null, null, new(1, ReplayStoragePressureReason.Recovery),
            null, null, new(1, ReplayStoragePressureReason.Recovery),
        }));
        Assert.That(governor.Target, Is.EqualTo(12));
    }

    [Test]
    public void A_pressured_window_resets_the_clean_run_toward_recovery()
    {
        var (governor, tallies, now) = Started();
        now += Window;
        governor.Observe(now, tallies.Add(100, 4, retries: 100), 32);
        now += Window;
        governor.Observe(now, tallies.Add(20, 4), 32);
        now += Window;
        governor.Observe(now, tallies.Add(20, 4), 32);
        now += Window;
        governor.Observe(now, tallies.Add(100, 4, retries: 100), 32);

        for (var window = 0; window < 2; window++)
        {
            now += Window;
            governor.Observe(now, tallies.Add(20, 4), 32);
        }

        Assert.That(governor.Target, Is.GreaterThanOrEqualTo(8),
            "two clean windows after a pressured one are not three in a row");
    }

    [Test]
    public void A_window_still_open_does_not_step()
    {
        var (governor, tallies, now) = Started();

        Assert.That(governor.IsDue(now + Window - 1), Is.False);
        Assert.That(governor.Observe(now + Window - 1, tallies.Add(100, 4, retries: 100), 32), Is.Null);
        Assert.That(governor.IsDue(now + Window), Is.True);
    }

    [TestCase(1, 31, 33)]
    [TestCase(3, 31, 33)]
    [TestCase(3, 60, 66)]
    [TestCase(3, 120, 132)]
    [TestCase(3, 250, 275)]
    public void A_ten_percent_drift_across_a_power_of_two_never_reads_as_doubled(int windows, int baselineMs, int driftMs)
    {
        var (governor, tallies, now) = Started();
        SeedBaseline(governor, tallies, ref now, baselineMs);

        for (var window = 0; window < windows; window++)
        {
            now += Window;
            Assert.That(governor.Observe(now, tallies.Add(20, driftMs), 32), Is.Null,
                $"{baselineMs} ms to {driftMs} ms is a drift, not a doubling");
        }

        Assert.That(governor.Target, Is.Zero);
    }

    [Test]
    public void Quiet_time_with_no_throttling_returns_every_withheld_permit_in_one_idle_step()
    {
        var (governor, tallies, now) = Started();
        now += Window;
        governor.Observe(now, tallies.Add(100, 4, retries: 100), 32);
        Assert.That(governor.Target, Is.EqualTo(8));

        now += ReplayStoragePressureGovernor.IdleWindowsToReset * Window;
        var step = governor.Observe(now, tallies.Add(3, 4), 32);

        Assert.That(step, Is.EqualTo(new ReplayStoragePressureStep(1, ReplayStoragePressureReason.Idle)));
        Assert.That(governor.Target, Is.Zero, "the next storm starts at the full ceiling");
    }

    [Test]
    public void Quiet_windows_accumulate_toward_the_idle_reset_but_a_throttle_retry_breaks_the_run()
    {
        var (governor, tallies, now) = Started();
        now += Window;
        governor.Observe(now, tallies.Add(100, 4, retries: 100), 32);

        for (var window = 1; window < ReplayStoragePressureGovernor.IdleWindowsToReset; window++)
        {
            now += Window;
            Assert.That(governor.Observe(now, tallies.Add(0, 4), 32), Is.Null);
        }

        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(0, 4, retries: 1), 32), Is.Null,
            "a throttle retry, however few reads, is not quiet");
        Assert.That(governor.Target, Is.EqualTo(8));

        for (var window = 1; window < ReplayStoragePressureGovernor.IdleWindowsToReset; window++)
        {
            now += Window;
            Assert.That(governor.Observe(now, tallies.Add(0, 4), 32), Is.Null);
        }

        now += Window;
        Assert.That(governor.Observe(now, tallies.Add(0, 4), 32)?.Reason, Is.EqualTo(ReplayStoragePressureReason.Idle));
    }

    [TestCase(408, true)]
    [TestCase(429, true)]
    [TestCase(500, true)]
    [TestCase(502, true)]
    [TestCase(503, true)]
    [TestCase(504, true)]
    [TestCase(0, false)]
    [TestCase(404, false)]
    [TestCase(409, false)]
    [TestCase(412, false)]
    [NonParallelizable]
    public void Only_throttling_and_unavailability_retries_count_as_storage_pressure(int status, bool counts)
    {
        ReplayStoragePressure.ResetForTest();
        try
        {
            ReplayStoragePressure.NoteStorageRetry(status);
            Assert.That(ReplayStoragePressure.Read().ThrottleRetries, Is.EqualTo(counts ? 1 : 0));
        }
        finally
        {
            ReplayStoragePressure.ResetForTest();
        }
    }

    [Test]
    public void The_p99_is_the_bucket_holding_the_ninety_ninth_percentile_read_of_the_window()
    {
        var tallies = new Tallies();
        var start = tallies.Add(1000, 1000);
        tallies.Add(99, 4);
        var end = tallies.Add(1, 3000);

        Assert.That(end.P99MillisecondsSince(start), Is.EqualTo(4.096).Within(1e-9),
            "99 of 100 window reads took 4 ms, in the [3.584, 4.096) ms bucket; earlier slow reads are outside the window");
        Assert.That(start.P99MillisecondsSince(start), Is.Null);
    }
}
