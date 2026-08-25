using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="TenantTokenBucket"/> (the lock-free GCRA hot path).</summary>
public sealed class TenantTokenBucketTests
{
    [Test]
    public void ComputeEmissionIntervalTicks_divides_frequency_by_rate()
    {
        Assert.That(TenantTokenBucket.ComputeEmissionIntervalTicks(10, Frequency), Is.EqualTo(Frequency / 10));
    }

    [Test]
    public void ComputeEmissionIntervalTicks_floors_non_positive_rate_to_one_op()
    {
        Assert.That(TenantTokenBucket.ComputeEmissionIntervalTicks(0, Frequency), Is.EqualTo(Frequency));
        Assert.That(TenantTokenBucket.ComputeEmissionIntervalTicks(-5, Frequency), Is.EqualTo(Frequency));
    }

    [Test]
    public void ComputeEmissionIntervalTicks_never_returns_below_one()
    {
        Assert.That(TenantTokenBucket.ComputeEmissionIntervalTicks(long.MaxValue, Frequency), Is.EqualTo(1));
    }

    [Test]
    public void ComputeBurstToleranceTicks_is_zero_when_burst_is_not_positive()
    {
        Assert.That(TenantTokenBucket.ComputeBurstToleranceTicks(100, 0, Frequency), Is.EqualTo(0));
        Assert.That(TenantTokenBucket.ComputeBurstToleranceTicks(100, -10, Frequency), Is.EqualTo(0));
    }

    [Test]
    public void ComputeBurstToleranceTicks_is_burst_tokens_times_emission()
    {
        // 100 ops/sec, 20% burst => 20 burst tokens; tau = 20 * emission.
        var emission = TenantTokenBucket.ComputeEmissionIntervalTicks(100, Frequency);
        Assert.That(
            TenantTokenBucket.ComputeBurstToleranceTicks(100, 20, Frequency),
            Is.EqualTo(20 * emission));
    }

    [Test]
    public void ComputeBurstToleranceTicks_rounds_a_positive_burst_up_to_at_least_one_token()
    {
        // 1 op/sec, 20% burst => 0.2 tokens, floored up to 1 token.
        var emission = TenantTokenBucket.ComputeEmissionIntervalTicks(1, Frequency);
        Assert.That(TenantTokenBucket.ComputeBurstToleranceTicks(1, 20, Frequency), Is.EqualTo(emission));
    }

    [Test]
    public void Properties_reflect_the_construction_parameters()
    {
        var bucket = new TenantTokenBucket(emissionIntervalTicks: 123, burstToleranceTicks: 456);

        Assert.Multiple(() =>
        {
            Assert.That(bucket.EmissionIntervalTicks, Is.EqualTo(123));
            Assert.That(bucket.BurstToleranceTicks, Is.EqualTo(456));
        });
    }

    [Test]
    public void Constructor_floors_emission_to_one_and_clamps_negative_tolerance_to_zero()
    {
        var bucket = new TenantTokenBucket(emissionIntervalTicks: 0, burstToleranceTicks: -1);

        Assert.Multiple(() =>
        {
            Assert.That(bucket.EmissionIntervalTicks, Is.EqualTo(1));
            Assert.That(bucket.BurstToleranceTicks, Is.EqualTo(0));
        });
    }

    [Test]
    public void TryAcquire_with_no_burst_admits_one_then_refuses_until_a_full_interval_elapses()
    {
        var emission = Frequency / 10; // 10 ops/sec
        var bucket = new TenantTokenBucket(emission, burstToleranceTicks: 0);
        var now = StartTimestamp;

        Assert.That(bucket.TryAcquire(now), Is.True, "first op admitted");
        Assert.That(bucket.TryAcquire(now), Is.False, "second op at same instant refused (no burst)");

        // Just short of a full interval is still refused; a full interval admits.
        Assert.That(bucket.TryAcquire(now + emission - 1), Is.False);
        Assert.That(bucket.TryAcquire(now + emission), Is.True);
    }

    [Test]
    public void TryAcquire_admits_a_bounded_burst_then_throttles()
    {
        var emission = Frequency / 100; // 100 ops/sec
        var tolerance = 5 * emission;   // burst of 5 extra tokens
        var bucket = new TenantTokenBucket(emission, tolerance);
        var now = StartTimestamp;

        // tau/T + 1 = 6 immediate admits, then throttled.
        var admitted = 0;
        for (var i = 0; i < 20; i++)
        {
            if (bucket.TryAcquire(now))
            {
                admitted++;
            }
        }

        Assert.That(admitted, Is.EqualTo(6));
        Assert.That(bucket.TryAcquire(now), Is.False);
    }

    [Test]
    public void TryAcquire_replenishes_one_token_per_interval_over_logical_time()
    {
        var emission = Frequency / 50; // 50 ops/sec, no burst
        var bucket = new TenantTokenBucket(emission, burstToleranceTicks: 0);
        var now = StartTimestamp;

        Assert.That(bucket.TryAcquire(now), Is.True);
        Assert.That(bucket.TryAcquire(now), Is.False);

        // After three intervals, exactly one token is available (no accrual beyond
        // capacity, because there is no burst tolerance).
        now += 3 * emission;
        Assert.That(bucket.TryAcquire(now), Is.True);
        Assert.That(bucket.TryAcquire(now), Is.False);
    }

    [Test]
    public void TryAcquire_does_not_accrue_credit_beyond_the_burst_while_idle()
    {
        var emission = Frequency / 10;
        var tolerance = 3 * emission; // burst of 3 extra
        var bucket = new TenantTokenBucket(emission, tolerance);

        // Idle for a long time, then hammer: capacity is bounded by the burst, not
        // by how long the bucket was idle.
        var now = StartTimestamp + 1_000 * emission;
        var admitted = 0;
        for (var i = 0; i < 50; i++)
        {
            if (bucket.TryAcquire(now))
            {
                admitted++;
            }
        }

        Assert.That(admitted, Is.EqualTo(4)); // tau/T + 1
    }

    [Test]
    public void ReadAndResetDemand_counts_admitted_ops_and_resets()
    {
        var emission = Frequency / 100;
        var bucket = new TenantTokenBucket(emission, 10 * emission);
        var now = StartTimestamp;

        bucket.TryAcquire(now);
        bucket.TryAcquire(now);
        bucket.TryAcquire(now);

        Assert.That(bucket.ReadAndResetDemand(), Is.EqualTo(3));
        Assert.That(bucket.ReadAndResetDemand(), Is.EqualTo(0), "reset after read");
    }

    [Test]
    public void ReadAndResetDemand_does_not_count_refused_ops()
    {
        var bucket = new TenantTokenBucket(Frequency, burstToleranceTicks: 0); // 1 op/sec
        var now = StartTimestamp;

        bucket.TryAcquire(now);        // admitted
        bucket.TryAcquire(now);        // refused
        bucket.TryAcquire(now);        // refused

        Assert.That(bucket.ReadAndResetDemand(), Is.EqualTo(1));
    }

    [Test]
    public void Matches_is_true_for_equal_parameters_and_false_otherwise()
    {
        var bucket = new TenantTokenBucket(100, 200);

        Assert.Multiple(() =>
        {
            Assert.That(bucket.Matches(100, 200), Is.True);
            Assert.That(bucket.Matches(101, 200), Is.False);
            Assert.That(bucket.Matches(100, 201), Is.False);
        });
    }

    [Test]
    public void Matches_normalises_candidate_parameters_the_same_way_the_constructor_does()
    {
        var bucket = new TenantTokenBucket(emissionIntervalTicks: 0, burstToleranceTicks: -5);

        // Constructor floored emission to 1 and tolerance to 0; Matches must apply
        // the same normalisation to its candidates.
        Assert.That(bucket.Matches(0, -5), Is.True);
        Assert.That(bucket.Matches(1, 0), Is.True);
    }

    [Test]
    public void TryAcquire_is_correct_under_concurrent_callers()
    {
        // A bucket sized so exactly N tokens are available at a fixed instant; with
        // "now" held constant across all threads (no time passes), the total number
        // of admits must equal that capacity regardless of interleaving - proving
        // the Interlocked CAS neither double-admits nor loses an admit.
        var emission = Frequency / 1000;
        var capacity = 200;
        var tolerance = (capacity - 1) * emission; // tau/T + 1 = capacity
        var bucket = new TenantTokenBucket(emission, tolerance);
        var now = StartTimestamp;

        var admitted = 0;
        Parallel.For(0, 4_000, _ =>
        {
            if (bucket.TryAcquire(now))
            {
                Interlocked.Increment(ref admitted);
            }
        });

        Assert.That(admitted, Is.EqualTo(capacity));
        Assert.That(bucket.ReadAndResetDemand(), Is.EqualTo(capacity));
    }
}
