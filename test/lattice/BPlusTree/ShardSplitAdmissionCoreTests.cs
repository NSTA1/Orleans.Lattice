using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Exhaustive coverage for <see cref="ShardSplitAdmissionCore"/>, the pure
/// decision core behind autonomic shard-split admission. Every clause is driven
/// directly, without a silo, so the production rule is pinned independently of
/// the monitor grain that applies it.
/// </summary>
[TestFixture]
public class ShardSplitAdmissionCoreTests
{
    private static ShardSplitAdmissionPolicy DefaultPolicy => new()
    {
        OpsPerSecondThreshold = 200,
        MinSkewRatio = LatticeOptions.DefaultHotShardMinSkewRatio,
        ConsolidationSkewRatio = LatticeOptions.DefaultHotShardConsolidationSkewRatio,
        MinShardEntries = LatticeOptions.DefaultHotShardMinShardEntries,
        MaxPhysicalShards = LatticeOptions.DefaultMaxPhysicalShardsPerTree,
    };

    private static ShardSplitSample HotWellOccupiedSample => new()
    {
        Rate = 1_000d,
        Entries = 100_000,
        OwnedSlots = 2,
        IsSplitting = false,
        InCooldown = false,
    };

    // --- ComputeRate ------------------------------------------------------

    [Test]
    public void ComputeRate_zero_window_returns_zero()
        => Assert.That(ShardSplitAdmissionCore.ComputeRate(1_000, 1_000, TimeSpan.Zero), Is.EqualTo(0d));

    [Test]
    public void ComputeRate_negative_window_returns_zero()
        => Assert.That(ShardSplitAdmissionCore.ComputeRate(1_000, 1_000, TimeSpan.FromSeconds(-5)), Is.EqualTo(0d));

    [Test]
    public void ComputeRate_no_operations_returns_zero()
        => Assert.That(ShardSplitAdmissionCore.ComputeRate(0, 0, TimeSpan.FromSeconds(30)), Is.EqualTo(0d));

    [Test]
    public void ComputeRate_sums_reads_and_writes_over_the_window()
        => Assert.That(
            ShardSplitAdmissionCore.ComputeRate(600, 400, TimeSpan.FromSeconds(10)),
            Is.EqualTo(100d).Within(1e-9));

    // --- ComputeMedianRate ------------------------------------------------

    [Test]
    public void ComputeMedianRate_empty_span_returns_zero()
        => Assert.That(ShardSplitAdmissionCore.ComputeMedianRate(Span<double>.Empty), Is.EqualTo(0d));

    [Test]
    public void ComputeMedianRate_single_element_returns_that_element()
    {
        Span<double> rates = stackalloc double[] { 42d };
        Assert.That(ShardSplitAdmissionCore.ComputeMedianRate(rates), Is.EqualTo(42d));
    }

    [Test]
    public void ComputeMedianRate_odd_length_returns_the_middle_element()
    {
        Span<double> rates = stackalloc double[] { 9d, 1d, 5d };
        Assert.That(ShardSplitAdmissionCore.ComputeMedianRate(rates), Is.EqualTo(5d));
    }

    [Test]
    public void ComputeMedianRate_even_length_returns_the_lower_median()
    {
        // Lower median, not the mean of the two middles: a two-shard tree with
        // one idle shard must report a median of zero so it reads as fully
        // concentrated rather than as half-loaded.
        Span<double> rates = stackalloc double[] { 800d, 500d };
        Assert.That(ShardSplitAdmissionCore.ComputeMedianRate(rates), Is.EqualTo(500d));
    }

    [Test]
    public void ComputeMedianRate_sorts_the_span_in_place()
    {
        Span<double> rates = stackalloc double[] { 3d, 1d, 2d };
        _ = ShardSplitAdmissionCore.ComputeMedianRate(rates);
        double first = rates[0], second = rates[1], third = rates[2];
        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(1d));
            Assert.That(second, Is.EqualTo(2d));
            Assert.That(third, Is.EqualTo(3d));
        });
    }

    // --- ComputeSkewRatio -------------------------------------------------

    [Test]
    public void ComputeSkewRatio_idle_tree_returns_zero()
        => Assert.That(ShardSplitAdmissionCore.ComputeSkewRatio(0d, 0d), Is.EqualTo(0d));

    [Test]
    public void ComputeSkewRatio_zero_median_with_load_returns_positive_infinity()
        => Assert.That(
            ShardSplitAdmissionCore.ComputeSkewRatio(1_000d, 0d),
            Is.EqualTo(double.PositiveInfinity));

    [Test]
    public void ComputeSkewRatio_uniform_load_returns_one()
        => Assert.That(ShardSplitAdmissionCore.ComputeSkewRatio(500d, 500d), Is.EqualTo(1d));

    [Test]
    public void ComputeSkewRatio_returns_max_over_median()
        => Assert.That(
            ShardSplitAdmissionCore.ComputeSkewRatio(900d, 300d),
            Is.EqualTo(3d).Within(1e-9));

    // --- Skew gates and the hysteresis seam -------------------------------

    [Test]
    public void IsSplitSkew_admits_exactly_at_the_configured_ratio()
        => Assert.That(ShardSplitAdmissionCore.IsSplitSkew(1.5d, 1.5d), Is.True);

    [Test]
    public void IsSplitSkew_refuses_below_the_configured_ratio()
        => Assert.That(ShardSplitAdmissionCore.IsSplitSkew(1.49d, 1.5d), Is.False);

    [Test]
    public void IsSplitSkew_is_disabled_when_the_ratio_is_at_or_below_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShardSplitAdmissionCore.IsSplitSkew(1d, 1d), Is.True);
            Assert.That(ShardSplitAdmissionCore.IsSplitSkew(0d, 0d), Is.True);
        });
    }

    [Test]
    public void IsConsolidationSkew_true_at_or_below_the_uniformity_ratio()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShardSplitAdmissionCore.IsConsolidationSkew(1d, 1.15d), Is.True);
            Assert.That(ShardSplitAdmissionCore.IsConsolidationSkew(1.15d, 1.15d), Is.True);
        });
    }

    [Test]
    public void IsConsolidationSkew_false_above_the_uniformity_ratio()
        => Assert.That(ShardSplitAdmissionCore.IsConsolidationSkew(1.16d, 1.15d), Is.False);

    [Test]
    public void AreTriggerRegionsDisjoint_true_when_consolidation_sits_below_split()
        => Assert.That(ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(1.15d, 1.5d), Is.True);

    [Test]
    public void AreTriggerRegionsDisjoint_false_when_the_regions_touch()
        => Assert.That(ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(1.5d, 1.5d), Is.False);

    [Test]
    public void AreTriggerRegionsDisjoint_false_when_the_regions_overlap()
        => Assert.That(ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(2d, 1.5d), Is.False);

    [Test]
    public void AreTriggerRegionsDisjoint_true_when_the_split_skew_gate_is_disabled()
        => Assert.That(ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(5d, 1d), Is.True);

    [Test]
    public void Shipped_defaults_leave_a_hysteresis_dead_band_between_the_triggers()
    {
        // The split trigger (S4) and the consolidation trigger both reason
        // against this statistic. If their regions ever meet, a tree can be
        // split and immediately consolidated in a loop.
        Assert.Multiple(() =>
        {
            Assert.That(
                ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(
                    LatticeOptions.DefaultHotShardConsolidationSkewRatio,
                    LatticeOptions.DefaultHotShardMinSkewRatio),
                Is.True,
                "shipped defaults must not let the split and consolidation triggers overlap");

            // A ratio strictly inside the band triggers neither.
            const double InBand = 1.3d;
            Assert.That(
                ShardSplitAdmissionCore.IsSplitSkew(InBand, LatticeOptions.DefaultHotShardMinSkewRatio),
                Is.False);
            Assert.That(
                ShardSplitAdmissionCore.IsConsolidationSkew(InBand, LatticeOptions.DefaultHotShardConsolidationSkewRatio),
                Is.False);
        });
    }

    // --- Structural gates -------------------------------------------------

    [Test]
    public void IsOverSplit_true_when_the_tree_has_grown_past_its_base()
        => Assert.That(ShardSplitAdmissionCore.IsOverSplit(1_110, 64), Is.True);

    [Test]
    public void IsOverSplit_false_at_the_base_shard_count()
        => Assert.That(ShardSplitAdmissionCore.IsOverSplit(64, 64), Is.False);

    [Test]
    public void IsOverSplit_false_when_the_base_shard_count_is_unknown()
        => Assert.That(ShardSplitAdmissionCore.IsOverSplit(1_110, 0), Is.False);

    [Test]
    public void HasSplitHeadroom_false_at_the_ceiling()
        => Assert.That(ShardSplitAdmissionCore.HasSplitHeadroom(256, 256), Is.False);

    [Test]
    public void HasSplitHeadroom_true_below_the_ceiling()
        => Assert.That(ShardSplitAdmissionCore.HasSplitHeadroom(255, 256), Is.True);

    [Test]
    public void HasSplitHeadroom_true_when_no_ceiling_is_configured()
        => Assert.That(ShardSplitAdmissionCore.HasSplitHeadroom(100_000, 0), Is.True);

    // --- Evaluate ---------------------------------------------------------

    [Test]
    public void Evaluate_admits_a_hot_well_occupied_shard_on_a_skewed_tree()
        => Assert.That(
            ShardSplitAdmissionCore.Evaluate(HotWellOccupiedSample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.Admitted));

    [Test]
    public void Evaluate_refuses_a_uniformly_loaded_tree_however_hot_it_is()
    {
        // The bulk-ingest shape: every shard far above the rate threshold, none
        // disproportionately loaded. Splitting relieves nothing.
        var sample = HotWellOccupiedSample with { Rate = 50_000d };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 1.02d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.UniformLoad));
    }

    [Test]
    public void Evaluate_refuses_a_shard_below_the_rate_threshold()
    {
        var sample = HotWellOccupiedSample with { Rate = 199d };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.BelowRateThreshold));
    }

    [Test]
    public void Evaluate_admits_a_shard_exactly_at_the_rate_threshold()
    {
        var sample = HotWellOccupiedSample with { Rate = 200d };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.Admitted));
    }

    [Test]
    public void Evaluate_refuses_a_shard_holding_too_few_entries()
    {
        // The measured pathology: ~33 records per leaf across a shattered tree.
        var sample = HotWellOccupiedSample with { Entries = 33 };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.LowOccupancy));
    }

    [Test]
    public void Evaluate_admits_a_shard_exactly_at_the_occupancy_floor()
    {
        var sample = HotWellOccupiedSample with { Entries = LatticeOptions.DefaultHotShardMinShardEntries };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.Admitted));
    }

    [Test]
    public void Evaluate_skips_the_occupancy_clause_when_occupancy_is_not_sampled()
    {
        // The monitor evaluates every shard cheaply first and probes occupancy
        // only for survivors, so an unsampled count must not fail the clause.
        var sample = HotWellOccupiedSample with { Entries = ShardSplitSample.EntriesNotSampled };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.Admitted));
    }

    [Test]
    public void Evaluate_skips_the_occupancy_clause_when_the_floor_is_disabled()
    {
        var policy = DefaultPolicy with { MinShardEntries = 0 };
        var sample = HotWellOccupiedSample with { Entries = 0 };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, policy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.Admitted));
    }

    [Test]
    public void Evaluate_refuses_a_shard_inside_its_split_cooldown()
    {
        var sample = HotWellOccupiedSample with { InCooldown = true };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.Cooldown));
    }

    [Test]
    public void Evaluate_refuses_a_shard_owning_a_single_virtual_slot()
    {
        var sample = HotWellOccupiedSample with { OwnedSlots = 1 };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.InsufficientSlots));
    }

    [Test]
    public void Evaluate_refuses_every_shard_once_the_tree_reaches_its_shard_ceiling()
        => Assert.That(
            ShardSplitAdmissionCore.Evaluate(
                HotWellOccupiedSample, DefaultPolicy, treeSkewRatio: double.PositiveInfinity,
                physicalShardCount: LatticeOptions.DefaultMaxPhysicalShardsPerTree),
            Is.EqualTo(ShardSplitAdmissionOutcome.ShardCeilingReached));

    [Test]
    public void Evaluate_reports_a_shard_that_is_already_splitting()
    {
        var sample = HotWellOccupiedSample with { IsSplitting = true };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, treeSkewRatio: 4d, physicalShardCount: 64),
            Is.EqualTo(ShardSplitAdmissionOutcome.AlreadySplitting));
    }

    [Test]
    public void Evaluate_reports_the_rate_clause_before_any_structural_clause()
    {
        // A cold shard on a ceiling-capped tree must read as "not hot", not as
        // a structural deferral, so the deferral counters stay interpretable.
        var sample = HotWellOccupiedSample with { Rate = 0d, Entries = 0, OwnedSlots = 0, InCooldown = true };
        Assert.That(
            ShardSplitAdmissionCore.Evaluate(
                sample, DefaultPolicy, treeSkewRatio: 1d,
                physicalShardCount: LatticeOptions.DefaultMaxPhysicalShardsPerTree),
            Is.EqualTo(ShardSplitAdmissionOutcome.BelowRateThreshold));
    }

    // --- End-to-end load shapes over the whole statistic -------------------

    [Test]
    public void Evaluate_refuses_every_shard_under_the_bulk_ingest_shape()
    {
        // 64 shards, every one streaming writes far above the threshold at the
        // same rate: the shape a bulk embed produces. Zero shards admitted.
        const int Shards = 64;
        Span<double> rates = stackalloc double[Shards];
        var maxRate = 0d;
        for (var i = 0; i < Shards; i++)
        {
            // A little jitter so this is not an artificially perfect signal.
            rates[i] = 5_000d + (i % 5);
            if (rates[i] > maxRate) maxRate = rates[i];
        }
        var skew = ShardSplitAdmissionCore.ComputeSkewRatio(
            maxRate, ShardSplitAdmissionCore.ComputeMedianRate(rates));

        var admitted = 0;
        for (var i = 0; i < Shards; i++)
        {
            var sample = HotWellOccupiedSample with { Rate = 5_000d };
            if (ShardSplitAdmissionCore.Evaluate(sample, DefaultPolicy, skew, Shards)
                == ShardSplitAdmissionOutcome.Admitted)
            {
                admitted++;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(skew, Is.LessThan(LatticeOptions.DefaultHotShardMinSkewRatio),
                "uniform bulk-ingest load must not read as skewed");
            Assert.That(admitted, Is.Zero, "a uniformly loaded tree must admit no splits");
        });
    }

    [Test]
    public void Evaluate_admits_the_hot_shard_under_a_genuinely_skewed_shape()
    {
        // 64 shards, one carrying ten times the background rate: the shape a
        // read-skewed production workload produces. Relief must still happen.
        const int Shards = 64;
        Span<double> rates = stackalloc double[Shards];
        for (var i = 0; i < Shards; i++) rates[i] = 300d;
        rates[7] = 3_000d;
        var maxRate = 3_000d;
        var skew = ShardSplitAdmissionCore.ComputeSkewRatio(
            maxRate, ShardSplitAdmissionCore.ComputeMedianRate(rates));

        var hot = HotWellOccupiedSample with { Rate = 3_000d };
        Assert.Multiple(() =>
        {
            Assert.That(skew, Is.GreaterThanOrEqualTo(LatticeOptions.DefaultHotShardMinSkewRatio),
                "a ten-times hot shard must read as skewed");
            Assert.That(
                ShardSplitAdmissionCore.Evaluate(hot, DefaultPolicy, skew, Shards),
                Is.EqualTo(ShardSplitAdmissionOutcome.Admitted));
        });
    }

    // --- Policy projection ------------------------------------------------

    [Test]
    public void FromOptions_projects_every_split_admission_knob()
    {
        var options = new LatticeOptions
        {
            HotShardOpsPerSecondThreshold = 321,
            HotShardMinSkewRatio = 2.75d,
            HotShardConsolidationSkewRatio = 1.05d,
            HotShardMinShardEntries = 4_096,
            MaxPhysicalShardsPerTree = 512,
        };

        var policy = ShardSplitAdmissionPolicy.FromOptions(options);

        Assert.Multiple(() =>
        {
            Assert.That(policy.OpsPerSecondThreshold, Is.EqualTo(321));
            Assert.That(policy.MinSkewRatio, Is.EqualTo(2.75d));
            Assert.That(policy.ConsolidationSkewRatio, Is.EqualTo(1.05d));
            Assert.That(policy.MinShardEntries, Is.EqualTo(4_096));
            Assert.That(policy.MaxPhysicalShards, Is.EqualTo(512));
        });
    }

    [Test]
    public void FromOptions_null_options_throws()
        => Assert.Throws<ArgumentNullException>(() => ShardSplitAdmissionPolicy.FromOptions(null!));

    // --- Allocation -------------------------------------------------------

    [Test]
    public void Evaluate_allocates_nothing_per_decision()
    {
        var policy = DefaultPolicy;
        var sample = HotWellOccupiedSample;

        // Warm up the JIT so first-call codegen does not count against the loop.
        _ = ShardSplitAdmissionCore.Evaluate(sample, policy, 4d, 64);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var admitted = 0;
        for (var i = 0; i < 10_000; i++)
        {
            if (ShardSplitAdmissionCore.Evaluate(sample, policy, 4d, 64)
                == ShardSplitAdmissionOutcome.Admitted)
            {
                admitted++;
            }
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.EqualTo(10_000));
            Assert.That(allocated, Is.EqualTo(0),
                "the split-admission decision must not allocate per sampled shard");
        });
    }

    [Test]
    public void Sampling_statistics_allocate_nothing_per_pass()
    {
        Span<double> rates = stackalloc double[64];
        for (var i = 0; i < rates.Length; i++) rates[i] = 100d + i;

        // Warm up the JIT (including the span sort) before measuring.
        _ = ShardSplitAdmissionCore.ComputeSkewRatio(
            163d, ShardSplitAdmissionCore.ComputeMedianRate(rates));
        _ = ShardSplitAdmissionCore.ComputeRate(1, 1, TimeSpan.FromSeconds(1));

        var before = GC.GetAllocatedBytesForCurrentThread();
        var sink = 0d;
        for (var i = 0; i < 1_000; i++)
        {
            sink += ShardSplitAdmissionCore.ComputeRate(600, 400, TimeSpan.FromSeconds(10));
            sink += ShardSplitAdmissionCore.ComputeSkewRatio(
                163d, ShardSplitAdmissionCore.ComputeMedianRate(rates));
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(sink, Is.GreaterThan(0d));
            Assert.That(allocated, Is.EqualTo(0),
                "rate, median, and skew computation must not allocate per monitor pass");
        });
    }
}
