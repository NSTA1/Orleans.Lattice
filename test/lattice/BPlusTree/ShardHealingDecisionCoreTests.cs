using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for <see cref="ShardHealingDecisionCore"/>, the pure predicate the
/// automatic over-split healing orchestrator applies once per sweep.
/// <para>
/// The suite's centre of gravity is the <b>oscillation</b> case. Adaptive
/// splitting and automatic healing are one control loop, and a loop whose two
/// halves can both fire on the same observation does not merely misbehave - it
/// churns a production tree indefinitely, splitting and re-consolidating shards
/// forever. That failure would only ever be seen in production, so it is proved
/// here exhaustively over the skew domain rather than sampled at a few points.
/// </para>
/// </summary>
[TestFixture]
public class ShardHealingDecisionCoreTests
{
    private static ShardHealingPolicy DefaultPolicy => ShardHealingPolicy.FromOptions(new LatticeOptions());

    /// <summary>
    /// An over-split, uniformly loaded, quiescent tree with a free admission
    /// slot: the sample that must admit. Every refusal test below perturbs
    /// exactly one field of this, so the field under test is provably the one
    /// that decided.
    /// </summary>
    private static ShardHealingSample AdmissibleSample => new()
    {
        PhysicalShardCount = 128,
        BaseShardCount = 8,
        SkewRatio = 1.0,
        MedianShardOpsPerSecond = 0d,
        InFlightConsolidations = 0,
        IsSplitting = false,
        InTreeMaintenance = false,
        InCooldown = false,
    };

    // --- ComputeBacklog ---------------------------------------------------

    [Test]
    public void ComputeBacklog_reports_the_excess_shard_count()
        => Assert.That(ShardHealingDecisionCore.ComputeBacklog(1110, 64), Is.EqualTo(1046));

    [Test]
    public void ComputeBacklog_is_zero_for_a_tree_at_its_base_count()
        => Assert.That(ShardHealingDecisionCore.ComputeBacklog(64, 64), Is.Zero);

    [Test]
    public void ComputeBacklog_is_zero_for_a_tree_below_its_base_count()
        => Assert.That(ShardHealingDecisionCore.ComputeBacklog(4, 64), Is.Zero);

    [Test]
    public void ComputeBacklog_is_zero_when_the_base_count_is_unknown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShardHealingDecisionCore.ComputeBacklog(1110, 0), Is.Zero,
                "an unknown base count must report no backlog rather than guess one");
            Assert.That(ShardHealingDecisionCore.ComputeBacklog(1110, -1), Is.Zero);
        });
    }

    // --- IsUnderBackpressure ----------------------------------------------

    [Test]
    public void IsUnderBackpressure_is_false_when_the_threshold_is_disabled()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShardHealingDecisionCore.IsUnderBackpressure(1_000_000d, 0d), Is.False,
                "zero must disable backpressure entirely so healing proceeds regardless of load");
            Assert.That(ShardHealingDecisionCore.IsUnderBackpressure(1_000_000d, -1d), Is.False);
        });
    }

    [Test]
    public void IsUnderBackpressure_is_true_at_and_above_the_threshold()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShardHealingDecisionCore.IsUnderBackpressure(200d, 200d), Is.True,
                "the threshold is inclusive, so a tree exactly at it yields");
            Assert.That(ShardHealingDecisionCore.IsUnderBackpressure(201d, 200d), Is.True);
        });
    }

    [Test]
    public void IsUnderBackpressure_is_false_below_the_threshold()
        => Assert.That(ShardHealingDecisionCore.IsUnderBackpressure(199.9d, 200d), Is.False);

    [Test]
    public void IsUnderBackpressure_ignores_an_idle_thousand_shard_tree()
    {
        // The measured damage shape: 1,110 physical shards each carrying a few
        // tens of entries and almost no traffic. A summed tree rate would make
        // this the busiest tree on the box and it would never heal; the median
        // correctly reports it idle.
        Assert.That(ShardHealingDecisionCore.IsUnderBackpressure(0.2d, 200d), Is.False,
            "a badly over-split but idle tree is exactly the tree that must heal");
    }

    // --- DecideStructural -------------------------------------------------

    [Test]
    public void DecideStructural_reports_disabled_when_the_kill_switch_is_off()
    {
        var policy = DefaultPolicy with { Enabled = false };
        Assert.That(ShardHealingDecisionCore.DecideStructural(128, 8, in policy),
            Is.EqualTo(ShardHealingDecision.Disabled));
    }

    [Test]
    public void DecideStructural_reports_admission_closed_when_the_concurrency_cap_is_zero()
    {
        var policy = DefaultPolicy with { MaxConcurrentConsolidations = 0 };
        Assert.That(ShardHealingDecisionCore.DecideStructural(128, 8, in policy),
            Is.EqualTo(ShardHealingDecision.AdmissionClosed));
    }

    [Test]
    public void DecideStructural_reports_not_over_split_for_a_healthy_tree()
    {
        var policy = DefaultPolicy;
        Assert.That(ShardHealingDecisionCore.DecideStructural(8, 8, in policy),
            Is.EqualTo(ShardHealingDecision.NotOverSplit));
    }

    [Test]
    public void DecideStructural_returns_null_when_every_structural_gate_is_open()
    {
        var policy = DefaultPolicy;
        Assert.That(ShardHealingDecisionCore.DecideStructural(128, 8, in policy), Is.Null,
            "an over-split tree with healing on must fall through to the load clauses");
    }

    [Test]
    public void DecideStructural_agrees_with_Decide_on_every_structural_refusal()
    {
        // The split between the cheap and expensive clauses is what lets a
        // healthy tree be settled without polling a single shard, so the two
        // entry points must not be able to disagree about it.
        var cases = new[]
        {
            (DefaultPolicy with { Enabled = false }, 128, 8),
            (DefaultPolicy with { MaxConcurrentConsolidations = 0 }, 128, 8),
            (DefaultPolicy, 8, 8),
            (DefaultPolicy, 8, 0),
        };

        Assert.Multiple(() =>
        {
            foreach (var (policy, physical, baseCount) in cases)
            {
                var structural = ShardHealingDecisionCore.DecideStructural(physical, baseCount, in policy);
                var sample = AdmissibleSample with { PhysicalShardCount = physical, BaseShardCount = baseCount };
                Assert.That(structural, Is.Not.Null);
                Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy), Is.EqualTo(structural!.Value),
                    $"Decide and DecideStructural disagreed for physical={physical}, base={baseCount}.");
            }
        });
    }

    // --- Decide -----------------------------------------------------------

    [Test]
    public void Decide_admits_an_over_split_uniformly_loaded_quiescent_tree()
    {
        var policy = DefaultPolicy;
        var sample = AdmissibleSample;
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.Admitted));
    }

    [Test]
    public void Decide_refuses_a_skewed_tree()
    {
        var policy = DefaultPolicy;
        // Just above the consolidation edge: folding here could recreate the
        // hot spot an adaptive split exists to relieve.
        var sample = AdmissibleSample with { SkewRatio = 1.2 };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.SkewedLoad));
    }

    [Test]
    public void Decide_refuses_while_tree_maintenance_is_in_flight()
    {
        var policy = DefaultPolicy;
        var sample = AdmissibleSample with { InTreeMaintenance = true };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.TreeMaintenance));
    }

    [Test]
    public void Decide_serialises_behind_an_in_flight_split()
    {
        var policy = DefaultPolicy;
        var sample = AdmissibleSample with { IsSplitting = true };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.SplitInFlight),
            "the consolidation coordinator refuses a donor or survivor with a split in flight, "
            + "so admitting here would produce a fault rather than progress");
    }

    [Test]
    public void Decide_refuses_inside_the_post_split_cooldown()
    {
        var policy = DefaultPolicy;
        var sample = AdmissibleSample with { InCooldown = true };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.Cooldown));
    }

    [Test]
    public void Decide_yields_under_backpressure()
    {
        var policy = DefaultPolicy;
        var sample = AdmissibleSample with { MedianShardOpsPerSecond = 5_000d };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.Backpressure));
    }

    [Test]
    public void Decide_reports_at_capacity_when_the_concurrency_cap_is_reached()
    {
        var policy = DefaultPolicy with { MaxConcurrentConsolidations = 2 };
        var sample = AdmissibleSample with { InFlightConsolidations = 2 };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.AtCapacity));
    }

    [Test]
    public void Decide_admits_below_the_concurrency_cap()
    {
        var policy = DefaultPolicy with { MaxConcurrentConsolidations = 2 };
        var sample = AdmissibleSample with { InFlightConsolidations = 1 };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.Admitted));
    }

    [Test]
    public void Decide_never_consolidates_on_shard_count_alone()
    {
        // The count-only trigger is the specific mistake that reintroduces
        // oscillation: a tree can be far past its base shard count and still be
        // carrying concentrated load that a fold would make worse.
        var policy = DefaultPolicy;
        var sample = AdmissibleSample with { PhysicalShardCount = 1110, BaseShardCount = 64, SkewRatio = 4.0 };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.SkewedLoad));
    }

    [Test]
    public void Decide_refuses_backpressure_before_capacity()
    {
        // Ordering is only about which reason is reported, but a tree that is
        // both loaded and at capacity should report the condition an operator
        // can act on.
        var policy = DefaultPolicy with { MaxConcurrentConsolidations = 1 };
        var sample = AdmissibleSample with { MedianShardOpsPerSecond = 5_000d, InFlightConsolidations = 1 };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.Backpressure));
    }

    [Test]
    public void Decide_reports_disabled_ahead_of_every_other_clause()
    {
        var policy = DefaultPolicy with { Enabled = false };
        var sample = AdmissibleSample with
        {
            SkewRatio = 9d,
            IsSplitting = true,
            InTreeMaintenance = true,
            InCooldown = true,
            MedianShardOpsPerSecond = 9_000d,
            InFlightConsolidations = 99,
        };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.Disabled),
            "the kill switch must be reported unambiguously, never masked by an incidental condition");
    }

    // --- The hysteresis contract with the splitter ------------------------

    [Test]
    public void Shipped_defaults_leave_a_dead_band_between_healing_and_splitting()
    {
        var options = new LatticeOptions();
        Assert.That(
            ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(
                options.HotShardConsolidationSkewRatio, options.HotShardMinSkewRatio),
            Is.True,
            "healing fires at or below the consolidation ratio and splitting at or above the split "
            + "ratio; the interval between them is the dead band in which neither acts");
    }

    [Test]
    public void No_skew_ratio_both_admits_a_split_and_admits_a_consolidation()
    {
        // The oscillation proof, swept exhaustively rather than sampled: at no
        // point in the skew domain may both control loops fire on one
        // observation. A tree that satisfied both would split, immediately
        // consolidate, split again, and churn forever - the one failure mode
        // that would appear only in production.
        var options = new LatticeOptions();
        var healingPolicy = ShardHealingPolicy.FromOptions(options);

        var overlaps = new List<double>();
        for (var i = 0; i <= 5_000; i++)
        {
            var skew = i / 1_000d;

            var splitAdmits = ShardSplitAdmissionCore.IsSplitSkew(skew, options.HotShardMinSkewRatio);
            var sample = AdmissibleSample with { SkewRatio = skew };
            var healAdmits = ShardHealingDecisionCore.Decide(in sample, in healingPolicy)
                == ShardHealingDecision.Admitted;

            if (splitAdmits && healAdmits) overlaps.Add(skew);
        }

        Assert.That(overlaps, Is.Empty,
            "skew ratios at which both loops fire: " + string.Join(", ", overlaps));
    }

    [Test]
    public void The_dead_band_admits_neither_loop()
    {
        // The dead band is not merely non-overlapping, it is genuinely empty:
        // between the two edges a tree is left exactly as it is, which is what
        // stops a tree whose skew wanders slightly from being churned.
        var options = new LatticeOptions();
        var healingPolicy = ShardHealingPolicy.FromOptions(options);

        var acted = new List<double>();
        for (var skew = options.HotShardConsolidationSkewRatio + 0.001d;
             skew < options.HotShardMinSkewRatio;
             skew += 0.005d)
        {
            var splitAdmits = ShardSplitAdmissionCore.IsSplitSkew(skew, options.HotShardMinSkewRatio);
            var sample = AdmissibleSample with { SkewRatio = skew };
            var healAdmits = ShardHealingDecisionCore.Decide(in sample, in healingPolicy)
                == ShardHealingDecision.Admitted;

            if (splitAdmits || healAdmits) acted.Add(skew);
        }

        Assert.That(acted, Is.Empty,
            "skew ratios inside the dead band at which a loop acted: " + string.Join(", ", acted));
    }

    [Test]
    public void A_uniform_bulk_ingest_heals_while_a_genuinely_hot_tree_does_not()
    {
        // The two shapes the epic actually cares about, decided side by side.
        var options = new LatticeOptions();
        var healingPolicy = ShardHealingPolicy.FromOptions(options);

        // Bulk ingest across 64 shards: every shard equally loaded, skew 1.0.
        Span<double> uniform = stackalloc double[64];
        uniform.Fill(5_000d);
        var uniformSkew = ShardSplitAdmissionCore.ComputeSkewRatio(
            5_000d, ShardSplitAdmissionCore.ComputeMedianRate(uniform));

        // One genuinely hot shard among many.
        Span<double> skewed = stackalloc double[64];
        skewed.Fill(100d);
        skewed[0] = 5_000d;
        var skewedSkew = ShardSplitAdmissionCore.ComputeSkewRatio(
            5_000d, ShardSplitAdmissionCore.ComputeMedianRate(skewed));

        var uniformSample = AdmissibleSample with { SkewRatio = uniformSkew };
        var skewedSample = AdmissibleSample with { SkewRatio = skewedSkew };

        Assert.Multiple(() =>
        {
            Assert.That(uniformSkew, Is.EqualTo(1.0d).Within(1e-9));
            Assert.That(ShardHealingDecisionCore.Decide(in uniformSample, in healingPolicy),
                Is.EqualTo(ShardHealingDecision.Admitted),
                "the shattered-by-bulk-ingest shape is exactly what healing exists to repair");
            Assert.That(ShardHealingDecisionCore.Decide(in skewedSample, in healingPolicy),
                Is.EqualTo(ShardHealingDecision.SkewedLoad),
                "a genuinely hot tree must be left for the splitter, not folded back down");
        });
    }

    [Test]
    public void A_fully_concentrated_tree_is_never_healed()
    {
        // ComputeSkewRatio reports positive infinity when the median is zero
        // but some shard is loaded. That is maximum concentration, so it must
        // land firmly on the splitter's side of the band.
        var policy = DefaultPolicy;
        var sample = AdmissibleSample with { SkewRatio = double.PositiveInfinity };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.SkewedLoad));
    }

    [Test]
    public void A_completely_idle_tree_is_healed()
    {
        // ComputeSkewRatio reports zero when nothing is loaded at all: there is
        // no concentration to preserve, so an idle over-split tree - the state a
        // restored volume starts in - is the easiest case to heal.
        var policy = DefaultPolicy;
        var sample = AdmissibleSample with { SkewRatio = 0d };
        Assert.That(ShardHealingDecisionCore.Decide(in sample, in policy),
            Is.EqualTo(ShardHealingDecision.Admitted));
    }

    // --- Policy projection ------------------------------------------------

    [Test]
    public void FromOptions_projects_every_healing_threshold()
    {
        var options = new LatticeOptions
        {
            ShardHealingEnabled = false,
            HotShardConsolidationSkewRatio = 1.05,
            MaxConcurrentShardConsolidations = 4,
            ShardHealingBackpressureOpsPerSecond = 750d,
        };

        var policy = ShardHealingPolicy.FromOptions(options);

        Assert.Multiple(() =>
        {
            Assert.That(policy.Enabled, Is.False);
            Assert.That(policy.ConsolidationSkewRatio, Is.EqualTo(1.05));
            Assert.That(policy.MaxConcurrentConsolidations, Is.EqualTo(4));
            Assert.That(policy.BackpressureOpsPerSecond, Is.EqualTo(750d));
        });
    }

    [Test]
    public void FromOptions_rejects_a_null_options_instance()
        => Assert.That(() => ShardHealingPolicy.FromOptions(null!), Throws.ArgumentNullException);

    [Test]
    public void FromOptions_carries_the_same_consolidation_ratio_the_splitter_uses()
    {
        // Both loops must read one number. If these ever diverge, the dead band
        // is a fiction and the two loops can be configured into a fight.
        var options = new LatticeOptions { HotShardConsolidationSkewRatio = 1.09 };
        Assert.That(
            ShardHealingPolicy.FromOptions(options).ConsolidationSkewRatio,
            Is.EqualTo(ShardSplitAdmissionPolicy.FromOptions(options).ConsolidationSkewRatio));
    }

    [Test]
    public void Shipped_defaults_are_healing_on_with_a_single_fold_and_backpressure_armed()
    {
        var policy = DefaultPolicy;
        Assert.Multiple(() =>
        {
            Assert.That(policy.Enabled, Is.True, "healing is default-on so an existing deployment repairs itself");
            Assert.That(policy.MaxConcurrentConsolidations, Is.EqualTo(1));
            Assert.That(policy.BackpressureOpsPerSecond, Is.GreaterThan(0d),
                "backpressure must be armed by default, or healing could not be called polite");
            Assert.That(policy.ConsolidationSkewRatio,
                Is.EqualTo(LatticeOptions.DefaultHotShardConsolidationSkewRatio));
        });
    }
}
