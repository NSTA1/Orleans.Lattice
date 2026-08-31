using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Allocation guards for the healing observe/decide path.
/// <para>
/// The orchestrator sweeps every tree forever, so an allocation per sweep is an
/// allocation per tree per interval for the life of the process. The decision
/// itself must therefore cost nothing on the heap, and the rate statistics it
/// consumes - which are recomputed over every physical shard of a tree that may
/// carry a thousand of them - must not scale with the shard count.
/// </para>
/// <para>
/// <b>Every assertion here is differential, never absolute.</b>
/// <c>Assert.That(allocated, Is.Zero)</c> against a GC counter passes in
/// isolation and fails in a larger batch: tiered JIT and on-stack replacement
/// can land inside the measured window, and whether they do depends on what the
/// shared test host has already compiled - so an unrelated sibling's tests flip
/// the result. Measuring the same loop at two sizes cancels any one-off cost,
/// because it appears in both windows, and leaves only the genuine per-iteration
/// term. The minimum over several repeats is taken so a single unlucky window
/// (a background GC's own bookkeeping, say) cannot decide the outcome.
/// </para>
/// <para>
/// Nothing on this path awaits, so
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/> is the right counter: it
/// is thread-affine and therefore immune to the process-wide noise that would
/// otherwise swamp a zero-allocation claim.
/// </para>
/// </summary>
[TestFixture]
public class ShardHealingDecisionCoreAllocationTests
{
    private const int NarrowIterations = 50_000;
    private const int WideIterations = 100_000;
    private const int Repeats = 5;

    /// <summary>
    /// Escape hatch for the battery test's allocation. A field of reference
    /// type that the loop assigns to on every iteration is the only reliable
    /// way to force a heap allocation under a modern JIT: escape analysis
    /// stack-allocates - or elides outright - a non-escaping allocation of
    /// constant size, so a "deliberately allocating" loop that keeps nothing
    /// alive allocates nothing at all and silently disarms the very test that
    /// exists to prove the probe can fail. Do not "simplify" this away.
    /// </summary>
    private static object? _escapeSink;

    private static ShardHealingPolicy DefaultPolicy => ShardHealingPolicy.FromOptions(new LatticeOptions());

    private static ShardHealingSample AdmissibleSample => new()
    {
        PhysicalShardCount = 1110,
        BaseShardCount = 64,
        SkewRatio = 1.0,
        MedianShardOpsPerSecond = 0.2d,
        InFlightConsolidations = 0,
    };

    /// <summary>
    /// Runs <paramref name="body"/> at two loop sizes and returns the smallest
    /// observed difference in allocated bytes across <see cref="Repeats"/>
    /// rounds. A per-iteration allocation shows up as a growth proportional to
    /// the extra iterations; a one-off cost cancels.
    /// <para>
    /// Every round is measured and the <b>minimum</b> is kept - the helper never
    /// short-circuits on the first non-positive sample. Short-circuiting is the
    /// third way an allocation probe silently fails: on a loop that genuinely
    /// allocates, one noisy round where the small window absorbed more noise
    /// than the large one would be reported as allocation-free.
    /// </para>
    /// <para>
    /// The minimum is deliberately <i>not</i> clamped at zero. Noise can only
    /// add allocation, so the minimum is the closest estimate of the true
    /// per-iteration term, and a genuine allocation appears in every round and
    /// survives it. Leaving the raw value makes a failure message report what
    /// was actually measured; clamping would be behaviour-neutral for both
    /// assertion directions used here (a negative growth satisfies
    /// <c>&lt;= 0</c> and fails <c>&gt; 0</c> either way).
    /// </para>
    /// <para>
    /// The warm-up is deliberately at the <b>full</b> loop size, not a token
    /// one. That matters twice: it moves tiered-JIT and on-stack-replacement
    /// costs outside every measured window, and it forces tier-1 promotion
    /// before the first measurement - so a loop whose allocation escape
    /// analysis can remove is caught even in a single-test run, rather than
    /// only in a batch large enough to have promoted the method by luck.
    /// </para>
    /// </summary>
    private static long MeasureGrowth(Action<int> body)
    {
        // Full-size warm-up outside every measured window, so tier-1 promotion
        // has already happened before the first measurement.
        body(WideIterations);

        var smallest = long.MaxValue;
        for (var round = 0; round < Repeats; round++)
        {
            var beforeNarrow = GC.GetAllocatedBytesForCurrentThread();
            body(NarrowIterations);
            var narrow = GC.GetAllocatedBytesForCurrentThread() - beforeNarrow;

            var beforeWide = GC.GetAllocatedBytesForCurrentThread();
            body(WideIterations);
            var wide = GC.GetAllocatedBytesForCurrentThread() - beforeWide;

            var growth = wide - narrow;
            if (growth < smallest) smallest = growth;
        }

        return smallest;
    }

    [Test]
    public void Decide_allocates_nothing_per_sweep()
    {
        var policy = DefaultPolicy;
        var sample = AdmissibleSample;
        var admitted = 0;

        var growth = MeasureGrowth(iterations =>
        {
            for (var i = 0; i < iterations; i++)
            {
                if (ShardHealingDecisionCore.Decide(in sample, in policy) == ShardHealingDecision.Admitted)
                    admitted++;
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.GreaterThan(0), "the measured loop must actually have reached a decision");
            Assert.That(growth, Is.LessThanOrEqualTo(0L),
                $"doubling the sweep count allocated {growth} extra bytes; the healing decision runs "
                + "continuously against every tree and must not allocate per sweep");
        });
    }

    [Test]
    public void DecideStructural_allocates_nothing_per_sweep()
    {
        // The clause a healthy tree is settled by, and therefore the one that
        // runs most often of anything in this feature.
        var policy = DefaultPolicy;
        var settled = 0;

        var growth = MeasureGrowth(iterations =>
        {
            for (var i = 0; i < iterations; i++)
            {
                if (ShardHealingDecisionCore.DecideStructural(64, 64, in policy) is not null) settled++;
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(settled, Is.GreaterThan(0));
            Assert.That(growth, Is.LessThanOrEqualTo(0L),
                $"doubling the sweep count allocated {growth} extra bytes; the structural clause is the "
                + "steady-state path for every healthy tree in the cluster");
        });
    }

    [Test]
    public void Backlog_and_backpressure_computation_allocate_nothing()
    {
        var sink = 0;

        var growth = MeasureGrowth(iterations =>
        {
            for (var i = 0; i < iterations; i++)
            {
                sink += ShardHealingDecisionCore.ComputeBacklog(1110, 64);
                if (ShardHealingDecisionCore.IsUnderBackpressure(0.2d, 200d)) sink++;
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(sink, Is.GreaterThan(0));
            Assert.That(growth, Is.LessThanOrEqualTo(0L),
                $"doubling the sweep count allocated {growth} extra bytes");
        });
    }

    [Test]
    public void Rate_statistics_do_not_allocate_per_shard()
    {
        // Not a re-proof of ShardSplitAdmissionCore, which S4 already covers in
        // isolation. This measures the ORCHESTRATOR'S use of those functions -
        // a caller-owned heap scratch buffer copied and sorted once per sweep at
        // the measured damage scale - which is a different claim from "the
        // function is pure": it is the copy and the in-place sort over 1,110
        // rates that must not allocate, because the healer recomputes the tree's
        // skew on every sweep for as long as the damage exists. A per-shard
        // allocation here would be a per-sweep allocation multiplied by the very
        // damage being healed.
        var rates = new double[1_110];
        for (var i = 0; i < rates.Length; i++) rates[i] = 0.2d + (i % 7) * 0.01d;
        var scratch = new double[rates.Length];
        var sink = 0d;

        var growth = MeasureGrowth(iterations =>
        {
            // The loop count here is the number of sweeps, not shards: a sweep
            // is the unit whose cost must not grow.
            var sweeps = iterations / 1_000;
            for (var s = 0; s < sweeps; s++)
            {
                rates.AsSpan().CopyTo(scratch);
                var median = ShardSplitAdmissionCore.ComputeMedianRate(scratch);
                sink += ShardSplitAdmissionCore.ComputeSkewRatio(0.26d, median);
                sink += ShardSplitAdmissionCore.ComputeRate(600, 400, TimeSpan.FromSeconds(10));
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(sink, Is.GreaterThan(0d));
            Assert.That(growth, Is.LessThanOrEqualTo(0L),
                $"doubling the sweep count allocated {growth} extra bytes; the median sort must operate "
                + "in place on a caller-owned buffer");
        });
    }

    [Test]
    public void Publishing_a_sweeps_metering_allocates_nothing()
    {
        // Metering runs on the same continuous path as the decision, so a tag
        // array allocated per publish would be an allocation per tree per
        // interval forever. The three- and two-tag Add/Record overloads take
        // their tags by value; binding instead to a params overload would show
        // up here as growth proportional to the sweep count.
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "alloc-probe-tree");
        var tenantTag = LatticeTenantLabel.ForTree("alloc-probe-tree");
        var decisionTag = LatticeMetrics.HealingNotOverSplitDecisionTag;

        var growth = MeasureGrowth(iterations =>
        {
            for (var i = 0; i < iterations; i++)
            {
                LatticeMetrics.ShardHealingBacklog.Record(0, treeTag, tenantTag);
                LatticeMetrics.ShardHealingDecisions.Add(1, treeTag, decisionTag, tenantTag);
            }
        });

        Assert.That(growth, Is.LessThanOrEqualTo(0L),
            $"doubling the sweep count allocated {growth} extra bytes publishing the healing metrics");
    }

    [Test]
    public void Allocation_probe_detects_a_loop_that_really_does_allocate()
    {
        // The battery test for the probe itself. Its allocation must PROVABLY
        // ESCAPE: assigning a fresh object to a static field of reference type
        // is a definite escape at every JIT tier with no constant-folding
        // surface, whereas a non-escaping `new long[1]` whose only use is
        // `.Length` is stack-allocated or elided outright - which would make
        // this test quietly become the false negative it exists to prevent.
        //
        // VERIFIED BY DELIBERATE FAILURE, not by assumption. Substituting the
        // non-escaping `sink += new long[1].Length` body makes this test fail
        // with "Expected: greater than 0, But was: 0" under BOTH
        // DOTNET_TieredCompilation=0 and default tiering, and restoring the
        // escape below returns it to green under both. Do not "simplify" the
        // static-field store away: the escape is load-bearing.
        var growth = MeasureGrowth(iterations =>
        {
            for (var i = 0; i < iterations; i++)
            {
                _escapeSink = new object();
            }
        });

        Assert.That(growth, Is.GreaterThan(0L),
            "the probe must report growth for a loop that genuinely allocates per iteration, "
            + "or it could not catch a real per-sweep regression");
        Assert.That(_escapeSink, Is.Not.Null);
    }
}
