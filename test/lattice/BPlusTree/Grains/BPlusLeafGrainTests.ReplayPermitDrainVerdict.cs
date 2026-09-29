using System.Diagnostics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for the arm attribution and the hold-time emission added for
/// issue #3921, at the seams themselves. The end-to-end regressions live in
/// <c>BPlusLeafGrainTests.ReplayPermitHoldRegime.cs</c>.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly TimeSpan DrainBound = TimeSpan.FromSeconds(5);

    [Test]
    [NonParallelizable]
    public void ClassifyReplayPermitQueueDrain_reports_draining_when_disabled_or_cold()
    {
        try
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);
            Assert.Multiple(() =>
            {
                Assert.That(
                    BPlusLeafGrain.ClassifyReplayPermitQueueDrain(DrainBound, out var cold),
                    Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.Draining),
                    "a cold gate has no evidence of harm and must admit");
                Assert.That(cold, Is.EqualTo(TimeSpan.Zero));
            });

            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.FromMinutes(5), sinceLastProgress: TimeSpan.FromMinutes(5));
            Assert.That(
                BPlusLeafGrain.ClassifyReplayPermitQueueDrain(TimeSpan.Zero, out _),
                Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.Draining),
                "a non-positive bound disables the predicate on both arms");
        }
        finally
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);
        }
    }

    [Test]
    [NonParallelizable]
    public void ClassifyReplayPermitQueueDrain_attributes_each_arm()
    {
        try
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(DrainBound, sinceLastProgress: TimeSpan.Zero);
            var waitExceeded = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(DrainBound, out _);

            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.Zero, sinceLastProgress: DrainBound + TimeSpan.FromSeconds(3));
            var noProgress = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(DrainBound, out var since);
            var legacy = BPlusLeafGrain.IsReplayPermitQueueNotDraining(DrainBound);

            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                DrainBound, sinceLastProgress: DrainBound + TimeSpan.FromSeconds(3));
            var both = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(DrainBound, out _);

            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.FromMilliseconds(2), sinceLastProgress: TimeSpan.FromMilliseconds(5));
            var draining = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(DrainBound, out _);

            Assert.Multiple(() =>
            {
                Assert.That(waitExceeded, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.WaitExceeded));
                Assert.That(noProgress, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.NoProgress),
                    "a zero smoothed wait with no progress for longer than the bound is the hold-time arm");
                Assert.That(since, Is.GreaterThanOrEqualTo(DrainBound + TimeSpan.FromSeconds(3)),
                    "the verdict must report how long the gate has gone without progress");
                Assert.That(legacy, Is.True,
                    "the boolean predicate must agree with the classification it now wraps");
                Assert.That(both, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.WaitExceeded),
                    "when both arms hold, the fresher smoothed-wait evidence is reported");
                Assert.That(draining, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.Draining));
            });
        }
        finally
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);
        }
    }

    [Test]
    public void DescribeReplayAdmissionRefusal_no_progress_names_the_hold_time_condition()
    {
        var message = BPlusLeafGrain.DescribeReplayAdmissionRefusal(
            BPlusLeafGrain.ReplayPermitDrainVerdict.NoProgress,
            queued: 24,
            bound: 24,
            LatticeReplayAdmissionClass.Interactive,
            ceiling: 6,
            smoothedWait: TimeSpan.Zero,
            sinceLastProgress: TimeSpan.FromSeconds(12),
            maxQueueWait: DrainBound);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("no permit has been released to the queue in 12 s"));
            Assert.That(message, Does.Contain("5000 ms"));
            Assert.That(message, Does.Contain("ceiling of 6 permit(s)"));
            Assert.That(message, Does.Contain(nameof(LatticeOptions.WalMaterialiserMaxConcurrentReplays)));
            Assert.That(message, Does.Contain("orleans.lattice.wal.replay.permit_hold"));
            Assert.That(message, Does.Contain("orleans.lattice.wal.replay.permits_served"));
            Assert.That(message, Does.Not.Contain(nameof(LatticeOptions.WalReplayPermitQueueDepthPerPermit)));
            Assert.That(message, Does.Not.Contain("smoothed"));
        });
    }

    [Test]
    public void DescribeReplayAdmissionRefusal_wait_exceeded_reports_the_smoothed_wait_and_the_depth_option()
    {
        var message = BPlusLeafGrain.DescribeReplayAdmissionRefusal(
            BPlusLeafGrain.ReplayPermitDrainVerdict.WaitExceeded,
            queued: 20,
            bound: 18,
            LatticeReplayAdmissionClass.Bulk,
            ceiling: 6,
            smoothedWait: TimeSpan.FromMilliseconds(7400),
            sinceLastProgress: TimeSpan.FromMilliseconds(40),
            maxQueueWait: DrainBound);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("smoothed queue wait is 7400 ms"));
            Assert.That(message, Does.Contain("20 admitted waiter(s)"));
            Assert.That(message, Does.Contain("Bulk caller"));
            Assert.That(message, Does.Contain(nameof(LatticeOptions.WalReplayPermitQueueDepthPerPermit)));
            Assert.That(message, Does.Not.Contain("no permit has been released"));
        });
    }

    [Test]
    public void Saturation_arm_tags_carry_their_documented_key_and_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeMetrics.TagSaturationArm, Is.EqualTo("arm"));
            Assert.That(LatticeMetrics.SaturationArmWaitExceeded,
                Is.EqualTo(new KeyValuePair<string, object?>("arm", "wait_exceeded")));
            Assert.That(LatticeMetrics.SaturationArmNoProgress,
                Is.EqualTo(new KeyValuePair<string, object?>("arm", "no_progress")));
            Assert.That(LatticeMetrics.SaturationArmGcShare,
                Is.EqualTo(new KeyValuePair<string, object?>("arm", "gc_share")));
        });
    }

    [Test]
    public void RecordSaturationRefusal_with_an_arm_carries_the_source_and_the_arm()
    {
        var treeId = UniqueReplayPermitTree();
        var refusals = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<string, object?>[]>();

        using (ListenForSaturationRefusals(treeId, refusals))
        {
            LatticeMetrics.RecordSaturationRefusal(
                treeId, LatticeSaturationSource.ReplayPermitAdmission, LatticeMetrics.SaturationArmNoProgress);
        }

        Assert.That(refusals, Has.Count.EqualTo(1));
        var tags = refusals.Single();
        Assert.Multiple(() =>
        {
            Assert.That(tags, Does.Contain(LatticeMetrics.SaturationArmNoProgress));
            Assert.That(tags, Does.Contain(LatticeMetrics.SaturationSourceTag(LatticeSaturationSource.ReplayPermitAdmission)));
            Assert.That(tags.Any(t => t.Key == LatticeTenantLabel.TagTenant), Is.True);
        });
    }

    [Test]
    public void Recording_a_permit_hold_allocates_nothing_per_call()
    {
        // Runs once per replay on the mass-reactivation path. A listener is
        // attached to both instruments, or Record/Add short-circuit and this
        // measures a disabled no-op.
        var observed = 0;
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            ["orleans.lattice.wal.replay.permit_hold", "orleans.lattice.wal.replay.permits_served"],
            l =>
            {
                l.SetMeasurementEventCallback<double>((_, _, _, _) => observed++);
                l.SetMeasurementEventCallback<long>((_, _, _, _) => observed++);
            });

        Assert.That(LatticeMetrics.WalReplayPermitHold.Enabled && LatticeMetrics.WalReplayPermitsServed.Enabled,
            Is.True, "instrument validation: both instruments must be enabled");

        var acquiredAt = Stopwatch.GetTimestamp();
        const string tree = "alloc-probe-hold";
        _ = LatticeTenantLabel.ForTree(tree);

        var bytes = MeasureAllocationsPerCall(
            () => BPlusLeafGrain.RecordReplayPermitHold(acquiredAt, tree, tree));

        // Known-positive control: the same emission with a boxed value-typed tag,
        // so a detector blind to allocation cannot pass this test.
        var boxedBytes = MeasureAllocationsPerCall(
            () => LatticeMetrics.WalReplayPermitHold.Record(
                1.0,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, tree),
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, 0)));

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.GreaterThan(0), "instrument validation: measurements must have arrived");
            Assert.That(boxedBytes, Is.GreaterThan(0), "detector validation: a boxed tag must register");
            Assert.That(bytes, Is.Zero, "recording a permit hold must not allocate");
        });
    }
}
