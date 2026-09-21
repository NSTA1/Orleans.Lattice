using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3185: the snapshot capture <b>drivers</b> must report the declines
/// they previously took in complete silence, and the deactivate-hook arm must
/// separate the decline that MINTS a frozen floor holder from the one that does
/// not.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this needs an instrument at all.</b> The #1535 no-loss gate in
/// <c>TryCaptureSnapshotOnDeactivateAsync</c> declines whenever this activation
/// neither advanced a checkpoint over cache-resident applies nor cold-rebuilt
/// its cache from the WAL start. That decline is <i>correct</i> - such a leaf
/// cannot honestly stamp coverage - so nothing here changes behaviour. What it
/// changes is visibility: the gate returned with no counter, no log and no
/// trace, while being the site at which a leaf carrying a checkpoint above its
/// durable coverage becomes a pin frozen below its own checkpoint. Such a pin is
/// dischargeable only by the WAL GC reactivation drive, which drives at most a
/// handful per sweep, so the rate of minting and the rate of discharge together
/// decide whether the frozen population converges or grows without bound. Only
/// the second of those two rates was ever measurable.
/// </para>
/// <para>
/// <b>Why the two arms must be distinct.</b> A leaf whose coverage is already
/// current declines the same gate, by the same predicate, and leaves nothing
/// behind. Folding both into one reason would make the series a count of
/// deactivations rather than of minted holders, and on a healthy estate the
/// harmless arm dominates - so the informative signal would be buried under a
/// number that rises with ordinary traffic. These tests assert the split, not
/// merely that something was counted.
/// </para>
/// <para>
/// <b>Why the stale fixture needs two partitions, which is not incidental.</b>
/// A single-partition leaf cannot hold this shape across an activation
/// boundary. If its snapshot covers its persisted checkpoint it is not stale;
/// if it does not, the rehydrate is refused, the leaf cold-rebuilds from the WAL
/// start, and that latches signal (b), captures on the way out and heals itself.
/// The frozen population is therefore made of leaves whose snapshot covers the
/// partition the rehydrate is validated against while leaving ANOTHER
/// partition's coverage behind its checkpoint - a leaf that rehydrates
/// perfectly, replays nothing, rebuilds nothing, and so satisfies neither
/// signal. That is the shape reproduced below, and a one-partition fixture would
/// quietly assert the self-healing path instead.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Collects the <c>reason</c> tag of every driver decline recorded while the
    /// returned listener is alive.
    /// </summary>
    private static IDisposable ListenForDriverDeclines(List<string> reasons) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.LeafSnapshotDriverDeclines,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var t in tags)
                {
                    if (t.Key == "reason" && t.Value is string reason)
                    {
                        reasons.Add(reason);
                    }
                }
            }));

    /// <summary>
    /// Builds a leaf that rehydrates from a snapshot covering
    /// <paramref name="coverageByPartition"/> over a checkpoint persisted at
    /// <paramref name="checkpointByPartition"/>.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateLeafForDriverDecline(
        ILeafReplayCoordinatorGrain coordinator,
        long[] coverageByPartition,
        long[] checkpointByPartition)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = coverageByPartition[0],
                Rows = [],
                CapturedAtTicks = 1L,
                SnapshotOffsetsByPartition = coverageByPartition,
            }));

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-3185-driver-declines";
        state.State.ProjectionCheckpointOffset = checkpointByPartition[0];
        state.State.ProjectionCheckpointOffsetAssigned = true;
        state.State.ProjectionCheckpointOffsetsByPartition = [.. checkpointByPartition];

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                WalPartitions = checkpointByPartition.Length,
                // Pinned high so the post-persist cadence driver cannot be what
                // captures here. A low threshold would let that path heal the
                // leaf and the decline under test would never be reached.
                LeafSnapshotReClassifyEveryNCheckpoints = 1000,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return (new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default()), state);
    }

    private static Task DeactivateLeafAsync(BPlusLeafGrain grain) =>
        ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

    /// <summary>
    /// THE load-bearing case. A leaf deactivating with a partition checkpoint
    /// banked above the offset its durable snapshot covers, and with neither
    /// #1535 signal latched, mints a frozen floor holder - and must say so.
    /// </summary>
    [Test]
    public async Task TryCaptureSnapshotOnDeactivateAsync_reports_a_stale_coverage_decline_when_it_mints_a_frozen_holder()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);

        // Partition 0's snapshot offset is AHEAD of its persisted checkpoint, so
        // the rehydrate is accepted rather than declined as already-absorbed -
        // which matters because a declined rehydrate falls through to the cold
        // rebuild from the WAL start, latches signal (b), and heals the leaf.
        // Partition 1 carries a banked checkpoint above its coverage, and that
        // is the pin that freezes.
        var (grain, state) = CreateLeafForDriverDecline(
            wal.Coordinator,
            coverageByPartition: [3L, 3L],
            checkpointByPartition: [3L, 3L]);

        await ActivateAsync(grain);

        // Bank a checkpoint on partition 1 above the offset the durable snapshot
        // covers. Done directly against persisted state rather than by driving a
        // replay, and that is deliberate: every route that ADVANCES a checkpoint
        // from inside this activation also latches the #1535 gate's signal (a),
        // which opens the gate and captures. The frozen population is made of
        // leaves that inherited such a checkpoint from an EARLIER activation -
        // one that advanced it and then died without running its deactivate hook
        // - so the shape under test is by construction not reachable from within
        // the activation that observes it. Seeding it is the only honest way to
        // present the classifier with the input production hands it.
        state.State.ProjectionCheckpointOffsetsByPartition![1] = 7L;

        Assert.Multiple(() =>
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(1), Is.EqualTo(7L),
                "precondition: partition 1 has a checkpoint banked above its coverage. Without "
                    + "this the leaf is not the shape that mints a frozen holder and the arm "
                    + "asserted below would be right for the wrong reason.");

            Assert.That(grain.DurableSnapshotCoverageForPartition(1), Is.EqualTo(3L),
                "precondition: that partition's coverage is PRESENT but stale. A -1 here would be "
                    + "the ABSENT population, which the zero-coverage repair already handles and "
                    + "which is not what holds the estate's floor.");
        });

        var reasons = new List<string>();
        using (ListenForDriverDeclines(reasons))
        {
            await DeactivateLeafAsync(grain);
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                reasons,
                Does.Contain(LatticeMetrics.DriverDeclineDeactivateCoverageStale.Value),
                "the decline must be attributed to the arm that means 'this deactivation left a "
                    + "pin frozen below its own checkpoint'. That count is the minting rate of "
                    + "the population the WAL GC reactivation drive has to discharge, and it is "
                    + "the only arm on this instrument worth alerting on.");

            Assert.That(
                reasons,
                Does.Not.Contain(LatticeMetrics.DriverDeclineDeactivateUnclassified.Value),
                "and it must be CLASSIFIED rather than swallowed. The classifier is wrapped so it "
                    + "can never fail a deactivation, which means a fault inside it degrades to "
                    + "this arm silently - so a fixture asserting only that SOME decline was "
                    + "counted would pass against a classifier that never worked.");
        });
    }

    /// <summary>
    /// The discriminating control. The same gate, declining on a leaf whose
    /// coverage is current on every partition, must NOT be counted as a mint.
    /// </summary>
    [Test]
    public async Task TryCaptureSnapshotOnDeactivateAsync_reports_a_current_coverage_decline_as_harmless()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);

        var (grain, _) = CreateLeafForDriverDecline(
            wal.Coordinator,
            coverageByPartition: [3L, 3L],
            checkpointByPartition: [3L, 3L]);

        await ActivateAsync(grain);

        // The SAME leaf as the stale fixture, minus its one seeded mutation. The
        // two tests therefore differ in exactly one variable - whether some
        // partition's checkpoint outran its coverage - so a pass here is
        // evidence about the classifier rather than about the setup.
        Assert.That(grain.DurableSnapshotCoverageForPartition(1), Is.EqualTo(3L),
            "precondition: nothing is banked above coverage anywhere, so this deactivation leaves "
                + "no frozen pin behind and the decline is pure cost avoidance.");

        var reasons = new List<string>();
        using (ListenForDriverDeclines(reasons))
        {
            await DeactivateLeafAsync(grain);
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                reasons,
                Does.Not.Contain(LatticeMetrics.DriverDeclineDeactivateCoverageStale.Value),
                "this is the whole point of splitting the arms. A harmless decline counted as a "
                    + "mint would make the series track deactivation volume, and because the "
                    + "harmless case dominates on a healthy estate the informative signal would "
                    + "be unreadable underneath it.");

            Assert.That(
                reasons,
                Does.Contain(LatticeMetrics.DriverDeclineDeactivateCoverageCurrent.Value),
                "and the harmless case must still be COUNTED, not left silent. Counting it is "
                    + "what distinguishes 'the gate declined and it was fine' from 'the gate was "
                    + "never reached at all', which is the ambiguity this instrument exists to "
                    + "remove.");
        });
    }
}
