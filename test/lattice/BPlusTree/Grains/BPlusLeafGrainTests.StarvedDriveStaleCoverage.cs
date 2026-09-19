using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3185: the reactivation drive must repair coverage that is STALE, not
/// merely coverage that is ABSENT.
/// </summary>
/// <remarks>
/// <para>
/// <b>The measured defect.</b> On a live estate the WAL GC's trim scan stopped
/// on <c>offset_floor</c> on every tree while the drive reported
/// <see cref="LeafStarvationDriveOutcome.Lifted"/> on almost every attempt. The
/// floor-holder census named the reason directly: every holder was a partition
/// whose published pin sat ~16,200 entries BELOW a durable checkpoint the leaf
/// had already banked. The pin is
/// <c>min(checkpoint, covered)</c>, so <c>covered</c> was the binding term and
/// it was not restamping.
/// </para>
/// <para>
/// <b>Why the drive declined them.</b> Its only coverage remedy was
/// <c>TryRepairZeroCoverageAsync</c>, gated on
/// <c>HasCheckpointedPartitionWithoutCoverage</c> - that is
/// <c>IsPartitionProvenCheckpointed(p) &amp;&amp; DurableSnapshotCoverageForPartition(p) &lt; 0</c>,
/// coverage ABSENT. A stale holder has <c>covered &gt;= 0</c>, so the predicate
/// was false, the repair returned without acting, and the republish that
/// followed flushed a byte-identical offset. The drive then graded itself
/// <c>Lifted</c> - correctly, on the cursor axis - while the floor it existed to
/// move had provably not moved.
/// </para>
/// <para>
/// <b>Why no other driver reached them.</b> The per-partition
/// <c>checkpoint &gt; covered</c> capture lives in
/// <c>MaybeRunPeriodicSnapshotRecheckAsync</c>, which the drive never called.
/// Its post-persist caller is behind a cadence counter that resets every
/// activation, and a GC-driven leaf persists about one checkpoint per drive. The
/// #3195 coverage-lag timer does call it off the cadence, but a timer only ticks
/// on a LIVE activation and the collector recycles a driven dormant leaf well
/// before its first jittered tick is due.
/// </para>
/// <para>
/// <b>The observable.</b> These tests read the offset the leaf actually
/// PUBLISHES - the <see cref="MaterialiserPinReport.CheckpointOffset"/> handed to
/// the cursor reporter - because that, and not the verdict or the in-memory
/// checkpoint, is the quantity <c>ComputeMaterialiserOffsetFloorAsync</c> takes
/// the minimum over to place the tree's WAL retention floor. Asserting the
/// verdict would re-assert what already worked: the verdict was honest
/// throughout, which is exactly why the defect survived eleven fixes on this
/// axis.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Builds a leaf whose partition carries durable snapshot coverage at
    /// <paramref name="coverageOffset"/> and hands back the cursor reporter it
    /// publishes through, so a test can read the pin offset the WAL GC would
    /// see rather than an internal proxy for it.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State,
        List<MaterialiserPinReport> Published) CreateCoveredLeafWithPinCapture(
        ILeafReplayCoordinatorGrain coordinator,
        long coverageOffset,
        RecordingLoggerFactory? logs = null)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = coverageOffset,
                Rows = [],
                CapturedAtTicks = 1L,
                SnapshotOffsetsByPartition = [coverageOffset],
            }));

        var published = new List<MaterialiserPinReport>();
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                published.AddRange(call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1));
                return Task.CompletedTask;
            });

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        if (logs is not null)
        {
            sc.AddSingleton<ILoggerFactory>(logs);
        }
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-3185-stale-coverage";
        state.State.ProjectionCheckpointOffset = -1L;

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                WalPartitions = 1,
                // Pinned high so the cadence gate CANNOT be what repairs this
                // leaf. A drive persists about one checkpoint, so a low
                // threshold could let the post-persist driver capture and the
                // fixture would go green on a path this issue is not about.
                LeafSnapshotReClassifyEveryNCheckpoints = 1000,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state, published);
    }

    /// <summary>
    /// THE load-bearing test. A drive of a leaf whose coverage is present but
    /// STALE must restamp that coverage from the checkpoint the leaf already
    /// holds, so the offset it publishes advances.
    /// <para>
    /// RED before the fix, and red on the assertion that matters rather than on
    /// a proxy: the drive advances the checkpoint to 3 and returns
    /// <c>Lifted</c> - both of which the pre-fix build already does, and both of
    /// which are asserted here as controls - while coverage stays at 0 and the
    /// published pin stays at <c>min(3, 0) == 0</c>. That frozen 0 is the whole
    /// estate symptom in miniature: the tree's floor cannot pass it, so the trim
    /// scan stops on <c>offset_floor</c> and not one entry is reclaimed.
    /// </para>
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_restamps_coverage_that_is_stale_so_the_published_pin_advances()
    {
        var wal = new GrowingWal();
        var logs = new RecordingLoggerFactory();
        var (grain, state, published) =
            CreateCoveredLeafWithPinCapture(wal.Coordinator, coverageOffset: 0L, logs);

        await ActivateAsync(grain);

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(0L),
            "precondition: coverage is PRESENT (not the -1 sentinel), which is precisely why the "
                + "drive's zero-coverage repair declines this leaf. A leaf at -1 would be repaired "
                + "by the pre-fix build and would assert nothing.");

        wal.GrowTo(3);
        published.Clear();

        var declines = new List<string>();
        using var declineListener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSnapshotCaptureDeclines,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var t in tags)
                {
                    declines.Add($"{t.Key}={t.Value}");
                }
            }));

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.That(
            logs.Warnings.Select(w => w.Message),
            Is.Empty,
            "the capture the drive reaches must actually succeed. That path swallows every "
                + "exception into a warning, so without this a capture that threw would present "
                + "identically to one that was never reached and the coverage assertion below "
                + "could be misread as the production defect.");

        Assert.That(
            declines,
            Is.Empty,
            "and it must not be DECLINED either. The capture gates return silently, so this "
                + "separates the three ways coverage can fail to move - never reached, declined, "
                + "or attempted and failed - which otherwise all present as an unchanged offset.");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the drive must have advanced the checkpoint, or the coverage assertion "
                    + "below would be satisfied by a leaf that simply had nothing to restamp.");

            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "control: the checkpoint as the per-partition ACCESSOR reports it, which is the "
                    + "quantity every capture gate actually reads. Partition 0 lives in the scalar "
                    + "slot and is reported as the -1 sentinel until ProjectionCheckpointOffsetAssigned "
                    + "is set (#2703), so a leaf can carry a raw scalar of 3 while every gate still "
                    + "sees -1 and declines.");

            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.Lifted),
                "control, and the point of the whole issue: the verdict was ALREADY honest on the "
                    + "cursor axis and stays honest. The defect was never a misreported verdict, so a "
                    + "fix that changed this would be fixing the wrong thing.");

            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                "THE assertion. Coverage must be restamped from the checkpoint the leaf already "
                    + "banked. Pre-fix it stays at 0: the drive's only coverage remedy is gated on "
                    + "covered < 0, so a partition whose coverage merely LAGS is declined, and the "
                    + "per-partition checkpoint > covered capture is never reached from a drive.");

            Assert.That(published, Is.Not.Empty,
                "control: the drive must republish the pin, or the offset below is stale for a "
                    + "reason unrelated to coverage.");

            Assert.That(published[^1].CheckpointOffset, Is.EqualTo(3L),
                "and the restamp must reach the wire. This is min(checkpoint, covered) as the leaf "
                    + "publishes it, which is the exact quantity ComputeMaterialiserOffsetFloorAsync "
                    + "takes the minimum over. Pre-fix it is min(3, 0) == 0, so the tree's offset "
                    + "floor cannot move and the trim scan stops on offset_floor with nothing "
                    + "reclaimed.");
        });
    }

    /// <summary>
    /// The negative direction, and the guard that keeps the fix from becoming an
    /// unconditional restamp. A drive that advances NOTHING must publish no new
    /// offset: coverage may only ever be claimed up to a checkpoint the leaf
    /// actually holds.
    /// <para>
    /// Without this, "always capture on every drive" would pass the test above
    /// while stamping coverage the cache cannot vouch for - which drops the
    /// #1535 no-loss gate and authorises trimming a prefix no consumer has read.
    /// </para>
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_claims_no_coverage_beyond_the_checkpoint_the_leaf_holds()
    {
        var wal = new GrowingWal();
        wal.GrowTo(2);
        var (grain, state, published) =
            CreateCoveredLeafWithPinCapture(wal.Coordinator, coverageOffset: 0L);

        // Activation drains the whole WAL, so the drive below finds nothing to
        // advance over and the checkpoint it could honestly claim is 2.
        await ActivateAsync(grain);
        published.Clear();

        _ = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2L),
                "control: the leaf is caught up, so the drive advanced nothing.");

            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.LessThanOrEqualTo(2L),
                "coverage may never exceed the checkpoint. A claim past it would be a claim over WAL "
                    + "this leaf has not applied, which drops the no-loss gate and licenses trimming "
                    + "committed data no consumer has read.");

            Assert.That(published.Select(r => r.CheckpointOffset), Is.All.LessThanOrEqualTo(2L),
                "and no such claim may reach the wire either, since the published pin is what the "
                    + "WAL GC trusts when it places the floor.");
        });
    }

    /// <summary>
    /// The drive must still repair coverage that is ABSENT. The fix replaces the
    /// drive's direct zero-coverage call with the shared recheck, which makes
    /// that call itself - this pins that the substitution did not drop the
    /// #2692 behaviour it subsumes.
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_still_repairs_coverage_that_is_absent()
    {
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, persistedCheckpoint: -1L);

        await ActivateAsync(grain);
        wal.GrowTo(3);

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "precondition: this leaf has NO coverage, the population #2692 repairs.");

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "the replay half of the drive is unchanged.");
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.NoAdvance),
                "and so is the verdict: this harness has no snapshot store, so nothing can cover the "
                    + "partition, and a drive that leaves it uncovered must not claim a lift.");
        });
    }
}

