using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3476: a published durable materialiser pin must never exceed the
/// leaf's PERSISTED projection checkpoint.
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect.</b> The pin was <c>min(GetCurrentCheckpointForPartition(p),
/// covered(p))</c>, and the current checkpoint is <c>max(persisted, pending)</c>.
/// Once the snapshot recheck restamps coverage from that same current value
/// (#3224), both arms of the <c>min</c> sit on the UNPERSISTED pending advance.
/// The WAL GC trims to the pin, the next activation replays from the persisted
/// checkpoint, finds the WAL trimmed past it, and throws
/// <c>LeafProjectionStaleException</c>. On the live estate that parked 965 leaves.
/// </para>
/// <para>
/// <b>Two changes, two disjoint fixtures.</b> The fix clamps the pin to the
/// persisted checkpoint in <c>ResolveDurablePinForPartition</c>, and makes the
/// starvation drive persist its pending advance before it restamps coverage and
/// publishes. Each fixture below observes exactly one of them: the clamp is only
/// observable on a publisher that does NOT persist first, and the drive's flush
/// is only observable as the drive's pin still ADVANCING under the clamp. A
/// single fixture over both would stay green with either change reverted.
/// </para>
/// <para>
/// <b>The observable.</b> Each report is paired with the persisted checkpoint the
/// leaf held at the instant it published, read inside the reporter callback. The
/// invariant is a relation between two values at one moment, so reading either
/// afterwards would let a later persist mask an over-report that the monotonic
/// pin store has already merged and can never lower.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>One durable pin publication and the leaf state it was published against.</summary>
    private readonly record struct PinPublication(
        long PublishedOffset,
        long PersistedCheckpoint,
        long CurrentCheckpoint,
        long Coverage);

    /// <summary>
    /// Builds a leaf with durable snapshot coverage at 0 whose checkpoint persist
    /// is COALESCED (an hour-long interval and an unreachable entry threshold), so
    /// a replayed advance stays pending rather than persisting on arrival. Every
    /// durable pin the leaf publishes - through the batched flush or the
    /// per-consumer mirror - is recorded with the checkpoint state behind it.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State,
        List<PinPublication> Published, List<long> DurableWrites) CreateCoalescingLeafWithPinCapture(
        ILeafReplayCoordinatorGrain coordinator,
        ILatticeFallOffLogDetector? detector = null,
        ILeafSnapshotStorageGrain? snapshotStore = null,
        ILogger<BPlusLeafGrain>? logger = null,
        int partitionCount = 1,
        TimeSpan? driveBudget = null,
        Func<Task>? beforePinFlush = null,
        Action<string>? onCoordinatorLookup = null,
        Action<long>? onDurablePinFlushed = null)
    {
        var snapshotStub = snapshotStore ?? Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 0L,
                Rows = [],
                CapturedAtTicks = 1L,
                SnapshotOffsetsByPartition = new long[partitionCount],
            }));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-3476-pin-persisted";
        state.State.ProjectionCheckpointOffset = -1L;

        var durableWrites = new List<long>();
        state.OnWriteState = s => durableWrites.Add(s.ProjectionCheckpointOffset);

        BPlusLeafGrain? grain = null;
        var published = new List<PinPublication>();

        void Record(long offset) => published.Add(new PinPublication(
            offset,
            state.State.ProjectionCheckpointOffset,
            grain!.GetCurrentCheckpointForPartition(0),
            grain.DurableSnapshotCoverageForPartition(0)));

        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                if (beforePinFlush is not null)
                    await beforePinFlush();
                foreach (var report in call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1))
                {
                    Record(report.CheckpointOffset);
                    onDurablePinFlushed?.Invoke(report.CheckpointOffset);
                }
            });
        reporter
            .When(r => r.NoteDurableMaterialiserFrontier(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<long>()))
            .Do(call => Record(call.ArgAt<long>(3)));

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        if (detector is not null)
            sc.AddSingleton(detector);
        if (logger is not null)
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            loggerFactory.CreateLogger(Arg.Any<string>()).Returns(logger);
            sc.AddSingleton(loggerFactory);
        }
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(call =>
        {
            onCoordinatorLookup?.Invoke(call.ArgAt<string>(0));
            return coordinator;
        });
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                // Coalesced persist: nothing below reaches either threshold, so a
                // replayed advance is PENDING until something flushes it. This is
                // the production shape; the zero interval the sibling fixtures pin
                // persists on every entry and hides the defect entirely.
                MaterialiserCheckpointInterval = TimeSpan.FromHours(1),
                MaterialiserCheckpointEntries = 1_000_000,
                WalPartitions = partitionCount,
                StarvationDriveBudget = driveBudget ?? TimeSpan.FromMinutes(1),
                LeafSnapshotReClassifyEveryNCheckpoints = 1000,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state, published, durableWrites);
    }

    /// <summary>
    /// THE clamp fixture. A leaf holding a pending advance that coverage has
    /// ALREADY been restamped over, and that then publishes without persisting,
    /// must publish its persisted checkpoint and not the pending one.
    /// <para>
    /// The publisher is the batched durable-pin flush itself, driven directly.
    /// This fixture used to reach it through graceful deactivation under an
    /// expired deadline (the checkpoint-flush barrier faulting before it
    /// persisted, and the frontier-pin barrier running anyway). Issue #3393
    /// made that barrier SKIP on an expired deadline, so no deactivation path
    /// reaches the pin with <c>pending &gt; persisted</c> any longer; the
    /// skip is pinned by the <c>DeactivationPinFinalAdvance</c> fixtures. The
    /// clamp is still the last line of defence for any publisher that does, so
    /// it is exercised here on the one method every publisher routes through.
    /// </para>
    /// <para>
    /// Why this fixture observes the <c>ResolveDurablePinForPartition</c> clamp
    /// and nothing else: coverage is 3 and the current checkpoint is 3, so the
    /// only term that can hold the pin at 0 is the persisted checkpoint. Revert
    /// the clamp and the pin reads <c>min(3, 3) == 3</c> against a persisted 0.
    /// </para>
    /// </summary>
    [Test]
    public async Task Batched_pin_flush_over_an_unpersisted_advance_publishes_no_pin_past_the_persisted_checkpoint()
    {
        var wal = new GrowingWal();
        var (grain, state, published, durableWrites) = CreateCoalescingLeafWithPinCapture(wal.Coordinator);

        // The activation replay applies 1..3 and, under the coalescing options,
        // leaves the advance PENDING. The coverage-lag tick then restamps
        // coverage from the CURRENT checkpoint - the #3224 path that lifts the
        // coverage arm of the min onto the pending advance.
        wal.GrowTo(3);
        await ActivateAsync(grain);
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero),
                "precondition: the replay applied real entries, so every publisher is live. A "
                    + "Zero clock returns before resolving any pin.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
                "precondition: the persisted checkpoint is the one the snapshot rehydrated, 0. "
                    + "Anything higher means the coalescing options did not hold the advance "
                    + "pending, and the fixture would assert nothing.");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "precondition: the pending advance is 3, strictly above persisted.");
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                "precondition: coverage was restamped from the pending checkpoint, so the "
                    + "coverage arm of the min no longer protects the persisted one. Without "
                    + "this the pre-fix pin is min(3, 0) == 0 and the fixture cannot redden.");
        });

        published.Clear();
        durableWrites.Clear();

        await grain.FlushDurableMaterialiserFrontierAsync();

        Assert.Multiple(() =>
        {
            Assert.That(durableWrites, Is.Empty,
                "control: the batched flush must not persist, or the clamp is not what is "
                    + "under test.");
            Assert.That(published, Is.Not.Empty,
                "control: the flush must publish, or an empty list satisfies the assertions "
                    + "below vacuously.");
            Assert.That(published.Select(p => p.CurrentCheckpoint), Is.All.EqualTo(3L),
                "control: every publication happened while the pending 3 was still unpersisted.");
            Assert.That(published.Select(p => p.PublishedOffset), Is.All.EqualTo(0L),
                "THE assertion. The pin must be the persisted checkpoint, 0. Pre-fix it is "
                    + "min(current 3, covered 3) == 3: the WAL GC trims through 3, the next "
                    + "activation replays from 0, and the replay throws LeafProjectionStaleException.");
            Assert.That(published.All(p => p.PublishedOffset <= p.PersistedCheckpoint), Is.True,
                "the #3476 invariant, stated as the contract #3453 builds on: at the moment of "
                    + "publication the pin is at or below the persisted checkpoint.");
        });
    }

    /// <summary>
    /// THE drive fixture. With a coalesced persist, a starvation drive that
    /// replays new WAL must still ADVANCE the pin it publishes - which under the
    /// clamp it can do only by persisting its pending advance first.
    /// <para>
    /// Why this fixture observes the drive's flush and nothing else: without it
    /// the replayed 3 stays pending, the clamp holds the pin at the persisted 0,
    /// and the drive publishes 0 - a drive that lifts nothing, which is exactly
    /// the dormant floor holder #3166 exists to lift. The clamp alone therefore
    /// fails this fixture on the published value, and with neither change the
    /// pre-fix pin of 3 over a persisted 0 fails it on the invariant.
    /// </para>
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_persists_a_coalesced_advance_before_it_publishes_the_pin()
    {
        var wal = new GrowingWal();
        var (grain, state, published, durableWrites) = CreateCoalescingLeafWithPinCapture(wal.Coordinator);

        await ActivateAsync(grain);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
            "precondition: the leaf starts persisted at the rehydrated coverage, 0.");

        wal.GrowTo(3);
        published.Clear();
        durableWrites.Clear();

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(durableWrites, Does.Contain(3L),
                "the drive must make its replayed advance DURABLE. The coalescing options would "
                    + "otherwise leave it pending for up to an hour.");
            Assert.That(published, Is.Not.Empty,
                "control: the drive must republish the pin.");
            Assert.That(published[^1].PublishedOffset, Is.EqualTo(3L),
                "and the pin it publishes must reach the replayed checkpoint. Without the drive's "
                    + "flush the clamp holds it at the persisted 0 and the drive lifts nothing.");
            Assert.That(published.All(p => p.PublishedOffset <= p.PersistedCheckpoint), Is.True,
                "the #3476 invariant: no publication on the drive path may exceed the checkpoint "
                    + "the leaf had persisted at that moment. Pre-fix the drive published 3 over 0.");
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.Lifted),
                "control: the drive grades itself Lifted, as the sibling fixtures pin.");
        });
    }
}
