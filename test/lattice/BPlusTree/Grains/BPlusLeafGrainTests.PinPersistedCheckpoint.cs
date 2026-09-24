using Microsoft.Extensions.DependencyInjection;
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
/// <b>Two changes, one fixture on this release line.</b> The fix clamps the pin
/// to the persisted checkpoint in <c>ResolveDurablePinForPartition</c>, and makes
/// the starvation drive persist its pending advance before it restamps coverage
/// and publishes. The drive's flush is observable here as the drive's pin still
/// ADVANCING under the clamp, and the fixture below pins it.
/// </para>
/// <para>
/// <b>Why the clamp has no fixture on release/9.7.</b> On <c>main</c> the clamp
/// is observed through graceful deactivation under an expired deadline, where
/// per-barrier containment (#3387) runs the frontier-pin barrier after the
/// checkpoint-flush barrier faults. This line predates that containment: a
/// faulted checkpoint flush skips the frontier-pin barrier, so that publisher
/// never reaches the pin with a pending advance above the persisted checkpoint,
/// and neither the activation publish nor the coverage-lag tick publishes in that
/// state either. The clamp ships here as defence in depth; the drive, which is
/// the path that produced #3476, is covered.
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
        ILeafReplayCoordinatorGrain coordinator)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 0L,
                Rows = [],
                CapturedAtTicks = 1L,
                SnapshotOffsetsByPartition = [0L],
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
            .Returns(call =>
            {
                foreach (var report in call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1))
                {
                    Record(report.CheckpointOffset);
                }
                return Task.CompletedTask;
            });
        reporter
            .When(r => r.NoteDurableMaterialiserFrontier(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<long>()))
            .Do(call => Record(call.ArgAt<long>(3)));

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
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
                WalPartitions = 1,
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
