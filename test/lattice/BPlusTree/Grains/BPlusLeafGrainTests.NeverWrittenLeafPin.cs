using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3453, leaf side: a never-written leaf (<c>Clock == Zero</c>) that has
/// scanned its partition through a persisted checkpoint <c>X</c> publishes
/// <c>(Zero, X)</c>, so a starvation drive can lift its <c>(Zero, -1)</c> pin.
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect.</b> <c>FlushDurableMaterialiserFrontierAsync</c> returned
/// before resolving any pin whenever the clock was Zero, and every release arm
/// of <c>ResolveDurablePinForPartition</c> resolves to <c>(clock, ...)</c>,
/// which for a Zero clock is the block pin itself. A leaf that owns no key in a
/// partition full of other leaves' writes therefore advanced its checkpoint on
/// every drive and republished nothing: the drive graded NoAdvance forever and
/// the WAL GC scheduler gave up on it.
/// </para>
/// <para>
/// <b>The observable.</b> Every published durable pin is recorded together
/// with the persisted checkpoint the leaf held at that instant, read inside the
/// reporter callback, exactly as the #3476 fixtures do. The invariant is a
/// relation between two values at one moment; reading the checkpoint
/// afterwards would let a later persist mask an over-report the monotonic pin
/// store can never lower.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    // The GrowingWal fixture stamps its entries with this tree id, so the leaf
    // must share it: its entries are then skipped for key range, not tree.
    private const string NeverWrittenTreeId = MaterialiserTreeId;

    /// <summary>One durable pin publication and the persisted checkpoint behind it.</summary>
    private readonly record struct NeverWrittenPinPublication(
        HybridLogicalClock Frontier,
        long PublishedOffset,
        long PersistedCheckpoint,
        long CurrentCheckpoint);

    /// <summary>
    /// Builds a leaf owning only the <c>["m", "n")</c> key range, with no
    /// snapshot to rehydrate from and a COALESCED checkpoint persist (an
    /// hour-long interval and an unreachable entry threshold), so a replayed
    /// advance stays pending until something flushes it - the production shape.
    /// Every batched durable pin publication is recorded.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State,
        List<NeverWrittenPinPublication> Published) CreateNeverWrittenLeafWithPinCapture(
        ILeafReplayCoordinatorGrain coordinator,
        long persistedCheckpoint = -1L,
        ILatticeFallOffLogDetector? detector = null,
        bool failSnapshotCapture = false)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        if (failSnapshotCapture)
        {
            // Every capture path faults, so no partition can gain durable
            // snapshot coverage: the data-bearing leaf stays uncovered.
            snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .ThrowsAsync(new InvalidOperationException("snapshot store unavailable"));
            snapshotStub.BeginStagedSnapshotAsync(Arg.Any<CancellationToken>())
                .ThrowsAsync(new InvalidOperationException("snapshot store unavailable"));
            snapshotStub.StageSnapshotSegmentAsync(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .ThrowsAsync(new InvalidOperationException("snapshot store unavailable"));
            snapshotStub.CommitStagedSnapshotAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .ThrowsAsync(new InvalidOperationException("snapshot store unavailable"));
        }

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = NeverWrittenTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        OwnsOnlyTheMRange()(state.State);

        BPlusLeafGrain? grain = null;
        var published = new List<NeverWrittenPinPublication>();

        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                foreach (var report in call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1))
                {
                    published.Add(new NeverWrittenPinPublication(
                        report.Frontier,
                        report.CheckpointOffset,
                        state.State.ProjectionCheckpointOffset,
                        grain!.GetCurrentCheckpointForPartition(0)));
                }
                return Task.CompletedTask;
            });

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        if (detector is not null)
            sc.AddSingleton(detector);
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

        return (grain, state, published);
    }

    /// <summary>
    /// Acceptance 1. A never-written leaf whose partition accrues only other
    /// leaves' writes after it activated is lifted by one drive: the drive
    /// persists the scanned-through advance, publishes <c>(Zero, X)</c> with X
    /// the persisted checkpoint, and grades itself Lifted.
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_lifts_a_never_written_leaf_by_publishing_its_persisted_checkpoint()
    {
        var wal = new GrowingWal();
        var (grain, state, published) = CreateNeverWrittenLeafWithPinCapture(wal.Coordinator);

        await ActivateAsync(grain);

        // Keys k1..k3 order below "m", so the leaf scans and skips all three.
        wal.GrowTo(3);
        published.Clear();

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.EqualTo(HybridLogicalClock.Zero),
                "precondition: the leaf applied nothing, so it is still never-written. Any other "
                    + "clock would route through the ordinary release arms and test nothing here.");
            Assert.That(grain.EntriesForTest, Is.Empty,
                "precondition: every entry belonged to another leaf.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "the drive must PERSIST the scanned-through advance before it publishes; the "
                    + "coalescing options would otherwise hold it pending for an hour.");
            Assert.That(published, Has.Count.EqualTo(1),
                "the drive must republish the pin. Pre-fix the flush returned at a Zero clock "
                    + "and nothing was published, so the seeded (Zero, -1) block pin stood.");
            Assert.That(published.Select(p => (p.Frontier, p.PublishedOffset)),
                Is.All.EqualTo((HybridLogicalClock.Zero, 3L)),
                "THE assertion: the release a Zero-clock leaf can express is (Zero, X), with X "
                    + "the persisted scanned-through checkpoint.");
            Assert.That(published.All(p => p.PublishedOffset == p.PersistedCheckpoint), Is.True,
                "at the moment of publication X equals the persisted checkpoint (#3476).");
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.Lifted),
                "the drive lifted the block and must say so. NoAdvance is what made the WAL GC "
                    + "scheduler loop on this leaf and give up.");
        });
    }

    /// <summary>
    /// Acceptance 2, the mutation fixture. A never-written leaf holding a
    /// PENDING advance above its persisted checkpoint that then publishes
    /// without persisting must publish the persisted value, never the pending
    /// one. The publisher is the batched durable-pin flush, driven directly:
    /// the expired-deadline deactivation this fixture used to go through now
    /// skips its frontier-pin barrier (issue #3393), so it no longer reaches
    /// the pin with a pending advance above the persisted checkpoint.
    /// </summary>
    [Test]
    public async Task Batched_pin_flush_of_a_never_written_leaf_with_a_pending_advance_publishes_the_persisted_checkpoint()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, published) = CreateNeverWrittenLeafWithPinCapture(
            wal.Coordinator, persistedCheckpoint: 1L);

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.EqualTo(HybridLogicalClock.Zero),
                "precondition: never-written.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
                "precondition: the coalescing options kept the replayed advance pending.");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "precondition: the pending advance is 3, strictly above the persisted 1, so "
                    + "publishing the pending value is observable.");
        });

        published.Clear();

        await grain.FlushDurableMaterialiserFrontierAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
                "control: the flush must not persist, or the publisher persisted first and the "
                    + "persisted-not-pending rule is not under test.");
            Assert.That(published, Is.Not.Empty,
                "the batched flush must publish the never-written release.");
            Assert.That(published.Select(p => p.CurrentCheckpoint), Is.All.EqualTo(3L),
                "control: every publication happened with the pending 3 still unpersisted.");
            Assert.That(published.Select(p => (p.Frontier, p.PublishedOffset)),
                Is.All.EqualTo((HybridLogicalClock.Zero, 1L)),
                "THE assertion: (Zero, persisted 1). Publishing the pending 3 lets the WAL GC "
                    + "trim through 3 while the next activation replays from 1 - the #3476 "
                    + "LeafProjectionStaleException, reached from a Zero clock.");
        });
    }

    /// <summary>
    /// Acceptance 3. After the WAL is trimmed through the X the drive published,
    /// a cold reactivation of the same leaf raises no
    /// <see cref="LeafProjectionStaleException"/>, and a write routed to the
    /// leaf above X is rebuilt.
    /// </summary>
    [TestCase(false, TestName = "Cold_reactivation_after_a_trim_through_the_published_never_written_pin_is_not_stale")]
    [TestCase(true, TestName = "Cold_reactivation_after_a_trim_through_the_published_never_written_pin_rebuilds_a_later_write")]
    public async Task Cold_reactivation_after_a_trim_through_the_published_never_written_pin(bool writeAboveX)
    {
        var wal = new GrowingWal();
        var (first, firstState, published) = CreateNeverWrittenLeafWithPinCapture(wal.Coordinator);
        await ActivateAsync(first);
        wal.GrowTo(3);
        published.Clear();
        await first.DriveStarvedCheckpointAsync();

        Assert.That(published, Is.Not.Empty,
            "precondition: the drive published a pin. Pre-fix it published nothing, so there "
                + "is no X the WAL GC could have trimmed through.");
        var x = published[^1].PublishedOffset;
        Assert.That(x, Is.EqualTo(3L), "precondition: X is the scanned-through 3.");

        // The WAL as the GC leaves it after trimming through X: nothing at or
        // below X is readable, and the oldest readable offset is X + 1. The
        // head is EXCLUSIVE (issue #2680): X + 1 on the emptied partition, and
        // X + 2 once one later entry sits at X + 1.
        var head = writeAboveX ? x + 2 : x + 1;
        var survivors = writeAboveX
            ? new[] { new CommitLogSliceEntry(x + 1, BuildCommittedSet("m-later", Encoding.UTF8.GetBytes("later"), treeId: NeverWrittenTreeId)) }
            : [];
        var trimmed = BuildCoordinator(head, survivors);
        trimmed.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(x + 1));

        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync(NeverWrittenTreeId, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(head));
        reader.GetTailOffsetAsync(NeverWrittenTreeId, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(x + 1));
        var detector = new LatticeFallOffLogDetector(
            new ServiceCollection().AddSingleton<ICommitLogReader>(reader).BuildServiceProvider());

        var (cold, coldState, _) = CreateNeverWrittenLeafWithPinCapture(
            trimmed, persistedCheckpoint: firstState.State.ProjectionCheckpointOffset, detector: detector);

        Assert.DoesNotThrowAsync(async () => await ActivateAsync(cold),
            "the published X is the persisted checkpoint, so the first offset the cold replay "
                + "needs is X + 1 - exactly the oldest readable offset. A pin above the persisted "
                + "checkpoint would leave that offset trimmed and throw here.");

        if (writeAboveX)
        {
            var later = await cold.GetAsync("m-later");
            Assert.Multiple(() =>
            {
                Assert.That(later is null ? null : Encoding.UTF8.GetString(later), Is.EqualTo("later"),
                    "the write above X that routed to this leaf was retained by the pin and rebuilt.");
                Assert.That(cold.GetCurrentCheckpointForPartition(0), Is.EqualTo(x + 1));
            });
        }
        else
        {
            Assert.That(cold.EntriesForTest, Is.Empty);
        }
    }

    /// <summary>
    /// The unchanged half: a never-written leaf whose checkpoint is still the
    /// <c>-1</c> sentinel has nothing to release, so the drive publishes
    /// nothing and grades NoAdvance, exactly as before. Its pin is the GC's
    /// empty-WAL rule to handle, not the leaf's.
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_publishes_nothing_for_a_never_written_leaf_with_nothing_scanned()
    {
        var wal = new GrowingWal();
        var (grain, state, published) = CreateNeverWrittenLeafWithPinCapture(wal.Coordinator);
        await ActivateAsync(grain);
        published.Clear();

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L),
                "control: an empty WAL leaves nothing to scan.");
            Assert.That(published, Is.Empty,
                "there is no persisted X, so the only expressible pin is (Zero, -1), which the "
                    + "seed already published; the flush must stay a no-op.");
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.NoAdvance));
        });
    }

    /// <summary>
    /// Acceptance 6, leaf half. A data-bearing partition without durable
    /// snapshot coverage still publishes the <c>(Zero, -1)</c> block pin
    /// (#1490, #1535): the never-written release is for leaves that own no row.
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_still_publishes_the_block_pin_for_an_uncovered_data_bearing_leaf()
    {
        var owned = new OwnedWal();
        var (dataGrain, dataState, dataPublished) = CreateNeverWrittenLeafWithPinCapture(
            owned.Coordinator, failSnapshotCapture: true);
        await ActivateAsync(dataGrain);

        // An owned write, so the leaf becomes data-bearing, and a foreign one.
        owned.Append(1, "m1");
        owned.Append(2, "a2");
        dataPublished.Clear();

        var verdict = await dataGrain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(dataState.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero),
                "precondition: the owned write made the leaf data-bearing.");
            Assert.That(dataState.State.ProjectionCheckpointOffset, Is.EqualTo(2L),
                "precondition: the drive persisted its advance.");
            Assert.That(dataPublished.Select(p => (p.Frontier, p.PublishedOffset)),
                Is.All.EqualTo((HybridLogicalClock.Zero, -1L)),
                "a partition whose only durable copy of its rows is the WAL prefix keeps the "
                    + "block pin; the #3453 release must not reach it.");
            Assert.That(dataPublished, Is.Not.Empty, "control: the drive published.");
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.NoAdvance));
        });
    }

    /// <summary>
    /// Acceptance 6, the live-row guard in isolation. A Zero-clock leaf that
    /// nonetheless holds a live cache row in the partition is not released,
    /// even with a persisted checkpoint. Dropping the live-row clause from the
    /// never-written predicate turns this red.
    /// </summary>
    [Test]
    public async Task DriveStarvedCheckpointAsync_does_not_release_a_zero_clock_partition_holding_a_live_row()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, published) = CreateNeverWrittenLeafWithPinCapture(
            wal.Coordinator, persistedCheckpoint: 3L);
        await ActivateAsync(grain);

        grain.EntriesForTest["m0"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("row"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
            IsTombstone = false,
        };
        published.Clear();

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.EqualTo(HybridLogicalClock.Zero), "precondition: Zero clock.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "precondition: a persisted scanned-through checkpoint the release would otherwise use.");
            Assert.That(published, Is.Empty,
                "a live row's only durable copy may be the WAL prefix at or below X, so the "
                    + "partition must not publish (Zero, X).");
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.NoAdvance));
        });
    }

    /// <summary>
    /// A WAL the test appends named keys to after activation. Its head is
    /// EXCLUSIVE, the next sequence to be assigned (issue #2680): 0 while
    /// empty, one above the newest entry afterwards.
    /// </summary>
    private sealed class OwnedWal
    {
        private readonly List<CommitLogSliceEntry> _entries = [];
        private long _head;

        internal ILeafReplayCoordinatorGrain Coordinator { get; }

        internal OwnedWal()
        {
            var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
            coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(_head));

            // Served from the live list, so entries appended after activation are
            // read too, through whichever overload the leaf's ownership selects.
            ReplaySliceStub.ServeBothOverloads(coord, _entries);
            Coordinator = coord;
        }

        internal void Append(long offset, string key)
        {
            _entries.Add(new CommitLogSliceEntry(
                offset,
                BuildCommittedSet(key, Encoding.UTF8.GetBytes($"v-{key}"), hlcPhysical: 100 + offset, treeId: NeverWrittenTreeId)));
            _head = offset + 1;
        }
    }
}
