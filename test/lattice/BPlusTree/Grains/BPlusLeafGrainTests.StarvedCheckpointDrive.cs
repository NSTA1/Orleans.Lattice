using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Grain-level regression tests for issue #2692 Half B: a leaf that is already
/// resident must be able to advance its own projection checkpoint on demand.
/// </summary>
/// <remarks>
/// <para>
/// <b>The reach argument, as narrowed by #2871.</b> It is no longer true that
/// <c>ReplayWalSinceCheckpointAsync</c> has a single call site inside
/// <c>OnActivateAsync</c> - #2871 added a second, and <c>GetTreeIdAsync()</c> is
/// no longer read-only. What survives, and is what this fixture pins, is
/// narrower: <c>EnsureReplayStarted()</c> latches on
/// <c>_replayBarrierSatisfied</c>, so a leaf whose activation replay
/// <i>succeeded</i> short-circuits every later touch for the life of that
/// activation. #2871 re-arms a leaf whose previous replay <b>faulted</b>; this
/// issue repairs the disjoint population whose replay <b>succeeded</b> and then
/// went stale. Neither subsumes the other. So the per-partition checkpoint
/// advance stays unreachable for a leaf that stays resident and healthy. Its
/// <see cref="LeafNodeState.ProjectionCheckpointOffset"/> stays at the "nothing
/// applied" sentinel indefinitely, that sentinel is what the WAL GC retention
/// floor takes the minimum over, and the tree can therefore never trim.
/// </para>
/// <para>
/// This is a claim about <i>reach</i>, not about frequency, which is why it is
/// pinned here rather than inferred from a deployment census. It holds for any
/// leaf that stays resident, whatever its write volume - and the census bears
/// that out in the direction that matters: the busiest tree in the cohort was
/// never blocked and a far quieter one was permanently blocked, so write volume
/// is not the discriminator and must not be used as the trigger.
/// </para>
/// <para>
/// <b>Why the checkpoint offset is the observable.</b> Asserting that the sweep
/// issued a call, or that the drive returned, would be asserting a proxy. The
/// quantity the WAL GC floor actually evaluates is the persisted checkpoint, so
/// that is what these tests read. The distinction is the whole defect: the old
/// sweep's call was delivered, returned cleanly, and was recorded as
/// <c>Completed</c> while achieving nothing.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// A coordinator whose WAL <b>grows after the leaf has activated</b>, which
    /// is the production shape this issue is about: the leaf came online, drained
    /// what existed, and has been serving ever since while its partition kept
    /// accruing entries it will never be asked to replay.
    /// </summary>
    private sealed class GrowingWal
    {
        private readonly List<CommitLogSliceEntry> _entries = [];

        /// <summary>Offset of the newest appended entry, or <c>-1</c> for an empty WAL.</summary>
        internal long NewestOffset { get; private set; } = -1L;

        /// <summary>
        /// The partition head this stub reports: EXCLUSIVE, the next sequence to be
        /// assigned, so always one above <see cref="NewestOffset"/> (issue #2680).
        /// Reporting the newest entry itself as the head would be a WAL state no
        /// production partition can reach.
        /// </summary>
        internal long Head => NewestOffset + 1;

        internal ILeafReplayCoordinatorGrain Coordinator { get; }

        /// <summary>
        /// Optional hook awaited inside <c>ReadSliceAsync</c>, so a test can make
        /// a slice read fail or park. Null for every test that does not set it,
        /// so the default read path is byte-identical to what it was.
        /// </summary>
        /// <remarks>
        /// Set it <b>after</b> activation. Activation replay reads through the
        /// same substitute, so a hook installed before it would perturb the
        /// activation this fixture depends on rather than the drive under test.
        /// </remarks>
        internal Func<Task>? OnRead { get; set; }

        internal GrowingWal()
        {
            var coord = Substitute.For<ILeafReplayCoordinatorGrain>();

            // Resolved per call rather than captured, so a test can grow the WAL
            // between activation and the drive.
            coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(Head));
            coord.ReadSliceAsync(
                    Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var fromExclusive = call.ArgAt<long>(0);
                    var toInclusive = call.ArgAt<long>(1);
                    var budget = call.ArgAt<int>(2);
                    var slice = new List<CommitLogSliceEntry>();
                    foreach (var e in _entries)
                    {
                        if (e.Offset <= fromExclusive) continue;
                        if (e.Offset > toInclusive) break;
                        slice.Add(e);
                        if (slice.Count >= budget) break;
                    }

                    var hook = OnRead;
                    return hook is null
                        ? Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice)
                        : Hooked(hook, slice);
                });

            Coordinator = coord;

            static async Task<IReadOnlyList<CommitLogSliceEntry>> Hooked(
                Func<Task> hook, List<CommitLogSliceEntry> slice)
            {
                await hook();
                return slice;
            }
        }

        /// <summary>Appends committed sets at offsets 1..<paramref name="through"/>.</summary>
        internal void GrowTo(long through)
        {
            for (var offset = NewestOffset + 1; offset <= through; offset++)
            {
                if (offset <= 0) continue;
                _entries.Add(new CommitLogSliceEntry(
                    offset,
                    BuildCommittedSet($"k{offset}", Encoding.UTF8.GetBytes($"v{offset}"))));
            }
            NewestOffset = through;
            ReachableWalFixture.EnsureReachable(Head, _entries);
        }
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_advances_the_checkpoint_of_a_leaf_that_is_already_resident()
    {
        // The whole of issue #2692 Half B in one test, stated as a contrast
        // between the call the sweep used to make and the call it makes now.
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, persistedCheckpoint: -1L);

        await ActivateAsync(grain);

        // The leaf is now resident with an empty WAL behind it, and its
        // partition starts accruing entries it will never be asked to replay.
        wal.GrowTo(3);

        // What the sweep did before this fix. It is a real grain call, it is
        // delivered, it returns cleanly, and the sweep recorded it as a
        // completed reactivation. This clause asserts that it achieves nothing,
        // which is the defect - and it doubles as a harness control: if the
        // fixture were replaying by some other route, it would fail here rather
        // than silently making the next clause pass for the wrong reason.
        _ = await grain.GetTreeIdAsync();

        // #2909/#2871 hardening, and the reason this clause is trustworthy at
        // all. The touch no longer RUNS a replay - it ARMS one and returns. So
        // reading the checkpoint straight afterwards would read an absence
        // produced by a replay that had not finished yet, which is byte-identical
        // to the absence this clause means to assert: that the touch achieved
        // nothing. Draining whatever the touch armed collapses the two. If the
        // touch ever does reach a replay, this drain lets it land and the
        // assertion below goes red - which is precisely what it must do, because
        // that would mean the defect this fixture exists to pin had been repaired
        // somewhere else and the fixture was now asserting a stale world.
        if (grain.ReplayBarrierForTest is { } armedByTouch)
        {
            await armedByTouch;
        }

        Assert.That(
            state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L),
            "the sweep's touch cannot advance a checkpoint on a leaf whose "
            + "activation replay already SUCCEEDED: EnsureReplayStarted latches "
            + "on _replayBarrierSatisfied, so the touch short-circuits for the "
            + "life of that activation. MEASURED past the deferral, not merely "
            + "read early - the drive armed by the touch is drained above, so a "
            + "-1 here is what the touch achieved and not when it was sampled.");

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "the drive must reach the same per-partition advance activation "
                + "reaches. This offset, not the fact that a call was issued, is "
                + "what LatticeWalGc takes the minimum over to place the WAL "
                + "retention floor.");
            Assert.That(
                verdict, Is.EqualTo(LeafStarvationDriveOutcome.NoAdvance),
                "MEASURED, and the subtlest property in this change: the "
                + "checkpoint advanced (asserted above) and the drive still must "
                + "NOT report the pin lifted, because this leaf's partition "
                + "carries no durable snapshot coverage. ResolveDurablePinForPartition "
                + "computes min(checkpoint, covered) and returns the Zero block pin "
                + "whenever that is negative, so an advance alone leaves the leaf "
                + "pinning the WAL exactly as before. Reporting 'lifted' here "
                + "would rebuild the defect one layer up: a verdict that reads "
                + "as repair while nothing was repaired. The paired fixture below "
                + "supplies the coverage and gets the opposite verdict.");
        });
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_reports_no_advance_when_the_wal_holds_nothing_past_the_checkpoint()
    {
        // The distinction that makes the verdict an enum rather than a bool. A
        // drive can run in full and lift nothing, and the sweep must be able to
        // tell that apart from a repair, or it rebuilds one layer down exactly
        // the vacuous success this issue is about.
        var wal = new GrowingWal();
        wal.GrowTo(2);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, persistedCheckpoint: -1L);

        await ActivateAsync(grain);

        // Activation already drained it, so there is nothing left to advance
        // over. The leaf is caught up, not starved.
        var before = state.State.ProjectionCheckpointOffset;
        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo(2L),
                "control: activation must have drained the backlog, or this test "
                + "would report 'no advance' for the wrong reason.");
            Assert.That(
                verdict, Is.EqualTo(LeafStarvationDriveOutcome.NoAdvance),
                "a drive that found nothing to do must say so on its own arm.");
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(2L),
                "and must leave the checkpoint exactly where it found it.");
        });
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_reports_memory_refused_when_the_slice_read_hits_heap_pressure()
    {
        // Exists because arming a tag value does not make it reachable. Priming
        // proves the PRIMING path ran; it says nothing about whether the
        // RECORDING path can ever be entered. A primed arm whose producing code
        // is unreachable stays frozen at zero forever and passes every metric
        // gate, reading to an operator as "measured, never happened". The only
        // thing that separates that from a genuinely quiet arm is a test that
        // drives the production path into it, which is what this is.
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, persistedCheckpoint: -1L);

        await ActivateAsync(grain);

        // Installed after activation, so what this perturbs is the drive's
        // replay and not the activation replay the fixture depends on.
        wal.GrowTo(3);
        wal.OnRead = () => throw new OutOfMemoryException(
            "simulated heap exhaustion inside the WAL slice read");

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                verdict, Is.EqualTo(LeafStarvationDriveOutcome.MemoryRefused),
                "a drive refused for heap pressure must report on its own arm. "
                + "Folded into NoAdvance it would present a transient resource "
                + "stall as a permanent structural block, which is the reading "
                + "that stops an operator looking any further.");
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L),
                "control: the drive must have reached the read and got no "
                + "further. A checkpoint that moved would mean the replay "
                + "completed and this verdict came from somewhere other than "
                + "the path under test.");
        });
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_reports_already_driving_for_a_reentrant_call()
    {
        // The second of the two arms priming alone cannot vouch for. This one is
        // reachable only under interleaving, which is precisely why it would
        // otherwise ship armed, frozen at zero, and indistinguishable from
        // correct-and-quiet.
        var wal = new GrowingWal();
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, persistedCheckpoint: -1L);

        await ActivateAsync(grain);

        wal.GrowTo(3);
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        wal.OnRead = () => parked.Task;

        // Deliberately not awaited. The in-flight flag is set before the first
        // await, so this call holds it while parked inside the replay.
        var inFlight = grain.DriveStarvedCheckpointAsync();

        Assert.That(
            inFlight.IsCompleted, Is.False,
            "control, and the clause that makes the assertion below mean "
            + "anything: had the first drive run to completion synchronously "
            + "there would be no drive in flight, the second call would return "
            + "its ordinary verdict, and this test would be asserting "
            + "reentrancy against a grain that was never reentered.");

        var reentrant = await grain.DriveStarvedCheckpointAsync();

        Assert.That(
            reentrant, Is.EqualTo(LeafStarvationDriveOutcome.AlreadyDriving),
            "a second drive arriving while one is in flight must decline on its "
            + "own arm rather than start a duplicate replay. The method carries "
            + "[AlwaysInterleave], so the sweep genuinely can re-enter it.");

        parked.SetResult();
        _ = await inFlight;
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_reports_not_driven_when_the_leaf_has_no_tree_id()
    {
        // Expected to stay at zero in production, because the sweep only selects
        // leaves a blocking report named. It is published rather than collapsed
        // so that a disagreement between the blocking report and the grain
        // surfaces as a number instead of being inferred from the absence of the
        // other arms.
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, persistedCheckpoint: -1L);

        await ActivateAsync(grain);
        wal.GrowTo(3);
        state.State.TreeId = null;

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                verdict, Is.EqualTo(LeafStarvationDriveOutcome.NotDriven),
                "a leaf with no tree id has no WAL to replay against.");
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L),
                "and nothing may be advanced on its behalf.");
        });
    }

    /// <summary>
    /// Builds a leaf whose partition already carries durable snapshot coverage,
    /// by wiring a snapshot storage grain that hands back a blob at activation.
    /// <para>
    /// This exists so the <see cref="LeafStarvationDriveOutcome.Lifted"/> arm is
    /// proven <b>reachable</b> and not merely enforced. Without it the suite
    /// would assert only that the drive declines to claim a lift, which a build
    /// that could never claim one would satisfy vacuously - and unreached code
    /// is the most common reason a clause turns out not to be perturbable.
    /// </para>
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateCoveredLeaf(
        ILeafReplayCoordinatorGrain coordinator,
        long coverageOffset)
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

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-starved-drive-covered";
        state.State.ProjectionCheckpointOffset = -1L;

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                WalPartitions = 1,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state);
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_reports_lifted_only_when_the_partition_is_also_covered()
    {
        // The other half of the both-halves property, and the control for the
        // clause above. This leaf is set up identically except that its
        // partition carries durable snapshot coverage, and the verdict flips.
        //
        // The pair is what makes either assertion mean anything: on its own,
        // "does not report Lifted" is satisfied by a build that can never report
        // Lifted at all, and "reports Lifted" is satisfied by a build that has
        // dropped the coverage clause entirely. Together they pin the
        // conjunction - and only the conjunction - because the two fixtures
        // differ in exactly one input.
        var wal = new GrowingWal();
        var (grain, state) = CreateCoveredLeaf(wal.Coordinator, coverageOffset: 0L);

        await ActivateAsync(grain);
        wal.GrowTo(3);

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the drive must have advanced, or 'Lifted' would be "
                + "reported for a leaf that did nothing.");
            Assert.That(
                verdict, Is.EqualTo(LeafStarvationDriveOutcome.Lifted),
                "with a checkpoint advanced AND the partition covered, "
                + "min(checkpoint, covered) is a real offset, the Zero block pin "
                + "resolves, and the leaf has genuinely stopped pinning the WAL.");
        });
    }
}
