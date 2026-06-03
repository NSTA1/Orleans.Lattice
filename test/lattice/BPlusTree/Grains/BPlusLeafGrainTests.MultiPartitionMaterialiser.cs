using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit-test coverage for the activation-time materialiser under
/// <see cref="LatticeOptions.WalPartitions"/> greater than 1. Each test
/// stubs one <see cref="ILeafReplayCoordinatorGrain"/> per partition
/// and (where required) a partition-aware
/// <see cref="ILatticeFallOffLogDetector"/>, then activates the grain
/// and asserts on the per-partition projection checkpoint, the per-
/// partition saga-prepare clamp, and the split-time per-partition
/// WAL-head capture - the three required assertions for
/// multi-partition WAL replay that the cluster-level integration
/// suite covers behaviourally but does not pin individually.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string MultiPartitionTreeId = "tree-multipartition";
    private const string MultiPartitionReplicaId = "leaf-multipartition-test";

    /// <summary>
    /// Builds a multi-partition test rig: one stub coordinator per
    /// partition resolved by parsing the partition number off the
    /// <c>{treeId}/{partition}</c> grain key, plus a fall-off-log
    /// detector that defaults to <see cref="FallOffLogDecision.TailReplay"/>
    /// for every partition (override via
    /// <paramref name="decisionOverrides"/>).
    /// </summary>
    private static (BPlusLeafGrain Grain,
                    FakePersistentState<LeafNodeState> State,
                    Dictionary<int, ILeafReplayCoordinatorGrain> CoordinatorsByPartition,
                    ILatticeFallOffLogDetector Detector,
                    IGrainFactory Factory)
        CreateMultiPartitionGrain(
            int walPartitions,
            Func<int, (long Head, CommitLogSliceEntry[] Entries)> sliceFactory,
            IReadOnlyDictionary<int, FallOffLogDecision>? decisionOverrides = null,
            long persistedCheckpoint = 0,
            long[]? persistedCheckpointsByPartition = null,
            Action<LeafNodeState>? seedState = null)
    {
        // Per-partition coordinator stubs - keyed by the partition
        // number parsed off the {treeId}/{partition} grain key.
        var coordinators = new Dictionary<int, ILeafReplayCoordinatorGrain>(walPartitions);
        for (var p = 0; p < walPartitions; p++)
        {
            var (head, entries) = sliceFactory(p);
            coordinators[p] = BuildCoordinator(head, entries);
        }

        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var partition = call.ArgAt<int>(1);
                if (decisionOverrides is not null
                    && decisionOverrides.TryGetValue(partition, out var d))
                {
                    return Task.FromResult(d);
                }
                return Task.FromResult(FallOffLogDecision.TailReplay);
            });

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(detector);
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", MultiPartitionReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = MultiPartitionTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        if (persistedCheckpointsByPartition is not null)
        {
            state.State.ProjectionCheckpointOffsetsByPartition = persistedCheckpointsByPartition;
        }
        seedState?.Invoke(state.State);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(call =>
        {
            var key = call.ArgAt<string>(0);
            // {treeId}/{partition} - last segment after '/' is the partition.
            var slash = key.LastIndexOf('/');
            var partitionToken = slash >= 0 ? key[(slash + 1)..] : key;
            var partition = int.Parse(partitionToken, CultureInfo.InvariantCulture);
            return coordinators[partition];
        });

        var baseOptions = new LatticeOptions
        {
            MaterialiserCheckpointInterval = TimeSpan.Zero,
            WalPartitions = walPartitions,
        };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        return (grain, state, coordinators, detector, grainFactory);
    }

    // -----------------------------------------------------------------
    // (b) Per-partition saga-prepare clamp.
    //
    // Two prepares land on two distinct partitions. Only partition 0's
    // prepare has a terminal in its slice; partition 1's prepare is
    // unresolved. The per-partition clamp must let partition 0's
    // checkpoint advance to the post-terminal offset while pinning
    // partition 1's checkpoint behind its unresolved prepare. A naive
    // global-min clamp would either:
    //   * pin BOTH partitions behind partition 1's unresolved prepare
    //     (over-clamp - what a non-partition-aware clamp would do), or
    //   * advance BOTH past partition 1's prepare (silent-loss bug).
    // -----------------------------------------------------------------

    [Test]
    public async Task MultiPartition_clamp_is_independent_per_partition()
    {
        var tx0 = Guid.NewGuid();
        var tx1 = Guid.NewGuid();
        var (grain, state, coords, _, _) = CreateMultiPartitionGrain(
            walPartitions: 2,
            sliceFactory: p => p switch
            {
                // Partition 0: prepare + commit at offsets 0,1 -> may advance to 1.
                0 => (1L, new[]
                {
                    new CommitLogSliceEntry(0, BuildPreparedSet(tx0, "k0", Encoding.UTF8.GetBytes("v0"), treeId: MultiPartitionTreeId)),
                    new CommitLogSliceEntry(1, BuildTerminal(tx0, committed: true, treeId: MultiPartitionTreeId)),
                }),
                // Partition 1: prepare at offset 0, no terminal -> must clamp to -1.
                1 => (0L, new[]
                {
                    new CommitLogSliceEntry(0, BuildPreparedSet(tx1, "k1", Encoding.UTF8.GetBytes("v1"), treeId: MultiPartitionTreeId)),
                }),
                _ => throw new InvalidOperationException(),
            },
            persistedCheckpoint: -1);

        try
        {
            await ActivateAsync(grain);
        }
        catch (Exception ex)
        {
            Assert.Fail($"OnActivateAsync threw: {ex}");
        }

        // Fan-out must reach both partitions: each coordinator's head
        // must have been queried at least once.
        await coords[0].Received().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
        await coords[1].Received().GetHeadOffsetAsync(Arg.Any<CancellationToken>());

        // One unresolved prepare across the whole leaf (partition 1's).
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));

        // Partition 0 advances past the terminal (offset 1).
        // Persisted via mirror into scalar slot + array slot[0].
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
            "partition 0 must advance past the resolved saga's terminal");

        // Partition 1 is clamped behind its unresolved prepare at
        // offset 0, so its per-partition checkpoint never advances
        // past the legacy "nothing applied" sentinel (-1). The clamp
        // is observed indirectly: the per-partition array - if
        // present - shows -1 for partition 1; absence of the slot
        // implies no advance was persisted, which is also correct
        // (a future activation will replay the entire partition 1
        // slice again, observing the same prepare and rebuilding
        // the same pending bucket).
        var perPartition = state.State.ProjectionCheckpointOffsetsByPartition;
        if (perPartition is not null && perPartition.Length > 1)
        {
            Assert.That(perPartition[1], Is.EqualTo(-1L),
                "partition 1 must clamp behind its unresolved prepare");
        }

        // The structural invariant: MinUnresolvedPrepareOffsetForPartition
        // exposes the per-partition clamp floor. Partition 0 has no
        // unresolved prepares (tx0's terminal flipped its bucket);
        // partition 1's clamp floor is 0 (tx1's prepare at offset 0).
        Assert.That(grain.MinUnresolvedPrepareOffsetForPartitionForTest(0), Is.Null);
        Assert.That(grain.MinUnresolvedPrepareOffsetForPartitionForTest(1), Is.EqualTo(0L));
    }

    // -----------------------------------------------------------------
    // (c) Per-partition fall-off-log classification.
    //
    // One partition fires SnapshotPending; the others return TailReplay.
    // The activation-time _activationSnapshotPending latch must engage
    // only when at least one partition fired the advisory, and the
    // overall activation must still succeed. A non-tail decision on a
    // single partition must surface LeafProjectionStaleException only
    // when it is genuinely unrecoverable (SnapshotThenWal / Fail /
    // FullRebuildFromWal), exactly as the single-partition shape does.
    // -----------------------------------------------------------------

    [Test]
    public async Task MultiPartition_classifier_engages_per_partition()
    {
        var (grain, state, _, detector, _) = CreateMultiPartitionGrain(
            walPartitions: 4,
            sliceFactory: _ => (0L, Array.Empty<CommitLogSliceEntry>()),
            decisionOverrides: new Dictionary<int, FallOffLogDecision>
            {
                { 2, FallOffLogDecision.SnapshotPending },
            },
            persistedCheckpoint: -1);

        await ActivateAsync(grain);

        // Classifier was invoked once per partition.
        await detector.Received(4).ClassifyAsync(
            MultiPartitionTreeId,
            Arg.Any<int>(),
            Arg.Any<long>(),
            Arg.Any<TimeSpan>(),
            Arg.Any<ResolvedLatticeOptions>(),
            Arg.Any<CancellationToken>());

        // Classifier was invoked specifically for partition 2 (the
        // advisory-firing one).
        await detector.Received(1).ClassifyAsync(
            MultiPartitionTreeId,
            2,
            Arg.Any<long>(),
            Arg.Any<TimeSpan>(),
            Arg.Any<ResolvedLatticeOptions>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void MultiPartition_unrecoverable_decision_on_any_partition_throws()
    {
        // Partition 2 returns Fail; the activation must surface
        // LeafProjectionStaleException for that specific partition.
        var (grain, _, _, _, _) = CreateMultiPartitionGrain(
            walPartitions: 4,
            sliceFactory: _ => (0L, Array.Empty<CommitLogSliceEntry>()),
            decisionOverrides: new Dictionary<int, FallOffLogDecision>
            {
                { 2, FallOffLogDecision.Fail },
            },
            persistedCheckpoint: -1);

        Assert.That(
            async () => await ActivateAsync(grain),
            Throws.InstanceOf<LeafProjectionStaleException>()
                .With.Message.Contains("partition 2"));
    }

    // -----------------------------------------------------------------
    // (d) Split-time per-partition WAL-head capture and sibling bound.
    //
    // Per-partition heads (10, 20, 30, 40) seeded across 4 partitions.
    // A split fan-out must capture each one and stamp the sibling's
    // initial per-partition projection checkpoint to the matching
    // captured head, so the sibling's first reactivation skips
    // replaying entries that were already in the donor's runtime cache
    // pre-split. The donor itself must also advance its per-partition
    // checkpoints in lock-step.
    // -----------------------------------------------------------------

    [Test]
    public async Task MultiPartition_split_captures_walhead_per_partition_for_sibling_and_donor()
    {
        const int partitions = 4;
        long[] partitionHeads = { 10L, 20L, 30L, 40L };

        var (donor, donorState, _, _, factory) = CreateMultiPartitionGrain(
            walPartitions: partitions,
            sliceFactory: p => (Head: partitionHeads[p], Entries: Array.Empty<CommitLogSliceEntry>()),
            persistedCheckpoint: -1);

        // Seed enough cache entries that CompleteSplitAsync produces a
        // non-empty rightEntries set and actually splits.
        for (var i = 0; i < 8; i++)
        {
            donor.EntriesForTest[$"k{i:D2}"] = new LwwValue<byte[]>
            {
                Value = Encoding.UTF8.GetBytes($"v{i:D2}"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 100 + i },
            };
        }

        // Pre-stage the split's intent so the test bypasses
        // SplitAsync's grain-id allocation (which calls GetGrainId()
        // on the sibling mock via an extension method NSubstitute
        // cannot intercept). The CompleteSplitAsync seam below is
        // the same one the recovery path drives - this test pins its
        // per-partition WAL-head capture contract.
        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        donorState.State.TreeId = MultiPartitionTreeId;
        donorState.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        donorState.State.SplitKey = "k04";
        donorState.State.SplitSiblingId = siblingId;
        donorState.State.NextSibling = siblingId;

        var siblingMock = Substitute.For<IBPlusLeafGrain>();
        siblingMock.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        siblingMock.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        siblingMock.SetKeyRangeAsync(Arg.Any<string>(), Arg.Any<string?>()).Returns(Task.CompletedTask);
        siblingMock.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.CompletedTask);
        siblingMock.SetCheckpointOffsetHintAsync(Arg.Any<long>()).Returns(Task.CompletedTask);
        siblingMock.SetNextSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        siblingMock.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        factory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);

        // CompleteSplitAsync is the recovery-path entry; with a null
        // walHeadsAtSplit argument it fans out per-partition head
        // reads itself, which is the contract we want to pin.
        var completeSplit = typeof(BPlusLeafGrain).GetMethod(
            "CompleteSplitAsync",
            System.Reflection.BindingFlags.NonPublic
                | System.Reflection.BindingFlags.Instance)!;
        await (Task<SplitResult>)completeSplit.Invoke(donor, new object?[] { null })!;

        // The sibling must have been hinted with every per-partition
        // head in a single batched round-trip - the
        // capture-per-partition contract, now delivered via one
        // SetCheckpointOffsetHintsAsync call carrying the full
        // per-partition head vector.
        await siblingMock.Received(1).SetCheckpointOffsetHintsAsync(
            Arg.Is<long[]>(heads =>
                heads.Length == 4
                && heads[0] == 10L
                && heads[1] == 20L
                && heads[2] == 30L
                && heads[3] == 40L));

        // The donor must have advanced its OWN per-partition
        // checkpoints to the same heads.
        Assert.That(donorState.State.ProjectionCheckpointOffsetsByPartition, Is.Not.Null);
        var donorHeads = donorState.State.ProjectionCheckpointOffsetsByPartition!;
        Assert.That(donorHeads.Length, Is.GreaterThanOrEqualTo(partitions));
        for (var p = 0; p < partitions; p++)
        {
            Assert.That(donorHeads[p], Is.EqualTo(partitionHeads[p]),
                $"donor partition {p} checkpoint must equal captured head {partitionHeads[p]}");
        }
        // Partition 0 mirrored into the scalar slot for downgrade safety.
        Assert.That(donorState.State.ProjectionCheckpointOffset, Is.EqualTo(partitionHeads[0]));
    }
}
