using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2280: the progress a COLD WAL replay makes
/// before it is cancelled is discarded, so the next activation re-reads the
/// whole readable window from offset 0 and the condition that caused the
/// cancellation is reproduced by the cancellation.
/// <para>
/// The defect is a REPRESENTATION defect, not a missing-machinery one.
/// <c>ReplayPartitionAsync</c> already calls <c>TryFlushRecoveredCeilingAsync</c>
/// at every slice boundary; on the cold path that call short-circuits on
/// <c>ceiling &lt;= GetCurrentCheckpointForPartition(partition)</c>. A cold
/// rebuild re-reads from offset 0 while the projection checkpoint still sits at
/// its persisted value <c>C_p</c>, so every offset below <c>C_p</c> is real
/// progress that a strictly monotonic checkpoint cannot express. Two distinct
/// quantities were collapsed onto one scalar: the APPLIED frontier (the
/// checkpoint, which must stay monotone) and the RE-READ frontier (how far this
/// activation has got from the WAL start, which was unrepresentable below the
/// checkpoint).
/// </para>
/// <para>
/// The fix separates them. The checkpoint is untouched and still monotone; the
/// re-read frontier is recorded alongside it and banked as per-partition
/// SNAPSHOT COVERAGE, which is already a per-partition quantity
/// (<c>LeafSnapshotBlob.SnapshotOffsetsByPartition</c>), already monotone at the
/// store, and already <c>min()</c>-ed against the checkpoint at the durable pin.
/// A claim BELOW the checkpoint can therefore only ever lower the trim floor,
/// which is the opposite direction from the durable-offset design ruled unsound
/// in the #2089 follow-up.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ColdBankTreeId = "tree-cold-bank";

    /// <summary>
    /// Persisted checkpoint of the partition the cancelled replay does reach.
    /// Deliberately far above every offset the replay re-reads, so the
    /// short-circuit that is the defect is live for the whole run.
    /// </summary>
    private const long ColdBankReachedCheckpoint = 100L;

    /// <summary>
    /// Persisted checkpoint of the partition the cancelled replay never reaches.
    /// Its only job is to be a non-sentinel value that a checkpoint-derived
    /// coverage claim would stamp and a re-read-derived one cannot.
    /// </summary>
    private const long ColdBankUnreachedCheckpoint = 200L;

    /// <summary>Highest offset the cancelled replay re-reads and applies.</summary>
    private const long ColdBankReReachedOffset = 30L;

    [Test]
    public async Task Cancelled_cold_replay_banks_the_re_read_frontier_as_durable_snapshot_coverage()
    {
        // CORE REGRESSION. A cold rebuild re-reads partition 0 from the WAL
        // start up to offset 30 and is then cancelled. Its persisted checkpoint
        // is 100, so every ceiling the replay computes is below it and the
        // monotonic short-circuit in TryFlushRecoveredCeilingAsync refuses to
        // advance the checkpoint - correctly, because lowering it would break
        // the #1492 monotonicity invariant.
        //
        // RED (pre-fix): the re-read is expressible nowhere, no snapshot is
        // captured (OnActivateAsync throws, so the graceful-deactivation capture
        // hook never runs either), and LoadAsync returns null. The next
        // activation starts again from offset 0.
        //
        // GREEN (post-fix): the re-read frontier is banked as snapshot coverage
        // for partition 0, so the next activation rehydrates and resumes from
        // 30 instead of 0.
        var loaded = await RunCancelledColdReplayAndLoadBankedBlobAsync();

        Assert.That(loaded, Is.Not.Null,
            "a cold replay cancelled mid-flight MUST bank the prefix it already re-read; discarding it is " +
            "what makes the next activation re-read the same window and reproduces the cancellation " +
            "(issue #2280). Banking has to happen INLINE during replay because Orleans does not run " +
            "OnDeactivateAsync when OnActivateAsync throws, and a cancelled cold replay leaves activation " +
            "by throwing");

        Assert.That(loaded!.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(loaded.SnapshotOffsetsByPartition![0], Is.EqualTo(ColdBankReReachedOffset),
            "the banked coverage for the re-read partition MUST be the RE-READ frontier (30), which is the " +
            "clamped ceiling TryFlushRecoveredCeilingAsync already computes - not the projection checkpoint " +
            "(100), which the cache does not back after a partial cold rebuild");
    }

    [Test]
    public async Task Cancelled_cold_replay_does_not_claim_coverage_for_a_partition_it_never_re_read()
    {
        // THE ABSENT SHAPE, AND THE REASON THIS TEST IS WRITTEN SEPARATELY.
        // Partition 1 is never reached: the cancellation lands while partition 0
        // is still replaying. An unreached partition produces NO evidence at the
        // capture site - no re-read rows to inspect, no ceiling to compare, no
        // branch to cover - so a suite that only asserts on the partition that
        // DID make progress passes identically whether or not the unreached one
        // is over-claimed. It has to be asserted on directly.
        //
        // The hazard is concrete. The ordinary capture path stamps
        // GetCurrentCheckpointForPartition(p) for every partition, on the
        // documented ground that a WARM capture's rows cover the checkpointed
        // prefix of every partition. That ground does not hold mid-cold-rebuild:
        // the cache holds only what this activation has re-read. Stamping 200
        // for partition 1 would assert durable coverage of a prefix no row in
        // the blob backs, and because the durable pin is min(checkpoint,
        // covered), it would raise partition 1's trim floor to 200 and authorise
        // the WAL GC to discard the only surviving copy.
        var loaded = await RunCancelledColdReplayAndLoadBankedBlobAsync();

        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(loaded.SnapshotOffsetsByPartition!.Length, Is.GreaterThan(1));
        Assert.That(loaded.SnapshotOffsetsByPartition[1], Is.EqualTo(-1L),
            "a partition the cold rebuild never re-read MUST be claimed as UNCOVERED (-1), never at its " +
            "persisted checkpoint (200). The snapshot rows cannot back a prefix this activation never read, " +
            "and the coverage-gated WAL GC trims on min(checkpoint, covered) - so an over-claim here " +
            "authorises trimming the only durable copy of that prefix");
    }

    /// <summary>
    /// Drives one cold activation that re-reads partition 0 up to
    /// <see cref="ColdBankReReachedOffset"/>, is then cancelled mid-replay
    /// before partition 1 is touched, and returns whatever blob the leaf banked
    /// (<see langword="null"/> when it banked nothing).
    /// </summary>
    private static async Task<LeafSnapshotBlob?> RunCancelledColdReplayAndLoadBankedBlobAsync()
    {
        const int partitions = 2;

        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var store = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);

        using var cts = new CancellationTokenSource();

        // Partition 0: one slice of applies, then a cancelled read. The slice is
        // absorbed and its ceiling recorded before the cancellation lands, which
        // is exactly the progress the pre-fix code discards.
        var slice = new List<CommitLogSliceEntry>
        {
            new(10L, BuildCommittedSet("cold-a", Encoding.UTF8.GetBytes("v-a"), treeId: ColdBankTreeId)),
            new(20L, BuildCommittedSet("cold-b", Encoding.UTF8.GetBytes("v-b"), treeId: ColdBankTreeId)),
            new(ColdBankReReachedOffset, BuildCommittedSet("cold-c", Encoding.UTF8.GetBytes("v-c"), treeId: ColdBankTreeId)),
        };

        var reads = 0;
        var partition0 = Substitute.For<ILeafReplayCoordinatorGrain>();
        partition0.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(90L));
        partition0.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        partition0.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (Interlocked.Increment(ref reads) == 1)
                {
                    return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice);
                }

                // The teardown the field sees: the activation's token is
                // cancelled while the replay is still walking the window.
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });

        // Partition 1 is wired but never reached, because partition 0 is
        // absorbed first and the cancellation lands inside it.
        var partition1 = Substitute.For<ILeafReplayCoordinatorGrain>();
        partition1.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(400L));
        partition1.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        partition1.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var leaf = CreateColdBankLeaf(
            store,
            partitions,
            partition0,
            partition1,
            new[] { ColdBankReachedCheckpoint, ColdBankUnreachedCheckpoint });

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await ((IGrainBase)leaf).OnActivateAsync(cts.Token),
            "precondition: the cold activation is torn down by a cancellation, which is the shape the " +
            "SELF-REINFORCING COLD REPLAY LOOP diagnostic reports in the field");

        return await store.LoadAsync(default);
    }

    /// <summary>
    /// Builds a cold leaf with a pre-existing per-partition checkpoint, an empty
    /// cache, and no durable snapshot - so activation elects the <c>-1</c> cold
    /// override and rebuilds from the WAL start. No fall-off-log detector is
    /// registered, so the classification step is skipped and the replay path
    /// under test runs unmediated.
    /// </summary>
    private static BPlusLeafGrain CreateColdBankLeaf(
        ILeafSnapshotStorageGrain snapshotStore,
        int walPartitions,
        ILeafReplayCoordinatorGrain partition0,
        ILeafReplayCoordinatorGrain partition1,
        long[] persistedCheckpoints)
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStore);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>())
            .Returns(ci => ci.ArgAt<string>(0).EndsWith("/1", StringComparison.Ordinal) ? partition1 : partition0);

        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ColdBankTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoints[0];
        state.State.ProjectionCheckpointOffsetsByPartition = (long[])persistedCheckpoints.Clone();

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
    }
}
