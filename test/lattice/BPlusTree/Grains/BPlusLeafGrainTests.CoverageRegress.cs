using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the WAL "fall off the log" defect whose root cause is
/// a DURABLE-COVERAGE REGRESSION. The coverage-gated WAL GC authorises trimming a
/// partition's prefix up to the in-memory monotonic-max durable pin
/// (<c>BPlusLeafGrain.RecordDurableSnapshotCoverage</c>, folded with
/// <c>Math.Max</c> so it never decreases within an activation), but the durable
/// recovery source is a SINGLE last-writer-wins blob
/// (<c>LeafSnapshotStorageGrain.SaveAsync</c>) that each capture recomputes from
/// the CURRENT (possibly regressed) checkpoints. When a later capture persists a
/// blob whose per-partition coverage for some partition is LOWER than an earlier
/// blob that already authorised a trim, the durable store covers LESS than the
/// executed trim while the pin still authorises it. On the next cold restart the
/// leaf rehydrates from the under-covering blob, advances the partition checkpoint
/// only to the lower offset, and the tail replay finds the WAL trimmed past
/// <c>checkpoint + 1</c> - <see cref="LeafProjectionStaleException"/> ("fall off
/// the log").
/// <para>
/// The invariant the fix restores: the durable snapshot store's per-partition
/// coverage is monotonic non-decreasing, so the latest loaded blob always covers
/// at least as far as any earlier blob that authorised a trim. The rehydrate path
/// already ASSUMES this (see the "Coverage is monotonic and we always load the
/// latest blob" note in <c>TryRehydrateFromSnapshotAsync</c>); the storage grain
/// must guarantee it by construction rather than blind-overwrite.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string CoverageRegressTreeId = "tree-coverage-regress";

    private static LeafSnapshotBlob NonZeroPartitionBlob(
        int walPartitions,
        int partition,
        long coveredOffset,
        string key,
        byte[] value,
        long hlcPhysical)
    {
        var perPartition = new long[walPartitions];
        Array.Fill(perPartition, -1L);
        perPartition[partition] = coveredOffset;
        return new LeafSnapshotBlob
        {
            // Partition 0 idle: the scalar mirrors partition 0 only, so a
            // non-zero-partition blob carries the -1 scalar sentinel.
            SnapshotOffset = -1L,
            Rows = new List<LeafSnapshotRow>
            {
                new(key, LwwValue<byte[]>.Create(value, new HybridLogicalClock { WallClockTicks = hlcPhysical })),
            },
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = perPartition,
        };
    }

    [Test]
    public async Task LeafSnapshotStorageGrain_save_does_not_regress_covered_partition_below_an_earlier_saved_blob()
    {
        // CORE INVARIANT. The durable snapshot store authorises the coverage-gated
        // WAL GC trim floor. Once a blob covering partition p at offset N has been
        // saved (and thereby authorised trimming p's [0, N] prefix), NO subsequent
        // save may lower the durable coverage of p below N - otherwise the sole
        // durable copy of [0, N] is gone while the WAL prefix that backed it has
        // been trimmed, stranding the leaf with an unrecoverable gap.
        //
        // RED (pre-fix): SaveAsync blind-overwrites, so the second (regressed)
        // blob replaces the first and LoadAsync reports coverage 2, dropping the
        // high-HLC row. GREEN (post-fix): the store merges per-partition so
        // coverage stays at 5 and the row that backs [0, 5] survives.
        const int partitions = 8;
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var store = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);

        // Earlier capture: covers partition p at offset 5 with the current value.
        var high = NonZeroPartitionBlob(
            partitions, dataPartition, coveredOffset: 5L,
            key: dataKey, value: Encoding.UTF8.GetBytes("v-at-5"), hlcPhysical: 900L);
        await store.SaveAsync(high, default);

        // Later capture whose recomputed per-partition coverage for p REGRESSED to
        // offset 2 (e.g. a rehydrate reset or projection rebuild lowered p's
        // checkpoint before this capture recomputed it from current state).
        var low = NonZeroPartitionBlob(
            partitions, dataPartition, coveredOffset: 2L,
            key: dataKey, value: Encoding.UTF8.GetBytes("v-at-2"), hlcPhysical: 200L);
        await store.SaveAsync(low, default);

        var loaded = await store.LoadAsync(default);
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(loaded.SnapshotOffsetsByPartition![dataPartition], Is.GreaterThanOrEqualTo(5L),
            "durable per-partition coverage MUST NOT regress below an earlier saved blob that already " +
            "authorised the WAL GC to trim that partition's prefix; a blind last-writer-wins overwrite " +
            "strands the trimmed prefix with no durable copy (LeafProjectionStaleException on cold restart)");

        var row = loaded.Rows.Single(r => r.Key == dataKey);
        Assert.That(row.Value.Timestamp.WallClockTicks, Is.EqualTo(900L),
            "the row backing the higher covered offset MUST survive the merge so the retained coverage is " +
            "actually row-backed (flooring the offset up without the backing rows would claim coverage the " +
            "blob cannot rehydrate)");
    }

    [Test]
    public void Reactivated_leaf_does_not_fall_off_log_after_a_later_capture_regressed_durable_coverage()
    {
        // END-TO-END. Drive the exact production sequence:
        //   (a) a partition's durable coverage is driven high to offset 5 and the
        //       pin authorises trimming its prefix to 5;
        //   (b) a later capture persists coverage 2 for that partition (< 5);
        //   (c) a cold leaf reactivates against a WAL trimmed to 5;
        //   (d) assert it does NOT throw LeafProjectionStaleException.
        //
        // WalPartitions = 1 keeps the reproduction on partition 0's scalar so the
        // rehydrate + detector path is exercised end to end. RED (pre-fix): the
        // regressed blob overwrites the durable store, the cold leaf rehydrates to
        // checkpoint 2 over a WAL whose oldest readable offset is 5, and the
        // fall-off-log detector (tail 5 > checkpoint 2 + 1) throws. GREEN
        // (post-fix): the store keeps coverage 5, the cold leaf rehydrates to
        // checkpoint 5, and the detector (tail 5, not > 5 + 1) elects a clean tail
        // replay.
        const int partitions = 1;

        // One real durable store shared by the warm capture and the cold restart.
        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var store = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);

        // Warm leaf: apply a key, checkpoint partition 0 at offset 5, capture. The
        // capture writes a durable blob covering partition 0 at 5 and the in-memory
        // pin authorises trimming [0, 5].
        var (warm, _) = CreateResidualLeafWithSnapshotStore(partitions, store, coordinatorTail: 1);
        var warmProjection = AsProjection(warm);
        warmProjection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v-at-5"), hlcPhysical: 900, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(0, 5))
        {
            warmProjection.SetCheckpointOffsetAsync(5, default).GetAwaiter().GetResult();
        }
        warmProjection.FlushCheckpointAsync(default).GetAwaiter().GetResult();
        warm.CaptureSnapshotAsync().GetAwaiter().GetResult();
        Assert.That(warm.DurableSnapshotCoverageForPartition(0), Is.EqualTo(5L),
            "precondition: partition 0 durable coverage is driven to 5 (the trim is authorised to 5)");

        // Regress: a later capture persists coverage 2 for partition 0. Modelled by
        // saving a lower blob through the SAME real store (the last-writer-wins
        // overwrite the production capture performs when its recomputed checkpoint
        // regressed).
        var regressed = new LeafSnapshotBlob
        {
            SnapshotOffset = 2L,
            Rows = new List<LeafSnapshotRow>
            {
                new("k1", LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v-at-2"), new HybridLogicalClock { WallClockTicks = 200L })),
            },
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = new[] { 2L },
        };
        store.SaveAsync(regressed, default).GetAwaiter().GetResult();

        // Cold restart: the WAL has been trimmed to oldest-readable offset 5 (the
        // prefix the coverage-5 blob authorised trimming), head at 10. The leaf's
        // durable checkpoint is 5 (its last flushed value).
        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync(ResidualTreeId, 0, Arg.Any<CancellationToken>()).Returns(Task.FromResult(10L));
        reader.GetTailOffsetAsync(ResidualTreeId, 0, Arg.Any<CancellationToken>()).Returns(Task.FromResult(5L));
        var detectorServices = new ServiceCollection().AddSingleton<ICommitLogReader>(reader).BuildServiceProvider();
        var detector = new LatticeFallOffLogDetector(detectorServices);

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(10L));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(5L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var cold = CreateCoverageRegressColdLeaf(store, detector, reader, coord, persistedCheckpoint: 5L);

        Assert.DoesNotThrowAsync(
            async () => await ((IGrainBase)cold).OnActivateAsync(CancellationToken.None),
            "a later capture that regressed a partition's durable coverage below an already-authorised trim " +
            "must not strand the cold restart with an unrecoverable prefix; the durable store must retain the " +
            "higher coverage so rehydrate advances the checkpoint past the trimmed prefix and the tail replay " +
            "is clean, NOT a LeafProjectionStaleException");
    }

    /// <summary>
    /// Builds a cold leaf wired to a shared real snapshot store, a real fall-off-log
    /// detector (backed by <paramref name="reader"/>), and a replay coordinator, with
    /// a pre-existing durable partition-0 checkpoint. Mirrors
    /// <see cref="CreateResidualLeafWithSnapshotStore"/> but additionally registers
    /// the detector + reader in the activation services so the activation-time
    /// fall-off-log classification runs exactly as it does in production.
    /// </summary>
    private static BPlusLeafGrain CreateCoverageRegressColdLeaf(
        ILeafSnapshotStorageGrain snapshotStore,
        ILatticeFallOffLogDetector detector,
        ICommitLogReader reader,
        ILeafReplayCoordinatorGrain coordinator,
        long persistedCheckpoint)
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var leafKey = Guid.NewGuid();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStore);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        sc.AddSingleton(reader);
        sc.AddSingleton<ILatticeFallOffLogDetector>(detector);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ResidualTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
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
