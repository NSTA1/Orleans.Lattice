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
/// Regression coverage for the durable-materialiser pin a multi-partition leaf
/// reports for a partition that holds committed-but-not-yet-checkpointed data.
/// <para>
/// A leaf seeds a <see cref="HybridLogicalClock.Zero"/> "block" pin per WAL
/// partition at birth, which disables the shared-shard WAL GC cursor trim until
/// the leaf checkpoints. On its first checkpoint the leaf flushes a real durable
/// frontier for every partition. The bug: it upgraded <em>every</em> partition's
/// pin to <c>(clock, GetCurrentCheckpointForPartition)</c>, so a partition that
/// had received foreground writes (data in the leaf cache and the WAL) but had
/// never durably checkpointed - its per-partition offset still <c>-1</c> - was
/// reported as a <c>(clock &gt; Zero, -1)</c> frontier. That pin is skipped by
/// the GC's per-offset floor (<c>ComputeMaterialiserOffsetFloorAsync</c> ignores
/// <c>-1</c>) yet its clock lifts the HLC floor, so the cross-partition global
/// offset floor derived from the checkpointed partitions authorises trimming the
/// un-checkpointed partition's low-offset entries - silently losing committed
/// data on the leaf's next cold rebuild (an idle deactivation then reactivation,
/// no restart). See the WAL GC uncheckpointed-pin durability incident.
/// </para>
/// <para>
/// The fix: a partition whose checkpointed prefix has no durable copy other
/// than the WAL must retain its Zero block pin - this covers a partition that
/// still holds live cache data but never durably checkpointed
/// (offset <c>&lt; 0</c>), AND a durably-checkpointed partition
/// (offset <c>&gt;= 0</c>) whose in-memory cache is momentarily empty and
/// whose prefix no snapshot covers (the empty-partition coverage-gate
/// recurrence - emptiness read from the transient cache cannot license
/// releasing a checkpointed partition). Only a genuinely empty partition that
/// also never checkpointed releases the block via a real <c>(clock, -1)</c>
/// frontier, keeping WAL trim live.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string UncheckpointedTreeId = "tree-uncheckpointed-pin";
    private const string UncheckpointedReplicaId = "leaf-uncheckpointed-pin";

    private static (BPlusLeafGrain Grain, ILeafCursorReporter Reporter, FakePersistentState<LeafNodeState> State)
        CreateGrainWithReporterForPartitions(int walPartitions, ILeafCursorReporter reporter)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", UncheckpointedReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = UncheckpointedTreeId;
        state.State.ProjectionCheckpointOffset = 0;

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, reporter, state);
    }

    [Test]
    public async Task FlushDurableFrontier_retains_block_pin_for_uncheckpointed_data_partition()
    {
        const int partitions = 4;

        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var (grain, _, state) = CreateGrainWithReporterForPartitions(partitions, reporter);
        var projection = AsProjection(grain);

        // A key that routes to a partition other than 0 - partition 0 is the one
        // we drive a checkpoint on (to fire the first durable-frontier barrier),
        // so the data-bearing partition must be a different one whose per-
        // partition checkpoint stays at the -1 "never applied" sentinel.
        string dataKey = Enumerable.Range(0, 4096)
            .Select(i => $"k{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) != 0);
        int dataPartition = WalPartitionHash.Compute(dataKey, partitions);

        // An empty partition (no cache key routes to it) that is also never
        // checkpointed - the narrowness control: it must still release its block.
        int emptyPartition = Enumerable.Range(1, partitions - 1)
            .First(p => p != dataPartition);

        // Apply a foreground-style write: advances the leaf clock past Zero and
        // stores the row in the cache for dataPartition, but does NOT advance
        // dataPartition's projection checkpoint (which stays at -1).
        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: UncheckpointedTreeId));

        Assert.That(state.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero));

        // Advance partition 0's checkpoint. With MaterialiserCheckpointInterval
        // == Zero this flushes immediately, which drives the first-barrier
        // FlushDurableMaterialiserFrontierAsync for every partition.
        await projection.SetCheckpointOffsetAsync(1, default);

        Assert.That(captured, Is.Not.Null, "the first checkpoint flush must fire the durable-frontier barrier");
        var reports = captured!;
        Assert.That(reports.Count, Is.EqualTo(partitions));

        var clock = state.State.Clock;

        // The data-bearing, never-checkpointed partition must retain a Zero
        // block pin so the WAL GC keeps its low-offset entries. Before the fix
        // it reported (clock > Zero, -1), which the GC's offset floor skips ->
        // its committed WAL entries become trimmable -> data loss on cold rebuild.
        Assert.That(reports[dataPartition].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            $"partition {dataPartition} holds un-checkpointed data and must keep its Zero block pin");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(-1L));

        // Narrowness: a genuinely empty, never-checkpointed partition still
        // releases its block by reporting the real (clock, -1) frontier, so
        // trimming stays live for the ubiquitous empty-partition pins.
        Assert.That(reports[emptyPartition].Frontier, Is.EqualTo(clock),
            $"empty partition {emptyPartition} must release its block so WAL trim proceeds");
        Assert.That(reports[emptyPartition].CheckpointOffset, Is.EqualTo(-1L));

        // A checkpointed partition whose cache is momentarily empty must NOT
        // release its block without snapshot coverage. Partition 0 here has a
        // durable checkpoint (offset 1) but no cache row and no covering
        // snapshot: emptiness is read from the transient in-memory cache, which
        // cannot prove the partition is genuinely dataless during the
        // pre-hydration window (a cold reactivation mid-replay, or after
        // tombstone reaping/compaction). Releasing it here is the "fall off the
        // log" recurrence - it would authorise the offset floor to trim the
        // un-snapshotted checkpointed prefix. So it retains the Zero block pin;
        // the block lifts once a snapshot covers partition 0's offset (snapshot
        // capture stamps every partition's checkpoint as covered, dataless or
        // not - see CaptureSnapshotAsync). Genuinely empty partitions (never
        // checkpointed, offset < 0, e.g. `emptyPartition` above) still release.
        Assert.That(reports[0].Frontier, Is.EqualTo(HybridLogicalClock.Zero));
        Assert.That(reports[0].CheckpointOffset, Is.EqualTo(-1L));
    }
}
