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
        CreateGrainWithReporterForPartitions(
            int walPartitions, ILeafCursorReporter reporter, long walHead = 64L,
            bool headReadThrows = false)
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

        // The per-partition WAL head. Every scenario here other than the #3103
        // regression assumes the partition's WAL actually holds the entries the
        // block pin exists to protect, so the head must be non-zero; an
        // unstubbed coordinator returns 0, which means "no entry was ever
        // appended" and correctly releases the pin.
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(headReadThrows
                ? Task.FromException<long>(new InvalidOperationException("head unavailable"))
                : Task.FromResult(walHead));
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>())
            .Returns(coordinator);

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

    /// <summary>
    /// Drives the #3103 shape: the data-bearing partition has never
    /// checkpointed <em>and</em> its WAL is empty (head offset <c>0</c> - no
    /// entry was ever appended). The pin must be released.
    /// <para>
    /// Left blocking this pin is permanent, not merely conservative. An empty
    /// WAL has nothing to replay, so the starved-checkpoint drive returns
    /// <c>NoAdvance</c> for ever, the per-partition checkpoint never leaves
    /// <c>-1</c>, and the zero-coverage repair's "checkpointed WITHOUT
    /// coverage" predicate is permanently unreachable. Because a block pin is
    /// tree-wide, one leaf in this state strands every other leaf's WAL in the
    /// same tree, which is the unbounded-retention failure #3103 reports. The
    /// state is reached by a WAL reset that preserves snapshots: the leaf
    /// rehydrates real rows from the surviving blob while the WAL behind them
    /// is gone.
    /// </para>
    /// </summary>
    [Test]
    public async Task FlushDurableFrontier_releases_block_pin_when_partition_wal_is_empty()
    {
        const int partitions = 4;

        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        // walHead 0 == "the WAL shard is empty" (see ICommitLogReader).
        var (grain, _, state) = CreateGrainWithReporterForPartitions(
            partitions, reporter, walHead: 0L);
        var projection = AsProjection(grain);

        string dataKey = Enumerable.Range(0, 4096)
            .Select(i => $"k{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) != 0);
        int dataPartition = WalPartitionHash.Compute(dataKey, partitions);

        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: UncheckpointedTreeId));
        await projection.SetCheckpointOffsetAsync(1, default);

        Assert.That(captured, Is.Not.Null);
        var reports = captured!;
        var clock = state.State.Clock;

        Assert.That(reports[dataPartition].Frontier, Is.EqualTo(clock),
            $"partition {dataPartition} has an EMPTY WAL, so its block pin protects "
            + "no committed prefix and must be released - retaining it strands the "
            + "whole tree's WAL for ever (#3103)");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(-1L),
            "releasing the block must not invent coverage - the offset stays at the "
            + "-1 never-applied sentinel, exactly as the genuinely-empty-partition "
            + "release already reports");
    }

    /// <summary>
    /// The narrowness control for the #3103 release above, pinned at the exact
    /// boundary: a single WAL entry (head <c>1</c>) is enough to make the
    /// partition's committed prefix real, so the block pin must be retained.
    /// <para>
    /// This is the half of the fix that must not regress. Releasing on anything
    /// other than a proven-empty WAL would authorise the shared-shard GC to
    /// trim a prefix no snapshot covers, which is the #1535 no-loss invariant
    /// and the #945 fall-off guard. The release predicate is therefore
    /// <c>head == 0</c>, not <c>head &lt;= checkpoint</c> or any other
    /// inequality that a non-empty WAL could satisfy.
    /// </para>
    /// </summary>
    [Test]
    public async Task FlushDurableFrontier_retains_block_pin_when_partition_wal_holds_one_entry()
    {
        const int partitions = 4;

        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var (grain, _, _) = CreateGrainWithReporterForPartitions(
            partitions, reporter, walHead: 1L);
        var projection = AsProjection(grain);

        string dataKey = Enumerable.Range(0, 4096)
            .Select(i => $"k{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) != 0);
        int dataPartition = WalPartitionHash.Compute(dataKey, partitions);

        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: UncheckpointedTreeId));
        await projection.SetCheckpointOffsetAsync(1, default);

        Assert.That(captured, Is.Not.Null);
        var reports = captured!;

        Assert.That(reports[dataPartition].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            $"partition {dataPartition} has ONE un-checkpointed WAL entry, so its "
            + "block pin protects a real committed prefix and must be retained");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(-1L));
    }

    /// <summary>
    /// The head probe must fail closed. A coordinator whose head read throws
    /// leaves the pin exactly as it was, because retaining WAL is always safe
    /// while releasing it on an unknown head could authorise trimming a live
    /// prefix.
    /// </summary>
    [Test]
    public async Task FlushDurableFrontier_retains_block_pin_when_wal_head_read_fails()
    {
        const int partitions = 4;

        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var (grain, _, _) = CreateGrainWithReporterForPartitions(
            partitions, reporter, walHead: 0L, headReadThrows: true);
        var projection = AsProjection(grain);

        string dataKey = Enumerable.Range(0, 4096)
            .Select(i => $"k{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) != 0);
        int dataPartition = WalPartitionHash.Compute(dataKey, partitions);

        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: UncheckpointedTreeId));
        await projection.SetCheckpointOffsetAsync(1, default);

        Assert.That(captured, Is.Not.Null);
        var reports = captured!;

        Assert.That(reports[dataPartition].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            "an unreadable WAL head must keep the block pin - the probe fails closed");
    }
}
