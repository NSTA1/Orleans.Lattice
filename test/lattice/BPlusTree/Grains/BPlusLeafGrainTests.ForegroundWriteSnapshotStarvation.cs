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
/// Deterministic reproduction of the unbounded-WAL-retention defect: a leaf
/// holding foreground-written rows that has never durably checkpointed any WAL
/// partition is refused a snapshot forever, so its block pins never lift and its
/// tree's WAL is never trimmed.
/// <para>
/// The two halves of the machine disagree about what "empty" means.
/// <c>ResolveDurablePinForPartition</c> decides liveness from the projection
/// cache (it takes <c>partitionsWithLiveData</c> as a parameter distinct from
/// the checkpoint, precisely because the two can diverge) and retains a
/// <see cref="HybridLogicalClock.Zero"/> block pin for a partition whose only
/// durable copy is the WAL prefix. The snapshot capture path instead infers
/// emptiness from the checkpoint sentinel alone: its comment reads "the
/// 'nothing applied' sentinel (-1) means the leaf has not yet absorbed any WAL
/// entry into its projection; capturing an empty cache would create a snapshot
/// the activation path is required to ignore, so the work is pure overhead."
/// </para>
/// <para>
/// Those are different propositions. The projection checkpoint tracks WAL
/// <em>replay</em> position, not cache contents, so a leaf whose rows arrived as
/// foreground writes has a populated cache and a checkpoint of <c>-1</c>. Both
/// capture gates short-circuit on it: the advisory gate returns when no
/// partition reports <c>checkpoint &gt;= 0</c>, and the periodic recheck returns
/// because <c>-1 &gt; -1</c> is false. The block pin is therefore set by the
/// half that distinguishes cache liveness from the checkpoint, and can only be
/// released by the half that conflates them.
/// </para>
/// <para>
/// That voids the documented bounded-retention guarantee ("every blocked prefix
/// is covered by a durable snapshot within at most
/// <c>LeafSnapshotReClassifyEveryNCheckpoints</c> checkpoints"): the cadence is
/// denominated in checkpoints, and this leaf never takes one. Retention is not
/// slow, it is unreachable.
/// </para>
/// <para>
/// RED (pre-fix): no snapshot is ever written, so the leaf's rows exist only as
/// un-replayed WAL and the block pin is retained forever - the shared-shard WAL
/// GC early-returns <c>outcome="idle"</c> and the tree's WAL grows without
/// bound. GREEN (post-fix, "Half A"): a leaf holding live cache rows is captured
/// even with no checkpoint, so its rows gain a durable copy.
/// </para>
/// <para>
/// Half A deliberately stops there and does NOT make retention bounded. Writing
/// the blob earns <em>durability</em> for this leaf's rows; it does not earn
/// <em>authority to trim</em>, which is a claim about what other consumers still
/// need, and a blob holding these rows says nothing about whether the
/// materialiser has consumed the corresponding WAL entries. The two retention
/// planes are coupled by a documented handoff -
/// <c>ComputeMaterialiserOffsetFloorAsync</c> skips a <c>-1</c> pin precisely
/// because "WAL retention is already enforced by the HLC block-pin branch" - so
/// lifting the block on an un-replayed partition would drop both protections at
/// once. On an existing deployment that would fire across every previously
/// starved leaf simultaneously and trim a large prefix no consumer has read.
/// The assertions below therefore pin BOTH halves of the contract: the capture
/// must happen, and the pin must NOT advance.
/// </para>
/// <para>
/// The empty-cache control must stay a no-op either way, so the fix keys on
/// cache liveness rather than simply deleting the gate.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ForegroundStarvationTreeId = "tree-foreground-write-starvation";

    private static (BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        Func<IReadOnlyList<MaterialiserPinReport>?> LastFlush,
        ILeafSnapshotStorageGrain SnapshotStub)
        CreateNeverCheckpointedLeaf(int walPartitions)
    {
        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ForegroundStarvationTreeId;

        // The leaf under test has never absorbed a WAL entry on ANY partition:
        // the scalar slot and every per-partition slot sit at the "nothing
        // applied" sentinel. This is the state of a leaf whose materialiser is
        // lagging behind sustained foreground write load.
        state.State.ProjectionCheckpointOffset = -1L;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                LeafSnapshotReClassifyEveryNCheckpoints = 1,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (grain, state, () => captured, snapshotStub);
    }

    [Test]
    public async Task Foreground_written_leaf_with_no_checkpoint_is_never_snapshotted_so_its_wal_pin_is_permanent()
    {
        const int partitions = 8;
        var (grain, state, lastFlush, snapshotStub) = CreateNeverCheckpointedLeaf(partitions);
        var projection = AsProjection(grain);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // A foreground write: the row lands in the projection cache and in the
        // WAL, and the leaf clock advances past Zero. It does NOT advance any
        // projection checkpoint, because the checkpoint tracks WAL replay
        // position and this row was never replayed - it was written.
        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: ForegroundStarvationTreeId));

        Assert.That(state.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero),
            "precondition: a foreground write was applied, so the cache is populated");
        for (var p = 0; p < partitions; p++)
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(p), Is.EqualTo(-1L),
                $"precondition: partition {p} has never durably checkpointed");
        }

        // The cadence capture is the ONLY documented path from a block pin to
        // coverage and trim. Run it repeatedly: the defect claim is that
        // retention is unreachable, not merely slow, so no number of captures
        // may change the outcome.
        for (var i = 0; i < 5; i++)
        {
            await grain.CaptureSnapshotAsync();
        }

        // HALF A (the fix under test): a leaf holding live cache rows is now
        // captured even though no partition has ever checkpointed, so its rows
        // gain a durable copy instead of existing only as un-replayed WAL.
        await snapshotStub.Received().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        // ...and that is ALL it does. Writing the blob earns DURABILITY for
        // this leaf's rows; it does not earn AUTHORITY TO TRIM, which is a
        // claim about what other consumers still need. A partition with rows
        // but no checkpoint makes no offset claim, so coverage stays at the
        // sentinel.
        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(-1L),
            "capturing a never-checkpointed partition must NOT stamp an offset claim: the blob proves the "
            + "rows are durable, not that the materialiser has consumed the corresponding WAL entries");

        // The guard that matters most. The two retention planes are coupled by
        // a documented handoff: ComputeMaterialiserOffsetFloorAsync SKIPS a -1
        // pin *because* the HLC block-pin branch is enforcing retention. So
        // advancing this pin while the offset is still -1 would remove BOTH
        // protections at once and authorise trimming a prefix the materialiser
        // has never replayed - silent loss of committed data, and on an upgrade
        // it would fire across every previously-starved leaf simultaneously.
        // This assertion exists to stop a future contributor "completing" the
        // fix by lifting the block here.
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        var reports = lastFlush();
        Assert.That(reports, Is.Not.Null, "the deactivation hook must flush a durable frontier");
        Assert.That(reports![dataPartition].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            $"partition {dataPartition} holds rows whose only durable WAL claim is un-replayed, so its block "
            + "pin MUST be retained even after a successful capture; lifting it drops both retention planes");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(-1L));
    }

    [Test]
    public async Task Genuinely_empty_never_checkpointed_leaf_is_still_not_snapshotted()
    {
        // Narrowness control. A leaf with an EMPTY cache and no checkpoint has
        // nothing to snapshot, and capturing it would write a blob the
        // activation path is required to ignore. This must stay a no-op after
        // the fix, which is why the fix has to key on cache liveness rather
        // than simply removing the sentinel gate.
        const int partitions = 8;
        var (grain, _, _, snapshotStub) = CreateNeverCheckpointedLeaf(partitions);

        await grain.CaptureSnapshotAsync();

        await snapshotStub.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "an empty, never-checkpointed leaf has nothing to cover and must not write a snapshot");
    }
}
