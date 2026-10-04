using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4545 through <see cref="PreparedBucketSweep"/>: the destination-side
/// shadow marker an adaptive split's sweep installs for an in-flight saga carries
/// the prepare's marked original stamp P, so the destination's read gate can
/// release it once the row it guards is stamped at or above P, even when the
/// saga's terminal never reaches the leaf that ends up holding the key.
/// <para>
/// The marker carries no stamp when P does not order the row: a resize copy
/// (whose mirrored writes mint their own stamps), an unmarked snapshot, and a
/// CRDT-delta prepare (which folds at its terminal's stamp, not at P). Each of
/// those keeps the original gate.
/// </para>
/// </summary>
[TestFixture]
public sealed class PreparedBucketSweepMarkerStampTests
{
    private static readonly HybridLogicalClock P = new() { WallClockTicks = 638_000_000_000_000_000, Counter = 5 };

    private sealed record Run(bool Marked, bool CarriedStamp, HybridLogicalClock Stamp, string? Route);

    private static async Task<Run> SweepInFlightSnapshotAsync(
        bool carryOriginalStamps,
        bool stampIsOriginal,
        bool isTombstone = false,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister,
        byte[]? delta = null)
    {
        var tx = Guid.NewGuid();
        var leafId = GrainId.Create("leaf", "sweep-source");
        var factory = Substitute.For<IGrainFactory>();

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(new List<PendingMutationSnapshot>
        {
            new()
            {
                TransactionId = tx,
                Key = "k",
                Value = isTombstone ? null : [7],
                IsTombstone = isTombstone,
                Timestamp = P,
                StampIsOriginal = stampIsOriginal,
                Mode = mode,
                Delta = delta,
            },
        });
        leaf.GetNextSiblingAsync().Returns((GrainId?)null);
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(tx).Returns(TxStatus.InFlight);
        registry.GetStatusForTerminalAsync(tx).Returns(TxStatus.InFlight);
        registry.GetStatusManyForTerminalAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.InFlight });
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry);

        var marked = false;
        var carried = false;
        HybridLogicalClock stamp = default;
        string? route = null;
        var target = Substitute.For<IShardRootGrain>();
        target.MarkSagaShadowAsync(tx, Arg.Any<IReadOnlyList<string>>())
            .Returns(_ =>
            {
                marked = true;
                carried = LatticeOriginalPrepareStampContext.TryGetStamp("k", out stamp);
                route = LatticeOriginalPrepareStampContext.PreparedRoute;
                return Task.CompletedTask;
            });

        await PreparedBucketSweep.RunAsync(
            factory, "tree", leafId, target, [0], 1, new PreparedBucketSweepProgress(), carryOriginalStamps);

        Assert.That(LatticeOriginalPrepareStampContext.HasStamps, Is.False, "the carrier must not leak past the call");
        return new Run(marked, carried, stamp, route);
    }

    [Test]
    public async Task A_split_sweep_marker_carries_a_marked_prepares_original_stamp()
    {
        var run = await SweepInFlightSnapshotAsync(carryOriginalStamps: true, stampIsOriginal: true);

        Assert.That(run.Marked, Is.True);
        Assert.That(run.CarriedStamp, Is.True);
        Assert.That(run.Stamp, Is.EqualTo(P));
        Assert.That(run.Route, Is.Null, "a marker install must never be classifiable as an original prepare by route");
    }

    [Test]
    public async Task A_split_sweep_marker_for_a_marked_tombstone_carries_its_original_stamp()
    {
        var run = await SweepInFlightSnapshotAsync(carryOriginalStamps: true, stampIsOriginal: true, isTombstone: true);

        Assert.That(run.Marked, Is.True);
        Assert.That(run.CarriedStamp, Is.True);
        Assert.That(run.Stamp, Is.EqualTo(P));
    }

    [Test]
    public async Task A_resize_sweep_marker_carries_no_stamp()
    {
        var run = await SweepInFlightSnapshotAsync(carryOriginalStamps: false, stampIsOriginal: true);

        Assert.That(run.Marked, Is.True);
        Assert.That(run.CarriedStamp, Is.False);
    }

    [Test]
    public async Task An_unmarked_snapshots_sweep_marker_carries_no_stamp()
    {
        var run = await SweepInFlightSnapshotAsync(carryOriginalStamps: true, stampIsOriginal: false);

        Assert.That(run.Marked, Is.True);
        Assert.That(run.CarriedStamp, Is.False);
    }

    [Test]
    public async Task A_crdt_delta_prepares_sweep_marker_carries_no_stamp()
    {
        var run = await SweepInFlightSnapshotAsync(
            carryOriginalStamps: true, stampIsOriginal: true, mode: LatticeMergeMode.GCounter, delta: [1]);

        Assert.That(run.Marked, Is.True);
        Assert.That(run.CarriedStamp, Is.False,
            "a delta folds at its terminal's stamp, so a row at or above P does not show the delta was applied");
    }
}
