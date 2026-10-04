using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522 through <see cref="PreparedBucketSweep"/>: a decided saga's
/// backstop terminal, applied by an adaptive split's retroactive sweep, carries
/// each marked prepare's original stamp P, so the target applies it under
/// last-writer-wins at P and a write acknowledged after the prepare - already on
/// the target at its source stamp - survives. An online resize copy mints its own
/// stamps for mirrored writes, which P does not order, so that caller keeps the
/// dominating backstop stamp; and an unmarked snapshot's stamp is never carried.
/// </summary>
[TestFixture]
public sealed class PreparedBucketSweepOriginalStampTests
{
    private static readonly HybridLogicalClock P = new() { WallClockTicks = 638_000_000_000_000_000, Counter = 2 };

    private sealed record Run(bool Called, bool CarriedStamp, HybridLogicalClock Stamp);

    private static async Task<Run> SweepCommittedSnapshotAsync(bool carryOriginalStamps, bool stampIsOriginal)
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
                Value = [7],
                Timestamp = P,
                StampIsOriginal = stampIsOriginal,
            },
        });
        leaf.GetNextSiblingAsync().Returns((GrainId?)null);
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(tx).Returns(TxStatus.Committed);
        registry.GetStatusForTerminalAsync(tx).Returns(TxStatus.Committed);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry);

        var called = false;
        var carried = false;
        HybridLogicalClock stamp = default;
        var target = Substitute.For<IShardRootGrain>();
        target.AppendTxTerminalAsync(tx, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns(_ =>
            {
                called = true;
                carried = LatticeOriginalPrepareStampContext.TryGetStamp("k", out stamp);
                return Task.FromResult<WalRecord?>(null);
            });

        await PreparedBucketSweep.RunAsync(
            factory, "tree", leafId, target, [0], 1, new PreparedBucketSweepProgress(), carryOriginalStamps);

        Assert.That(LatticeOriginalPrepareStampContext.HasStamps, Is.False, "the carrier must not leak past the call");
        return new Run(called, carried, stamp);
    }

    [Test]
    public async Task A_split_sweep_backstop_carries_a_marked_prepares_original_stamp()
    {
        var run = await SweepCommittedSnapshotAsync(carryOriginalStamps: true, stampIsOriginal: true);

        Assert.That(run.Called, Is.True);
        Assert.That(run.CarriedStamp, Is.True);
        Assert.That(run.Stamp, Is.EqualTo(P));
    }

    [Test]
    public async Task A_resize_snapshot_sweep_backstop_carries_no_stamp()
    {
        var run = await SweepCommittedSnapshotAsync(carryOriginalStamps: false, stampIsOriginal: true);

        Assert.That(run.Called, Is.True);
        Assert.That(run.CarriedStamp, Is.False);
    }

    [Test]
    public async Task An_unmarked_snapshot_never_carries_its_stamp()
    {
        var run = await SweepCommittedSnapshotAsync(carryOriginalStamps: true, stampIsOriginal: false);

        Assert.That(run.Called, Is.True);
        Assert.That(run.CarriedStamp, Is.False);
    }
}
