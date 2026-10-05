using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522: the online resize copy's prepared-bucket sweep carries each
/// marked prepare's original stamp onto the destination copy. The resize mirror
/// ships every write at the source copy's own stamps, so the destination is on
/// the source's clock lineage: a replayed prepare is bucketed there AT its
/// stamp, and a decided saga's backstop is applied there at its stamp rather
/// than over a write acknowledged after the prepare.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private static readonly HybridLogicalClock SweptStamp = new() { WallClockTicks = 4_000, Counter = 5 };

    private static PendingMutationSnapshot MarkedPreparedSet(Guid txid, string key) => new()
    {
        TransactionId = txid,
        Key = key,
        Value = [7, 7, 7],
        Timestamp = SweptStamp,
        IsTombstone = false,
        ExpiresAtTicks = 0,
        OriginClusterId = null,
        VectorClock = null,
        StampIsOriginal = true,
    };

    private static object? CarriedStamps() =>
        RequestContext.Get(LatticeEventConstants.OriginalPrepareStampsRequestContextKey);

    [Test]
    public async Task Online_shadow_begin_replays_a_marked_in_flight_prepare_carrying_its_original_stamp()
    {
        var txid = Guid.NewGuid();
        var key = KeyOnShardZero();
        var h = CreatePreparedSweepHarness(MarkedPreparedSet(txid, key), TxStatus.InFlight);
        object? carried = null;
        h.Destination0.When(d => d.SetAsync(key, Arg.Any<byte[]>())).Do(_ => carried = CarriedStamps());

        await h.Grain.BeginShadowForwardAllShardsAsync();

        await h.Destination0.Received(1).SetAsync(key, Arg.Any<byte[]>());
        Assert.That(carried, Is.InstanceOf<Dictionary<string, HybridLogicalClock>>(),
            "the resize copy's sweep must carry the prepare's original stamp");
        Assert.That(((Dictionary<string, HybridLogicalClock>)carried!)[key], Is.EqualTo(SweptStamp));
    }

    [Test]
    public async Task Online_shadow_begin_backstops_a_decided_marked_prepare_at_its_original_stamp()
    {
        var txid = Guid.NewGuid();
        var key = KeyOnShardZero();
        var h = CreatePreparedSweepHarness(MarkedPreparedSet(txid, key), TxStatus.Committed);
        object? carried = null;
        h.Destination0.When(d => d.AppendTxTerminalAsync(txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>()))
            .Do(_ => carried = CarriedStamps());

        await h.Grain.BeginShadowForwardAllShardsAsync();

        await h.Destination0.Received(1).AppendTxTerminalAsync(txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
        Assert.That(carried, Is.InstanceOf<Dictionary<string, HybridLogicalClock>>(),
            "the backstop must carry the prepare's original stamp, not fall back to a dominating stamp");
        Assert.That(((Dictionary<string, HybridLogicalClock>)carried!)[key], Is.EqualTo(SweptStamp));
    }
}
