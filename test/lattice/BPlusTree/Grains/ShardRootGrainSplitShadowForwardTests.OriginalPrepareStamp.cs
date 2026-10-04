using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522: a split's live shadow-forward of a saga prepare carries the
/// local prepare's original stamp P when the local leaf marked it, so the
/// destination buckets the prepare AT P and marks it; it never carries the
/// prepared route, and it carries nothing for an unmarked prepare.
/// </summary>
public partial class ShardRootGrainSplitShadowForwardTests
{
    private static readonly HybridLogicalClock ForwardedP = new() { WallClockTicks = 638_000_000_000_000_000, Counter = 9 };

    private sealed record ForwardCapture(bool Forwarded, bool CarriedStamp, HybridLogicalClock Stamp, string? Route);

    private static async Task<ForwardCapture> PreparedForwardAsync(bool localPrepareMarked, bool delete = false)
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));
        var txid = Guid.NewGuid();
        h.Leaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(new List<PendingMutationSnapshot>
        {
            new() { TransactionId = txid, Key = "k", Value = [1, 2], Timestamp = ForwardedP, StampIsOriginal = localPrepareMarked },
        });

        var forwarded = false;
        var carried = false;
        HybridLogicalClock stamp = default;
        string? route = null;
        void Capture()
        {
            forwarded = true;
            carried = LatticeOriginalPrepareStampContext.TryGetStamp("k", out stamp);
            route = LatticeOriginalPrepareStampContext.PreparedRoute;
        }

        h.ShadowTarget.SetAsync("k", Arg.Any<byte[]>()).Returns(_ => { Capture(); return Task.CompletedTask; });
        h.ShadowTarget.DeleteAsync("k").Returns(_ => { Capture(); return Task.FromResult(true); });

        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                LatticeOriginalPrepareStampContext.StampPreparedRoute($"{TreeId}/{SourceShardIndex}");
                try
                {
                    if (delete)
                        await h.Grain.DeleteAsync("k");
                    else
                        await h.Grain.SetAsync("k", [1, 2]);
                }
                finally
                {
                    Orleans.Runtime.RequestContext.Remove(LatticeEventConstants.PreparedRouteRequestContextKey);
                }
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        return new ForwardCapture(forwarded, carried, stamp, route);
    }

    [Test]
    public async Task A_prepared_shadow_forward_carries_the_local_prepares_original_stamp_and_no_route()
    {
        var capture = await PreparedForwardAsync(localPrepareMarked: true);

        Assert.That(capture.Forwarded, Is.True);
        Assert.That(capture.CarriedStamp, Is.True);
        Assert.That(capture.Stamp, Is.EqualTo(ForwardedP));
        Assert.That(capture.Route, Is.Null, "a forward must never be classifiable as an original prepare by route");
    }

    [Test]
    public async Task A_prepared_shadow_forward_of_an_unmarked_prepare_carries_no_stamp()
    {
        var capture = await PreparedForwardAsync(localPrepareMarked: false);

        Assert.That(capture.Forwarded, Is.True);
        Assert.That(capture.CarriedStamp, Is.False);
    }

    [Test]
    public async Task A_prepared_delete_shadow_forward_carries_the_local_prepares_original_stamp()
    {
        var capture = await PreparedForwardAsync(localPrepareMarked: true, delete: true);

        Assert.That(capture.Forwarded, Is.True);
        Assert.That(capture.CarriedStamp, Is.True);
        Assert.That(capture.Stamp, Is.EqualTo(ForwardedP));
    }
}
