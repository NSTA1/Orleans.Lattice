using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522: a terminal that arrives carrying the saga's original prepare
/// stamps is mirrored to the resize destination with them. The resize mirror
/// ships every write at this copy's own stamps and every prepare carrying its
/// own, so the destination is on this copy's clock lineage and its backstop
/// applies each key at its prepare stamp.
/// </summary>
public partial class ShardRootGrainShadowForwardTests
{
    private static readonly HybridLogicalClock MirrorStampP = new() { WallClockTicks = 9_000, Counter = 1 };

    private static List<object?> CaptureMirroredStamps(IShardRootGrain destination)
    {
        var seen = new List<object?>();
        destination.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns(_ =>
            {
                lock (seen) seen.Add(RequestContext.Get(LatticeEventConstants.OriginalPrepareStampsRequestContextKey));
                return Task.FromResult<WalRecord?>(null);
            });
        return seen;
    }

    [TestCase(false, TestName = "AppendTxTerminalAsync_mirrors_a_terminal_with_its_original_stamps_before_the_swap")]
    [TestCase(true, TestName = "AppendTxTerminalAsync_mirrors_a_terminal_with_its_original_stamps_over_the_swapped_closure")]
    public async Task AppendTxTerminalAsync_mirrors_a_terminal_to_the_resize_destination_with_its_original_stamps(bool swapped)
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, swapped ? ShadowForwardPhase.Rejecting : ShadowForwardPhase.Drained);
        var mirrored = CaptureMirroredStamps(DestinationShard(h, ShardIndex));
        var carried = new Dictionary<string, HybridLogicalClock> { ["k"] = MirrorStampP };

        using (LatticeOriginalPrepareStampContext.With(carried))
        {
            await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true, new Dictionary<string, byte[]> { ["k"] = [1] });


        }

        Assert.That(mirrored, Is.Not.Empty, "PRECONDITION: the terminal was mirrored to the resize destination");
        Assert.That(mirrored, Has.All.SameAs(carried), "the original stamps travel with the mirrored terminal");
    }
}
