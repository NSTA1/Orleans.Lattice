using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522 (PR2a): a terminal that arrives carrying the saga's original
/// prepare stamps is mirrored to the resize destination without them. P was
/// minted on this copy's clocks, and the mirror re-mints a mirrored prepare on
/// the destination, so on the destination P orders nothing and the destination's
/// backstop keeps its dominating stamp. Carrying P there is unsafe until the
/// mirror itself carries it (PR2b).
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

    [TestCase(false, TestName = "AppendTxTerminalAsync_mirrors_a_terminal_without_its_original_stamps_before_the_swap")]
    [TestCase(true, TestName = "AppendTxTerminalAsync_mirrors_a_terminal_without_its_original_stamps_over_the_swapped_closure")]
    public async Task AppendTxTerminalAsync_mirrors_a_terminal_to_the_resize_destination_without_its_original_stamps(bool swapped)
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, swapped ? ShadowForwardPhase.Rejecting : ShadowForwardPhase.Drained);
        var mirrored = CaptureMirroredStamps(DestinationShard(h, ShardIndex));
        var carried = new Dictionary<string, HybridLogicalClock> { ["k"] = MirrorStampP };

        using (LatticeOriginalPrepareStampContext.With(carried))
        {
            await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true, new Dictionary<string, byte[]> { ["k"] = [1] });

            Assert.That(RequestContext.Get(LatticeEventConstants.OriginalPrepareStampsRequestContextKey), Is.SameAs(carried),
                "the strip is scoped to the mirror: the caller's stamps are restored");
        }

        Assert.That(mirrored, Is.Not.Empty, "PRECONDITION: the terminal was mirrored to the resize destination");
        Assert.That(mirrored, Has.All.Null, "the original stamps never reach the resized copy");
    }
}
