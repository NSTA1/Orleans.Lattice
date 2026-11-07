using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The leaf's side of the key-filtered unstamped watermark (#5851, a5e2e2ef's K2 and K4;
/// model mutations in <c>WalLiveAdvanceCore</c>, #5860): a split reads its cut for the
/// range the donor owns in the same turn - the whole pre-split range, widened by any
/// fold - and a widening path's own hand-off close still holds the hint, however clean
/// the filtered watermark.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task UnstampedFilterUsesStaleRange()
    {
        // ReadPositionHonestUnstampedFilterUsesStaleRange (K2, K4): a donor whose range
        // widened before the split reads its cut for the widened range, never the range it
        // held before, nor the half it narrows to.
        (string? Low, string? High)? read = null;
        await PreArmSplitAsync(
            new WalSplitCut(Head: 42, ForeignHigh: -1, UnstampedWatermark: -1),
            frontier: 30,
            configure: rig => rig.Shard
                .When(s => s.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()))
                .Do(call => read = (call.ArgAt<string?>(1), call.ArgAt<string?>(2))),
            onState: (state, _) =>
            {
                state.State.LowKeyInclusive = "k";
                state.State.HighKeyExclusive = "l";
            },
            beforeLastWrite: state => state.State.LowKeyInclusive = "a");

        Assert.That(read, Is.EqualTo(((string?)"a", (string?)"l")),
            "the cut was not read for the range the donor owns at the split");
    }

    [Test]
    public async Task WidenWithoutDrain()
    {
        // ReadPositionHonestWidenWithoutDrain (K4): the widening path's own close bounds the
        // absorbed span's in-flight records, so the hint stays at the donor's frontier until
        // it has read past the close, even when the filtered watermark and the foreign high
        // are both below it.
        var (_, _, sibling, _) = await PreArmSplitAsync(
            new WalSplitCut(Head: 42, ForeignHigh: -1, UnstampedWatermark: -1),
            frontier: 30,
            onState: (_, rig) => rig.SetLeafMarkerAsync(m =>
            {
                HandOffMarkerCore.Open(m, "reclaim:absorbed", null, null);
                return HandOffMarkerCore.Close(m, "reclaim:absorbed", [40]);
            }).GetAwaiter().GetResult());

        await sibling.Received(1).SetCheckpointOffsetHintsAsync(Arg.Is<long[]>(h => h.Length == 1 && h[0] == 30L));
    }
}
