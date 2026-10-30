using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LeafCacheGrainTests
{
    [Test]
    public async Task Refresh_uses_one_leaf_call_and_still_delegates_pending_keys()
    {
        var (cache, leaf) = CreateGrain(new LatticeOptions { CacheTtl = TimeSpan.Zero });
        var delta = EmptyDelta();
        delta.Entries["k"] = new LwwValue<byte[]> { Value = [1] };
        leaf.GetCacheRefreshAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns((new List<string> { "k" }, delta));
        leaf.GetAsync("k").Returns(new byte[] { 2 });
        leaf.ClearReceivedCalls();

        Assert.That(await cache.GetAsync("k"), Is.EqualTo(new byte[] { 2 }));

        await leaf.Received(1).GetCacheRefreshAsync(LeafDeliveryCursor.Empty);
        await leaf.Received(1).GetAsync("k");
        await leaf.DidNotReceive().GetPendingKeysAsync();
        await leaf.DidNotReceive().GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }
}
