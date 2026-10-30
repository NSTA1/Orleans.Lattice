using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task GetCacheRefreshAsync_preserves_snapshot_incremental_and_stale_cursor_delivery()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", new byte[] { 1 });
        var snapshot = await grain.GetCacheRefreshAsync(LeafDeliveryCursor.Empty);
        Assert.That(snapshot.Delta.Entries.Keys, Is.EquivalentTo(new[] { "a" }));
        Assert.That(snapshot.PendingKeys, Is.Empty);

        await grain.SetAsync("b", new byte[] { 2 });
        var incremental = await grain.GetCacheRefreshAsync(snapshot.Delta.DeliveryCursor);
        Assert.That(incremental.Delta.Entries.Keys, Is.EquivalentTo(new[] { "b" }));
        var atHead = await grain.GetCacheRefreshAsync(incremental.Delta.DeliveryCursor);
        Assert.That(atHead.Delta.IsEmpty, Is.True);

        var stale = await grain.GetCacheRefreshAsync(incremental.Delta.DeliveryCursor with { Sequence = long.MaxValue });
        Assert.That(stale.Delta.Entries.Keys, Is.EquivalentTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetCacheRefreshAsync_reports_pending_keys_without_exposing_prepared_values()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", new byte[] { 1 });
        var txid = Guid.NewGuid();
        using (LatticeApplyOffsetContext.BeginScope(partition: 0, offset: 5))
            AsProjection(grain).Apply(BuildPreparedSet(txid, "a", Encoding.UTF8.GetBytes("prepared"), treeId: "test-tree"));

        var refresh = await grain.GetCacheRefreshAsync(LeafDeliveryCursor.Empty);

        Assert.That(refresh.PendingKeys, Is.EquivalentTo(new[] { "a" }));
        Assert.That(refresh.Delta.Entries["a"].Value, Is.EqualTo(new byte[] { 1 }));
    }
}
