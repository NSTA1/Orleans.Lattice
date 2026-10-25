using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Summary_reads_preserve_pending_transaction_visibility()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "b", [2]);
        await PreparePendingDeleteAsync(grain, txid, "a");

        using (InFlight(txid))
        {
            Assert.That((await grain.CountAndNextSiblingAsync(null, null)).Count, Is.EqualTo(1));
            Assert.That((await grain.GetStatsAndNextSiblingAsync()).Stats.LiveKeys, Is.EqualTo(1));
        }
        using (Committed(txid))
        {
            Assert.That((await grain.CountAndNextSiblingAsync(null, null)).Count, Is.EqualTo(1));
            var stats = (await grain.GetStatsAndNextSiblingAsync()).Stats;
            Assert.That(stats.LiveKeys, Is.EqualTo(1));
            Assert.That(stats.Tombstones, Is.EqualTo(1));
        }
    }

    [TestCase(null, null, 2)]
    [TestCase("b", "d", 1)]
    [TestCase("z", null, 0)]
    public async Task CountAndNextSiblingAsync_preserves_range_count_and_topology(
        string? start, string? end, int expected)
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", [1]);
        await grain.SetAsync("c", [2]);
        await grain.SetAsync("b", [3]);
        await grain.DeleteAsync("b");
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "z";
        state.State.NextSibling = GrainId.Create("leaf", "next");

        var page = await grain.CountAndNextSiblingAsync(start, end);

        Assert.Multiple(() =>
        {
            Assert.That(page.Count, Is.EqualTo(expected));
            Assert.That(page.Link.KeyRange.LowKeyInclusive, Is.EqualTo("a"));
            Assert.That(page.Link.KeyRange.HighKeyExclusive, Is.EqualTo("z"));
            Assert.That(page.Link.NextSibling, Is.EqualTo(state.State.NextSibling));
        });
        Assert.That(page.Count, Is.EqualTo(await grain.CountAsync(start, end)));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task GetStatsAndNextSiblingAsync_preserves_statistics_and_tail(bool hasSuccessor)
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", [1, 2]);
        await grain.SetAsync("b", [3]);
        await grain.DeleteAsync("b");
        state.State.HighKeyExclusive = "m";
        state.State.NextSibling = hasSuccessor ? GrainId.Create("leaf", "next") : null;

        var expected = await grain.GetStatsAsync();
        var page = await grain.GetStatsAndNextSiblingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(page.Stats, Is.EqualTo(expected));
            Assert.That(page.Stats.LiveKeys, Is.EqualTo(1));
            Assert.That(page.Stats.Tombstones, Is.EqualTo(1));
            Assert.That(page.Link.KeyRange.HighKeyExclusive, Is.EqualTo("m"));
            Assert.That(page.Link.NextSibling, Is.EqualTo(state.State.NextSibling));
        });
    }
}
