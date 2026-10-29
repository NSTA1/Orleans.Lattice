using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task GetBootstrapCaptureAsync_preserves_delta_live_visibility_and_chain(bool tail)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "z";
        var grain = CreateGrain(state);
        await grain.SetAsync("live", [1]);
        await grain.SetAsync("deleted", [2]);
        await grain.DeleteAsync("deleted");
        await grain.SetAsync("expired", [3], DateTimeOffset.UtcNow.AddHours(-1).Ticks);
        if (!tail) await grain.SetNextSiblingAsync(CombinedPageSibling);

        var (delta, live) = await grain.GetBootstrapCaptureAsync(new VersionVector());
        var link = await grain.GetKeyRangeAndNextSiblingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries.Keys, Is.EquivalentTo(new[] { "live", "deleted", "expired" }));
            Assert.That(delta.Entries["deleted"].IsTombstone, Is.True);
            Assert.That(live.Entries.Select(row => row.Key), Is.EqualTo(new[] { "live" }));
            Assert.That(live.Link, Is.EqualTo(link));
        });
    }

    [Test]
    public async Task GetBootstrapCaptureAsync_caught_up_delta_does_not_hide_live_rows()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("live", [1]);

        var (delta, live) = await grain.GetBootstrapCaptureAsync(state.State.Version.Clone());

        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries, Is.Empty);
            Assert.That(live.Entries.Select(row => row.Key), Is.EqualTo(new[] { "live" }));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetLiveRawEntriesAndPendingAsync_preserves_slot_filter_and_chain(bool tail)
    {
        const int virtualShards = 4096;
        var grain = CreateGrain();
        await grain.SetAsync("live", [1]);
        if (!tail) await grain.SetNextSiblingAsync(CombinedPageSibling);
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();
        var projection = AsProjection(grain);
        projection.Apply(BuildPreparedSet(txA, "a", [2], treeId: "bootstrap-reads"));
        projection.Apply(BuildPreparedSet(txB, "b", [3], treeId: "bootstrap-reads"));
        var slots = new[] { ShardMap.GetVirtualSlot("a", virtualShards) };
        Assert.That(slots[0], Is.Not.EqualTo(ShardMap.GetVirtualSlot("b", virtualShards)));

        var (entries, pending) = await grain.GetLiveRawEntriesAndPendingAsync(slots, virtualShards);
        var next = await grain.GetNextSiblingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(row => row.Key), Is.EqualTo(new[] { "live" }));
            Assert.That(pending.Mutations.Select(row => row.TransactionId), Is.EqualTo(new[] { txA }));
            Assert.That(pending.NextSibling, Is.EqualTo(next));
        });
    }

    [Test]
    public void GetLiveRawEntriesAndPendingAsync_rejects_null_slots()
    {
        var grain = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.GetLiveRawEntriesAndPendingAsync(null!, 4096));
    }
}
