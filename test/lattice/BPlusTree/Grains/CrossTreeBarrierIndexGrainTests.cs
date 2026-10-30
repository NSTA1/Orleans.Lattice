using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class CrossTreeBarrierIndexGrainTests
{
    [Test]
    public async Task GetAllAsync_returns_a_detached_deduplicated_union_without_writes()
    {
        var state = new FakePersistentState<CrossTreeBarrierIndexState>();
        state.State.Barriers.UnionWith(["live", "both", "BOTH"]);
        state.State.Tombstones.UnionWith(["dead", "both"]);
        var grain = new CrossTreeBarrierIndexGrain(state);

        var all = await grain.GetAllAsync();
        await grain.AddAsync("later");
        await grain.RemoveTombstoneAsync("dead");

        Assert.That(all, Is.EquivalentTo(new[] { "live", "both", "BOTH", "dead" }));
        Assert.That(await grain.GetAllAsync(), Is.EquivalentTo(new[] { "live", "both", "BOTH", "later" }));
        Assert.That(state.WriteCount, Is.EqualTo(2), "the reads do not persist");
    }

    [Test]
    public async Task GetAllAsync_returns_an_initialized_empty_array_for_an_empty_index()
    {
        var grain = new CrossTreeBarrierIndexGrain(new FakePersistentState<CrossTreeBarrierIndexState>());

        var all = await grain.GetAllAsync();

        Assert.That(all.IsDefault, Is.False);
        Assert.That(all, Is.Empty);
    }
}
