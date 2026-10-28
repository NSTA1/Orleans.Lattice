using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task SetBinding_preserves_two_ordered_writes_and_is_idempotent()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var writes = new List<(string? Tree, int? Shard)>();
        state.OnWriteState = row => writes.Add((row.TreeId, row.ShardIndex));
        var grain = CreateGrain(state);

        await grain.SetBindingAsync("binding-tree", 3);
        await grain.SetBindingAsync("other-tree", 4);

        Assert.That(writes, Is.EqualTo(new (string?, int?)[] { ("binding-tree", null), ("binding-tree", 3) }));
        Assert.That(state.State.TreeId, Is.EqualTo("binding-tree"));
        Assert.That(state.State.ShardIndex, Is.EqualTo(3));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task SetBinding_completes_a_partially_bound_leaf_without_overwriting_identity(bool treeAlreadyBound)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeAlreadyBound ? "original-tree" : null;
        state.State.ShardIndex = treeAlreadyBound ? null : 7;
        var grain = CreateGrain(state);

        await grain.SetBindingAsync("binding-tree", 3);

        Assert.That(state.State.TreeId, Is.EqualTo(treeAlreadyBound ? "original-tree" : "binding-tree"));
        Assert.That(state.State.ShardIndex, Is.EqualTo(treeAlreadyBound ? 3 : 7));
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [TestCase(1)]
    [TestCase(2)]
    public async Task SetBinding_failed_write_propagates_and_retry_finishes_binding(int failingWrite)
    {
        var state = new FakePersistentState<LeafNodeState>();
        var failure = new InvalidOperationException("binding persist failed");
        var attempts = 0;
        state.BeforeWrite = () =>
        {
            if (++attempts == failingWrite)
                state.ThrowOnWrite = failure;
            return Task.CompletedTask;
        };
        var grain = CreateGrain(state);

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(() => grain.SetBindingAsync("binding-tree", 3));

        Assert.That(thrown, Is.SameAs(failure));
        Assert.That(state.State.TreeId, Is.EqualTo(failingWrite == 1 ? null : "binding-tree"));
        Assert.That(state.State.ShardIndex, Is.Null);
        Assert.That(state.WriteCount, Is.EqualTo(failingWrite - 1));

        await grain.SetBindingAsync("binding-tree", 3);

        Assert.That(state.State.TreeId, Is.EqualTo("binding-tree"));
        Assert.That(state.State.ShardIndex, Is.EqualTo(3));
        Assert.That(state.WriteCount, Is.EqualTo(2), "Retry must retain the successful prefix, not repeat it.");
    }
}
