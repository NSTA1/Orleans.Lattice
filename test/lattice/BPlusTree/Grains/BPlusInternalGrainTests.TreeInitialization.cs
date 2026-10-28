using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusInternalGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Tree_initialization_persists_binding_before_topology(bool childList)
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        var writes = new List<(string? Tree, int Children)>();
        state.OnWriteState = s => writes.Add((s.TreeId, s.Children.Count));

        await InitializeForTreeAsync(childList);

        Assert.That(writes.Take(2), Is.EqualTo(new[] { ("tree", 0), ("tree", 2) }));
        Assert.That((await grain.RouteWithMetadataAsync("fox")).ChildId, Is.EqualTo(Child1));
        Assert.That(await grain.GetTreeIdAsync(), Is.EqualTo("tree"));

        Task InitializeForTreeAsync(bool list) => list
            ? grain.InitializeWithChildrenForTreeAsync("tree", [null, "fox"], [Child0, Child1], true)
            : grain.InitializeForTreeAsync("tree", "fox", Child0, Child1, true);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Tree_initialization_binding_failure_leaves_topology_uninitialized_and_retry_succeeds(bool childList)
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        state.ThrowOnWrite = new InvalidOperationException("binding persist failed");

        Assert.ThrowsAsync<InvalidOperationException>(Initialize);

        Assert.That(state.State.TreeId, Is.Null);
        Assert.That(state.State.Children, Is.Empty);
        await Initialize();
        Assert.That(state.State.TreeId, Is.EqualTo("tree"));
        Assert.That(state.State.Children.Count, Is.EqualTo(2));

        Task Initialize() => childList
            ? grain.InitializeWithChildrenForTreeAsync("tree", [null, "fox"], [Child0, Child1], true)
            : grain.InitializeForTreeAsync("tree", "fox", Child0, Child1, true);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Tree_initialization_topology_failure_keeps_durable_binding_and_retry_reuses_it(bool childList)
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        state.OnWriteState = _ =>
        {
            if (state.WriteCount == 1)
                state.ThrowOnWrite = new InvalidOperationException("topology persist failed");
        };
        var originalClock = state.State.Clock;

        Assert.ThrowsAsync<InvalidOperationException>(Initialize);

        Assert.That(state.State.TreeId, Is.EqualTo("tree"));
        Assert.That(state.State.Children, Is.Empty);
        Assert.That(state.State.Clock, Is.EqualTo(originalClock));
        Assert.That(state.WriteCount, Is.EqualTo(1));
        var retryWrites = new List<int>();
        state.OnWriteState = s => retryWrites.Add(s.Children.Count);
        await Initialize();
        Assert.That(state.State.Children.Count, Is.EqualTo(2));
        Assert.That(retryWrites, Is.Not.Empty);
        Assert.That(retryWrites, Has.All.EqualTo(2),
            "Retry must start with topology, not repeat binding on the still-empty node; digest seeds may write afterwards.");

        Task Initialize() => childList
            ? grain.InitializeWithChildrenForTreeAsync("tree", [null, "fox"], [Child0, Child1], true)
            : grain.InitializeForTreeAsync("tree", "fox", Child0, Child1, true);
    }
}
