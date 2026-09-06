using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <c>BPlusInternalGrain.RemoveChildAsync</c>, the topology
/// half of empty-leaf chain reclaim.
/// <para>
/// The tree could always grow a range's leaf count through
/// <c>AcceptSplitAsync</c> and never shrink it. This is the inverse, and it is
/// the dangerous direction: removing the wrong separator does not slow the
/// tree down, it makes a live range route somewhere no reader looks. These
/// tests pin the removal's guards - the leftmost catch-all it must refuse, the
/// unknown child it must decline rather than throw over, and the mid-split
/// window in which its work would be silently reverted by recovery - because
/// each of those refusals is load-bearing rather than defensive.
/// </para>
/// </summary>
public partial class BPlusInternalGrainTests
{
    private static async Task<(BPlusInternalGrain Grain, FakePersistentState<InternalNodeState> State)>
        CreateThreeChildNodeAsync()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("mole", Child2);
        return (grain, state);
    }

    [Test]
    public async Task RemoveChild_removes_a_middle_child_and_leaves_its_siblings()
    {
        var (grain, state) = await CreateThreeChildNodeAsync();

        var removed = await grain.RemoveChildAsync(Child1);

        Assert.That(removed, Is.True);
        Assert.That(state.State.Children.Select(c => c.ChildId),
            Is.EqualTo(new[] { Child0, Child2 }));
    }

    /// <summary>
    /// The point of the removal: the range the departing child covered must
    /// route to the child on its left afterwards. If it routed anywhere else
    /// the reclaim would have moved a live key range out from under its data.
    /// </summary>
    [Test]
    public async Task RemoveChild_routes_the_departed_range_to_the_left_neighbour()
    {
        var (grain, _) = await CreateThreeChildNodeAsync();

        var (before, _) = await grain.RouteWithMetadataAsync("goat");
        Assert.That(before, Is.EqualTo(Child1), "precondition: 'goat' routes to the child being removed");

        await grain.RemoveChildAsync(Child1);

        var (after, _) = await grain.RouteWithMetadataAsync("goat");
        Assert.That(after, Is.EqualTo(Child0),
            "the vacated range must fall to the left neighbour, which has widened to cover it");
    }

    /// <summary>
    /// The leftmost child carries the null separator and is the catch-all for
    /// every key below the first real separator. Removing it would leave that
    /// range owned by nobody, because there is no child to its left to inherit
    /// it. The shard root filters these out before it calls, so this is the
    /// structural backstop that keeps the invariant local to the node that
    /// owns it.
    /// </summary>
    [Test]
    public async Task RemoveChild_refuses_the_leftmost_catch_all_child()
    {
        var (grain, state) = await CreateThreeChildNodeAsync();

        var removed = await grain.RemoveChildAsync(Child0);

        Assert.That(removed, Is.False);
        Assert.That(state.State.Children.Select(c => c.ChildId),
            Is.EqualTo(new[] { Child0, Child1, Child2 }));
    }

    /// <summary>
    /// A reclaim re-driven after a crash re-issues a removal that already
    /// landed. That must read as an ordinary declined removal rather than an
    /// exception, or the resume path would fail on exactly the state it exists
    /// to repair.
    /// </summary>
    [Test]
    public async Task RemoveChild_declines_a_child_it_does_not_have()
    {
        var (grain, state) = await CreateThreeChildNodeAsync();

        var removed = await grain.RemoveChildAsync(Child3);

        Assert.That(removed, Is.False);
        Assert.That(state.State.Children.Count, Is.EqualTo(3));
    }

    /// <summary>
    /// A removal applied while this node's own split is mid-flight would be
    /// silently undone: the recovery branch in <c>AcceptSplitCoreAsync</c>
    /// rebuilds the child list from the persisted split state, which knows
    /// nothing of the removal. Declining leaves the leaf for the next pass,
    /// which is free because reclaim is background work.
    /// </summary>
    [Test]
    public async Task RemoveChild_declines_while_a_split_of_this_node_is_in_progress()
    {
        var (grain, state) = await CreateThreeChildNodeAsync();
        state.State.SplitState = SplitState.SplitInProgress;

        var removed = await grain.RemoveChildAsync(Child1);

        Assert.That(removed, Is.False);
        Assert.That(state.State.Children.Count, Is.EqualTo(3));
    }

    [Test]
    public async Task RemoveChild_advances_HLC()
    {
        var (grain, state) = await CreateThreeChildNodeAsync();

        var clockBefore = state.State.Clock;
        await grain.RemoveChildAsync(Child1);

        Assert.That(state.State.Clock > clockBefore, Is.True);
    }

    [Test]
    public async Task RemoveChild_persists_the_shortened_child_list()
    {
        var (grain, state) = await CreateThreeChildNodeAsync();

        var writesBefore = state.WriteCount;
        await grain.RemoveChildAsync(Child1);

        Assert.That(state.WriteCount, Is.GreaterThan(writesBefore),
            "a topology change that is not persisted is lost on the next activation");
    }

    /// <summary>
    /// Class B divergence guard, matching <c>AcceptSplitCoreAsync</c>: when the
    /// persist fails, this activation must not keep routing against a topology
    /// no peer and no future activation shares.
    /// </summary>
    [Test]
    public async Task RemoveChild_reverts_the_child_list_when_the_persist_fails()
    {
        var (grain, state) = await CreateThreeChildNodeAsync();

        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.RemoveChildAsync(Child1));

        Assert.That(state.State.Children.Select(c => c.ChildId),
            Is.EqualTo(new[] { Child0, Child1, Child2 }),
            "a failed persist must leave the in-memory topology as storage still describes it");

        var (routed, _) = await grain.RouteWithMetadataAsync("goat");
        Assert.That(routed, Is.EqualTo(Child1),
            "routing must still reach the child that storage says is present");
    }
}
