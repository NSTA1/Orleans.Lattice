using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class BPlusInternalGrainTests
{
    private static readonly GrainId Child0 = GrainId.Create("leaf", "child-0");
    private static readonly GrainId Child1 = GrainId.Create("leaf", "child-1");
    private static readonly GrainId Child2 = GrainId.Create("leaf", "child-2");
    private static readonly GrainId Child3 = GrainId.Create("leaf", "child-3");

    private static BPlusInternalGrain CreateGrain(FakePersistentState<InternalNodeState>? state = null)
    {
        state ??= new FakePersistentState<InternalNodeState>();
        var context = Substitute.For<IGrainContext>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return new BPlusInternalGrain(context, state, grainFactory, optionsMonitor);
    }

    // --- InitializeAsync ---

    [Test]
    public async Task Initialize_creates_two_children()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        Assert.That(state.State.Children.Count, Is.EqualTo(2));
        Assert.That(state.State.Children[0].SeparatorKey, Is.Null);
        Assert.That(state.State.Children[0].ChildId, Is.EqualTo(Child0));
        Assert.That(state.State.Children[1].SeparatorKey, Is.EqualTo("fox"));
        Assert.That(state.State.Children[1].ChildId, Is.EqualTo(Child1));
    }

    [Test]
    public async Task Initialize_advances_HLC()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        var clockBefore = state.State.Clock;
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        Assert.That(state.State.Clock > clockBefore, Is.True);
    }

    // --- RouteWithMetadataAsync ---

    [Test]
    public async Task Route_returns_leftmost_for_key_below_all_separators()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var (result, _) = await grain.RouteWithMetadataAsync("ant");
        Assert.That(result, Is.EqualTo(Child0));
    }

    [Test]
    public async Task Route_returns_right_child_for_exact_separator_match()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var (result, _) = await grain.RouteWithMetadataAsync("fox");
        Assert.That(result, Is.EqualTo(Child1));
    }

    [Test]
    public async Task Route_returns_right_child_for_key_above_separator()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var (result, _) = await grain.RouteWithMetadataAsync("zebra");
        Assert.That(result, Is.EqualTo(Child1));
    }

    [Test]
    public async Task Route_with_multiple_separators_picks_correct_child()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("monkey", Child2);
        await grain.AcceptSplitAsync("rabbit", Child3);

        // "ant" < "fox" → Child0
        Assert.That((await grain.RouteWithMetadataAsync("ant")).ChildId, Is.EqualTo(Child0));
        // "fox" >= "fox" → Child1
        Assert.That((await grain.RouteWithMetadataAsync("fox")).ChildId, Is.EqualTo(Child1));
        // "lion" >= "fox" but < "monkey" → Child1
        Assert.That((await grain.RouteWithMetadataAsync("lion")).ChildId, Is.EqualTo(Child1));
        // "monkey" >= "monkey" → Child2
        Assert.That((await grain.RouteWithMetadataAsync("monkey")).ChildId, Is.EqualTo(Child2));
        // "penguin" >= "monkey" but < "rabbit" → Child2
        Assert.That((await grain.RouteWithMetadataAsync("penguin")).ChildId, Is.EqualTo(Child2));
        // "zebra" >= "rabbit" → Child3
        Assert.That((await grain.RouteWithMetadataAsync("zebra")).ChildId, Is.EqualTo(Child3));
    }

    // --- AcceptSplitAsync ---

    [Test]
    public async Task AcceptSplit_inserts_new_separator()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        await grain.AcceptSplitAsync("monkey", Child2);

        Assert.That(state.State.Children.Count, Is.EqualTo(3));
        Assert.That(state.State.Children[2].SeparatorKey, Is.EqualTo("monkey"));
        Assert.That(state.State.Children[2].ChildId, Is.EqualTo(Child2));
    }

    [Test]
    public async Task AcceptSplit_maintains_sort_order()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        // Insert out of order — "ant" < "fox", should go before "fox".
        await grain.AcceptSplitAsync("ant", Child2);

        Assert.That(state.State.Children.Count, Is.EqualTo(3));
        Assert.That(state.State.Children[0].SeparatorKey, Is.Null);
        Assert.That(state.State.Children[1].SeparatorKey, Is.EqualTo("ant"));
        Assert.That(state.State.Children[1].ChildId, Is.EqualTo(Child2));
        Assert.That(state.State.Children[2].SeparatorKey, Is.EqualTo("fox"));
    }

    [Test]
    public async Task AcceptSplit_is_idempotent_for_duplicate_delivery()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        await grain.AcceptSplitAsync("monkey", Child2);
        var result = await grain.AcceptSplitAsync("monkey", Child2);

        Assert.That(result, Is.Null);
        Assert.That(state.State.Children.Count, Is.EqualTo(3));
    }

    [Test]
    public async Task AcceptSplit_returns_null_when_under_capacity()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var result = await grain.AcceptSplitAsync("monkey", Child2);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task AcceptSplit_advances_HLC()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var clockBefore = state.State.Clock;
        await grain.AcceptSplitAsync("monkey", Child2);

        Assert.That(state.State.Clock > clockBefore, Is.True);
    }

    [Test]
    public async Task AcceptSplit_does_not_advance_HLC_on_duplicate()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("monkey", Child2);

        var clockBefore = state.State.Clock;
        await grain.AcceptSplitAsync("monkey", Child2);

        Assert.That(state.State.Clock, Is.EqualTo(clockBefore));
    }

    // --- ChildrenAreLeaves ---

    [Test]
    public async Task Initialize_sets_childrenAreLeaves_true()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        Assert.That(state.State.ChildrenAreLeaves, Is.True);
        Assert.That(await grain.AreChildrenLeavesAsync(), Is.True);
    }

    [Test]
    public async Task Initialize_sets_childrenAreLeaves_false()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: false);

        Assert.That(state.State.ChildrenAreLeaves, Is.False);
        Assert.That(await grain.AreChildrenLeavesAsync(), Is.False);
    }

    // --- Split recovery ---

    [Test]
    public async Task AcceptSplit_recovers_in_progress_split_and_forwards_promotion_to_sibling()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        // Simulate crash mid-split: persist split intent.
        var siblingId = GrainId.Create("internal", Guid.NewGuid().ToString());
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "fox";
        state.State.SplitSiblingId = siblingId;
        state.State.SplitRightChildren =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child1 }
        ];
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child0 }
        ];

        // "zebra" >= "fox" → recovery completes then forwards the promotion to the sibling.
        var result = await grain.AcceptSplitAsync("zebra", Child3);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.PromotedKey, Is.EqualTo("fox"));
        Assert.That(result.NewSiblingId, Is.EqualTo(siblingId));
        // The promotion was NOT inserted locally — only Child0 remains.
        Assert.That(state.State.Children, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task AcceptSplit_recovers_and_inserts_promotion_locally_when_below_split_key()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var siblingId = GrainId.Create("internal", Guid.NewGuid().ToString());
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "fox";
        state.State.SplitSiblingId = siblingId;
        state.State.SplitRightChildren =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child1 }
        ];
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child0 }
        ];

        // "ant" < "fox" → recovery completes then inserts the promotion locally.
        var result = await grain.AcceptSplitAsync("ant", Child2);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.PromotedKey, Is.EqualTo("fox"));
        // The promotion WAS inserted locally.
        Assert.That(state.State.Children.Count, Is.EqualTo(2));
        Assert.That(state.State.Children[1].SeparatorKey, Is.EqualTo("ant"));
        Assert.That(state.State.Children[1].ChildId, Is.EqualTo(Child2));
    }

    [Test]
    public async Task Recovery_reuses_persisted_sibling_id()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var siblingId = GrainId.Create("internal", Guid.NewGuid().ToString());
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "fox";
        state.State.SplitSiblingId = siblingId;
        state.State.SplitRightChildren =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child1 }
        ];
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child0 }
        ];

        var result = await grain.AcceptSplitAsync("ant", Child2);

        // The recovered split must use the original persisted sibling ID.
        Assert.That(result!.NewSiblingId, Is.EqualTo(siblingId));
    }

    [Test]
    public async Task Recovery_clears_split_right_children()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "fox";
        state.State.SplitSiblingId = GrainId.Create("internal", Guid.NewGuid().ToString());
        state.State.SplitRightChildren =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child1 }
        ];
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = Child0 }
        ];

        await grain.AcceptSplitAsync("ant", Child2);

        Assert.That(state.State.SplitRightChildren, Is.Null);
        Assert.That(state.State.SplitState, Is.EqualTo(
            Orleans.Lattice.Primitives.SplitState.SplitComplete));
    }

    // --- GetLeftmostChildAsync / GetRightmostChildAsync ---

    [Test]
    public async Task GetLeftmostChild_returns_first_child()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var result = await grain.GetLeftmostChildAsync();
        Assert.That(result, Is.EqualTo(Child0));
    }

    [Test]
    public async Task GetRightmostChild_returns_last_child()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var result = await grain.GetRightmostChildAsync();
        Assert.That(result, Is.EqualTo(Child1));
    }

    [Test]
    public async Task GetRightmostChild_returns_last_after_accept_split()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("monkey", Child2);

        var result = await grain.GetRightmostChildAsync();
        Assert.That(result, Is.EqualTo(Child2));
    }

    [Test]
    public async Task GetLeftmostChild_unchanged_after_accept_split()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("monkey", Child2);

        var result = await grain.GetLeftmostChildAsync();
        Assert.That(result, Is.EqualTo(Child0));
    }

    // --- SetTreeIdAsync idempotency ---

    [Test]
    public async Task SetTreeId_is_idempotent()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        await grain.SetTreeIdAsync("tree-1");
        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));

        await grain.SetTreeIdAsync("tree-2");
        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));
    }

    // --- RouteWithMetadataAsync ---

    [Test]
    public async Task RouteWithMetadata_returns_child_and_leaf_flag()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var (childId, childrenAreLeaves) = await grain.RouteWithMetadataAsync("ant");

        Assert.That(childId, Is.EqualTo(Child0));
        Assert.That(childrenAreLeaves, Is.True);
    }

    [Test]
    public async Task RouteWithMetadata_returns_false_when_children_are_internal()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: false);

        var (childId, childrenAreLeaves) = await grain.RouteWithMetadataAsync("zebra");

        Assert.That(childId, Is.EqualTo(Child1));
        Assert.That(childrenAreLeaves, Is.False);
    }

    // --- GetLeftmostChildWithMetadataAsync / GetRightmostChildWithMetadataAsync ---

    [Test]
    public async Task GetLeftmostChildWithMetadata_returns_first_child_and_leaf_flag()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var (childId, childrenAreLeaves) = await grain.GetLeftmostChildWithMetadataAsync();

        Assert.That(childId, Is.EqualTo(Child0));
        Assert.That(childrenAreLeaves, Is.True);
    }

    [Test]
    public async Task GetRightmostChildWithMetadata_returns_last_child_and_leaf_flag()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var (childId, childrenAreLeaves) = await grain.GetRightmostChildWithMetadataAsync();

        Assert.That(childId, Is.EqualTo(Child1));
        Assert.That(childrenAreLeaves, Is.True);
    }

    [Test]
    public async Task GetLeftmostChildWithMetadata_returns_false_for_internal_children()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: false);

        var (_, childrenAreLeaves) = await grain.GetLeftmostChildWithMetadataAsync();

        Assert.That(childrenAreLeaves, Is.False);
    }

    [Test]
    public async Task GetRightmostChildWithMetadata_reflects_accept_split()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("monkey", Child2);

        var (childId, _) = await grain.GetRightmostChildWithMetadataAsync();

        Assert.That(childId, Is.EqualTo(Child2));
    }
}
