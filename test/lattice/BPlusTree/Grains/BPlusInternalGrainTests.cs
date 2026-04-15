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
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return new BPlusInternalGrain(state, grainFactory, optionsMonitor);
    }

    // --- InitializeAsync ---

    [Fact]
    public async Task Initialize_creates_two_children()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        Assert.Equal(2, state.State.Children.Count);
        Assert.Null(state.State.Children[0].SeparatorKey);
        Assert.Equal(Child0, state.State.Children[0].ChildId);
        Assert.Equal("fox", state.State.Children[1].SeparatorKey);
        Assert.Equal(Child1, state.State.Children[1].ChildId);
    }

    [Fact]
    public async Task Initialize_advances_HLC()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        var clockBefore = state.State.Clock;
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        Assert.True(state.State.Clock > clockBefore);
    }

    // --- RouteAsync ---

    [Fact]
    public async Task Route_returns_leftmost_for_key_below_all_separators()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var result = await grain.RouteAsync("ant");
        Assert.Equal(Child0, result);
    }

    [Fact]
    public async Task Route_returns_right_child_for_exact_separator_match()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var result = await grain.RouteAsync("fox");
        Assert.Equal(Child1, result);
    }

    [Fact]
    public async Task Route_returns_right_child_for_key_above_separator()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var result = await grain.RouteAsync("zebra");
        Assert.Equal(Child1, result);
    }

    [Fact]
    public async Task Route_with_multiple_separators_picks_correct_child()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("monkey", Child2);
        await grain.AcceptSplitAsync("rabbit", Child3);

        // "ant" < "fox" → Child0
        Assert.Equal(Child0, await grain.RouteAsync("ant"));
        // "fox" >= "fox" → Child1
        Assert.Equal(Child1, await grain.RouteAsync("fox"));
        // "lion" >= "fox" but < "monkey" → Child1
        Assert.Equal(Child1, await grain.RouteAsync("lion"));
        // "monkey" >= "monkey" → Child2
        Assert.Equal(Child2, await grain.RouteAsync("monkey"));
        // "penguin" >= "monkey" but < "rabbit" → Child2
        Assert.Equal(Child2, await grain.RouteAsync("penguin"));
        // "zebra" >= "rabbit" → Child3
        Assert.Equal(Child3, await grain.RouteAsync("zebra"));
    }

    // --- AcceptSplitAsync ---

    [Fact]
    public async Task AcceptSplit_inserts_new_separator()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        await grain.AcceptSplitAsync("monkey", Child2);

        Assert.Equal(3, state.State.Children.Count);
        Assert.Equal("monkey", state.State.Children[2].SeparatorKey);
        Assert.Equal(Child2, state.State.Children[2].ChildId);
    }

    [Fact]
    public async Task AcceptSplit_maintains_sort_order()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        // Insert out of order — "ant" < "fox", should go before "fox".
        await grain.AcceptSplitAsync("ant", Child2);

        Assert.Equal(3, state.State.Children.Count);
        Assert.Null(state.State.Children[0].SeparatorKey);
        Assert.Equal("ant", state.State.Children[1].SeparatorKey);
        Assert.Equal(Child2, state.State.Children[1].ChildId);
        Assert.Equal("fox", state.State.Children[2].SeparatorKey);
    }

    [Fact]
    public async Task AcceptSplit_is_idempotent_for_duplicate_delivery()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        await grain.AcceptSplitAsync("monkey", Child2);
        var result = await grain.AcceptSplitAsync("monkey", Child2);

        Assert.Null(result);
        Assert.Equal(3, state.State.Children.Count);
    }

    [Fact]
    public async Task AcceptSplit_returns_null_when_under_capacity()
    {
        var grain = CreateGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var result = await grain.AcceptSplitAsync("monkey", Child2);
        Assert.Null(result);
    }

    [Fact]
    public async Task AcceptSplit_advances_HLC()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var clockBefore = state.State.Clock;
        await grain.AcceptSplitAsync("monkey", Child2);

        Assert.True(state.State.Clock > clockBefore);
    }

    [Fact]
    public async Task AcceptSplit_does_not_advance_HLC_on_duplicate()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        await grain.AcceptSplitAsync("monkey", Child2);

        var clockBefore = state.State.Clock;
        await grain.AcceptSplitAsync("monkey", Child2);

        Assert.Equal(clockBefore, state.State.Clock);
    }

    // --- ChildrenAreLeaves ---

    [Fact]
    public async Task Initialize_sets_childrenAreLeaves_true()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        Assert.True(state.State.ChildrenAreLeaves);
        Assert.True(await grain.AreChildrenLeavesAsync());
    }

    [Fact]
    public async Task Initialize_sets_childrenAreLeaves_false()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: false);

        Assert.False(state.State.ChildrenAreLeaves);
        Assert.False(await grain.AreChildrenLeavesAsync());
    }

    // --- Split recovery ---

    [Fact]
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

        Assert.NotNull(result);
        Assert.Equal("fox", result.PromotedKey);
        Assert.Equal(siblingId, result.NewSiblingId);
        // The promotion was NOT inserted locally — only Child0 remains.
        Assert.Single(state.State.Children);
    }

    [Fact]
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

        Assert.NotNull(result);
        Assert.Equal("fox", result.PromotedKey);
        // The promotion WAS inserted locally.
        Assert.Equal(2, state.State.Children.Count);
        Assert.Equal("ant", state.State.Children[1].SeparatorKey);
        Assert.Equal(Child2, state.State.Children[1].ChildId);
    }

    [Fact]
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
        Assert.Equal(siblingId, result!.NewSiblingId);
    }

    [Fact]
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

        Assert.Null(state.State.SplitRightChildren);
        Assert.Equal(
            Orleans.Lattice.Primitives.SplitState.SplitComplete,
            state.State.SplitState);
    }
}
