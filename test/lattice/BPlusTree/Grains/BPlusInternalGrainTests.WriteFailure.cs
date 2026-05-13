using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the "persisted / in-memory divergence on
/// <c>WriteStateAsync</c> failure" anti-pattern (bug-hunter Class B). Every
/// mutation method on <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusInternalGrain"/>
/// mutates <c>state.State</c> in memory before awaiting
/// <c>state.WriteStateAsync()</c>. If the persist call throws, the activation
/// is left serving in-memory routes and clocks that were never durably
/// committed; a peer silo - or any future reactivation - would route
/// differently from this activation's reads, silently violating the routing
/// invariant.
/// </summary>
public partial class BPlusInternalGrainTests
{
    [Test]
    public async Task AcceptSplit_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: a fully-initialised internal node with two children and
        // a stamped HLC. The initial WriteStateAsync from InitializeAsync
        // must succeed so that the pre-mutation baseline is the state we
        // assert against.
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var childrenBefore = state.State.Children
            .Select(c => (c.SeparatorKey, c.ChildId))
            .ToArray();
        var clockBefore = state.State.Clock;

        // Arrange: the next WriteStateAsync (the one inside AcceptSplitAsync)
        // will throw, simulating a transient storage failure.
        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        // Act: AcceptSplitAsync mutates Children and Clock in memory before
        // awaiting WriteStateAsync. The thrown exception must leave the
        // activation in a state that matches what a peer reading from
        // storage would observe - i.e. the pre-mutation values.
        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.AcceptSplitAsync("monkey", Child2));

        // Assert: in-memory Children list must be the pre-mutation list.
        // If this fails, the activation will continue routing as though
        // the split landed - while every peer (and any future reactivation
        // of this grain) sees the unmodified two-child topology. The
        // divergence is exactly the Class B "persisted / in-memory
        // divergence on write failure" anti-pattern.
        var childrenAfter = state.State.Children
            .Select(c => (c.SeparatorKey, c.ChildId))
            .ToArray();
        Assert.That(childrenAfter, Is.EqualTo(childrenBefore),
            "Children mutated in memory survived a failing WriteStateAsync; "
            + "subsequent routes on this activation diverge from any peer "
            + "or future reactivation.");

        // Assert: in-memory Clock must also be the pre-mutation value, for
        // the same reason. A leaked Tick advances a high-water mark that
        // no peer observes.
        Assert.That(state.State.Clock, Is.EqualTo(clockBefore),
            "Clock advanced in memory survived a failing WriteStateAsync.");
    }

    [Test]
    public void Initialize_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Fresh grain: pre-mutation state is "uninitialised" (empty children
        // list, default ChildrenAreLeaves, default Clock).
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        var childrenBefore = state.State.Children.Count;
        var childrenAreLeavesBefore = state.State.ChildrenAreLeaves;
        var clockBefore = state.State.Clock;

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true));

        // If the in-memory Children stays at the post-mutation value, every
        // RouteWithMetadataAsync against this activation returns Child0 / Child1
        // while a peer (or this grain's next reactivation) would route from
        // an uninitialised empty Children list - the same Class B divergence
        // anti-pattern as AcceptSplit above.
        Assert.That(state.State.Children.Count, Is.EqualTo(childrenBefore),
            "Children mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.ChildrenAreLeaves, Is.EqualTo(childrenAreLeavesBefore),
            "ChildrenAreLeaves mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.Clock, Is.EqualTo(clockBefore),
            "Clock advanced in memory survived a failing WriteStateAsync.");
    }

    [Test]
    public void InitializeWithChildren_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        var childrenBefore = state.State.Children.Count;
        var childrenAreLeavesBefore = state.State.ChildrenAreLeaves;
        var clockBefore = state.State.Clock;

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        var separatorKeys = new List<string?> { null, "fox" };
        var childIds = new List<GrainId> { Child0, Child1 };

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.InitializeWithChildrenAsync(
                separatorKeys, childIds, childrenAreLeaves: true));

        Assert.That(state.State.Children.Count, Is.EqualTo(childrenBefore),
            "Children mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.ChildrenAreLeaves, Is.EqualTo(childrenAreLeavesBefore),
            "ChildrenAreLeaves mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.Clock, Is.EqualTo(clockBefore),
            "Clock advanced in memory survived a failing WriteStateAsync.");
    }

    [Test]
    public void SetTreeId_reverts_in_memory_TreeId_when_WriteStateAsync_throws()
    {
        // SetTreeIdAsync is particularly insidious: it has an idempotency
        // guard (`if (TreeId is not null) return;`). If a failing
        // WriteStateAsync leaves TreeId mutated in memory, every subsequent
        // call short-circuits the no-op branch and the grain is permanently
        // in a divergent state (in-memory TreeId set, persisted TreeId null)
        // until the activation eventually deactivates.
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);

        Assert.That(state.State.TreeId, Is.Null,
            "Test precondition: fresh grain has no TreeId.");

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.SetTreeIdAsync("my-tree"));

        Assert.That(state.State.TreeId, Is.Null,
            "TreeId mutated in memory survived a failing WriteStateAsync; "
            + "the idempotency guard now short-circuits every retry, "
            + "leaving the grain permanently divergent from storage.");
    }

    [Test]
    public void AcceptSplit_recovery_branch_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // The recovery branch of AcceptSplitAsync enters when the persisted
        // state shows SplitState=SplitInProgress (an earlier SplitAsync
        // persisted its intent but the activation died before the caller
        // observed the SplitResult). Recovery calls CompleteSplitAsync(),
        // which mutates two fields in memory BEFORE the WriteStateAsync at
        // BPlusInternalGrain.AcceptSplitAsync:146:
        //   * state.State.SplitState  : SplitInProgress -> SplitComplete
        //   * state.State.SplitRightChildren : populated -> null
        // If WriteStateAsync throws, the in-memory mutations leak. The
        // recovery branch is then guarded by `SplitState == SplitInProgress`
        // (line 143), so every retry from the same activation observes
        // SplitState=SplitComplete in memory and SKIPS the recovery branch
        // entirely. The caller never receives the prior split's pending
        // SplitResult (PromotedKey, NewSiblingId) - that structural
        // promotion is silently dropped, breaking the parent's chain of
        // split promotions until the activation is recycled.
        var state = new FakePersistentState<InternalNodeState>
        {
            State =
            {
                TreeId = "tree-1",
                ChildrenAreLeaves = true,
                Children =
                [
                    new ChildEntry { SeparatorKey = null, ChildId = Child0 },
                    new ChildEntry { SeparatorKey = "fox", ChildId = Child1 },
                ],
                SplitState = global::Orleans.Lattice.Primitives.SplitState.SplitInProgress,
                SplitKey = "monkey",
                SplitSiblingId = GrainId.Create("internal", "sibling-mid-split"),
                SplitRightChildren =
                [
                    new ChildEntry { SeparatorKey = null, ChildId = Child2 },
                    new ChildEntry { SeparatorKey = "rabbit", ChildId = Child3 },
                ],
            },
        };
        var grain = CreateGrain(state);

        var splitStateBefore = state.State.SplitState;
        var splitRightChildrenBefore = state.State.SplitRightChildren;

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        // Caller's promoted key < SplitKey ("monkey"), so the recovery
        // branch would fall through to insert in THIS node after the
        // recovery's WriteStateAsync. The throw happens at the recovery
        // write itself - we never reach the fall-through insert.
        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.AcceptSplitAsync("apple", Child3));

        Assert.That(state.State.SplitState, Is.EqualTo(splitStateBefore),
            "SplitState advanced from SplitInProgress to SplitComplete in "
            + "memory survived a failing WriteStateAsync; the recovery "
            + "branch guard (state.State.SplitState == SplitInProgress) "
            + "now short-circuits on every retry from this activation, "
            + "silently dropping the prior split's PromotedKey/NewSiblingId "
            + "from the caller's promotion chain.");
        Assert.That(state.State.SplitRightChildren, Is.EqualTo(splitRightChildrenBefore),
            "SplitRightChildren cleared in memory survived a failing "
            + "WriteStateAsync; subsequent CompleteSplitAsync resumptions "
            + "from this activation would dereference a null list.");
    }
}
