using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the internal-node changes behind issue #3523: the upward
/// digest publish is deferred until <c>_splitGate</c> is released, a publish
/// fault is contained on the entry points whose own mutation is already
/// durable, and a division the forwarded sibling makes during split recovery
/// is returned rather than discarded. Each of these used to lose a
/// <see cref="SplitResult"/>, leaving a node created, persisted, and routed to
/// by nothing.
/// </summary>
public partial class BPlusInternalGrainTests
{
    private static readonly ChildDigestSnapshot DeferredSnapshot = new()
    {
        Hash = Bytes16(0x5A), EntryCount = 3, CheckpointOffset = 7,
    };

    /// <summary>
    /// An initialised two-child node whose parent is <see cref="DigestParent"/>.
    /// The parent is assigned after initialisation so the initial seeding does
    /// not reach the stub.
    /// </summary>
    private static async Task<(BPlusInternalGrain Grain, FakePersistentState<InternalNodeState> State, IGrainFactory Factory, IBPlusInternalGrain Parent)>
        CreateParentedNodeAsync()
    {
        var (grain, state, factory) = CreateDigestGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        state.State.ParentId = DigestParent;

        var parent = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parent);
        return (grain, state, factory, parent);
    }

    private static GrainId InterruptSplit(FakePersistentState<InternalNodeState> state)
    {
        var siblingId = GrainId.Create("internal", Guid.NewGuid().ToString());
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "fox";
        state.State.SplitSiblingId = siblingId;
        state.State.SplitRightChildren = [new ChildEntry { SeparatorKey = null, ChildId = Child1 }];
        state.State.Children = [new ChildEntry { SeparatorKey = null, ChildId = Child0 }];
        return siblingId;
    }

    [Test]
    public async Task AcceptSplit_recovery_forward_returns_the_division_the_sibling_made_as_well()
    {
        var (grain, state, factory) = CreateDigestGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        var siblingId = InterruptSplit(state);

        var siblingDivision = new SplitResult
        {
            PromotedKey = "wolf",
            NewSiblingId = GrainId.Create("internal", "sibling-of-sibling"),
        };
        var sibling = Substitute.For<IBPlusInternalGrain>();
        sibling.AcceptSplitAsync("zebra", Child3).Returns(Task.FromResult<SplitResult?>(siblingDivision));
        factory.GetGrain<IBPlusInternalGrain>(siblingId).Returns(sibling);

        var result = await grain.AcceptSplitAsync("zebra", Child3);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result!.PromotedKey, Is.EqualTo("fox"), "the recovered division stays primary");
            Assert.That(result.NewSiblingId, Is.EqualTo(siblingId));
            Assert.That(result.Additional, Is.Not.Null.And.Length.EqualTo(1),
                "the sibling's own division is owed to the parent too");
            Assert.That(result.Additional!.Value[0].PromotedKey, Is.EqualTo("wolf"));
            Assert.That(result.Additional!.Value[0].NewSiblingId, Is.EqualTo(siblingDivision.NewSiblingId));
        });
    }

    [Test]
    public async Task AcceptSplit_recovery_forward_with_no_sibling_division_returns_only_the_recovered_division()
    {
        var (grain, state, factory) = CreateDigestGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        var siblingId = InterruptSplit(state);

        var sibling = Substitute.For<IBPlusInternalGrain>();
        sibling.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>()).Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusInternalGrain>(siblingId).Returns(sibling);

        var result = await grain.AcceptSplitAsync("zebra", Child3);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result!.PromotedKey, Is.EqualTo("fox"));
            Assert.That(result.Additional, Is.Null);
        });
    }

    [Test]
    public async Task OnChildDigestPublished_publishes_upward_only_after_releasing_the_split_gate()
    {
        var (grain, _, _, parent) = await CreateParentedNodeAsync();

        // The parent re-enters this node's gate from inside the publish, as a
        // parent mid-split does when it re-parents a moved child. Were the
        // publish still made under the gate, the two would wait on each other.
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(_ => grain.SetParentAsync(DigestParent));

        await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot).WaitAsync(TimeSpan.FromSeconds(10));

        await parent.Received(1).OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task OnChildDigestPublished_propagates_an_upward_publish_fault_to_its_caller()
    {
        var (grain, _, _, parent) = await CreateParentedNodeAsync();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));

        Assert.That(async () => await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot),
            Throws.InstanceOf<TimeoutException>());
    }

    [Test]
    public async Task A_faulted_upward_publish_stays_pending_and_the_next_entry_point_redrives_it()
    {
        var (grain, _, _, parent) = await CreateParentedNodeAsync();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));
        try { await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot with { PublishSequence = 5 }); }
        catch (TimeoutException) { }

        parent.ClearReceivedCalls();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);

        // An out-of-order (older-sequence) publish is dropped without marking
        // anything pending of its own, so the only publish it can make is the
        // one the fault left owed.
        await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot with { PublishSequence = 1 });

        await parent.Received(1).OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task A_successful_upward_publish_is_not_resent_by_the_next_entry_point()
    {
        var (grain, _, _, parent) = await CreateParentedNodeAsync();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);
        await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot with { PublishSequence = 5 });
        parent.ClearReceivedCalls();

        await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot with { PublishSequence = 1 });

        await parent.DidNotReceive().OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task AcceptSplit_returns_its_division_when_the_deferred_upward_publish_faults()
    {
        var (grain, state, factory, parent) = await CreateParentedNodeAsync();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));
        try { await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot); } catch (TimeoutException) { }

        var siblingId = InterruptSplit(state);
        factory.GetGrain<IBPlusInternalGrain>(siblingId).Returns(Substitute.For<IBPlusInternalGrain>());
        parent.ClearReceivedCalls();

        var result = await grain.AcceptSplitAsync("ant", Child2);

        Assert.That(result, Is.Not.Null, "a durable division must reach the caller even when the publish faults");
        Assert.That(result!.NewSiblingId, Is.EqualTo(siblingId));
        await parent.Received().OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task RemoveChild_reports_the_removal_when_the_deferred_upward_publish_faults()
    {
        var (grain, state, _, parent) = await CreateParentedNodeAsync();
        await grain.AcceptSplitAsync("hog", Child2);
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));
        try { await grain.OnChildDigestPublishedAsync(Child1, DeferredSnapshot); } catch (TimeoutException) { }

        var removed = await grain.RemoveChildAsync(Child1);

        Assert.That(removed, Is.True);
        Assert.That(state.State.Children.Select(c => c.ChildId), Does.Not.Contain(Child1));
    }

    [Test]
    public async Task Initialize_queues_behind_a_split_gate_held_by_an_in_flight_accept_split()
    {
        var (grain, state, factory) = CreateDigestGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        var siblingId = InterruptSplit(state);

        // The recovery forward awaits the sibling while holding the gate.
        var forward = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sibling = Substitute.For<IBPlusInternalGrain>();
        sibling.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>()).Returns(forward.Task);
        factory.GetGrain<IBPlusInternalGrain>(siblingId).Returns(sibling);

        var accept = grain.AcceptSplitAsync("zebra", Child3);
        await sibling.Received(1).AcceptSplitAsync("zebra", Child3);

        var initialize = grain.InitializeAsync("goat", Child2, Child3, childrenAreLeaves: true);
        await Task.Delay(50);
        Assert.That(initialize.IsCompleted, Is.False, "Initialize must queue behind the gated split turn");

        forward.TrySetResult(null);
        await Task.WhenAll(accept, initialize).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(state.State.Children.Select(c => c.ChildId), Is.EqualTo(new[] { Child2, Child3 }),
            "the queued seeding runs after, not interleaved with, the split turn");
    }

    [Test]
    public async Task Initialize_publishes_upward_after_seeding_when_a_publish_is_owed()
    {
        var (grain, _, _, parent) = await CreateParentedNodeAsync();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));
        try { await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot); } catch (TimeoutException) { }
        parent.ClearReceivedCalls();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);

        await grain.InitializeAsync("goat", Child2, Child3, childrenAreLeaves: true);

        await parent.Received(1).OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [TestCase(nameof(IBPlusInternalGrain.SetParentAsync))]
    [TestCase(nameof(IBPlusInternalGrain.OnChildDigestPublishedAsync))]
    [TestCase(nameof(IBPlusInternalGrain.GetChildDigestSnapshotAsync))]
    public void Digest_chain_method_is_always_interleave(string methodName)
    {
        var method = typeof(IBPlusInternalGrain).GetMethod(methodName);

        Assert.That(method, Is.Not.Null);
        Assert.That(method!.GetCustomAttributes(typeof(Orleans.Concurrency.AlwaysInterleaveAttribute), inherit: false),
            Is.Not.Empty,
            "a digest-chain call reaching a node mid-turn must interleave, or a child publishing to a parent "
            + "that is itself calling down into that child deadlocks");
    }
}
