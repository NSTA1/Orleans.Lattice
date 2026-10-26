using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The internal-node division seeds its new sibling with the whole right half in
/// one call instead of an initialise plus one accept-split call per remaining child.
/// </summary>
public partial class BPlusInternalGrainTests
{
    [Test]
    public async Task Split_completion_seeds_the_sibling_with_its_whole_right_half_in_one_call()
    {
        var (grain, state, factory) = CreateDigestGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var siblingId = GrainId.Create("internal", Guid.NewGuid().ToString());
        var right = new List<ChildEntry>
        {
            new() { SeparatorKey = null, ChildId = Child1 },
            new() { SeparatorKey = "goat", ChildId = Child2 },
            new() { SeparatorKey = "hog", ChildId = Child3 },
            new() { SeparatorKey = "ibex", ChildId = GrainId.Create("leaf", "child-4") },
        };
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "fox";
        state.State.SplitSiblingId = siblingId;
        state.State.SplitRightChildren = right;
        state.State.Children = [new ChildEntry { SeparatorKey = null, ChildId = Child0 }];

        var sibling = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(siblingId).Returns(sibling);

        var result = await grain.AcceptSplitAsync("ant", Child2);

        Assert.That(result, Is.Not.Null);
        await sibling.Received(1).InitializeWithChildrenAsync(
            Arg.Is<List<string?>>(s => s.SequenceEqual(new string?[] { null, "goat", "hog", "ibex" })),
            Arg.Is<List<GrainId>>(i => i.SequenceEqual(right.Select(c => c.ChildId))),
            true);
        await sibling.DidNotReceive().InitializeAsync(
            Arg.Any<string>(), Arg.Any<GrainId>(), Arg.Any<GrainId>(), Arg.Any<bool>());
        await sibling.DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
    }
}
