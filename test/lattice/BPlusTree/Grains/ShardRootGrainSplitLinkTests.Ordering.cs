using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The order in which one split's divisions are linked (issue #3523). When a
/// parent divides on accepting a division, its own division must be linked
/// before the next division at the lower level re-descends: until it is, the
/// root still routes the parent's whole range to the donor half, so a later
/// separator above the parent's split key lands where no descent will reach it
/// once the parent's sibling is linked.
/// </summary>
public sealed partial class ShardRootGrainSplitLinkTests
{
    [Test]
    public async Task A_parent_division_is_linked_before_the_next_division_below_it_descends()
    {
        var h = CreateTwoLevelHarness();
        var firstSibling = GrainId.Create("leaf", "first-sibling");
        var secondSibling = GrainId.Create("leaf", "second-sibling");
        var leftParentSibling = GrainId.Create("internal", "left-parent-sibling");

        // One write reports two leaf divisions under the left parent: "d" and
        // a forwarded sibling's "k".
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "d",
            NewSiblingId = firstSibling,
            ChildIsLeaf = true,
            Additional = ImmutableArray.Create(LeafSplit("k", secondSibling)),
        }));

        // Accepting "d" divides the left parent at "f": everything from "f" up
        // to the root's "m" now lives under its new sibling.
        h.AddInternal(leftParentSibling, Table(true, (null, firstSibling)));
        h.Node(LeftParentId).AcceptSplitAsync("d", firstSibling).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "f",
            NewSiblingId = leftParentSibling,
            ChildIsLeaf = false,
        }));

        // The root routes to that sibling only once its division is linked.
        h.Node(RootId).AcceptSplitAsync("f", leftParentSibling).Returns(_ =>
        {
            h.Routing[RootId] = Table(false, (null, LeftParentId), ("f", leftParentSibling), ("m", RightParentId));
            return Task.FromResult<SplitResult?>(null);
        });

        await h.Grain.SetAsync("c", [1]);

        Received.InOrder(() =>
        {
            h.Node(LeftParentId).AcceptSplitAsync("d", firstSibling);
            h.Node(RootId).AcceptSplitAsync("f", leftParentSibling);
            h.Node(leftParentSibling).AcceptSplitAsync("k", secondSibling);
        });
        await h.Node(LeftParentId).DidNotReceive().AcceptSplitAsync("k", Arg.Any<GrainId>());
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }
}
