using System.Collections.Immutable;
using System.Reflection;
using NSubstitute;
using Orleans.Concurrency;
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

    // --- Issue #4795: the donor's unlinked-split marker is retired only after the link intent is durable ---

    [Test]
    public void Capture_split_link_is_marked_always_interleave()
    {
        var method = typeof(IShardRootGrain).GetMethod(nameof(IShardRootGrain.LinkLeafSplitFromCaptureAsync));

        Assert.That(method, Is.Not.Null);
        Assert.That(method!.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false), Is.Not.Null,
            "Capture can hold a leaf's split gate while an interleaved root write waits on that leaf. " +
            "The root must admit the capture link so the write and capture cannot deadlock.");
    }

    [Test]
    public async Task A_recorded_leaf_split_acknowledges_the_link_to_its_donor()
    {
        var h = CreateTwoLevelHarness();
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(
            LeafSplit("d") with { Donor = LeftLeafId }));

        await h.Grain.SetAsync("c", [1]);

        await h.Leaf(LeftLeafId).Received(1).AcknowledgeSplitLinkRecordedAsync(SiblingId);
    }

    [Test]
    public async Task A_capture_link_can_enter_while_another_split_acknowledges_its_donor()
    {
        var h = CreateTwoLevelHarness();
        var captureSibling = GrainId.Create("leaf", "capture-sibling");
        Task? captureLink = null;
        var captureCompletedBeforeAcknowledgementReturned = false;

        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(
            LeafSplit("d") with { Donor = LeftLeafId }));
        h.Leaf(LeftLeafId).AcknowledgeSplitLinkRecordedAsync(Arg.Any<GrainId>())
            .Returns(async _ =>
            {
                captureLink = h.Grain.LinkLeafSplitFromCaptureAsync(
                    LeafSplit("e", captureSibling) with { Donor = LeftLeafId });
                captureCompletedBeforeAcknowledgementReturned =
                    await Task.WhenAny(captureLink, Task.Delay(TimeSpan.FromSeconds(1))) == captureLink;
            });

        await h.Grain.SetAsync("c", [1]);
        await captureLink!;

        Assert.That(captureCompletedBeforeAcknowledgementReturned, Is.True,
            "The root must release its split-link gate before awaiting a donor acknowledgement, " +
            "so a capture link from that donor can make progress.");
    }

    [Test]
    public async Task A_capture_link_during_parent_seeding_is_deferred_without_waiting_for_the_root_gate()
    {
        var h = CreateTwoLevelHarness();
        var captureSibling = GrainId.Create("leaf", "capture-sibling");
        Task? captureLink = null;
        var captureReturnedWhileParentWaited = false;
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(
            LeafSplit("d") with { Donor = LeftLeafId }));
        h.Node(LeftParentId).AcceptSplitAsync("d", SiblingId).Returns(async _ =>
        {
            captureLink = h.Grain.LinkLeafSplitFromCaptureAsync(
                LeafSplit("e", captureSibling) with { Donor = LeftLeafId });
            captureReturnedWhileParentWaited =
                await Task.WhenAny(captureLink, Task.Delay(TimeSpan.FromSeconds(1))) == captureLink;
            return (SplitResult?)null;
        });

        await h.Grain.SetAsync("c", [1]);
        await captureLink!;

        Assert.That(captureReturnedWhileParentWaited, Is.True,
            "A parent seeding the donor must not wait for capture to acquire the root's held split-link gate.");
        await h.Node(LeftParentId).Received(1).AcceptSplitAsync("e", captureSibling);
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task A_capture_split_does_not_call_back_to_acknowledge_its_waiting_donor()
    {
        var h = CreateTwoLevelHarness();

        await h.Grain.LinkLeafSplitFromCaptureAsync(
            LeafSplit("d") with { Donor = LeftLeafId });

        await h.Leaf(LeftLeafId).DidNotReceive().AcknowledgeSplitLinkRecordedAsync(Arg.Any<GrainId>());
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task A_capture_split_during_root_promotion_is_deferred_until_the_root_is_installed()
    {
        var h = CreateTwoLevelHarness();
        h.State.State.RootNodeId = LeftLeafId;
        h.State.State.RootIsLeaf = true;
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(
            LeafSplit("d") with { Donor = LeftLeafId }));

        var captureSibling = GrainId.Create("leaf", "capture-sibling");
        Task? captureLink = null;
        var captureReturnedDuringPromotion = false;
        h.NewRoot.InitializeAsync(Arg.Any<string>(), Arg.Any<GrainId>(), Arg.Any<GrainId>(), Arg.Any<bool>())
            .Returns(async _ =>
            {
                h.Routing[NewRootId] = Table(true, (null, LeftLeafId), ("d", SiblingId));
                h.Internals[NewRootId] = h.NewRoot;
                captureLink = h.Grain.LinkLeafSplitFromCaptureAsync(
                    LeafSplit("e", captureSibling) with { Donor = LeftLeafId });
                captureReturnedDuringPromotion =
                    await Task.WhenAny(captureLink, Task.Delay(TimeSpan.FromSeconds(1))) == captureLink;
            });

        await h.Grain.SetAsync("c", [1]);
        await captureLink!;

        Assert.That(captureReturnedDuringPromotion, Is.True,
            "Capture on the old root leaf must return after persisting its link intent; " +
            "root initialization waits to seed that same leaf's parent.");
        await h.NewRoot.Received(1).AcceptSplitAsync("e", captureSibling);
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task A_failed_acknowledgement_does_not_fail_the_write()
    {
        var h = CreateTwoLevelHarness();
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(
            LeafSplit("d") with { Donor = LeftLeafId }));
        h.Leaf(LeftLeafId).AcknowledgeSplitLinkRecordedAsync(Arg.Any<GrainId>())
            .Returns(Task.FromException(new InvalidOperationException("donor unavailable")));

        await h.Grain.SetAsync("c", [1]);

        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }
}
