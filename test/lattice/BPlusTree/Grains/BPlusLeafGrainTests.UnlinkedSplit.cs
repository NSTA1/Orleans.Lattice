using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression cover for issue #4795: a completed leaf division whose
/// <see cref="SplitResult"/> never reaches a durable shard-root link intent
/// (the root faults after the leaf returns) must be handed back to the root,
/// not left as a populated, chained sibling that no parent routes to.
/// </summary>
/// <remarks>
/// Driven through the public <c>SetAsync</c> entry point: the defect was that
/// nothing re-surfaced the division, not that linking failed when invoked.
/// </remarks>
public partial class BPlusLeafGrainTests
{
    private static async Task<(BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, SplitResult Split)>
        CompleteADivisionAsync()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingMock = Substitute.For<IBPlusLeafGrain>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        grainFactory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);
        var grain = BuildGrain(context, state, grainFactory);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        // The durable state of a division interrupted before its migration;
        // the next write completes it and returns the SplitResult.
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.TreeId = "test-tree";

        var split = await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));
        Assert.That(split, Is.Not.Null, "Completing the interrupted division must return its SplitResult.");
        return (grain, state, split!);
    }

    [Test]
    public async Task Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling()
    {
        var (grain, state, lost) = await CompleteADivisionAsync();
        // The shard root faults here: `lost` is dropped before any link intent is recorded.

        Assert.That(state.State.SplitInFlight, Is.False);
        Assert.That(state.State.UnlinkedSplitSiblingId, Is.EqualTo(lost.NewSiblingId),
            "The division is complete but unacknowledged, so the obligation to link it must be durable.");

        var again = await grain.SetAsync("c", Encoding.UTF8.GetBytes("4"));

        Assert.That(again, Is.Not.Null,
            "Before the fix the donor reported no interrupted split and the sibling stayed unlinked for ever.");
        Assert.That(again!.NewSiblingId, Is.EqualTo(lost.NewSiblingId));
        Assert.That(again.PromotedKey, Is.EqualTo(lost.PromotedKey));
        Assert.That(again.ChildIsLeaf, Is.True);
    }

    [Test]
    public async Task Acknowledging_the_recorded_link_retires_the_marker_so_later_writes_do_not_resurface()
    {
        var (grain, state, split) = await CompleteADivisionAsync();
        await grain.AcknowledgeSplitLinkRecordedAsync(split.NewSiblingId);

        Assert.That(state.State.UnlinkedSplitSiblingId, Is.Null);
        Assert.That(state.State.UnlinkedSplitKey, Is.Null);

        var next = await grain.SetAsync("c", Encoding.UTF8.GetBytes("4"));
        Assert.That(next, Is.Null);
    }

    [Test]
    public async Task Acknowledging_a_different_sibling_keeps_the_marker()
    {
        var (grain, state, split) = await CompleteADivisionAsync();
        await grain.AcknowledgeSplitLinkRecordedAsync(GrainId.Create("leaf", Guid.NewGuid().ToString()));

        Assert.That(state.State.UnlinkedSplitSiblingId, Is.EqualTo(split.NewSiblingId));
    }

    [Test]
    public async Task Completed_split_result_names_its_donor_for_the_acknowledgement()
    {
        var (_, _, split) = await CompleteADivisionAsync();

        Assert.That(split.Donor, Is.EqualTo(GrainId.Create("leaf", "test-leaf")));
    }
}
