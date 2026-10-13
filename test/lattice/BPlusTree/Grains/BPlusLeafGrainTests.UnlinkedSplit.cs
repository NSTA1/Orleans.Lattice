using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Reflection;
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
        CompleteADivisionAsync(IGrainFactory? factory = null)
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = factory ?? Substitute.For<IGrainFactory>();
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
    public async Task Acknowledging_a_recorded_link_does_not_wait_for_an_in_flight_split()
    {
        var (grain, state, split) = await CompleteADivisionAsync();
        var gateField = typeof(BPlusLeafGrain).GetField("_splitGate", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(gateField, Is.Not.Null, "_splitGate field not found - was it renamed?");
        var gate = (SemaphoreSlim)gateField!.GetValue(grain)!;
        Assert.That(gate.Wait(0), Is.True, "gate should start free");

        Task acknowledgement;
        bool completedWhileGateHeld;
        try
        {
            acknowledgement = grain.AcknowledgeSplitLinkRecordedAsync(split.NewSiblingId);
            completedWhileGateHeld = await Task.WhenAny(acknowledgement, Task.Delay(TimeSpan.FromSeconds(1)))
                == acknowledgement;
        }
        finally
        {
            gate.Release();
        }

        await acknowledgement;
        Assert.That(completedWhileGateHeld, Is.True,
            "A capture recheck nested in a split must not wait for the split gate it is nested under.");
        Assert.That(state.State.UnlinkedSplitSiblingId, Is.EqualTo(split.NewSiblingId),
            "A contended acknowledgement must leave the durable marker for a later retry.");

        await grain.AcknowledgeSplitLinkRecordedAsync(split.NewSiblingId);
        Assert.That(state.State.UnlinkedSplitSiblingId, Is.Null);
    }

    [Test]
    public async Task Completed_split_result_names_its_donor_for_the_acknowledgement()
    {
        var (_, _, split) = await CompleteADivisionAsync();

        Assert.That(split.Donor, Is.EqualTo(GrainId.Create("leaf", "test-leaf")));
    }

    /// <summary>
    /// Issue #4918. A capture nested inside a gated division (the checkpoint-hint
    /// loop at the end of <c>SplitAsync</c>) used to drain the durable unlinked
    /// marker by calling the shard root while the outer division still held this
    /// leaf's <c>_splitGate</c>. When that link overflows the parent, the parent's
    /// new sibling re-parents this leaf through <c>SetParentAsync</c>, which is
    /// admitted (<c>[AlwaysInterleave]</c>, #4914) but waits on the held gate: the
    /// root waits on the leaf, the leaf's gate holder waits on the root. The stub
    /// root reproduces that callback, so the pre-fix behaviour hangs here.
    /// </summary>
    [Test]
    public async Task Nested_capture_under_a_held_split_gate_defers_the_unlinked_link_instead_of_deadlocking()
    {
        var reparentedTo = GrainId.Create("internal", "parent-sibling");
        var factory = Substitute.For<IGrainFactory>();
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shardRoot);
        BPlusLeafGrain? donor = null;
        var links = new List<SplitResult>();
        shardRoot.LinkLeafSplitFromCaptureAsync(Arg.Any<SplitResult>())
            .Returns(async ci =>
            {
                links.Add(ci.Arg<SplitResult>());

                // The link overflowed the parent; its split re-parents the moved
                // children, this donor among them, before the root call returns.
                await donor!.SetParentAsync(reparentedTo);
            });

        var (grain, state, split) = await CompleteADivisionAsync(factory);
        donor = grain;
        Assert.That(state.State.UnlinkedSplitSiblingId, Is.EqualTo(split.NewSiblingId));

        var gateField = typeof(BPlusLeafGrain).GetField("_splitGate", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(gateField, Is.Not.Null, "_splitGate field not found - was it renamed?");
        var gate = (SemaphoreSlim)gateField!.GetValue(grain)!;
        Assert.That(gate.Wait(0), Is.True, "gate should start free");

        Task<bool> nestedCapture;
        bool completedWhileGateHeld;
        try
        {
            // The outer division holds the gate, suspended in its hint loop,
            // while the nested capture re-runs the byte-overflow seam.
            nestedCapture = InvokeCaptureByteOverflowSeam(grain);
            completedWhileGateHeld = await Task.WhenAny(nestedCapture, Task.Delay(TimeSpan.FromSeconds(1)))
                == nestedCapture;
        }
        finally
        {
            gate.Release();
        }

        if (!completedWhileGateHeld)
        {
            // Pre-fix: releasing the gate lets the parked SetParentAsync through.
            await nestedCapture;
        }

        Assert.Multiple(() =>
        {
            Assert.That(completedWhileGateHeld, Is.True,
                "THE ASSERTION. A capture nested under a held split gate must not call the root, whose "
                + "re-parenting callback waits on that same gate.");
            Assert.That(links, Is.Empty, "the nested capture must defer the link, not attempt it");
            Assert.That(state.State.UnlinkedSplitSiblingId, Is.EqualTo(split.NewSiblingId),
                "a deferred link must leave the durable marker for a later turn");
        });

        // Once the gate is free the next capture links it: deferral loses nothing.
        await InvokeCaptureByteOverflowSeam(grain);

        Assert.Multiple(() =>
        {
            Assert.That(links, Has.Count.EqualTo(1));
            Assert.That(links[0].NewSiblingId, Is.EqualTo(split.NewSiblingId));
            Assert.That(links[0].PromotedKey, Is.EqualTo(split.PromotedKey));
            Assert.That(state.State.ParentId, Is.EqualTo(reparentedTo));
            Assert.That(state.State.UnlinkedSplitSiblingId, Is.Null,
                "the root's acknowledgement retires the marker once the link lands");
        });
    }

    // The capture seam that drains the unlinked marker; maxLeafBytes 0 disables
    // the byte-overflow division so only the drain runs.
    private static Task<bool> InvokeCaptureByteOverflowSeam(BPlusLeafGrain grain)
    {
        var method = typeof(BPlusLeafGrain).GetMethod(
            "TrySplitForByteOverflowAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(method, Is.Not.Null, "TrySplitForByteOverflowAsync not found - was it renamed?");
        return (Task<bool>)method!.Invoke(grain, [128, 0L])!;
    }
}
