using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #6166: a division whose completing write landed in storage but was reported as an etag
/// conflict that the landed-write check could not adopt. The node retires, so its caller
/// never receives the division, and the durable row says it is complete, so no recovery
/// replays it. A redelivery of the accept that caused it must hand the division back;
/// otherwise the new sibling - holding the node's whole right half - is never linked, and
/// every key under it is unreachable.
/// </summary>
public partial class BPlusInternalGrainTests
{
    [TestCase("k999", true)]
    [TestCase("k010x", false)]
    public async Task A_redelivered_accept_returns_the_division_whose_completing_write_landed_but_was_reported_lost(string promotedKey, bool movesToSibling)
    {
        var (grain, state, factory) = CreateDigestGrain();
        var rightHalf = Substitute.For<IBPlusInternalGrain, IGrainBase>();
        var rightContext = Substitute.For<IGrainContext>();
        rightContext.GrainId.Returns(GrainId.Create("internal", "landed-right-half"));
        ((IGrainBase)rightHalf).GrainContext.Returns(rightContext);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(rightHalf);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>(), Arg.Any<string?>()).Returns(rightHalf);
        await grain.InitializeAsync("k000", Child0, Child1, childrenAreLeaves: true);
        state.State.TreeId = "landed-tree";
        const int max = 128; // TestOptionsResolver's default
        for (var i = state.State.Children.Count; i <= max; i++)
        {
            state.State.Children.Add(new ChildEntry { SeparatorKey = $"k{i:D3}", ChildId = GrainId.Create("leaf", $"landed-{i}") });
        }

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var copier = services.GetRequiredService<DeepCopier>();
        InternalNodeState? durable = null;
        var writes = 0;
        state.OnWriteState = written =>
        {
            durable = copier.Copy(written);

            // The split's intent persists; the write that completes it lands, then reports a
            // conflict the node cannot adopt.
            if (++writes == 2)
            {
                throw new InconsistentStateException("landed, then reported as an etag conflict", "stored", "current");
            }
        };
        var trigger = GrainId.Create("leaf", "landed-new");

        Assert.ThrowsAsync<InconsistentStateException>(async () => await grain.AcceptSplitAsync(promotedKey, trigger));
        Assert.Multiple(() =>
        {
            Assert.That(durable!.SplitState, Is.EqualTo(Orleans.Lattice.Primitives.SplitState.SplitComplete), "precondition: the completing write landed");
            Assert.That(durable.SplitSiblingId, Is.Not.Null, "precondition: the node divided");
            Assert.That(durable.Children.Any(c => c.ChildId == trigger), Is.EqualTo(!movesToSibling), "precondition: the new child's half");
        });

        // A fresh activation reloads the landed row, and the caller re-delivers the accept.
        var (fresh, freshState, freshFactory) = CreateDigestGrain(new FakePersistentState<InternalNodeState> { State = durable! });
        var sibling = Substitute.For<IBPlusInternalGrain>();
        freshFactory.GetGrain<IBPlusInternalGrain>(durable!.SplitSiblingId!.Value).Returns(sibling);
        var result = await fresh.AcceptSplitAsync(promotedKey, trigger);

        Assert.Multiple(() =>
        {
            Assert.That(result?.NewSiblingId, Is.EqualTo(durable.SplitSiblingId), "the division the lost write completed was not handed back, so its sibling is never linked");
            Assert.That(result?.PromotedKey, Is.EqualTo(durable.SplitKey));
            Assert.That(freshState.State.Children.Count(c => c.ChildId == trigger), Is.EqualTo(movesToSibling ? 0 : 1), "the re-delivered child was inserted again, or into the wrong half");
        });
        await sibling.Received(movesToSibling ? 1 : 0).AcceptSplitAsync(promotedKey, trigger);
    }
}
