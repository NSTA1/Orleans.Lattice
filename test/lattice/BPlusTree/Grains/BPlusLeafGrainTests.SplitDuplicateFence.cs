using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The split's half of #5841's rule A in the leaf itself: the donor's narrowed state is
/// durable before its split fence reads the leaf's epoch row, so either the fence sees a
/// duplicate's claim, or the duplicate's state check sees the narrowing. It is the code
/// half of <c>HandOffBoundSplitReadsRowBeforePersist</c> in eager mode; under pre-arm the
/// shard root fences at link time, which follows the donor's persist by construction
/// (<see cref="ShardRootGrainSplitLinkTests"/>).
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task SplitReadsRowBeforePersist()
    {
        var rig = new LiveAdvanceRig();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(
            state,
            liveAdvance: rig,
            configureGrainFactory: f => f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard));
        await SetStampedAsync(grain, "a", "v");
        Assert.That(grain.AppendFenceForTesting!.HasClaimedAnywhere, Is.True, "precondition: the donor claimed, so its row exists");

        // A split resumed from its persisted intent, its sibling's marker opened at birth.
        var sibling = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var clock = HybridLogicalClock.Zero;
        for (var i = 0; i < 127; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            grain.EntriesForTest[$"key-{i:D4}"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes($"v-{i}"), clock);
        }

        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = "key-0064";
        state.State.SplitSiblingId = sibling;
        state.State.NextSibling = sibling;
        await HandOffMarkerRows.UpdateLeafAsync(rig.Storage, sibling.GetGuidKey(), m =>
        {
            m.Initialised = true;
            return HandOffMarkerCore.Open(m, BPlusLeafGrain.SplitHandOffToken(rig.LeafId), null, null);
        });

        var events = new List<string>();
        state.OnWriteState = written =>
        {
            if (written.UnlinkedSplitSiblingId == sibling && written.HighKeyExclusive == "key-0064")
            {
                events.Add("narrow");
            }
        };
        rig.OnStorageRead = stateName =>
        {
            if (stateName == WalAppendFenceRows.LeafEpochGrainType)
            {
                events.Add("row");
            }
        };

        clock = HybridLogicalClock.Tick(clock);
        var result = await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["new-key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), clock),
        });

        var marker = (await HandOffMarkerRows.ReadLeafAsync(rig.Storage, sibling.GetGuidKey())).State;
        var narrowed = events.IndexOf("narrow");
        Assert.Multiple(async () =>
        {
            Assert.That(result?.NewSiblingId, Is.EqualTo(sibling), "precondition: the split completed");
            Assert.That(narrowed, Is.GreaterThanOrEqualTo(0), "precondition: the narrowing was persisted");
            Assert.That(events.Skip(narrowed + 1), Does.Contain("row"), "the split fence did not read the epoch row after the narrowing was durable");
            Assert.That(events.Take(narrowed + 1).Count(e => e == "row"), Is.Zero,
                "the split fence read the epoch row before the narrowing was durable");
            Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(rig.Storage, rig.LeafId)).State.Epoch, Is.EqualTo(2), "the split did not bump the row");
            Assert.That(marker.Open, Is.Empty, "the sibling's marker was not closed");
        });
    }
}
