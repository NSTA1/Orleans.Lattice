using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusInternalGrainTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task GetSubtreeDigestAndRoutingAsync_preserves_digest_and_defensive_child_order(bool childrenAreLeaves)
    {
        var (grain, state, _) = CreateDigestGrain();
        state.State.Children = [
            new ChildEntry { ChildId = DigestChild0 },
            new ChildEntry { SeparatorKey = "m", ChildId = DigestChild1 },
        ];
        state.State.ChildrenAreLeaves = childrenAreLeaves;
        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0xAB), EntryCount = 7, CheckpointOffset = 42,
        });
        var expected = await grain.GetSubtreeProjectionDigestAsync();
        var writes = state.WriteCount;

        var (digest, routing) = await grain.GetSubtreeDigestAndRoutingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(digest.Hash, Is.EqualTo(expected.Hash));
            Assert.That(digest.EntryCount, Is.EqualTo(expected.EntryCount));
            Assert.That(digest.CheckpointOffset, Is.EqualTo(expected.CheckpointOffset));
            Assert.That(routing.ChildIds, Is.EqualTo(new[] { DigestChild0, DigestChild1 }));
            Assert.That(routing.SeparatorKeys, Is.EqualTo(new string?[] { null, "m" }));
            Assert.That(routing.ChildrenAreLeaves, Is.EqualTo(childrenAreLeaves));
            Assert.That(state.WriteCount, Is.EqualTo(writes), "Merkle observations must remain read-only");
        });
        routing.SeparatorKeys[1] = "changed";
        routing.ChildIds[0] = DigestParent;
        var again = await grain.GetSubtreeDigestAndRoutingAsync();
        Assert.That(again.Routing.SeparatorKeys[1], Is.EqualTo("m"));
        Assert.That(again.Routing.ChildIds[0], Is.EqualTo(DigestChild0));
    }

    [Test]
    public void GetSubtreeDigestAndRoutingAsync_retains_the_disabled_digest_gate()
    {
        var (grain, _, _) = CreateDigestGrain(new LatticeOptions { MaintainProjectionDigest = false });
        Assert.That(async () => await grain.GetSubtreeDigestAndRoutingAsync(), Throws.InvalidOperationException);
    }
}
