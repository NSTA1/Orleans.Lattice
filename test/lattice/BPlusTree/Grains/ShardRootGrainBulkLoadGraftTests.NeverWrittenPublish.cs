using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5812, member B, review Q2: a never-written shard's bulk load skips its switch,
/// and decides that at publish. The check that the shard still has no root and the
/// publish are one synchronous segment under the root-seed gate, so a batch write that
/// was already seeding a root when the load reached its publish is not overwritten -
/// before this, the seed's assignment landed after the publish and replaced the loaded
/// root, losing the whole load - and the load falls back to the switched path, which
/// replaces the seeded root and relocates its rows.
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    [Test]
    public async Task A_never_written_load_does_not_lose_its_root_to_a_batch_write_seeding_one_concurrently()
    {
        var h = CreateHarness();
        var bindingReleased = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        IBPlusLeafGrain? seeded = null;
        h.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var leaf = ResolveLeaf(h.Leaves, call.ArgAt<Guid>(0));
            leaf.SetBindingAsync(Arg.Any<string>(), Arg.Any<int>()).Returns(_ =>
            {
                seeded = leaf;
                return bindingReleased.Task;
            });
            return leaf;
        });

        var batch = h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k9999"] = LwwValue<byte[]>.Create([9], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
        });
        Assert.That(seeded, Is.Not.Null, "precondition: the batch write is seeding the shard's root");

        var load = h.Grain.BulkLoadRawAsync("op-1", RawEntries(5));
        bindingReleased.SetResult();
        await batch;
        await load;

        var seededId = ((IGrainBase)seeded!).GrainContext.GrainId;
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("op-1"));
            Assert.That(h.State.State.RootNodeId, Is.Not.EqualTo(seededId),
                "the loaded root is published and kept: the concurrent seed did not overwrite it");
            Assert.That(seeded.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.ClearGrainStateAsync)),
                Is.True, "the seeded root was replaced, its rows relocated, and it was cleared");
        });
    }
}
