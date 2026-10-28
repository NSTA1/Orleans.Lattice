using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task BulkLoad_initializes_each_internal_node_and_its_tree_binding_in_one_call(bool raw)
    {
        var h = CreateHarness(maxLeafKeys: 2);

        if (raw)
            await h.Grain.BulkLoadRawAsync("bound-nodes", RawEntries(7));
        else
            await h.Grain.BulkLoadAsync("bound-nodes", Pairs(7));

        Assert.That(h.Internals, Is.Not.Empty);
        foreach (var node in h.Internals.Values)
        {
            await node.Received(1).InitializeWithChildrenForTreeAsync(
                TreeId,
                Arg.Is<List<string?>>(keys => keys.Count == 4 && keys[0] == null),
                Arg.Is<List<GrainId>>(ids => ids.Count == 4),
                true);
            await node.DidNotReceive().SetTreeIdAsync(Arg.Any<string>());
            await node.DidNotReceive().InitializeWithChildrenAsync(
                Arg.Any<List<string?>>(), Arg.Any<List<GrainId>>(), Arg.Any<bool>());
        }
        Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("bound-nodes"));
    }
}
