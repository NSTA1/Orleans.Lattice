using System.Reflection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    [Test]
    public async Task BulkAppend_bears_its_first_leaf_alone_and_the_rest_concurrently()
    {
        const string operationId = "concurrent-births";
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        h.Leaf(existing).GetAppendTailAsync().Returns((2, "a"));

        // The three leaves the append mints, registered up front so their births can be held.
        var deterministicGuid = typeof(ShardRootGrain).GetMethod("DeterministicGuid", BindingFlags.NonPublic | BindingFlags.Static)!;
        var births = new TaskCompletionSource[3];
        var leaves = new IBPlusLeafGrain[3];
        for (var i = 0; i < 3; i++)
        {
            var key = (Guid)deterministicGuid.Invoke(null, [$"{TreeId}/0/append/{operationId}/leaf/{i}"])!;
            births[i] = new TaskCompletionSource();
            leaves[i] = MakeLeaf(key);
            leaves[i].InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(births[i].Task);
            h.Leaves[key] = leaves[i];
        }

        var append = h.Grain.BulkAppendAsync(operationId, Pairs(6));
        await WaitForInitAsync(leaves[0]);
        Assert.Multiple(() =>
        {
            Assert.That(InitCalls(leaves[1]) + InitCalls(leaves[2]), Is.Zero,
                "no leaf is born before the first, whose low bound a retry reads to keep the layout");
        });

        births[0].SetResult();
        await WaitForInitAsync(leaves[1]);
        await WaitForInitAsync(leaves[2]);
        Assert.That(append.IsCompleted, Is.False, "precondition: the later births are still held");

        births[1].SetResult();
        births[2].SetResult();
        await append;

        foreach (var leaf in leaves)
        {
            await leaf.Received(1).InitializeSiblingAsync(Arg.Any<SiblingInitialization>());
            await leaf.Received(1).MergeBulkSeedAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<WalRoutingStamp>());
        }

        Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo(operationId));

        static int InitCalls(IBPlusLeafGrain leaf) => leaf.ReceivedCalls().Count(c =>
            c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync));

        static async Task WaitForInitAsync(IBPlusLeafGrain leaf)
        {
            for (var i = 0; i < 500 && InitCalls(leaf) == 0; i++)
            {
                await Task.Delay(10);
            }

            Assert.That(InitCalls(leaf), Is.EqualTo(1), "the leaf's birth began while the others were held");
        }
    }
}
