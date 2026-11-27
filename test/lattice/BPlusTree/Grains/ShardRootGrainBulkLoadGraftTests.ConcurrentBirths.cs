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

        // Every leaf the append mints holds its birth on the next gate, in the order births begin.
        var births = Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource()).ToArray();
        var begun = 0;
        h.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var key = call.ArgAt<Guid>(0);
            if (!h.Leaves.TryGetValue(key, out var leaf))
            {
                leaf = MakeLeaf(key);
                leaf.InitializeSiblingAsync(Arg.Any<SiblingInitialization>())
                    .Returns(_ => births[Interlocked.Increment(ref begun) - 1].Task);
                h.Leaves[key] = leaf;
            }

            return leaf;
        });

        var append = h.Grain.BulkAppendAsync(operationId, Pairs(6));
        await WaitForBirthsAsync(1);
        await Task.Delay(100);
        Assert.That(Volatile.Read(ref begun), Is.EqualTo(1),
            "no leaf is born before the first, whose low bound a retry reads to keep the layout");

        births[0].SetResult();
        await WaitForBirthsAsync(3);
        Assert.That(append.IsCompleted, Is.False, "precondition: the later births are still held");

        births[1].SetResult();
        births[2].SetResult();
        await append;

        var newLeaves = h.Leaves.Values.Where(l => l.ReceivedCalls().Any(c =>
            c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))).ToArray();
        Assert.That(newLeaves, Has.Length.EqualTo(3));
        foreach (var leaf in newLeaves)
        {
            await leaf.Received(1).InitializeSiblingAsync(Arg.Any<SiblingInitialization>());
            await leaf.Received(1).MergeBulkSeedAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<WalRoutingStamp>());
        }

        Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo(operationId));

        async Task WaitForBirthsAsync(int count)
        {
            for (var i = 0; i < 500 && Volatile.Read(ref begun) < count; i++)
            {
                await Task.Delay(10);
            }

            Assert.That(Volatile.Read(ref begun), Is.EqualTo(count), "these births began while the others were held");
        }
    }
}
