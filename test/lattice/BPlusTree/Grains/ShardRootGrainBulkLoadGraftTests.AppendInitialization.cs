using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    [TestCase(0, 3)]
    [TestCase(1, 2)]
    [TestCase(2, 1)]
    public async Task BulkAppend_reads_only_count_and_fills_remaining_capacity(int existingCount, int expectedTake)
    {
        var h = CreateHarness(maxLeafKeys: 3);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var leaf = h.Leaf(existing);
        leaf.CountAsync().Returns(existingCount);

        await h.Grain.BulkAppendAsync("count-only", Pairs(3));

        await leaf.Received(1).CountAsync();
        await leaf.DidNotReceive().GetKeysAsync();
        await leaf.Received(1).MergeEntriesAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(rows => rows.Count == expectedTake));
    }

    [Test]
    public async Task BulkAppend_batches_birth_metadata_and_preplans_the_unpublished_chain()
    {
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        h.Leaf(existing).CountAsync().Returns(2);

        await h.Grain.BulkAppendAsync("append-batch", Pairs(5));

        var newLeaves = h.Leaves.Values.Where(l => l.ReceivedCalls().Any(c =>
            c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))).ToArray();
        Assert.That(newLeaves, Has.Length.EqualTo(3));
        var inits = newLeaves.Select(l => (SiblingInitialization)l.ReceivedCalls()
            .Single(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))
            .GetArguments()[0]!).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(inits.All(i => i.TreeId == TreeId && i.ShardIndex == 0), Is.True);
            Assert.That(inits.Count(i => i.PrevSibling is null), Is.EqualTo(1));
            Assert.That(inits.Count(i => i.NextSibling is null), Is.EqualTo(1));
            Assert.That(inits.Select(i => i.NextSibling).Where(i => i is not null),
                Is.EquivalentTo(newLeaves.Skip(1).Select(l => (GrainId?)l.GetGrainId())));
            Assert.That(inits.Select(i => i.PrevSibling).Where(i => i is not null),
                Is.EquivalentTo(newLeaves.Take(2).Select(l => (GrainId?)l.GetGrainId())));
        });
        foreach (var leaf in newLeaves)
        {
            await leaf.DidNotReceive().SetTreeIdAsync(Arg.Any<string>());
            await leaf.DidNotReceive().SetShardIndexAsync(Arg.Any<int>());
            await leaf.DidNotReceive().SetNextSiblingAsync(Arg.Any<GrainId>());
            Received.InOrder(() =>
            {
                leaf.InitializeSiblingAsync(Arg.Any<SiblingInitialization>());
                leaf.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
            });
        }
        Assert.That(newLeaves.Sum(l => l.ReceivedCalls().Count(c =>
            c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.SetPrevSiblingAsync))), Is.EqualTo(1),
            "only the first leaf's graft attachment still needs a separate link write");
    }

    [Test]
    public async Task BulkAppend_waits_for_birth_pin_and_does_not_publish_failed_initialization()
    {
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        var next = h.AddLeaf();
        var leaf = h.Leaf(next);
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        h.Leaf(existing).CountAsync().Returns(2);
        h.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(leaf);
        var pin = new TaskCompletionSource();
        leaf.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(pin.Task);

        var append = h.Grain.BulkAppendAsync("pin-gate", Pairs(2));
        Assert.That(append.IsCompleted, Is.False);
        await leaf.DidNotReceive().MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
        await h.Leaf(existing).DidNotReceive().SetNextSiblingAsync(Arg.Any<GrainId>());
        var failure = new IOException("birth pin failed");
        pin.SetException(failure);
        Assert.That(async () => await append, Throws.Exception.SameAs(failure));
        Assert.That(h.State.State.PendingBulkGraft, Is.Null);
        Assert.That(h.State.State.RootNodeId, Is.EqualTo(existing));

        leaf.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        await h.Grain.BulkAppendAsync("pin-gate", Pairs(2));
        await leaf.Received(1).MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
        Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("pin-gate"));
    }
}
