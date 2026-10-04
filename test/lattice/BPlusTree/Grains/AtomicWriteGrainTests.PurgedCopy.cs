using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4475: a saga still bound to a resize's old copy when that copy is
/// purged redelivers each refused terminal to the copy the tree resolves to now,
/// instead of failing the broadcast on every retry and never completing.
/// </summary>
public partial class AtomicWriteGrainTests
{
    private const string ResizedCopyId = TreeId + "/resized/r1";

    private const string PurgedRefusal =
        "Tree 'atomic-tree' has been purged and no longer exists. A read does not recreate it; create the tree or write to it to reuse the id.";

    [Test]
    public async Task MarkOneShardAsync_redelivers_a_terminal_a_purged_copy_refuses_to_the_resized_copy()
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var first = "key-0";
        var second = Enumerable.Range(1, 256).Select(i => $"key-{i}").First(k => map.Resolve(k) != map.Resolve(first));

        var purged = false;
        var resizedShard = Substitute.For<IShardRootGrain>();
        resizedShard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));
        var redeliveredTo = new List<string>();
        var (grain, _, _, lattice, shard) = CreateGrain(configureFactory: f =>
        {
            f.GetGrain<ITreeDeletionGrain>(TreeId).IsDiscardedAsync().Returns(false);
            f.GetGrain<ITreeDeletionGrain>(TreeId).HoldsCompletedPurgeAsync().Returns(_ => Task.FromResult(purged));
            f.GetGrain<IShardRootGrain>(Arg.Is<string>(k => k.StartsWith(ResizedCopyId + "/", StringComparison.Ordinal)))
                .Returns(ci =>
                {
                    lock (redeliveredTo) redeliveredTo.Add(ci.ArgAt<string>(0));
                    return resizedShard;
                });
        });
        var copyRouting = new RoutingInfo(TreeId, map);
        var resizedRouting = new RoutingInfo(ResizedCopyId, map);
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<RoutingInfo>(purged ? resizedRouting : copyRouting));
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns<Task<WalRecord?>>(_ =>
            {
                purged = true;
                throw new InvalidOperationException(PurgedRefusal);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries((first, [1]), (second, [2])));

        var expected = new[] { map.Resolve(first), map.Resolve(second) }
            .Distinct().Select(i => $"{ResizedCopyId}/{i}").ToArray();
        Assert.That(redeliveredTo.Distinct(), Is.EquivalentTo(expected),
            "each refused terminal reaches the resized copy's shard at the same index and each key's owner there");
        await resizedShard.Received().AppendTxTerminalAsync(
            Arg.Any<Guid>(), true, null, Arg.Any<CancellationToken>(), Arg.Any<bool>());
        await resizedShard.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Is<IReadOnlyDictionary<string, byte[]>?>(v => v != null), Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    [Test]
    public void MarkOneShardAsync_surfaces_a_purged_copy_refusal_when_the_tree_still_resolves_to_that_copy()
    {
        var (grain, _, _, _, shard) = CreateGrain(configureFactory: f =>
        {
            f.GetGrain<ITreeDeletionGrain>(TreeId).IsDiscardedAsync().Returns(false);
            f.GetGrain<ITreeDeletionGrain>(TreeId).HoldsCompletedPurgeAsync().Returns(true);
        });
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns<Task<WalRecord?>>(_ => throw new InvalidOperationException(PurgedRefusal));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));

        Assert.That(ex!.Message, Does.Contain("purged"));
    }
}
