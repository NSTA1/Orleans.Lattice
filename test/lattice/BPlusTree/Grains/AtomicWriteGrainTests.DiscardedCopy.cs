using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4474: a terminal refused by the copy a resize undo discarded counts as
/// delivered - the copy's batch is discarded with it - and is never re-sent to
/// the copy the tree resolves to now, nor retried until the saga stalls.
/// </summary>
public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task MarkOneShardAsync_counts_a_deleted_tree_refusal_by_a_discarded_copy_as_delivered()
    {
        var (grain, _, _, lattice, shard) = CreateGrain(
            configureFactory: f => f.GetGrain<ITreeDeletionGrain>(TreeId).IsDiscardedAsync().Returns(true));
        var attempts = 0;
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns<Task<WalRecord?>>(_ =>
            {
                attempts++;
                throw new InvalidOperationException("This tree has been deleted and is no longer accessible.");
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(1), "the refusal is not retried");
        // One routing read to prepare and one for the broadcast's drift pass:
        // no refresh follows the refusal to another copy.
        await lattice.Received(2).GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MarkOneShardAsync_never_follows_a_stale_tree_refusal_by_a_discarded_copy()
    {
        var (grain, _, _, lattice, shard) = CreateGrain(
            configureFactory: f => f.GetGrain<ITreeDeletionGrain>(TreeId).IsDiscardedAsync().Returns(true));
        var attempts = 0;
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns<Task<WalRecord?>>(_ =>
            {
                attempts++;
                throw new StaleTreeRoutingException(TreeId, TreeId, "restored-copy");
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(1), "the terminal is not re-sent after the refusal");
        await lattice.Received(2).GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void MarkOneShardAsync_surfaces_a_deleted_tree_refusal_by_a_copy_that_was_not_discarded()
    {
        var (grain, _, _, _, shard) = CreateGrain(
            configureFactory: f => f.GetGrain<ITreeDeletionGrain>(TreeId).IsDiscardedAsync().Returns(false));
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns<Task<WalRecord?>>(_ => throw new InvalidOperationException("This tree has been deleted and is no longer accessible."));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));

        Assert.That(ex!.Message, Does.Contain("deleted"));
    }

    [Test]
    public async Task BroadcastTerminals_does_not_append_a_terminal_a_discarded_copy_took_before_the_discard()
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var first = "key-0";
        var second = Enumerable.Range(1, 256).Select(i => $"key-{i}").First(k => map.Resolve(k) != map.Resolve(first));

        var writer = Substitute.For<ICommitLogWriter>();
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ICommitLogWriter)).Returns(writer);
        var (grain, _, _, _, shard) = CreateGrain(
            configureFactory: f => f.GetGrain<ITreeDeletionGrain>(TreeId).IsDiscardedAsync().Returns(true),
            activationServices: services);
        var attempts = 0;
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns<Task<WalRecord?>>(_ =>
            {
                if (attempts++ == 0)
                    return Task.FromResult<WalRecord?>(new WalRecord { TreeId = TreeId, Op = MutationKind.TxCommit, Key = "0" });
                throw new InvalidOperationException("This tree has been deleted and is no longer accessible.");
            });

        await grain.ExecuteAsync(TreeId, MakeEntries((first, [1]), (second, [2])));

        Assert.That(attempts, Is.EqualTo(2), "precondition: one shard took the terminal and one refused it");
        await writer.DidNotReceive().AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }
}
