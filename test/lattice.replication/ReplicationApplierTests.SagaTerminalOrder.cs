using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4499: a receiver-side deferral of a saga's prepare must be
/// order-preserving per saga. A batch run that defers a prepare (here, a
/// duplicate of an in-flight delivery) must defer a later terminal of the same
/// saga in that run too, or the terminal applies before the prepare and the
/// saga is visible torn until the re-delivery lands.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyBatchAsync_defers_a_terminal_behind_a_deferred_prepare_of_the_same_saga()
    {
        var (applier, _, apply, _) = CreateApplier();
        var txid = Guid.NewGuid();
        var prepare = SagaPrepare("k", Hlc(10), txid);
        var terminal = SagaCommit(Hlc(20), txid);
        var firstApply = new TaskCompletionSource();
        apply.ApplyPreparedSetAsync(
                "k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, Arg.Any<VersionVector?>(), Arg.Any<long>(),
                txid, 2, 0, Arg.Any<byte[]?>(), Arg.Any<LatticeMergeMode>())
            .Returns(firstApply.Task, Task.CompletedTask);

        var first = applier.ApplyAsync(prepare);
        Assert.That(first.IsCompleted, Is.False, "The first delivery of the prepare must be held mid-apply.");

        var batch = await applier.ApplyBatchAsync(new[] { prepare, terminal });

        Assert.That(batch.Deferred, Is.True);
        await apply.DidNotReceiveWithAnyArgs().ApplyTxTerminalAsync(
            default, default, default, default, default!, default, default, default, default);

        firstApply.SetException(new TimeoutException("simulated abort"));
        Assert.ThrowsAsync<TimeoutException>(async () => await first);

        var resent = await applier.ApplyBatchAsync(new[] { prepare, terminal });

        Assert.That(resent.Deferred, Is.False);
        await apply.Received(1).ApplyTxTerminalAsync(
            txid, true, 1, Hlc(20), RemoteCluster, Arg.Any<int>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_still_applies_a_terminal_of_another_saga_behind_a_deferred_prepare()
    {
        var (applier, _, apply, _) = CreateApplier();
        var txid = Guid.NewGuid();
        var otherTxid = Guid.NewGuid();
        var prepare = SagaPrepare("k", Hlc(10), txid);
        var firstApply = new TaskCompletionSource();
        apply.ApplyPreparedSetAsync(
                "k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, Arg.Any<VersionVector?>(), Arg.Any<long>(),
                txid, 2, 0, Arg.Any<byte[]?>(), Arg.Any<LatticeMergeMode>())
            .Returns(firstApply.Task);

        _ = applier.ApplyAsync(prepare);
        var batch = await applier.ApplyBatchAsync(new[] { prepare, SagaCommit(Hlc(20), otherTxid) });

        Assert.That(batch.Deferred, Is.True);
        await apply.Received(1).ApplyTxTerminalAsync(
            otherTxid, true, 1, Hlc(20), RemoteCluster, Arg.Any<int>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<CancellationToken>());
        firstApply.SetResult();
    }

    private static WalRecord SagaPrepare(string key, HybridLogicalClock hlc, Guid txid) =>
        SetEntry(key, hlc) with
        {
            TransactionId = txid,
            IsPrepared = true,
            AtomicBatchSize = 2,
            AtomicBatchIndex = 0,
        };

    private static WalRecord SagaCommit(HybridLogicalClock hlc, Guid txid) =>
        SetEntry(string.Empty, hlc) with
        {
            Op = MutationKind.TxCommit,
            Value = null!,
            TransactionId = txid,
            ShardIndex = 1,
        };
}
