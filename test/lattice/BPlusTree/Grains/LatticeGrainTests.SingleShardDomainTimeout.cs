using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    [Test]
    public void SetManyAsync_single_shard_envelope_preserves_domain_timeout_followup()
    {
        const string treeId = "single-shard-domain-timeout-followup";
        var (grain, factory) = CreateGrain(
            treeId,
            new LatticeOptions { SetManyEnvelopeBudget = TimeSpan.FromSeconds(30) },
            shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shard = SetupShardRoot(factory);
        var refusal = new LatticeTransactionOutcomeUnavailableException("callee domain refusal");
        shard.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.FromException(refusal));

        var actual = Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(
            () => grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]));

        Assert.That(actual, Is.SameAs(refusal));
    }

    [Test]
    public void SetManyAsync_single_shard_envelope_preserves_callee_timeout()
    {
        const string treeId = "single-shard-callee-timeout";
        var (grain, factory) = CreateGrain(
            treeId,
            new LatticeOptions { SetManyEnvelopeBudget = TimeSpan.FromSeconds(30) },
            shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shard = SetupShardRoot(factory);
        var timeout = new TimeoutException("callee timeout");
        shard.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.FromException(timeout));

        var actual = Assert.ThrowsAsync<TimeoutException>(
            () => grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]));

        Assert.That(actual, Is.SameAs(timeout));
    }

    [Test]
    public void SetManyAsync_single_shard_envelope_preserves_callee_cancellation()
    {
        const string treeId = "single-shard-callee-cancellation";
        var (grain, factory) = CreateGrain(
            treeId,
            new LatticeOptions { SetManyEnvelopeBudget = TimeSpan.FromSeconds(30) },
            shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shard = SetupShardRoot(factory);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        shard.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.FromCanceled(cancellation.Token));

        var actual = Assert.ThrowsAsync<TaskCanceledException>(
            () => grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]));

        Assert.That(actual!.CancellationToken, Is.EqualTo(cancellation.Token));
    }
}
