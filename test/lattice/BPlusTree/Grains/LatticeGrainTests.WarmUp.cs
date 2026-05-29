using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeGrain.WarmUpAsync(System.Threading.CancellationToken)"/>.
/// Covers the public contract: pre-activates every physical shard root via a
/// read-only probe, refuses calls against system trees, and propagates
/// cooperative cancellation before any probe is dispatched.
/// </summary>
public partial class LatticeGrainTests
{
    [Test]
    public async Task WarmUpAsync_probes_every_physical_shard_root()
    {
        var (grain, factory) = CreateGrain(shardCount: 4);
        var shardRoot = SetupShardRoot(factory);
        shardRoot.WarmUpAsync().Returns(Task.CompletedTask);

        await grain.WarmUpAsync();

        // Default ShardMap distributes 4096 virtual slots over 4 physical
        // shards, so the warm-up fan-out issues one WarmUpAsync probe
        // per distinct physical shard. The shared NSubstitute root is
        // returned for every shard id, so the call count equals the
        // physical-shard count.
        await shardRoot.Received(4).WarmUpAsync();
    }

    [Test]
    public void WarmUpAsync_rejects_system_trees()
    {
        var (grain, _) = CreateGrain(treeId: LatticeConstants.SystemTreePrefix + "registry");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.WarmUpAsync());
    }

    [Test]
    public void WarmUpAsync_honors_pre_cancelled_token()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => grain.WarmUpAsync(cts.Token));

        // No probe should have been dispatched once cancellation was
        // observed at the top of the call.
        shardRoot.DidNotReceive().WarmUpAsync();
    }
}
