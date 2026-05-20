using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the bounded-retry behaviour of the public
/// <see cref="ILattice"/> surface when a shard root throws
/// <see cref="StaleShardRoutingException"/>. The split choreography in
/// <c>TreeShardSplitGrain.SwapAsync</c> enters Reject on the source shard
/// before the registry's <c>ShardMap</c> flip, opening a window during
/// which a fresh map fetch can still return the pre-flip map. A single-shot
/// retry can therefore re-observe the same stale map and re-throw - the
/// original failure mode that escaped the public surface during the
/// concurrent-read leg of <c>ShardSplitIntegrationTests</c>. The tests in
/// this file lock the bounded-retry behaviour: every public read/write path
/// must absorb at least two consecutive <see cref="StaleShardRoutingException"/>
/// throws and only surface the eventual success.
/// </summary>
public partial class LatticeGrainTests
{
    // --- Multi-throw bounded-retry contract ---
    //
    // The shape every test below shares:
    //   * registry.GetShardMapAsync returns the pre-flip map for the
    //     first N calls, then flips to the post-flip map (the same
    //     wall-clock progression the split-grain registry update
    //     produces, replayed deterministically in unit-test time).
    //   * The pre-flip map routes the key to shard 0, which throws
    //     StaleShardRoutingException unconditionally (the Reject-phase
    //     gate in ShardRootGrain.Split).
    //   * The post-flip map routes the key to shard 1, which succeeds.
    //   * The test asserts (a) the public call ultimately succeeded,
    //     (b) the LatticeGrain refetched the shard map at least N+1
    //     times (one per throw + the final successful resolution),
    //     and (c) the success-path shard was eventually called.

    private const int MultiThrowCount = 3;

    private static (IShardRootGrain Shard0, IShardRootGrain Shard1) SetupTwoShardSplitWindow(
        IGrainFactory factory,
        ILatticeRegistry registry,
        string treeId,
        int stalefetchCount)
    {
        // Pre-flip map: every slot owned by shard 0. Post-flip map: every
        // slot owned by shard 1. The LatticeGrain caches the resolved map
        // for the lifetime of a successful call, so each retry pass must
        // re-query the registry through InvalidateShardMap; the call-count
        // gate below proves the retry loop honours that invalidation.
        var preFlip = new ShardMap { Slots = [0, 0], Version = 1 };
        var postFlip = new ShardMap { Slots = [1, 1], Version = 2 };
        var fetchCount = 0;
        registry.GetShardMapAsync(treeId).Returns(_ =>
            Task.FromResult<ShardMap?>(fetchCount++ < stalefetchCount ? preFlip : postFlip));

        var shard0 = Substitute.For<IShardRootGrain>();
        var shard1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>()).Returns(shard1);
        return (shard0, shard1);
    }

    [Test]
    public async Task GetAsync_absorbs_repeated_StaleShardRoutingException_throws()
    {
        // Pre-fix behaviour: the single-shot try/catch in GetAsyncCore
        // re-routed once, re-fetched the (still pre-flip) map, and
        // re-threw on the second hit - escaping the public ILattice
        // surface. The bounded-retry helper must loop until the
        // registry serves the post-flip map.
        const string treeId = "stale-routing-get";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        var (shard0, shard1) = SetupTwoShardSplitWindow(factory, registry, treeId, stalefetchCount: MultiThrowCount);

        shard0.GetAsync("k1").Returns<Task<byte[]?>>(_ =>
            throw new StaleShardRoutingException(
                sourceShardIndex: 0, targetShardIndex: 1, virtualSlot: 0));
        shard1.GetAsync("k1").Returns(Task.FromResult<byte[]?>([7]));

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.EqualTo(new byte[] { 7 }));
        await registry.Received().GetShardMapAsync(treeId);
        // Each stale throw invalidates the cache, forcing a refetch; the
        // final successful resolution adds one more. >= MultiThrowCount + 1
        // is the minimum (a single-shot retry would top out at 2).
        Assert.That(registry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)),
            Is.GreaterThanOrEqualTo(MultiThrowCount + 1));
        await shard1.Received(1).GetAsync("k1");
    }

    [Test]
    public async Task SetAsync_absorbs_repeated_StaleShardRoutingException_throws()
    {
        const string treeId = "stale-routing-set";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        SetupCompactionGrain(factory, treeId);
        var (shard0, shard1) = SetupTwoShardSplitWindow(factory, registry, treeId, stalefetchCount: MultiThrowCount);

        shard0.SetAsync("k1", Arg.Any<byte[]>()).Returns<Task>(_ =>
            throw new StaleShardRoutingException(
                sourceShardIndex: 0, targetShardIndex: 1, virtualSlot: 0));
        shard1.SetAsync("k1", Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        await grain.SetAsync("k1", [1]);

        await shard1.Received(1).SetAsync("k1", Arg.Any<byte[]>());
        Assert.That(registry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)),
            Is.GreaterThanOrEqualTo(MultiThrowCount + 1));
    }

    [Test]
    public async Task DeleteAsync_absorbs_repeated_StaleShardRoutingException_throws()
    {
        const string treeId = "stale-routing-delete";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        SetupCompactionGrain(factory, treeId);
        var (shard0, shard1) = SetupTwoShardSplitWindow(factory, registry, treeId, stalefetchCount: MultiThrowCount);

        shard0.DeleteAsync("k1").Returns<Task<bool>>(_ =>
            throw new StaleShardRoutingException(
                sourceShardIndex: 0, targetShardIndex: 1, virtualSlot: 0));
        shard1.DeleteAsync("k1").Returns(Task.FromResult(true));

        var result = await grain.DeleteAsync("k1");

        Assert.That(result, Is.True);
        await shard1.Received(1).DeleteAsync("k1");
    }

    [Test]
    public async Task ExistsAsync_absorbs_repeated_StaleShardRoutingException_throws()
    {
        const string treeId = "stale-routing-exists";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        var (shard0, shard1) = SetupTwoShardSplitWindow(factory, registry, treeId, stalefetchCount: MultiThrowCount);

        shard0.ExistsAsync("k1").Returns<Task<bool>>(_ =>
            throw new StaleShardRoutingException(
                sourceShardIndex: 0, targetShardIndex: 1, virtualSlot: 0));
        shard1.ExistsAsync("k1").Returns(Task.FromResult(true));

        var result = await grain.ExistsAsync("k1");

        Assert.That(result, Is.True);
        await shard1.Received(1).ExistsAsync("k1");
    }

    [Test]
    public async Task GetAsync_surfaces_OperationCanceledException_when_token_cancels_mid_retry()
    {
        // Companion to the absorb-tests: the bounded-retry helper must
        // cooperatively pre-empt on cancellation, otherwise a hung split
        // (registry never flips) would block the caller for the full
        // 60-second budget. The helper checks the token at the top of
        // every loop iteration; this test pins that contract by cancelling
        // after the first throw and asserting the second iteration sees
        // the token before re-invoking the shard root.
        const string treeId = "stale-routing-cancel";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);

        // Pre-flip map only: every refetch returns the same shard-0-owned
        // map, and shard 0 throws unconditionally. Without cancellation
        // the helper would loop for the full budget.
        var map = new ShardMap { Slots = [0, 0], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));
        var shard0 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard0);

        using var cts = new CancellationTokenSource();
        var throwCount = 0;
        shard0.GetAsync("k1").Returns<Task<byte[]?>>(_ =>
        {
            // Cancel after the first throw so the helper's top-of-loop
            // token check trips on the next iteration. This proves the
            // helper does keep retrying past a single throw (otherwise
            // the test would surface StaleShardRoutingException, not
            // OperationCanceledException) AND honours cancellation.
            throwCount++;
            cts.Cancel();
            throw new StaleShardRoutingException(
                sourceShardIndex: 0, targetShardIndex: 1, virtualSlot: 0);
        });

        Assert.That(
            async () => await grain.GetAsync("k1", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(throwCount, Is.EqualTo(1),
            "Helper must check the cancellation token before retrying, not after attempting the operation.");
    }
}
