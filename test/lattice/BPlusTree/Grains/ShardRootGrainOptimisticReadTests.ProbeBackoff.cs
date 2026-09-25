using System.Reflection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Unstamped_probe_retries_after_expiry_or_routing_invalidation(bool routingChange)
    {
        var (grain, _, leaf, _) = CreateGrain(warmStamp: false);
        leaf.GetWithVersionAsync("k").Returns(new VersionedValue());
        await grain.TryGetOptimisticAsync("k");
        await grain.GetAsync("k");
        var field = typeof(ShardRootGrain).GetField("_unstampedLeafProbeRetryAfter",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "Probe backoff renamed; update the test arrangement.");
        var backoff = (Dictionary<GrainId, long>)field!.GetValue(grain)!;
        Assert.That(backoff[RootLeafId] - Environment.TickCount64, Is.InRange(1, 30_000));
        leaf.GetWithVersionAsync("k").Returns(Stamped(null));
        await grain.GetAsync("k");
        await leaf.Received(1).GetWithVersionAsync("k");

        if (routingChange)
            grain.InvalidateRoutingTable(InternalRootId);
        else
            backoff[RootLeafId] = Environment.TickCount64 - 1;
        await grain.GetAsync("k");
        await leaf.Received(2).GetWithVersionAsync("k");
        Assert.That(backoff.ContainsKey(RootLeafId), Is.False);
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.True);
    }

    [Test]
    public async Task Routing_invalidation_during_unstamped_probe_does_not_restore_backoff()
    {
        var (grain, _, leaf, _) = CreateGrain(warmStamp: false);
        await grain.TryGetOptimisticAsync("k");
        var reply = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k").Returns(reply.Task);
        var read = grain.GetAsync("k");
        grain.InvalidateRoutingTable(InternalRootId);
        reply.SetResult(new VersionedValue());
        await read;
        leaf.GetWithVersionAsync("k").Returns(Stamped(null));
        await grain.GetAsync("k");
        await leaf.Received(2).GetWithVersionAsync("k");
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.True);
    }

    [Test]
    public async Task Pending_key_does_not_evict_sibling_ownership_or_probe_again_on_serial_fallback()
    {
        var (grain, _, proxy, _) = CreateGrain(warmStamp: false);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(RootLeafId);
        var factory = Substitute.For<IGrainFactory>();
        var leaf = new BPlusLeafGrain(context, new FakePersistentState<LeafNodeState>(), factory,
            TestOptionsResolver.Create(factory: factory),
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        proxy.GetWithVersionAsync(Arg.Any<string>())
            .Returns(call => leaf.GetWithVersionAsync(call.Arg<string>()));
        await grain.TryGetOptimisticAsync("sibling");
        await grain.GetAsync("sibling");
        Assert.That((await grain.TryGetOptimisticAsync("sibling")).IsValidated, Is.True);
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
                await leaf.SetAsync("pending", [1]);
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
        using (LatticeRegistrySnapshotContext.BeginScope(
            new Dictionary<Guid, TxStatus> { [txid] = TxStatus.InFlight }))
        {
            for (var i = 0; i < 3; i++)
            {
                Assert.That((await grain.TryGetOptimisticAsync("pending")).IsValidated, Is.False);
                await grain.GetAsync("pending");
                Assert.That((await grain.TryGetOptimisticAsync("sibling")).IsValidated, Is.True);
            }
        }
        // Only the necessary optimistic adjudications, no serial warmup probes.
        await proxy.Received(3).GetWithVersionAsync("pending");
    }

    [Test]
    public async Task Delayed_generation_mismatch_does_not_evict_a_serially_rewarmed_stamp()
    {
        var (grain, _, leaf, _) = CreateGrain();
        var delayedReply = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k").Returns(delayedReply.Task);
        var delayedRead = grain.TryGetOptimisticAsync("k");
        leaf.GetWithVersionAsync("k").Returns(Stamped(null, 2));
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.False);
        await grain.GetAsync("k");
        delayedReply.SetResult(Stamped(null, 2));
        Assert.That((await delayedRead).IsValidated, Is.False);
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.True);
    }

    [Test]
    public async Task Unstamped_probe_backoff_is_shared_by_keys_on_the_same_leaf()
    {
        var (grain, _, leaf, _) = CreateGrain(warmStamp: false);
        leaf.GetWithVersionAsync(Arg.Any<string>()).Returns(new VersionedValue());
        await grain.TryGetOptimisticAsync("a");
        await grain.GetAsync("a");
        await grain.GetAsync("b");
        await grain.GetAsync("c");
        await leaf.Received(1).GetWithVersionAsync(Arg.Any<string>());
    }
}
