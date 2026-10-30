using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task GetAsync_reuses_stable_stamped_warmup_without_cache_call(bool present)
    {
        var (grain, _, leaf, cache) = CreateGrain(warmStamp: false);
        await grain.TryGetOptimisticAsync("k");
        var value = present ? Bytes("primary") : null;
        leaf.GetWithVersionAsync("k").Returns(Stamped(value));
        cache.GetAsync("k").Returns(Bytes("must-not-read"));

        Assert.That(await grain.GetAsync("k"), Is.EqualTo(value));
        await leaf.Received(1).GetWithVersionAsync("k");
        await cache.DidNotReceive().GetAsync("k");
    }

    [Test]
    public async Task GetAsync_unstamped_warmup_keeps_cache_visibility_gate()
    {
        var (grain, _, leaf, cache) = CreateGrain(warmStamp: false);
        await grain.TryGetOptimisticAsync("k");
        leaf.GetWithVersionAsync("k").Returns(new VersionedValue { Value = Bytes("unproved") });
        cache.GetAsync("k").Returns(Bytes("visible"));

        Assert.That(await grain.GetAsync("k"), Is.EqualTo(Bytes("visible")));
        await cache.Received(1).GetAsync("k");
    }

    [Test]
    public async Task GetAsync_warmup_invalidated_during_await_keeps_cache_read()
    {
        var (grain, _, leaf, cache) = CreateGrain(warmStamp: false);
        await grain.TryGetOptimisticAsync("k");
        var gate = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k").Returns(gate.Task);
        cache.GetAsync("k").Returns(Bytes("current"));
        var read = grain.GetAsync("k");
        grain.BeginRoutingMutation();
        grain.EndRoutingMutation();
        gate.SetResult(Stamped(Bytes("stale")));

        Assert.That(await read, Is.EqualTo(Bytes("current")));
        await cache.Received(1).GetAsync("k");
    }
}
