using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class TreeEmptinessProbeTests
{
    [Test]
    public async Task Probe_first_shard_has_keys_does_not_activate_remaining_shards()
    {
        var factory = Substitute.For<IGrainFactory>();
        var first = Substitute.For<IShardRootGrain>();
        first.AnyBoundedAsync(null).Returns(new ShardAnyPage { Found = true });
        factory.GetGrain<IShardRootGrain>("tree/3").Returns(first);

        Assert.That(await TreeEmptinessProbe.IsObservablyEmptyAsync(
            factory, "tree", [3, 5, 9], Timeout.InfiniteTimeSpan), Is.False);

        factory.DidNotReceive().GetGrain<IShardRootGrain>("tree/5");
        factory.DidNotReceive().GetGrain<IShardRootGrain>("tree/9");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Probe_first_shard_empty_drives_remaining_pages_before_believing_empty(bool found)
    {
        var factory = Substitute.For<IGrainFactory>();
        var first = Substitute.For<IShardRootGrain>();
        var second = Substitute.For<IShardRootGrain>();
        first.AnyBoundedAsync(null).Returns(new ShardAnyPage());
        second.AnyBoundedAsync(null).Returns(new ShardAnyPage { ResumeFromInclusive = "next" });
        second.AnyBoundedAsync("next").Returns(new ShardAnyPage { Found = found });
        factory.GetGrain<IShardRootGrain>("tree/0").Returns(first);
        factory.GetGrain<IShardRootGrain>("tree/1").Returns(second);

        Assert.That(await TreeEmptinessProbe.IsObservablyEmptyAsync(
            factory, "tree", [0, 1], Timeout.InfiniteTimeSpan), Is.EqualTo(!found));
        await second.Received(1).AnyBoundedAsync("next");
    }

    [Test]
    public async Task Probe_blocked_first_shard_is_covered_by_the_whole_probe_budget()
    {
        var factory = Substitute.For<IGrainFactory>();
        var first = Substitute.For<IShardRootGrain>();
        var pending = new TaskCompletionSource<ShardAnyPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        first.AnyBoundedAsync(null).Returns(pending.Task);
        factory.GetGrain<IShardRootGrain>("tree/0").Returns(first);
        try
        {
            Assert.That(await TreeEmptinessProbe.IsObservablyEmptyAsync(
                factory, "tree", [0, 1], TimeSpan.FromMilliseconds(20)).WaitAsync(TimeSpan.FromSeconds(5)), Is.False);
            factory.DidNotReceive().GetGrain<IShardRootGrain>("tree/1");
        }
        finally
        {
            pending.TrySetException(new InvalidOperationException("late probe failure"));
        }
    }

    [Test]
    public async Task Probe_fault_is_inconclusive_and_empty_shard_set_is_empty()
    {
        var factory = Substitute.For<IGrainFactory>();
        var shard = Substitute.For<IShardRootGrain>();
        shard.AnyBoundedAsync(null).Returns(Task.FromException<ShardAnyPage>(new InvalidOperationException("unavailable")));
        factory.GetGrain<IShardRootGrain>("tree/0").Returns(shard);
        Assert.That(await TreeEmptinessProbe.IsObservablyEmptyAsync(
            factory, "tree", [0], Timeout.InfiniteTimeSpan), Is.False);
        Assert.That(await TreeEmptinessProbe.IsObservablyEmptyAsync(
            factory, "tree", [], Timeout.InfiniteTimeSpan), Is.True);
    }
}
