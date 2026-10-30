using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class HotShardMonitorGrainTests
{
    [Test]
    public async Task RunSamplingPass_still_defers_a_busy_split_until_the_next_pass()
    {
        var (grain, _, _, split, _, shardOf, _) = CreateGrain();
        shardOf(1).StubHotness(new ShardHotness
        {
            Reads = 10_000, Window = TimeSpan.FromSeconds(10),
        });
        split.SplitAsync(1).Returns(Task.FromException(new InvalidOperationException("busy")));

        await grain.RunSamplingPassAsync();
        await grain.RunSamplingPassAsync();

        await split.Received(2).SplitAsync(1);
    }

    [Test]
    public void RunSamplingPass_preserves_domain_refusals_from_split_dispatch()
    {
        var (grain, _, _, split, _, shardOf, _) = CreateGrain();
        shardOf(1).StubHotness(new ShardHotness
        {
            Reads = 10_000, Window = TimeSpan.FromSeconds(10),
        });
        var fault = new LeafProjectionStaleException("projection unavailable");
        split.SplitAsync(1).Returns(Task.FromException(fault));

        var actual = Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.RunSamplingPassAsync());

        Assert.That(actual, Is.SameAs(fault));
        split.Received(1).SplitAsync(1);
    }
}
