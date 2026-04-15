using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

public class ShardRoutingTests
{
    [Fact]
    public void GetShardIndex_is_deterministic()
    {
        var a = ShardRouterGrain.GetShardIndex("hello", 64);
        var b = ShardRouterGrain.GetShardIndex("hello", 64);
        Assert.Equal(a, b);
    }

    [Fact]
    public void GetShardIndex_stays_in_range()
    {
        var keys = new[] { "a", "b", "foo", "bar", "customer-12345", "", "z" };
        foreach (var key in keys)
        {
            var index = ShardRouterGrain.GetShardIndex(key, 64);
            Assert.InRange(index, 0, 63);
        }
    }

    [Fact]
    public void GetShardIndex_distributes_across_shards()
    {
        var shards = new HashSet<int>();
        for (int i = 0; i < 1000; i++)
        {
            shards.Add(ShardRouterGrain.GetShardIndex($"key-{i}", 64));
        }

        // With 1000 random-ish keys and 64 shards, we should hit a reasonable number.
        Assert.True(shards.Count > 30, $"Expected >30 distinct shards, got {shards.Count}");
    }
}
