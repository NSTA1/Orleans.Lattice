using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

public class ShardRoutingTests
{
    [Test]
    public void GetShardIndex_is_deterministic()
    {
        var a = LatticeGrain.GetShardIndex("hello", 64);
        var b = LatticeGrain.GetShardIndex("hello", 64);
        Assert.That(b, Is.EqualTo(a));
    }

    [Test]
    public void GetShardIndex_stays_in_range()
    {
        var keys = new[] { "a", "b", "foo", "bar", "customer-12345", "", "z" };
        foreach (var key in keys)
        {
            var index = LatticeGrain.GetShardIndex(key, 64);
            Assert.That(index, Is.InRange(0, 63));
        }
    }

    [Test]
    public void GetShardIndex_distributes_across_shards()
    {
        var shards = new HashSet<int>();
        for (int i = 0; i < 1000; i++)
        {
            shards.Add(LatticeGrain.GetShardIndex($"key-{i}", 64));
        }

        // With 1000 random-ish keys and 64 shards, we should hit a reasonable number.
        Assert.That(shards.Count > 30, Is.True, $"Expected >30 distinct shards, got {shards.Count}");
    }
}
