using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="TxRegistryHighWaterCache"/>, the per-silo cache of
/// each tree's durable registry shard high-water mark (issue #3501). An entry may
/// only grow, is clamped to the shard-count bound, and is scoped to one grain
/// factory.
/// </summary>
[TestFixture]
public class TxRegistryHighWaterCacheTests
{
    [Test]
    public void Get_is_zero_for_a_tree_never_observed()
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.That(TxRegistryHighWaterCache.Get(factory, "tree-unseen"), Is.Zero);
    }

    [Test]
    public void Observe_keeps_the_larger_mark()
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryHighWaterCache.Observe(factory, "tree", 3), Is.EqualTo(3));
            Assert.That(TxRegistryHighWaterCache.Observe(factory, "tree", 1), Is.EqualTo(3), "A lower observation never shrinks the entry.");
            Assert.That(TxRegistryHighWaterCache.Observe(factory, "tree", 5), Is.EqualTo(5));
            Assert.That(TxRegistryHighWaterCache.Get(factory, "tree"), Is.EqualTo(5));
        });
    }

    [TestCase(-3, 0)]
    [TestCase(10_000, LatticeOptions.MaxTxRegistryShardCount)]
    public void Observe_clamps_to_the_shard_count_bound(int observed, int expected)
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.That(TxRegistryHighWaterCache.Observe(factory, "tree", observed), Is.EqualTo(expected));
    }

    [Test]
    public void Entries_are_scoped_to_one_grain_factory_and_one_tree()
    {
        var first = Substitute.For<IGrainFactory>();
        var second = Substitute.For<IGrainFactory>();
        TxRegistryHighWaterCache.Observe(first, "tree", 4);

        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryHighWaterCache.Get(second, "tree"), Is.Zero);
            Assert.That(TxRegistryHighWaterCache.Get(first, "other-tree"), Is.Zero);
        });
    }
}
