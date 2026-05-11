using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class WalPartitionHashTests
{
    [Test]
    public void Compute_returns_zero_when_partitions_is_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalPartitionHash.Compute("anything", 1), Is.EqualTo(0));
            Assert.That(WalPartitionHash.Compute("", 1), Is.EqualTo(0));
            Assert.That(WalPartitionHash.Compute("\u00ff\u1234", 1), Is.EqualTo(0));
        });
    }

    [Test]
    public void Compute_returns_value_in_range_for_arbitrary_keys()
    {
        for (var i = 0; i < 200; i++)
        {
            var partition = WalPartitionHash.Compute($"key-{i}", 8);
            Assert.That(partition, Is.InRange(0, 7));
        }
    }

    [Test]
    public void Compute_is_deterministic_for_same_input()
    {
        var a = WalPartitionHash.Compute("hello", 16);
        var b = WalPartitionHash.Compute("hello", 16);

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Compute_distributes_keys_across_partitions()
    {
        var hits = new HashSet<int>();
        for (var i = 0; i < 256; i++)
        {
            hits.Add(WalPartitionHash.Compute($"k-{i}", 8));
        }

        // FNV-1a over 256 distinct keys should hit more than one bucket.
        Assert.That(hits.Count, Is.GreaterThan(1));
    }

    [Test]
    public void Compute_throws_on_null_key()
    {
        Assert.That(
            () => WalPartitionHash.Compute(null!, 4),
            Throws.ArgumentNullException);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Compute_throws_on_non_positive_partitions(int partitions)
    {
        Assert.That(
            () => WalPartitionHash.Compute("k", partitions),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
