using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class WalResumeTokenTests
{
    [Test]
    public void Default_value_has_zero_shard_and_offset()
    {
        var def = default(WalResumeToken);

        Assert.Multiple(() =>
        {
            Assert.That(def.ShardIndex, Is.EqualTo(0));
            Assert.That(def.Offset, Is.EqualTo(0L));
        });
    }

    [Test]
    public void With_initialiser_sets_properties()
    {
        var sut = new WalResumeToken { ShardIndex = 3, Offset = 17 };

        Assert.Multiple(() =>
        {
            Assert.That(sut.ShardIndex, Is.EqualTo(3));
            Assert.That(sut.Offset, Is.EqualTo(17L));
        });
    }

    [Test]
    public void Records_with_same_values_are_equal()
    {
        var a = new WalResumeToken { ShardIndex = 2, Offset = 5 };
        var b = new WalResumeToken { ShardIndex = 2, Offset = 5 };

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Records_with_different_offsets_are_not_equal()
    {
        var a = new WalResumeToken { ShardIndex = 2, Offset = 5 };
        var b = new WalResumeToken { ShardIndex = 2, Offset = 6 };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Records_with_different_shards_are_not_equal()
    {
        var a = new WalResumeToken { ShardIndex = 1, Offset = 5 };
        var b = new WalResumeToken { ShardIndex = 2, Offset = 5 };

        Assert.That(a, Is.Not.EqualTo(b));
    }
}
