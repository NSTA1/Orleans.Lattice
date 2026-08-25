using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="LocalUsageSample"/>: the additive join used by the
/// cross-cluster fold (commutative, associative, with <see cref="LocalUsageSample.Empty"/>
/// as identity) and the per-tree roll-up that sums bytes / keys / memory and counts
/// the trees.
/// </summary>
[TestFixture]
public sealed class LocalUsageSampleTests
{
    [Test]
    public void Empty_is_the_zero_sample()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LocalUsageSample.Empty.Bytes, Is.EqualTo(0));
            Assert.That(LocalUsageSample.Empty.Keys, Is.EqualTo(0));
            Assert.That(LocalUsageSample.Empty.MemoryBytes, Is.EqualTo(0));
            Assert.That(LocalUsageSample.Empty.TreeCount, Is.EqualTo(0));
            Assert.That(LocalUsageSample.Empty.IsEmpty, Is.True);
        });
    }

    [Test]
    public void IsEmpty_is_false_when_any_dimension_is_nonzero()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Sample(bytes: 1).IsEmpty, Is.False);
            Assert.That(Sample(keys: 1).IsEmpty, Is.False);
            Assert.That(Sample(memoryBytes: 1).IsEmpty, Is.False);
            Assert.That(Sample(treeCount: 1).IsEmpty, Is.False);
        });
    }

    [Test]
    public void Add_sums_every_dimension()
    {
        var sum = Sample(1, 2, 3, 4).Add(Sample(10, 20, 30, 40));

        Assert.Multiple(() =>
        {
            Assert.That(sum.Bytes, Is.EqualTo(11));
            Assert.That(sum.Keys, Is.EqualTo(22));
            Assert.That(sum.MemoryBytes, Is.EqualTo(33));
            Assert.That(sum.TreeCount, Is.EqualTo(44));
        });
    }

    [Test]
    public void Add_is_commutative()
    {
        var a = Sample(1, 2, 3, 4);
        var b = Sample(5, 6, 7, 8);

        Assert.That(a.Add(b), Is.EqualTo(b.Add(a)));
    }

    [Test]
    public void Add_is_associative()
    {
        var a = Sample(1, 2, 3, 4);
        var b = Sample(5, 6, 7, 8);
        var c = Sample(9, 10, 11, 12);

        Assert.That(a.Add(b).Add(c), Is.EqualTo(a.Add(b.Add(c))));
    }

    [Test]
    public void Empty_is_the_additive_identity()
    {
        var a = Sample(1, 2, 3, 4);

        Assert.Multiple(() =>
        {
            Assert.That(a.Add(LocalUsageSample.Empty), Is.EqualTo(a));
            Assert.That(LocalUsageSample.Empty.Add(a), Is.EqualTo(a));
        });
    }

    [Test]
    public void RollUp_sums_per_tree_dimensions_and_counts_the_trees()
    {
        var rolled = LocalUsageSample.RollUp([Tree(100, 1, 10), Tree(200, 2, 20), Tree(300, 3, 30)]);

        Assert.Multiple(() =>
        {
            Assert.That(rolled.Bytes, Is.EqualTo(600));
            Assert.That(rolled.Keys, Is.EqualTo(6));
            Assert.That(rolled.MemoryBytes, Is.EqualTo(60));
            Assert.That(rolled.TreeCount, Is.EqualTo(3), "tree count is the number of trees, not a sum of a dimension");
        });
    }

    [Test]
    public void RollUp_of_no_trees_is_empty()
    {
        Assert.That(LocalUsageSample.RollUp([]), Is.EqualTo(LocalUsageSample.Empty));
    }

    [Test]
    public void RollUp_null_trees_throws()
    {
        Assert.That(() => LocalUsageSample.RollUp(null!), Throws.ArgumentNullException);
    }
}
