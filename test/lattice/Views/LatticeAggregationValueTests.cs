namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for the <see cref="LatticeAggregationValue"/> codec.</summary>
[TestFixture]
public class LatticeAggregationValueTests
{
    [Test]
    public void EncodeInt64_round_trips()
    {
        var bytes = LatticeAggregationValue.EncodeInt64(42);

        Assert.That(LatticeAggregationValue.DecodeInt64(bytes), Is.EqualTo(42L));
    }

    [Test]
    public void EncodeInt64_is_eight_bytes()
    {
        Assert.That(LatticeAggregationValue.EncodeInt64(1), Has.Length.EqualTo(8));
    }

    [Test]
    public void EncodeDouble_round_trips()
    {
        var bytes = LatticeAggregationValue.EncodeDouble(3.14159);

        Assert.That(LatticeAggregationValue.DecodeDouble(bytes), Is.EqualTo(3.14159));
    }

    [Test]
    public void DecodeInt64_wrong_length_throws()
    {
        Assert.That(() => LatticeAggregationValue.DecodeInt64([1, 2, 3]), Throws.ArgumentException);
    }

    [Test]
    public void DecodeDouble_wrong_length_throws()
    {
        Assert.That(() => LatticeAggregationValue.DecodeDouble([1, 2, 3]), Throws.ArgumentException);
    }

    [Test]
    public void DecodeInt64_null_throws()
    {
        Assert.That(() => LatticeAggregationValue.DecodeInt64(null!), Throws.ArgumentNullException);
    }
}
