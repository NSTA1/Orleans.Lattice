namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the seeded generator that makes training reproducible.
/// </summary>
[TestFixture]
public sealed class VectorRandomTests
{
    [Test]
    public void The_same_seed_produces_the_same_stream()
    {
        var left = new VectorRandom(12_345);
        var right = new VectorRandom(12_345);

        for (var i = 0; i < 64; i++)
        {
            Assert.That(right.NextUInt64(), Is.EqualTo(left.NextUInt64()));
        }
    }

    [Test]
    public void Different_seeds_produce_different_streams()
    {
        var left = new VectorRandom(1);
        var right = new VectorRandom(2);

        var differs = false;
        for (var i = 0; i < 16 && !differs; i++)
        {
            differs = left.NextUInt64() != right.NextUInt64();
        }

        Assert.That(differs, Is.True);
    }

    [Test]
    public void A_zero_seed_still_produces_a_non_degenerate_stream()
    {
        var random = new VectorRandom(0);

        var distinct = new HashSet<ulong>();
        for (var i = 0; i < 32; i++)
        {
            distinct.Add(random.NextUInt64());
        }

        Assert.That(distinct, Has.Count.GreaterThan(1));
    }

    [Test]
    public void NextInt32_stays_inside_the_requested_bound()
    {
        var random = new VectorRandom(7);

        for (var i = 0; i < 10_000; i++)
        {
            Assert.That(random.NextInt32(10), Is.InRange(0, 9));
        }
    }

    [Test]
    public void NextInt32_with_a_bound_of_one_always_returns_zero()
    {
        var random = new VectorRandom(7);

        for (var i = 0; i < 100; i++)
        {
            Assert.That(random.NextInt32(1), Is.EqualTo(0));
        }
    }

    [Test]
    public void NextInt32_covers_its_whole_range()
    {
        var random = new VectorRandom(11);
        var seen = new HashSet<int>();

        for (var i = 0; i < 5_000; i++)
        {
            seen.Add(random.NextInt32(8));
        }

        Assert.That(seen, Has.Count.EqualTo(8));
    }
}
