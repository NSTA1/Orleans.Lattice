using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class StableSecretSetHashTests
{
    [Test]
    public void Compute_returns_same_token_for_same_input()
    {
        var a = StableSecretSetHash.Compute(new[] { "alpha", "beta" });
        var b = StableSecretSetHash.Compute(new[] { "alpha", "beta" });
        Assert.That(b, Is.EqualTo(a));
    }

    [Test]
    public void Compute_distinguishes_partition_boundaries()
    {
        var a = StableSecretSetHash.Compute(new[] { "ab", "c" });
        var b = StableSecretSetHash.Compute(new[] { "a", "bc" });
        Assert.That(b, Is.Not.EqualTo(a));
    }

    [Test]
    public void Compute_distinguishes_order()
    {
        var a = StableSecretSetHash.Compute(new[] { "alpha", "beta" });
        var b = StableSecretSetHash.Compute(new[] { "beta", "alpha" });
        Assert.That(b, Is.Not.EqualTo(a));
    }

    [Test]
    public void Compute_handles_empty_list()
    {
        var a = StableSecretSetHash.Compute(Array.Empty<string?>());
        Assert.That(a, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Compute_throws_when_entries_null()
    {
        Assert.That(
            () => StableSecretSetHash.Compute(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Compute_treats_null_entry_as_empty_string()
    {
        var withNull = StableSecretSetHash.Compute(new string?[] { null, "x" });
        var withEmpty = StableSecretSetHash.Compute(new string?[] { string.Empty, "x" });
        Assert.That(withNull, Is.EqualTo(withEmpty));
    }
}
