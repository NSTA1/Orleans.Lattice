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

    [Test]
    public void Compute_distinguishes_embedded_nul_from_partition_boundaries()
    {
        // Regression: a single entry carrying embedded NUL characters must not
        // alias with a multi-entry set whose partition boundaries fall on those
        // NULs. With the former single-NUL separator both inputs produced the
        // identical byte stream 61 00 00 00 00 00 63 00 00 and collided.
        var single = StableSecretSetHash.Compute(new[] { "a\u0000\u0000c" });
        var split = StableSecretSetHash.Compute(new[] { "a", "\u0000", "c" });
        Assert.That(split, Is.Not.EqualTo(single));
    }

    [Test]
    public void Compute_distinguishes_nul_entry_from_repeated_empty_entries()
    {
        // Regression: {"\0"} is one partition holding a NUL code unit; three
        // empty partitions are a different ordered set. The former single-NUL
        // separator hashed both to the same 00 00 00 byte stream.
        var nul = StableSecretSetHash.Compute(new[] { "\u0000" });
        var threeEmpties = StableSecretSetHash.Compute(new string?[] { null, null, null });
        Assert.That(threeEmpties, Is.Not.EqualTo(nul));
    }
}
