using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class LatticeReplicationSharedSecretTests
{
    [Test]
    public void Generate_default_produces_url_safe_base64_string_of_expected_entropy()
    {
        var s = LatticeReplicationSharedSecret.Generate();
        Assert.That(s, Is.Not.Null.And.Not.Empty);
        Assert.That(s.Length, Is.GreaterThanOrEqualTo(LatticeReplicationSharedSecret.MinimumLength));
        foreach (var c in s)
        {
            Assert.That(
                (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_',
                Is.True,
                $"unexpected char '{c}' in url-safe base64 output");
        }
    }

    [Test]
    public void Generate_two_calls_produce_distinct_secrets()
    {
        var a = LatticeReplicationSharedSecret.Generate();
        var b = LatticeReplicationSharedSecret.Generate();
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Generate_throws_when_byte_length_below_floor()
    {
        Assert.That(
            () => LatticeReplicationSharedSecret.Generate(byteLength: 23),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Generate_accepts_floor_exactly()
    {
        var s = LatticeReplicationSharedSecret.Generate(byteLength: 24);
        Assert.That(s, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Generate_accepts_very_large_byte_length()
    {
        var s = LatticeReplicationSharedSecret.Generate(byteLength: 512);
        Assert.That(s.Length, Is.GreaterThan(LatticeReplicationSharedSecret.MinimumLength));
    }

    [Test]
    public void IsWellFormed_returns_false_for_null_empty_or_too_short()
    {
        Assert.That(LatticeReplicationSharedSecret.IsWellFormed(null), Is.False);
        Assert.That(LatticeReplicationSharedSecret.IsWellFormed(string.Empty), Is.False);
        Assert.That(LatticeReplicationSharedSecret.IsWellFormed("   "), Is.False);
        Assert.That(LatticeReplicationSharedSecret.IsWellFormed(new string('a', LatticeReplicationSharedSecret.MinimumLength - 1)), Is.False);
    }

    [Test]
    public void IsWellFormed_returns_true_for_well_formed_secret()
    {
        Assert.That(LatticeReplicationSharedSecret.IsWellFormed(LatticeReplicationSharedSecret.Generate()), Is.True);
    }

    [Test]
    public void FixedTimeEquals_returns_true_for_identical_strings()
    {
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals("abc-123", "abc-123"), Is.True);
    }

    [Test]
    public void FixedTimeEquals_returns_false_for_different_strings()
    {
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals("abc-123", "abc-124"), Is.False);
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals("abc", "abcd"), Is.False);
    }

    [Test]
    public void FixedTimeEquals_returns_false_for_null_inputs()
    {
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals(null, "abc"), Is.False);
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals("abc", null), Is.False);
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals(null, null), Is.False);
    }

    [Test]
    public void FixedTimeEquals_returns_true_for_equal_inputs_that_force_heap_fallback()
    {
        // Stack threshold inside FixedTimeEquals is 256 bytes; values
        // whose UTF-8 max-byte-count exceeds that threshold must take
        // the heap-allocated fallback path. Use a length > 256/3 to
        // exceed the max-byte-count even after multibyte expansion.
        var big = new string('x', 400);
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals(big, big), Is.True);
    }

    [Test]
    public void FixedTimeEquals_returns_false_for_unequal_long_inputs_that_force_heap_fallback()
    {
        var bigA = new string('x', 400);
        var bigB = new string('x', 400).Insert(200, "Y");
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals(bigA, bigB), Is.False);
    }

    [Test]
    public void FixedTimeEquals_handles_mixed_branch_sizes()
    {
        // a stays in stack buffer, b goes to heap.
        var small = new string('x', 32);
        var big = new string('x', 400);
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals(small, big), Is.False);
        Assert.That(LatticeReplicationSharedSecret.FixedTimeEquals(big, small), Is.False);
    }

    [Test]
    public void IsWellFormed_returns_true_for_arbitrary_long_strings_even_if_not_base64()
    {
        // IsWellFormed is documented as a length-only diagnostic helper,
        // not a security gate. Pin that contract so a future "validate
        // alphabet" tightening doesn't silently land.
        var nonBase64 = new string('!', LatticeReplicationSharedSecret.MinimumLength);
        Assert.That(LatticeReplicationSharedSecret.IsWellFormed(nonBase64), Is.True);
    }
}
