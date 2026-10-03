using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Tests.Internal;

/// <summary>
/// Covers <see cref="IncrementalHashFraming"/>, the length-prefixed string framing
/// the deterministic fingerprints feed through an incremental hash.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class IncrementalHashFramingTests
{
    [Test]
    public void AppendLengthPrefixed_writes_a_little_endian_length_then_the_utf8_bytes()
    {
        var expected = SHA256.HashData([3, 0, 0, 0, .. Encoding.UTF8.GetBytes("abc")]);

        Assert.That(Fingerprint("abc"), Is.EqualTo(expected));
    }

    [Test]
    public void AppendLengthPrefixed_frames_values_so_a_boundary_shift_changes_the_hash()
    {
        Assert.That(Fingerprint("ab", "c"), Is.Not.EqualTo(Fingerprint("a", "bc")));
    }

    [Test]
    public void AppendLengthPrefixed_handles_a_value_longer_than_the_stack_buffer()
    {
        var longValue = new string('x', 2_000);
        var expected = SHA256.HashData([.. BitConverter.GetBytes(2_000), .. Encoding.UTF8.GetBytes(longValue)]);

        Assert.That(Fingerprint(longValue), Is.EqualTo(expected));
    }

    private static byte[] Fingerprint(params string[] values)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lenPrefix = stackalloc byte[4];
        foreach (var value in values)
        {
            IncrementalHashFraming.AppendLengthPrefixed(hash, value, lenPrefix);
        }

        return hash.GetHashAndReset();
    }
}
