using System.Security.Cryptography;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Helpers for generating and validating
/// <c>Orleans.Lattice.Replication</c> shared secrets. The format is
/// deliberately opaque: a cryptographically-strong random byte string
/// rendered as URL-safe base-64. Operators do not need to memorise the
/// format; they only need to copy the output into the
/// <see cref="LatticeReplicationEnvironmentVariables.Secret"/>
/// environment variable on every peer.
/// </summary>
public static class LatticeReplicationSharedSecret
{
    /// <summary>
    /// The minimum acceptable secret length in URL-safe base-64
    /// characters. A value below this floor produces a brittle credential
    /// (under 192 bits of entropy after base-64 expansion); the
    /// authenticator does not enforce this floor at runtime because
    /// rotation may briefly accept legacy secrets that pre-date the
    /// floor, but <see cref="Generate"/> always produces values well
    /// above it.
    /// </summary>
    public const int MinimumLength = 32;

    /// <summary>
    /// Generates a fresh shared secret. The default byte length is 32,
    /// which renders to a 43-character URL-safe base-64 string and
    /// supplies 256 bits of entropy. Callers that need a longer secret
    /// (e.g. compliance regimes that mandate a specific minimum) pass
    /// <paramref name="byteLength"/> explicitly.
    /// </summary>
    /// <param name="byteLength">
    /// The number of random bytes to draw before base-64 encoding.
    /// Must be greater than or equal to 24 (192 bits of entropy).
    /// </param>
    public static string Generate(int byteLength = 32)
    {
        if (byteLength < 24)
        {
            throw new ArgumentOutOfRangeException(
                nameof(byteLength),
                byteLength,
                "Shared secrets must carry at least 192 bits of entropy; pass byteLength >= 24.");
        }

        Span<byte> buffer = stackalloc byte[256];
        if (byteLength > buffer.Length)
        {
            var heap = new byte[byteLength];
            RandomNumberGenerator.Fill(heap);
            return ToUrlSafeBase64(heap);
        }

        var slice = buffer[..byteLength];
        RandomNumberGenerator.Fill(slice);
        return ToUrlSafeBase64(slice);
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="secret"/> is
    /// non-null, non-empty, and at least <see cref="MinimumLength"/>
    /// characters long. Diagnostic helper for the startup safety
    /// validator; not a security gate on the hot path.
    /// </summary>
    public static bool IsWellFormed(string? secret)
    {
        return !string.IsNullOrWhiteSpace(secret) && secret.Length >= MinimumLength;
    }

    /// <summary>
    /// Constant-time string comparison. Used by the authenticator to
    /// compare the presented credential against every accepted secret
    /// without leaking which prefix matched. Returns
    /// <see langword="false"/> when either input is null.
    /// </summary>
    /// <remarks>
    /// On .NET 7+, <see cref="CryptographicOperations.FixedTimeEquals(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>
    /// returns <see langword="false"/> on length-mismatched spans
    /// rather than throwing, so unequal-length inputs are safe.
    /// </remarks>
    public static bool FixedTimeEquals(string? a, string? b)
    {
        if (a is null || b is null)
        {
            return false;
        }

        // Stackalloc for typical secrets (well-formed values are 43
        // chars URL-safe base-64, so 43 UTF-8 bytes). The 256-byte
        // ceiling covers every operator-generated secret we expect
        // without heap-allocating; the rare overlong outlier falls
        // back to heap allocation rather than truncating.
        var utf8 = System.Text.Encoding.UTF8;
        var maxA = utf8.GetMaxByteCount(a.Length);
        var maxB = utf8.GetMaxByteCount(b.Length);
        const int StackThreshold = 256;
        Span<byte> stackA = stackalloc byte[StackThreshold];
        Span<byte> stackB = stackalloc byte[StackThreshold];
        var bufA = maxA <= StackThreshold ? stackA : (Span<byte>)new byte[maxA];
        var bufB = maxB <= StackThreshold ? stackB : (Span<byte>)new byte[maxB];
        var lenA = utf8.GetBytes(a, bufA);
        var lenB = utf8.GetBytes(b, bufB);
        try
        {
            return CryptographicOperations.FixedTimeEquals(bufA[..lenA], bufB[..lenB]);
        }
        finally
        {
            // Wipe the transient UTF-8 plaintext copies of both secrets from
            // the stack (or heap, for the overlong fallback) before returning
            // so a later stack-frame reuse or heap read cannot recover them.
            CryptographicOperations.ZeroMemory(bufA[..lenA]);
            CryptographicOperations.ZeroMemory(bufB[..lenB]);
        }
    }

    private static string ToUrlSafeBase64(ReadOnlySpan<byte> bytes)
    {
        var raw = Convert.ToBase64String(bytes);
        // RFC 4648 URL-safe alphabet: '+' -> '-', '/' -> '_', drop padding.
        return raw.Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }
}
