using System.Security.Cryptography;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Computes stable, content-addressed digests for backup payloads: the lowercase
/// hexadecimal SHA-256 of the bytes. The capture engine records the same digest
/// for each artifact (restore and backup-health verification re-hash the bytes
/// against it) and derives the backup id from it, so a retried capture that
/// produces identical content registers the same backup id. The artifact itself
/// is not deduplicated: each capture writes it under its own per-capture artifact
/// id.
/// </summary>
public static class BackupContentHash
{
    /// <summary>
    /// Computes the lowercase hexadecimal SHA-256 content address of
    /// <paramref name="content"/>.
    /// </summary>
    /// <param name="content">The bytes to address.</param>
    /// <returns>The 64-character lowercase hexadecimal digest.</returns>
    public static string Compute(ReadOnlySpan<byte> content)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(content, digest);
        return Convert.ToHexStringLower(digest);
    }

    /// <summary>
    /// Computes the lowercase hexadecimal SHA-256 content address of an ordered
    /// sequence of chunks, as if the chunks were concatenated. Lets a streaming
    /// producer content-address a payload without buffering it whole.
    /// </summary>
    /// <param name="chunks">The ordered chunks to address. Must not be <c>null</c>.</param>
    /// <returns>The 64-character lowercase hexadecimal digest.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="chunks"/> is <c>null</c>.</exception>
    public static string Compute(IEnumerable<ReadOnlyMemory<byte>> chunks)
    {
        ArgumentNullException.ThrowIfNull(chunks);
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var chunk in chunks)
        {
            hasher.AppendData(chunk.Span);
        }

        return ToHexLowerAndReset(hasher);
    }

    /// <summary>
    /// Formats the hash accumulated in <paramref name="hasher"/> as the lowercase
    /// hexadecimal digest and resets it for reuse.
    /// </summary>
    /// <remarks>
    /// The parameterless <see cref="IncrementalHash.GetHashAndReset()"/> returns a
    /// freshly allocated 32-byte array that every caller here reads once, hands to
    /// the hex formatter, and drops. Filling a stack span instead removes that
    /// per-hash allocation from the capture, restore, and health-verification
    /// paths, which hash once per artifact and so pay it per artifact.
    /// </remarks>
    /// <param name="hasher">The accumulating hasher. Must not be <c>null</c>.</param>
    /// <returns>The 64-character lowercase hexadecimal digest.</returns>
    internal static string ToHexLowerAndReset(IncrementalHash hasher)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        hasher.GetHashAndReset(digest);
        return Convert.ToHexStringLower(digest);
    }
}
