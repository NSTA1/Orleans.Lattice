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
    public static string Compute(ReadOnlySpan<byte> content) =>
        Convert.ToHexStringLower(SHA256.HashData(content));

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

        return Convert.ToHexStringLower(hasher.GetHashAndReset());
    }
}
