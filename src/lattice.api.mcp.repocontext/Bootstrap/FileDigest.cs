using System.Security.Cryptography;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Computes the stable content digest a bootstrap scan stamps onto every
/// <see cref="FileNode"/>. The digest is the lower-case hex SHA-256 of the file's
/// raw bytes, so it is deterministic and independent of path, timestamp, or
/// platform: an unchanged file always hashes to the same value, which is what
/// lets a re-run detect "nothing changed" and skip the write.
/// </summary>
internal static class FileDigest
{
    /// <summary>
    /// Computes the lower-case hex SHA-256 digest of <paramref name="content"/>.
    /// </summary>
    /// <param name="content">The file bytes to digest.</param>
    /// <returns>A 64-character lower-case hex string.</returns>
    internal static string Compute(ReadOnlySpan<byte> content)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(content, hash);
        return Convert.ToHexStringLower(hash);
    }
}
