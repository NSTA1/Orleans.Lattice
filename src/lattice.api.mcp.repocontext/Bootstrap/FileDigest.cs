using System.IO.Hashing;
using System.Security.Cryptography;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Computes and compares the stable content digest a bootstrap scan stamps onto
/// every <see cref="FileNode"/>. A digest is a deterministic, lower-case hex
/// fingerprint of a file's raw bytes - independent of path, timestamp, or platform
/// - so an unchanged file always fingerprints to the same value, which is what lets
/// a re-run detect "nothing changed" and skip the write and the re-embed.
/// <para>
/// <b>Non-cryptographic by design.</b> The digest is content-change detection only,
/// never a security boundary, so the default algorithm is <see cref="XxHash128"/> -
/// the same non-cryptographic, roughly ten-times-cheaper-than-SHA-256 fingerprint
/// the core library already uses for its projection digests. On a large cold walk,
/// where the read-and-hash dominates, this is the difference that keeps ingestion
/// cheap.
/// </para>
/// <para>
/// <b>Self-describing and non-breaking.</b> A modern digest is written
/// <c>"&lt;algo&gt;:&lt;hex&gt;"</c> (for example <c>"xx128:9a3f..."</c>); a legacy
/// bare 64-character hex string with no prefix is an implicit SHA-256 digest.
/// <see cref="Matches(string, ReadOnlySpan{byte})"/> always recomputes the content
/// under the <em>stored</em> digest's own algorithm, so a store written before the
/// switch keeps reconciling correctly with no forced re-hash of the whole tree: a
/// file that never changes keeps its legacy digest (and is skipped by the walk's
/// stat fast-path anyway), while a file that changes is rewritten with the modern
/// digest, so the algorithm migrates lazily as content evolves.
/// </para>
/// </summary>
internal static class FileDigest
{
    /// <summary>The algorithm tag prefixing a modern XxHash128 digest.</summary>
    private const string XxHash128Prefix = "xx128:";

    /// <summary>The explicit algorithm tag for a SHA-256 digest.</summary>
    private const string Sha256Prefix = "sha256:";

    /// <summary>
    /// Computes the default modern content digest of <paramref name="content"/>:
    /// the tagged, lower-case hex XxHash128 fingerprint (<c>"xx128:"</c> followed by
    /// 32 hex characters).
    /// </summary>
    /// <param name="content">The file bytes to digest.</param>
    /// <returns>The modern tagged digest string.</returns>
    internal static string Compute(ReadOnlySpan<byte> content)
    {
        Span<byte> hash = stackalloc byte[16];
        XxHash128.Hash(content, hash);
        return XxHash128Prefix + Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Reports whether <paramref name="content"/> still matches
    /// <paramref name="storedDigest"/>, recomputing the content's fingerprint under
    /// the <em>stored</em> digest's own algorithm so the comparison is correct even
    /// when the stored value predates the current default algorithm. A stored digest
    /// with no algorithm prefix is treated as a legacy SHA-256 digest.
    /// </summary>
    /// <param name="storedDigest">The digest currently stored for the file. Must not
    /// be <see langword="null"/>.</param>
    /// <param name="content">The file's current bytes.</param>
    /// <returns><see langword="true"/> when the content is unchanged relative to the
    /// stored digest.</returns>
    internal static bool Matches(string storedDigest, ReadOnlySpan<byte> content)
    {
        ArgumentNullException.ThrowIfNull(storedDigest);
        return string.Equals(ComputeUnder(storedDigest, content), storedDigest, StringComparison.Ordinal);
    }

    /// <summary>
    /// Recomputes <paramref name="content"/>'s digest in the exact string shape the
    /// stored digest uses, so a byte-for-byte string comparison decides equality.
    /// </summary>
    private static string ComputeUnder(string storedDigest, ReadOnlySpan<byte> content)
    {
        if (storedDigest.StartsWith(XxHash128Prefix, StringComparison.Ordinal))
        {
            return Compute(content);
        }

        // Legacy: an explicit "sha256:" prefix, or a bare hex string (which the
        // original implementation wrote unprefixed) - both are SHA-256.
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(content, hash);
        var hex = Convert.ToHexStringLower(hash);
        return storedDigest.StartsWith(Sha256Prefix, StringComparison.Ordinal)
            ? Sha256Prefix + hex
            : hex;
    }
}
