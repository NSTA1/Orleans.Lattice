using System.Buffers;
using System.IO.Hashing;
using System.Security.Cryptography;
using System.Text;

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

    /// <summary>The raw byte width of an XxHash128 fingerprint.</summary>
    private const int XxHash128Bytes = 16;

    /// <summary>
    /// The widest digest string any shape here produces: the <c>"sha256:"</c> tag
    /// plus 64 hex characters. Every other shape is shorter, so one buffer of this
    /// width formats all of them without a per-shape size calculation.
    /// </summary>
    private const int MaxDigestChars = 7 + (SHA256.HashSizeInBytes * 2);

    /// <summary>
    /// The exact UTF-8 byte width of a modern tagged digest: the six-character
    /// <c>"xx128:"</c> tag plus 32 hex characters, all ASCII.
    /// </summary>
    internal const int Utf8DigestBytes = 6 + (XxHash128Bytes * 2);

    /// <summary>
    /// Computes the default modern content digest of <paramref name="content"/>:
    /// the tagged, lower-case hex XxHash128 fingerprint (<c>"xx128:"</c> followed by
    /// 32 hex characters).
    /// </summary>
    /// <param name="content">The file bytes to digest.</param>
    /// <returns>The modern tagged digest string.</returns>
    internal static string Compute(ReadOnlySpan<byte> content)
    {
        Span<char> text = stackalloc char[MaxDigestChars];
        return new string(text[..ComputeModern(content, text)]);
    }

    /// <summary>
    /// Writes the modern tagged digest of <paramref name="content"/> into
    /// <paramref name="destination"/> as ASCII bytes, returning the count written
    /// (always <see cref="Utf8DigestBytes"/>). This is what lets a caller that is
    /// feeding a hash fold the digest straight in without materialising it as a
    /// string first.
    /// </summary>
    /// <param name="content">The bytes to digest.</param>
    /// <param name="destination">Receives the tagged digest. Must hold at least
    /// <see cref="Utf8DigestBytes"/> bytes.</param>
    /// <returns>The number of bytes written.</returns>
    internal static int ComputeUtf8(ReadOnlySpan<byte> content, Span<byte> destination)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, Utf8DigestBytes);
        Span<char> text = stackalloc char[Utf8DigestBytes];
        var length = ComputeModern(content, text);
        for (var i = 0; i < length; i++)
        {
            // Every character of a tagged digest is ASCII, so the narrowing is
            // exact and no transcoder is needed.
            destination[i] = (byte)text[i];
        }

        return length;
    }

    /// <summary>
    /// Formats the modern tagged digest of <paramref name="content"/> into
    /// <paramref name="destination"/>, returning the character count written.
    /// </summary>
    private static int ComputeModern(ReadOnlySpan<byte> content, Span<char> destination)
    {
        Span<byte> hash = stackalloc byte[XxHash128Bytes];
        XxHash128.Hash(content, hash);
        XxHash128Prefix.CopyTo(destination);
        Convert.TryToHexStringLower(hash, destination[XxHash128Prefix.Length..], out var hexChars);
        return XxHash128Prefix.Length + hexChars;
    }

    /// <summary>
    /// The UTF-8 scratch budget <see cref="Compute(ReadOnlySpan{char})"/> takes on the
    /// stack before renting. Most digested slices - a field, a property, a short method -
    /// transcode well inside it, so the common case rents nothing at all.
    /// </summary>
    private const int StackTranscodeBytes = 512;

    /// <summary>
    /// Computes the same digest as <see cref="Compute(ReadOnlySpan{byte})"/> for text
    /// that is already in memory as UTF-16, transcoding it through a stack or pooled
    /// buffer rather than through a throwaway array. This is what lets a caller digest a
    /// slice of a file it already holds without materialising that slice as a string and
    /// then materialising its UTF-8 encoding a second time.
    /// </summary>
    /// <param name="text">The text to digest.</param>
    /// <returns>The modern tagged digest string.</returns>
    internal static string Compute(ReadOnlySpan<char> text)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(text.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackTranscodeBytes
            ? stackalloc byte[StackTranscodeBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        var written = 0;
        try
        {
            written = Encoding.UTF8.GetBytes(text, buffer);
            return Compute(buffer[..written]);
        }
        finally
        {
            if (rented is not null)
            {
                rented.AsSpan(0, written).Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
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
        if (storedDigest.Length > MaxDigestChars)
        {
            return false;
        }

        Span<char> recomputed = stackalloc char[MaxDigestChars];
        var length = ComputeUnder(storedDigest, content, recomputed);
        return storedDigest.AsSpan().SequenceEqual(recomputed[..length]);
    }

    /// <summary>
    /// Recomputes <paramref name="content"/>'s digest in the exact string shape the
    /// stored digest uses, so a byte-for-byte comparison decides equality. The
    /// result is formatted into <paramref name="destination"/> rather than returned
    /// as a string: a reconcile pass calls this once per file on every walk and the
    /// recomputed digest is discarded the moment the comparison is made, so
    /// materialising it would allocate a string per file for nothing.
    /// </summary>
    private static int ComputeUnder(string storedDigest, ReadOnlySpan<byte> content, Span<char> destination)
    {
        if (storedDigest.StartsWith(XxHash128Prefix, StringComparison.Ordinal))
        {
            return ComputeModern(content, destination);
        }

        // Legacy: an explicit "sha256:" prefix, or a bare hex string (which the
        // original implementation wrote unprefixed) - both are SHA-256.
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(content, hash);
        var offset = 0;
        if (storedDigest.StartsWith(Sha256Prefix, StringComparison.Ordinal))
        {
            Sha256Prefix.CopyTo(destination);
            offset = Sha256Prefix.Length;
        }

        Convert.TryToHexStringLower(hash, destination[offset..], out var hexChars);
        return offset + hexChars;
    }
}
