using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Deterministic, opaque token helpers for the <c>repocontext_context</c> tool's
/// reuse economics. A <b>receipt</b> uniquely and stably names one delivered unit
/// (a path pointer, a body span, or an outline symbol) so a caller can hand it back
/// on a later call to suppress exactly that unit; a <b>content hash</b> names a file
/// version; a <b>possession token</b> pairs a path with a content hash to name a
/// whole-file version the caller already holds.
/// <para>
/// Every token is a pure function of its inputs (a SHA-256 over the tokenizer-stable
/// UTF-8 encoding of the parts), so the same unit always yields the same receipt on
/// any replica and across calls, and the token is opaque to the caller (a lowercase
/// hex digest that reveals nothing about the content). Because a receipt or a
/// possession token can only ever <b>suppress</b> delivery - never widen it - a
/// forged or stale token is fail-safe: at worst the caller under-receives content it
/// claimed to already hold.
/// </para>
/// </summary>
internal static class RepoContextReuse
{
    /// <summary>The unit kind for a path pointer delivered at <see cref="RepoContextContextDetail.Paths"/>.</summary>
    internal const string PointerKind = "pointer";

    /// <summary>The unit kind for a whole-body span delivered at <see cref="RepoContextContextDetail.Slices"/>.</summary>
    internal const string SpanKind = "span";

    /// <summary>The unit kind for one declared-symbol line delivered at <see cref="RepoContextContextDetail.Outline"/>.</summary>
    internal const string OutlineKind = "outline";

    /// <summary>The reuse-acknowledgement kind used when a whole file is suppressed by a possession claim.</summary>
    internal const string FileKind = "file";

    private const char PossessionSeparator = '\u0000';
    private const char KnownSeparator = '@';

    /// <summary>
    /// Computes the stable content hash of a file version: the lowercase hex SHA-256
    /// of the UTF-8 encoding of <paramref name="content"/>. Two identical bodies hash
    /// equal; any change yields a different hash.
    /// </summary>
    /// <param name="content">The file body text. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="content"/> is null.</exception>
    internal static string ContentHash(string content)
    {
        ArgumentNullException.ThrowIfNull(content);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(content), hash);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Computes the stable, opaque receipt for one delivered unit as the lowercase
    /// hex SHA-256 over <c>repoId, path, contentHash, kind, unitKey</c>. The
    /// <paramref name="unitKey"/> distinguishes sibling units of the same file and
    /// kind (an outline symbol's fully-qualified name); it is empty for the single
    /// pointer or span a file carries.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="path">The repository-relative file path. Must not be <see langword="null"/>.</param>
    /// <param name="contentHash">The file version content hash. Must not be <see langword="null"/>.</param>
    /// <param name="kind">The unit kind (<see cref="PointerKind"/>, <see cref="SpanKind"/>, or <see cref="OutlineKind"/>). Must not be <see langword="null"/>.</param>
    /// <param name="unitKey">The per-file, per-kind unit discriminator (empty for a pointer or span). Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    internal static string Receipt(string repoId, string path, string contentHash, string kind, string unitKey)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(contentHash);
        ArgumentNullException.ThrowIfNull(kind);
        ArgumentNullException.ThrowIfNull(unitKey);

        var input = $"{repoId}{PossessionSeparator}{path}{PossessionSeparator}{contentHash}{PossessionSeparator}{kind}{PossessionSeparator}{unitKey}";
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(input), hash);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Builds the whole-file possession token for a file version: <c>path\0hash</c>.
    /// The <c>NUL</c> separator cannot appear in a path, so the token is unambiguous.
    /// </summary>
    /// <param name="path">The repository-relative file path. Must not be <see langword="null"/>.</param>
    /// <param name="contentHash">The file version content hash. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    internal static string PossessionToken(string path, string contentHash)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(contentHash);
        return string.Concat(path, PossessionSeparator.ToString(), contentHash);
    }

    /// <summary>
    /// Parses a wire <c>known</c> claim of the form <c>path@hash</c> into its path and
    /// content-hash parts, splitting on the <b>last</b> <c>'@'</c> so a path that
    /// itself contains an <c>'@'</c> parses correctly. Fails closed: a claim with no
    /// separator, an empty path, or an empty hash returns <see langword="false"/> and
    /// is ignored by the caller rather than honoured.
    /// </summary>
    /// <param name="known">The wire claim. May be <see langword="null"/> or empty.</param>
    /// <param name="path">The parsed path, when the method returns <see langword="true"/>.</param>
    /// <param name="contentHash">The parsed content hash, when the method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the claim is well formed; otherwise <see langword="false"/>.</returns>
    internal static bool TryParseKnown(string? known, out string path, out string contentHash)
    {
        path = string.Empty;
        contentHash = string.Empty;
        if (string.IsNullOrEmpty(known))
        {
            return false;
        }

        var at = known.LastIndexOf(KnownSeparator);
        if (at <= 0 || at >= known.Length - 1)
        {
            return false;
        }

        path = known[..at];
        contentHash = known[(at + 1)..];
        return true;
    }
}
