using System.Buffers;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using System.Text.Unicode;

namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// The Shell's port of F1's app UI bundle rules (<c>Orleans.Lattice.Apps.AppUiBundle</c> and
/// <c>AppManifestValidator.ValidateUiEntryFragment</c>, issue #3808): path and digest shape,
/// the media-type allow-list, the size caps, the bundle digest, and the entry-fragment check.
/// </summary>
/// <remarks>
/// The Shell deliberately does not reference <c>lattice.apps</c>, a silo-side package that
/// would pull the core library, auth, membership and Orleans server dependencies into the
/// Explorer UI. These are pure functions, so they are ported here and
/// <c>AppFrameBundleRulesParityTests</c> pins them byte-for-byte to F1 over a hostile
/// corpus. That parity test is the permanent guard: change F1 and it fails until this
/// port follows.
/// </remarks>
internal static class AppFrameBundleRules
{
    /// <summary>The most assets one bundle may list.</summary>
    public const int MaxAssets = 256;

    /// <summary>The largest single asset, in bytes (2 MiB).</summary>
    public const int MaxAssetBytes = 2 * 1024 * 1024;

    /// <summary>The largest whole bundle, in bytes (16 MiB).</summary>
    public const int MaxBundleBytes = 16 * 1024 * 1024;

    /// <summary>The longest bundle path, in characters.</summary>
    public const int MaxPathLength = 256;

    /// <summary>The media type of the entry fragment.</summary>
    public const string HtmlMediaType = "text/html";

    /// <summary>The media type of a stylesheet.</summary>
    public const string CssMediaType = "text/css";

    /// <summary>The media type of a script.</summary>
    public const string JavaScriptMediaType = "text/javascript";

    /// <summary>The only media types a bundle asset may declare, compared ordinally.</summary>
    public static readonly FrozenSet<string> AllowedMediaTypes = new[]
    {
        HtmlMediaType, CssMediaType, JavaScriptMediaType, "image/svg+xml", "image/png", "image/webp", "font/woff2", "application/json",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Returns whether <paramref name="path"/> is a normalised bundle path: 1 to
    /// <see cref="MaxPathLength"/> characters of lower-case ASCII letters, digits, <c>.</c>,
    /// <c>_</c>, <c>-</c> and <c>/</c>; relative; no empty, <c>.</c> or <c>..</c> segment; no
    /// trailing <c>/</c>. Null is never valid.
    /// </summary>
    /// <param name="path">The candidate path.</param>
    /// <returns><see langword="true"/> when the path is normalised.</returns>
    public static bool IsValidPath([NotNullWhen(true)] string? path)
    {
        if (string.IsNullOrEmpty(path) || path.Length > MaxPathLength)
        {
            return false;
        }

        var segmentStart = 0;
        for (var i = 0; i <= path.Length; i++)
        {
            if (i == path.Length || path[i] == '/')
            {
                var length = i - segmentStart;
                if (length == 0 || (length == 1 && path[segmentStart] == '.') ||
                    (length == 2 && path[segmentStart] == '.' && path[segmentStart + 1] == '.'))
                {
                    return false;
                }

                segmentStart = i + 1;
                continue;
            }

            if (path[i] is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '.' and not '_' and not '-')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Returns whether <paramref name="digest"/> is 64 lower-case hexadecimal characters; null is never valid.</summary>
    /// <param name="digest">The candidate digest.</param>
    /// <returns><see langword="true"/> when the digest is well formed.</returns>
    public static bool IsValidDigest([NotNullWhen(true)] string? digest)
    {
        if (digest is not { Length: 64 })
        {
            return false;
        }

        foreach (var c in digest)
        {
            if (c is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Computes the bundle digest: SHA-256, as lower-case hexadecimal, over the UTF-8 lines
    /// <c>"{path}\0{digest}\n"</c> of every asset, sorted ordinally by path (then digest).
    /// </summary>
    /// <param name="assets">The bundle's <c>(path, digest)</c> pairs; each must be non-null.</param>
    /// <returns>The bundle digest.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="assets"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">A path or digest is <see langword="null"/>.</exception>
    public static string ComputeBundleDigest(IReadOnlyList<(string Path, string Digest)> assets)
    {
        ArgumentNullException.ThrowIfNull(assets);
        var sorted = new (string Path, string Digest)[assets.Count];
        for (var i = 0; i < sorted.Length; i++)
        {
            var asset = assets[i];
            if (asset.Path is null || asset.Digest is null)
            {
                throw new ArgumentException("Every asset must have a path and a digest.", nameof(assets));
            }

            sorted[i] = asset;
        }

        Array.Sort(sorted, static (x, y) =>
        {
            var byPath = string.CompareOrdinal(x.Path, y.Path);
            return byPath != 0 ? byPath : string.CompareOrdinal(x.Digest, y.Digest);
        });

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = ArrayPool<byte>.Shared.Rent(1024);
        try
        {
            foreach (var (path, digest) in sorted)
            {
                var needed = Encoding.UTF8.GetMaxByteCount(path.Length + digest.Length) + 2;
                if (needed > buffer.Length)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = ArrayPool<byte>.Shared.Rent(needed);
                }

                var written = Encoding.UTF8.GetBytes(path, buffer);
                buffer[written++] = 0;
                written += Encoding.UTF8.GetBytes(digest, buffer.AsSpan(written));
                buffer[written++] = (byte)'\n';
                hash.AppendData(buffer, 0, written);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        Span<byte> result = stackalloc byte[32];
        hash.GetHashAndReset(result);
        return Convert.ToHexStringLower(result);
    }

    /// <summary>Computes the lower-case hexadecimal SHA-256 of <paramref name="bytes"/>.</summary>
    /// <param name="bytes">The bytes to hash.</param>
    /// <returns>The digest.</returns>
    public static string ComputeDigest(ReadOnlySpan<byte> bytes)
    {
        Span<byte> result = stackalloc byte[32];
        SHA256.HashData(bytes, result);
        return Convert.ToHexStringLower(result);
    }

    /// <summary>
    /// Returns whether the entry asset's bytes are an acceptable fragment, exactly as F1's
    /// <c>ValidateUiEntryFragment</c> judges them: at most <see cref="MaxAssetBytes"/>,
    /// well-formed UTF-8, no <c>&lt;script</c> element (any case, any suffix), and no
    /// <c>&lt;html</c> or <c>&lt;head</c> element.
    /// </summary>
    /// <param name="utf8Fragment">The entry asset's bytes.</param>
    /// <returns><see langword="true"/> when F1 would report no error.</returns>
    public static bool IsValidEntryFragment(ReadOnlySpan<byte> utf8Fragment)
    {
        if (utf8Fragment.Length > MaxAssetBytes)
        {
            return false;
        }

        return Utf8.IsValid(utf8Fragment)
            && !ContainsTag(utf8Fragment, "script"u8, prefixOnly: true)
            && !ContainsTag(utf8Fragment, "html"u8, prefixOnly: false)
            && !ContainsTag(utf8Fragment, "head"u8, prefixOnly: false);
    }

    private static bool ContainsTag(ReadOnlySpan<byte> html, ReadOnlySpan<byte> name, bool prefixOnly)
    {
        for (var start = html.IndexOf((byte)'<'); start >= 0;)
        {
            var rest = html[(start + 1)..];
            if (rest.Length >= name.Length && Ascii.EqualsIgnoreCase(rest[..name.Length], name) &&
                (prefixOnly || rest.Length == name.Length || rest[name.Length] is (byte)' ' or (byte)'\t' or (byte)'\n' or (byte)'\f' or (byte)'\r' or (byte)'/' or (byte)'>'))
            {
                return true;
            }

            var next = rest.IndexOf((byte)'<');
            if (next < 0)
            {
                return false;
            }

            start += next + 1;
        }

        return false;
    }
}
