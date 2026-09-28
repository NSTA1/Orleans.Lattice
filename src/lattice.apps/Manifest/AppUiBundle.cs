using System.Buffers;
using System.Collections.Frozen;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Rules and helpers for app UI bundle format v1: path normalisation, digest shape, the
/// media-type allow-list, the size caps, and the bundle digest that is the bundle's cache identity.
/// </summary>
public static class AppUiBundle
{
    /// <summary>The most assets one bundle may list.</summary>
    public const int MaxAssets = 256;

    /// <summary>The largest single asset, in bytes (2 MiB); enforced by whoever reads the asset bytes.</summary>
    public const int MaxAssetBytes = 2 * 1024 * 1024;

    /// <summary>The largest whole bundle, in bytes (16 MiB); enforced by whoever reads the asset bytes.</summary>
    public const int MaxBundleBytes = 16 * 1024 * 1024;

    /// <summary>The longest bundle path, in characters.</summary>
    public const int MaxPathLength = 256;

    /// <summary>The only media types a bundle asset may declare, compared ordinally.</summary>
    public static IReadOnlySet<string> AllowedMediaTypes { get; } = new[]
    {
        "text/html", "text/css", "text/javascript", "image/svg+xml", "image/png", "image/webp", "font/woff2", "application/json",
    }.ToFrozenSet(StringComparer.Ordinal);

    internal static IReadOnlySet<string> IconMediaTypes { get; } =
        new[] { "image/svg+xml", "image/png", "image/webp" }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Returns whether <paramref name="path"/> is a normalised bundle path: 1 to
    /// <see cref="MaxPathLength"/> characters of lower-case ASCII letters, digits, <c>.</c>,
    /// <c>_</c>, <c>-</c> and <c>/</c>; relative (no leading <c>/</c>); no empty, <c>.</c> or
    /// <c>..</c> segment; no trailing <c>/</c>. Backslashes, drive letters, percent-encoding,
    /// queries and fragments are therefore all rejected. Null is never valid.
    /// </summary>
    public static bool IsValidPath(string? path)
    {
        if (string.IsNullOrEmpty(path) || path.Length > MaxPathLength)
            return false;
        var segmentStart = 0;
        for (var i = 0; i <= path.Length; i++)
        {
            if (i == path.Length || path[i] == '/')
            {
                var length = i - segmentStart;
                if (length == 0 || (length == 1 && path[segmentStart] == '.') ||
                    (length == 2 && path[segmentStart] == '.' && path[segmentStart + 1] == '.'))
                    return false;
                segmentStart = i + 1;
                continue;
            }
            if (path[i] is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '.' and not '_' and not '-')
                return false;
        }
        return true;
    }

    /// <summary>Returns whether <paramref name="digest"/> is 64 lower-case hexadecimal characters; null is never valid.</summary>
    public static bool IsValidDigest(string? digest)
    {
        if (digest is not { Length: 64 })
            return false;
        foreach (var c in digest)
            if (c is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
                return false;
        return true;
    }

    /// <summary>
    /// Computes the bundle digest: SHA-256, as lower-case hexadecimal, over the UTF-8 lines
    /// <c>"{path}\0{digest}\n"</c> of every asset, sorted ordinally by path.
    /// </summary>
    /// <param name="assets">The bundle's assets; each must have a non-null path and digest.</param>
    /// <exception cref="ArgumentNullException"><paramref name="assets"/> is null.</exception>
    /// <exception cref="ArgumentException">An asset, or its path or digest, is null.</exception>
    public static string ComputeBundleDigest(IReadOnlyCollection<AppUiAsset> assets)
    {
        ArgumentNullException.ThrowIfNull(assets);
        var sorted = new AppUiAsset[assets.Count];
        var count = 0;
        foreach (var asset in assets)
        {
            if (asset?.Path is null || asset.Digest is null)
                throw new ArgumentException("Every asset must have a path and a digest.", nameof(assets));
            if (count == sorted.Length)
                throw new ArgumentException("The collection yielded more assets than its count.", nameof(assets));
            sorted[count++] = asset;
        }
        if (count != sorted.Length)
            throw new ArgumentException("The collection yielded fewer assets than its count.", nameof(assets));

        // Digest is the tie-break so duplicate paths (which validation rejects) still hash deterministically.
        Array.Sort(sorted, static (x, y) =>
        {
            var byPath = string.CompareOrdinal(x.Path, y.Path);
            return byPath != 0 ? byPath : string.CompareOrdinal(x.Digest, y.Digest);
        });

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = ArrayPool<byte>.Shared.Rent(1024);
        try
        {
            foreach (var asset in sorted)
            {
                var needed = Encoding.UTF8.GetMaxByteCount(asset.Path.Length + asset.Digest.Length) + 2;
                if (needed > buffer.Length)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = ArrayPool<byte>.Shared.Rent(needed);
                }
                var written = Encoding.UTF8.GetBytes(asset.Path, buffer);
                buffer[written++] = 0;
                written += Encoding.UTF8.GetBytes(asset.Digest, buffer.AsSpan(written));
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
}
