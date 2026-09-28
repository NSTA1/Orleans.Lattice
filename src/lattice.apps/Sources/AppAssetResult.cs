using System.Security.Cryptography;

namespace Orleans.Lattice.Apps.Sources;

/// <summary>
/// The structured outcome of <see cref="IAppCatalogSource.OpenAssetAsync"/>. Only an
/// <see cref="AppAssetStatus.Opened"/> result carries bytes, and the only way to create one is
/// <see cref="Verify"/>, which computes the content's SHA-256 digest and compares it with the expected digest,
/// so a source cannot hand out unverified bytes. A failure is data rather than an exception.
/// </summary>
public sealed class AppAssetResult
{
    /// <summary>The length, in characters, of a SHA-256 digest written as lower-case hex.</summary>
    public const int Sha256HexLength = 64;

    private const int Sha256Length = 32;

    private AppAssetResult(
        AppAssetStatus status,
        string path,
        ReadOnlyMemory<byte> content,
        string? mediaType,
        string? actualSha256,
        IReadOnlyList<AppManifestError> errors)
    {
        Status = status;
        Path = path;
        Content = content;
        MediaType = mediaType;
        ActualSha256 = actualSha256;
        Errors = errors;
    }

    /// <summary>The outcome category.</summary>
    public AppAssetStatus Status { get; }

    /// <summary>Whether the asset was opened and verified, so <see cref="Content"/> and <see cref="MediaType"/> are set.</summary>
    public bool IsOpened => Status == AppAssetStatus.Opened;

    /// <summary>The asset path the outcome concerns, as the caller supplied it.</summary>
    public string Path { get; }

    /// <summary>The verified asset bytes when opened; empty otherwise.</summary>
    public ReadOnlyMemory<byte> Content { get; }

    /// <summary>The asset's media type when opened; null otherwise.</summary>
    public string? MediaType { get; }

    /// <summary>
    /// The lower-case hex SHA-256 digest of the bytes the source read, on a
    /// <see cref="AppAssetStatus.DigestMismatch"/> where the bytes were read; otherwise null.
    /// </summary>
    public string? ActualSha256 { get; }

    /// <summary>Read-only diagnostics; empty when opened and non-empty otherwise.</summary>
    public IReadOnlyList<AppManifestError> Errors { get; }

    /// <summary>
    /// Verifies <paramref name="content"/> against <paramref name="expectedSha256"/> and returns an
    /// <see cref="AppAssetStatus.Opened"/> result carrying the bytes only when the digests are equal; otherwise
    /// a <see cref="AppAssetStatus.DigestMismatch"/> that carries no bytes. An expected digest that is not
    /// <see cref="Sha256HexLength"/> lower-case hex characters is a mismatch.
    /// </summary>
    /// <param name="path">The asset path, as the caller supplied it.</param>
    /// <param name="content">The bytes the source read. They are not copied, so the caller must not mutate them afterwards.</param>
    /// <param name="mediaType">The asset's media type.</param>
    /// <param name="expectedSha256">The expected SHA-256 digest, as lower-case hex.</param>
    /// <exception cref="ArgumentNullException"><paramref name="path"/>, <paramref name="mediaType"/> or <paramref name="expectedSha256"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="mediaType"/> is empty.</exception>
    public static AppAssetResult Verify(string path, ReadOnlyMemory<byte> content, string mediaType, string expectedSha256)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(mediaType);
        ArgumentNullException.ThrowIfNull(expectedSha256);
        if (mediaType.Length == 0)
            throw new ArgumentException("A media type must not be empty.", nameof(mediaType));

        Span<byte> expected = stackalloc byte[Sha256Length];
        if (!TryParseSha256Hex(expectedSha256, expected))
        {
            return new(AppAssetStatus.DigestMismatch, path, default, null, null,
                [new("digest-format", "$.sha256", "The expected digest is not a lower-case hex SHA-256 digest.")]);
        }

        Span<byte> actual = stackalloc byte[Sha256Length];
        SHA256.HashData(content.Span, actual);
        if (!CryptographicOperations.FixedTimeEquals(actual, expected))
        {
            return new(AppAssetStatus.DigestMismatch, path, default, null, Convert.ToHexStringLower(actual),
                [new("digest-mismatch", "$.sha256", $"The content of asset '{path}' does not match its expected digest.")]);
        }

        return new(AppAssetStatus.Opened, path, content, mediaType, null, []);
    }

    /// <summary>Creates an outcome for an asset the source does not hold, or a path that is not valid.</summary>
    /// <param name="path">The asset path, as the caller supplied it.</param>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is <c>null</c>.</exception>
    public static AppAssetResult NotFound(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return new(AppAssetStatus.NotFound, path, default, null, null,
            [new("not-found", "$.path", "No such asset is available from this source.")]);
    }

    /// <summary>Creates an outcome for an asset the source holds but cannot serve now.</summary>
    /// <param name="path">The asset path, as the caller supplied it.</param>
    /// <param name="reason">Why the asset cannot be served; must not be empty.</param>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> or <paramref name="reason"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is empty.</exception>
    public static AppAssetResult NotAvailable(string path, string reason)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(reason);
        if (reason.Length == 0)
            throw new ArgumentException("A reason must not be empty.", nameof(reason));
        return new(AppAssetStatus.NotAvailable, path, default, null, null, [new("not-available", "$.path", reason)]);
    }

    /// <summary>Whether <paramref name="value"/> is a SHA-256 digest written as lower-case hex.</summary>
    /// <param name="value">The candidate digest; null is invalid.</param>
    public static bool IsSha256Hex(string? value)
    {
        Span<byte> scratch = stackalloc byte[Sha256Length];
        return value is not null && TryParseSha256Hex(value, scratch);
    }

    private static bool TryParseSha256Hex(string value, Span<byte> destination)
    {
        if (value.Length != Sha256HexLength)
            return false;
        for (var i = 0; i < Sha256Length; i++)
        {
            var high = HexValue(value[2 * i]);
            var low = HexValue(value[(2 * i) + 1]);
            if ((high | low) < 0)
                return false;
            destination[i] = (byte)((high << 4) | low);
        }

        return true;
    }

    private static int HexValue(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        _ => -1,
    };
}
