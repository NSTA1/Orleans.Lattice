using Microsoft.Extensions.Logging;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Opens one bundle asset through its source with the manifest-pinned digest and returns its bytes only when
/// they are whole and verified: an opened asset within <see cref="AppUiBundle.MaxAssetBytes"/>, and, for the UI
/// entry, a valid entry fragment (<see cref="AppManifestValidator.ValidateUiEntryFragment(ReadOnlySpan{byte})"/>).
/// Every other outcome is null, never partial bytes; a digest mismatch or a rejected asset is logged.
/// </summary>
internal static partial class AppsAssetReader
{
    /// <summary>A verified asset's bytes and media type.</summary>
    /// <param name="Bytes">The verified bytes.</param>
    /// <param name="MediaType">The media type the source reported.</param>
    public readonly record struct VerifiedAsset(ReadOnlyMemory<byte> Bytes, string MediaType);

    /// <summary>Opens and verifies one asset.</summary>
    /// <param name="source">The source the app version came from.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="version">The exact app version.</param>
    /// <param name="path">The asset path the manifest declares.</param>
    /// <param name="sha256">The digest the manifest pins.</param>
    /// <param name="logger">The logger rejections are reported to.</param>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    /// <param name="isEntry">Whether the asset is the UI entry fragment, which must also pass fragment validation.</param>
    /// <returns>The verified asset, or null.</returns>
    public static async ValueTask<VerifiedAsset?> OpenAsync(
        IAppCatalogSource source,
        AppSlug slug,
        AppVersion version,
        string path,
        string sha256,
        ILogger logger,
        CancellationToken cancellationToken,
        bool isEntry = false)
    {
        var result = await source.OpenAssetAsync(slug, version, path, sha256, cancellationToken).ConfigureAwait(false);
        switch (result.Status)
        {
            case AppAssetStatus.Opened when result.MediaType is { } mediaType:
                if (result.Content.Length > AppUiBundle.MaxAssetBytes)
                {
                    LogRejected(logger, slug.Value, version.Value, path, "it exceeds the per-asset byte bound");
                    return null;
                }

                if (isEntry && AppManifestValidator.ValidateUiEntryFragment(result.Content.Span).Count > 0)
                {
                    LogRejected(logger, slug.Value, version.Value, path, "it is not a valid UI entry fragment");
                    return null;
                }

                return new VerifiedAsset(result.Content, mediaType);
            case AppAssetStatus.DigestMismatch:
                LogDigestMismatch(logger, slug.Value, version.Value, path);
                return null;
            default:
                return null;
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning,
        Message = "Asset '{Path}' of app '{Slug}' version '{Version}' failed digest verification and was not served.")]
    private static partial void LogDigestMismatch(ILogger logger, string slug, string version, string path);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning,
        Message = "Asset '{Path}' of app '{Slug}' version '{Version}' was not served because {Reason}.")]
    private static partial void LogRejected(ILogger logger, string slug, string version, string path, string reason);
}
