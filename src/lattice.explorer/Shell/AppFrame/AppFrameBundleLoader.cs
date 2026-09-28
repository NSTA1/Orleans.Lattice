using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// The circuit's gate and loader for app UI bundles: it authorises each launch against
/// the user's workspace, then fetches every asset on the user's credential and verifies
/// it before anything reaches the frame (epic #3807, E4).
/// </summary>
/// <remarks>
/// <para>
/// Registered scoped, so it belongs to one circuit and uses that circuit's credential-aware
/// <see cref="ILatticeAppWorkspace"/>. A <see langword="null"/> workspace (no transport
/// registered) refuses every launch.
/// </para>
/// <para>
/// The order is fixed: <see cref="AuthorizeAsync"/> is the per-launch gate, and
/// <see cref="LoadAsync"/> accepts only a launch this loader issued. The shared
/// <see cref="AppFrameBundleCache"/> is consulted only inside <see cref="LoadAsync"/>, so a
/// cache hit can never skip the gate.
/// </para>
/// </remarks>
internal sealed partial class AppFrameBundleLoader(
    ILatticeAppWorkspace? workspace,
    AppFrameBundleCache cache,
    ILogger<AppFrameBundleLoader> logger)
{
    /// <summary>
    /// The per-launch workspace gate: the app must be in the caller's
    /// <see cref="ILatticeAppWorkspace.ListMyAppsAsync"/>, enabled, and declare a UI whose
    /// minimum protocol the host speaks.
    /// </summary>
    /// <param name="appSlug">The app slug.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The authorised launch, or the reason it was refused.</returns>
    public async Task<AppFrameLaunchResult> AuthorizeAsync(string? appSlug, CancellationToken cancellationToken = default)
    {
        if (workspace is null || string.IsNullOrEmpty(appSlug))
        {
            return AppFrameLaunchResult.Refused(AppFrameFailure.NoGrant);
        }

        try
        {
            var mine = await workspace.ListMyAppsAsync(cancellationToken).ConfigureAwait(false);
            WorkspaceAppSummary? summary = null;
            if (!mine.IsDefault)
            {
                foreach (var candidate in mine)
                {
                    if (candidate is not null && string.Equals(candidate.Slug, appSlug, StringComparison.Ordinal))
                    {
                        summary = candidate;
                        break;
                    }
                }
            }

            if (summary is null)
            {
                return AppFrameLaunchResult.Refused(AppFrameFailure.NoGrant);
            }

            if (!summary.HasUi)
            {
                return AppFrameLaunchResult.Refused(AppFrameFailure.NoUi);
            }

            var descriptor = await workspace.DescribeMyAppAsync(appSlug, cancellationToken).ConfigureAwait(false);
            if (descriptor is null
                || !string.Equals(descriptor.Slug, appSlug, StringComparison.Ordinal)
                || descriptor.State != AppLifecycleState.Enabled
                || descriptor.InstallRevision != summary.InstallRevision)
            {
                return AppFrameLaunchResult.Refused(AppFrameFailure.NoGrant);
            }

            if (descriptor.Ui is not { } ui)
            {
                return AppFrameLaunchResult.Refused(AppFrameFailure.NoUi);
            }

            if (ui.MinProtocol > AppFrameProtocol.Version)
            {
                return AppFrameLaunchResult.Refused(AppFrameFailure.ProtocolUnsupported);
            }

            return new AppFrameLaunchResult(new AppFrameLaunch(this, descriptor, ui), default);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            LogWorkspaceFailed(logger, appSlug, exception.GetType().Name);
            return AppFrameLaunchResult.Refused(AppFrameFailure.Unavailable);
        }
    }

    /// <summary>
    /// Fetches and verifies every asset of an authorised launch: the asset list's shape, the
    /// bundle digest, each asset's media type, size and SHA-256, the running bundle size, and
    /// the entry fragment.
    /// </summary>
    /// <param name="launch">A launch this loader issued.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The verified bundle, or the reason it was refused.</returns>
    public async Task<AppFrameBundleResult> LoadAsync(AppFrameLaunch? launch, CancellationToken cancellationToken = default)
    {
        if (launch is null || !ReferenceEquals(launch.Issuer, this) || workspace is null)
        {
            return AppFrameBundleResult.Refused(AppFrameFailure.NoGrant);
        }

        var ui = launch.Ui;
        if (!TryValidateDeclaration(ui, out var declaredFailure))
        {
            LogBundleRefused(logger, launch.Slug, declaredFailure);
            return AppFrameBundleResult.Refused(declaredFailure);
        }

        var assets = ImmutableArray.CreateBuilder<AppFrameBundleAsset>(ui.Assets.Length);
        long total = 0;
        try
        {
            foreach (var declared in ui.Assets)
            {
                ReadOnlyMemory<byte> bytes;
                if (!cache.TryGet(launch, declared.Path, out bytes))
                {
                    var fetched = await workspace.GetUiAssetAsync(launch.Slug, declared.Path, cancellationToken).ConfigureAwait(false);
                    if (fetched is null)
                    {
                        return AppFrameBundleResult.Refused(AppFrameFailure.NoGrant);
                    }

                    if (!string.Equals(fetched.Path, declared.Path, StringComparison.Ordinal)
                        || !string.Equals(fetched.MediaType, declared.MediaType, StringComparison.Ordinal)
                        || fetched.Bytes.Length > AppFrameBundleRules.MaxAssetBytes)
                    {
                        LogBundleRefused(logger, launch.Slug, AppFrameFailure.BundleInvalid);
                        return AppFrameBundleResult.Refused(AppFrameFailure.BundleInvalid);
                    }

                    if (!string.Equals(fetched.Sha256, declared.Sha256, StringComparison.Ordinal)
                        || !string.Equals(AppFrameBundleRules.ComputeDigest(fetched.Bytes.Span), declared.Sha256, StringComparison.Ordinal))
                    {
                        LogBundleRefused(logger, launch.Slug, AppFrameFailure.DigestMismatch);
                        return AppFrameBundleResult.Refused(AppFrameFailure.DigestMismatch);
                    }

                    bytes = fetched.Bytes;
                    cache.TryAdd(launch, declared.Path, bytes);
                }

                total += bytes.Length;
                if (total > AppFrameBundleRules.MaxBundleBytes)
                {
                    LogBundleRefused(logger, launch.Slug, AppFrameFailure.BundleInvalid);
                    return AppFrameBundleResult.Refused(AppFrameFailure.BundleInvalid);
                }

                if (string.Equals(declared.Path, ui.Entry, StringComparison.Ordinal)
                    && !AppFrameBundleRules.IsValidEntryFragment(bytes.Span))
                {
                    LogBundleRefused(logger, launch.Slug, AppFrameFailure.BundleInvalid);
                    return AppFrameBundleResult.Refused(AppFrameFailure.BundleInvalid);
                }

                assets.Add(new AppFrameBundleAsset(declared.Path, declared.MediaType, declared.Sha256, bytes));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            LogWorkspaceFailed(logger, launch.Slug, exception.GetType().Name);
            return AppFrameBundleResult.Refused(AppFrameFailure.Unavailable);
        }

        return new AppFrameBundleResult(new AppFrameBundle(launch, assets.MoveToImmutable()), default);
    }

    /// <summary>
    /// Re-checks a launch against the workspace and returns why it no longer holds, or
    /// <see langword="null"/> while it is still current. An app the caller can no longer see
    /// revokes; a workspace that cannot be reached does not (the request that prompted the
    /// check has already failed closed).
    /// </summary>
    /// <param name="launch">The launch to re-check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <c>lattice.revoked</c> reason, or <see langword="null"/> when the launch is current.</returns>
    public async Task<string?> GetRevocationAsync(AppFrameLaunch launch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(launch);
        if (workspace is null || !ReferenceEquals(launch.Issuer, this))
        {
            return AppFrameProtocol.RevokedClosed;
        }

        WorkspaceAppDescriptor? descriptor;
        try
        {
            descriptor = await workspace.DescribeMyAppAsync(launch.Slug, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            LogWorkspaceFailed(logger, launch.Slug, exception.GetType().Name);
            return null;
        }

        return descriptor switch
        {
            null => AppFrameProtocol.RevokedClosed,
            { State: AppLifecycleState.Disabled } => AppFrameProtocol.RevokedDisabled,
            { State: AppLifecycleState.Uninstalled or AppLifecycleState.NotInstalled } => AppFrameProtocol.RevokedUninstalled,
            { State: not AppLifecycleState.Enabled } => AppFrameProtocol.RevokedClosed,
            _ when !string.Equals(descriptor.Version, launch.Version, StringComparison.Ordinal) => AppFrameProtocol.RevokedUpgraded,
            _ when descriptor.InstallRevision != launch.InstallRevision => AppFrameProtocol.RevokedRevision,
            _ => null,
        };
    }

    /// <summary>Validates the declared asset list before any byte is fetched.</summary>
    private static bool TryValidateDeclaration(AppUiDescriptor ui, out AppFrameFailure failure)
    {
        failure = AppFrameFailure.BundleInvalid;
        if (ui.Assets.IsDefaultOrEmpty || ui.Assets.Length > AppFrameBundleRules.MaxAssets)
        {
            return false;
        }

        var mediaTypes = new Dictionary<string, string>(ui.Assets.Length, StringComparer.Ordinal);
        var pairs = new (string Path, string Digest)[ui.Assets.Length];
        for (var i = 0; i < ui.Assets.Length; i++)
        {
            var asset = ui.Assets[i];
            if (asset is null
                || !AppFrameBundleRules.IsValidPath(asset.Path)
                || !AppFrameBundleRules.IsValidDigest(asset.Sha256)
                || asset.MediaType is null
                || !AppFrameBundleRules.AllowedMediaTypes.Contains(asset.MediaType)
                || !mediaTypes.TryAdd(asset.Path, asset.MediaType))
            {
                return false;
            }

            pairs[i] = (asset.Path, asset.Sha256);
        }

        if (!HasMediaType(mediaTypes, ui.Entry, AppFrameBundleRules.HtmlMediaType))
        {
            return false;
        }

        if (!ui.Styles.IsDefault)
        {
            foreach (var style in ui.Styles)
            {
                if (!HasMediaType(mediaTypes, style, AppFrameBundleRules.CssMediaType))
                {
                    return false;
                }
            }
        }

        if (!ui.Scripts.IsDefault)
        {
            foreach (var script in ui.Scripts)
            {
                if (script is null || !HasMediaType(mediaTypes, script.Path, AppFrameBundleRules.JavaScriptMediaType))
                {
                    return false;
                }
            }
        }

        if (!AppFrameBundleRules.IsValidDigest(ui.BundleDigest)
            || !string.Equals(AppFrameBundleRules.ComputeBundleDigest(pairs), ui.BundleDigest, StringComparison.Ordinal))
        {
            failure = AppFrameFailure.BundleDigestMismatch;
            return false;
        }

        return true;
    }

    private static bool HasMediaType(Dictionary<string, string> mediaTypes, string? path, string expected) =>
        path is not null
        && mediaTypes.TryGetValue(path, out var mediaType)
        && string.Equals(mediaType, expected, StringComparison.Ordinal);

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "The app UI bundle for '{AppSlug}' was refused: {Failure}.")]
    private static partial void LogBundleRefused(ILogger logger, string appSlug, AppFrameFailure failure);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "The app workspace failed while opening '{AppSlug}' ({ExceptionType}).")]
    private static partial void LogWorkspaceFailed(ILogger logger, string appSlug, string exceptionType);
}
