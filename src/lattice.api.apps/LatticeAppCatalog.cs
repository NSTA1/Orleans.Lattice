using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The in-process implementation of <see cref="ILatticeAppCatalog"/>: the administrative view of the configured
/// app sources and what each one offers, joined with the active tenant's installs. It composes the
/// <see cref="AppSourceSet"/> the apps add-on resolves through, the app registry and the activation
/// pipeline's recorded status.
/// </summary>
/// <remarks>
/// <para>
/// <b>Order of every verb.</b> (1) Validate caller input; (2) resolve the caller's active tenant; (3) authorize
/// <see cref="LatticeOperation.AppInstall"/> over the cluster-wide scope through the shared access gate; only
/// then (4) touch a source or the registry. A denied caller learns nothing about what exists.
/// </para>
/// <para>
/// <b>Never activates.</b> Describing and reading an icon resolve and open assets only; no app code is loaded.
/// Every response carries slugs, source keys and app-local names only, and every exception is funnelled
/// through <see cref="AppsControlExceptionSanitizer"/>.
/// </para>
/// </remarks>
internal sealed partial class LatticeAppCatalog : ILatticeAppCatalog
{
    private static readonly LatticeAppCatalogCapabilities AllGranted = new()
    {
        CanListSources = true,
        CanListAvailable = true,
        CanDescribeFromSource = true,
        CanGetIcon = true,
    };

    private static readonly LatticeAppCatalogCapabilities NoneGranted = new();

    private readonly AppSourceSet _sources;
    private readonly IAppRegistry _registry;
    private readonly IAppActivationPipeline _pipeline;
    private readonly ILatticeAccessGate _gate;
    private readonly ITenantContextResolver _tenants;
    private readonly ILatticeMembershipContext? _membership;
    private readonly ILogger _logger;

    /// <summary>Initializes a new <see cref="LatticeAppCatalog"/>.</summary>
    /// <param name="sources">The composed app sources the catalogue browses.</param>
    /// <param name="registry">The app registry installs are joined against.</param>
    /// <param name="pipeline">The activation pipeline whose recorded status marks failed installs.</param>
    /// <param name="gate">The shared access gate every verb authorizes through.</param>
    /// <param name="tenants">The active-tenant resolver.</param>
    /// <param name="membership">The membership context resolving the caller, or null for anonymous.</param>
    /// <param name="logger">The logger digest failures are reported to, or null.</param>
    /// <exception cref="ArgumentNullException">A required dependency is null.</exception>
    public LatticeAppCatalog(
        AppSourceSet sources,
        IAppRegistry registry,
        IAppActivationPipeline pipeline,
        ILatticeAccessGate gate,
        ITenantContextResolver tenants,
        ILatticeMembershipContext? membership = null,
        ILogger<LatticeAppCatalog>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(sources);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(tenants);
        _sources = sources;
        _registry = registry;
        _pipeline = pipeline;
        _gate = gate;
        _tenants = tenants;
        _membership = membership;
        _logger = logger ?? NullLogger<LatticeAppCatalog>.Instance;
    }

    /// <summary>
    /// Returns the source set the catalogue browses for a registered <see cref="IAppSource"/>: the set itself,
    /// a set of one for a lone catalogue source, and an empty set for a source that cannot enumerate.
    /// </summary>
    /// <param name="source">The registered app source.</param>
    /// <returns>The source set.</returns>
    public static AppSourceSet ToSourceSet(IAppSource source) => source switch
    {
        AppSourceSet set => set,
        IAppCatalogSource catalog => new AppSourceSet([catalog]),
        _ => new AppSourceSet([]),
    };

    /// <inheritdoc />
    public async Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await AppsFacadeAccess.AuthorizeInstallAsync(_gate, _membership, cancellationToken).ConfigureAwait(false);

            var sources = _sources.Sources;
            var builder = ImmutableArray.CreateBuilder<AppSourceSummary>(sources.Count);
            foreach (var source in sources)
            {
                var descriptor = source.Descriptor;
                builder.Add(new AppSourceSummary
                {
                    Key = descriptor.Key,
                    DisplayName = descriptor.DisplayName,
                    Kind = descriptor.Kind == AppSourceKind.Dynamic ? AppSourceSummaryKind.Dynamic : AppSourceSummaryKind.Static,
                    Capabilities = (AppSourceSummaryCapabilities)(int)descriptor.Capabilities,
                });
            }

            return builder.MoveToImmutable();
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, ownApp: null, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppDescriptor?> DescribeFromSourceAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var key = ParseSourceKey(sourceKey);
            var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
            AppVersion? requested = version is null ? null : AppsControlMapping.ParseVersion(version, nameof(version));
            var tenant = await AppsFacadeAccess.ResolveTenantAsync(_tenants, cancellationToken).ConfigureAwait(false);
            await AppsFacadeAccess.AuthorizeInstallAsync(_gate, _membership, cancellationToken).ConfigureAwait(false);

            if (await ResolveAsync(key, slug, requested, cancellationToken).ConfigureAwait(false) is not { } resolved)
            {
                return null;
            }

            var (manifest, provenance) = resolved;
            var record = await _registry.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
            var matching = record is not null
                && record.Version == manifest.Identity.Version
                && string.Equals(record.Provenance.Source, key, StringComparison.Ordinal)
                    ? record
                    : null;

            AppLifecycleState state;
            if (matching is null)
            {
                state = AppLifecycleState.NotInstalled;
            }
            else if (matching.State == AppRegistryLifecycleState.Uninstalled)
            {
                state = AppLifecycleState.Uninstalled;
            }
            else
            {
                var status = await _pipeline.GetStatusAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
                state = AppsControlMapping.ToWireState(matching.State, status);
            }

            var live = matching is { State: not AppRegistryLifecycleState.Uninstalled } ? matching : null;
            var conflicts = await _registry.GetTreeOwnershipConflictsAsync(
                    tenant, manifest, AppsControlMapping.OwnershipProbeProvenance(live, provenance), cancellationToken)
                .ConfigureAwait(false);
            return AppsControlMapping.ToDescriptor(manifest, provenance, state, live, conflicts);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppIconAsset?> GetIconAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var key = ParseSourceKey(sourceKey);
            var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
            AppVersion? requested = version is null ? null : AppsControlMapping.ParseVersion(version, nameof(version));
            await AppsFacadeAccess.AuthorizeInstallAsync(_gate, _membership, cancellationToken).ConfigureAwait(false);

            if (await ResolveAsync(key, slug, requested, cancellationToken).ConfigureAwait(false) is not { } resolved
                || resolved.Manifest.Presentation?.Icon is not { } icon
                || !_sources.TryGet(key, out var source))
            {
                return null;
            }

            var opened = await AppsAssetReader.OpenAsync(
                source, slug, resolved.Manifest.Identity.Version, icon.Path, icon.Digest, _logger, cancellationToken)
                .ConfigureAwait(false);
            return opened is { } asset
                ? new AppIconAsset { Bytes = asset.Bytes, MediaType = asset.MediaType, Sha256 = icon.Digest }
                : null;
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await AppsFacadeAccess.AuthorizeInstallAsync(_gate, _membership, cancellationToken).ConfigureAwait(false);
            return AllGranted;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return NoneGranted;
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, ownApp: null, out var sanitized))
        {
            throw sanitized;
        }
    }

    private static string ParseSourceKey(string? sourceKey)
    {
        if (!AppSourceDescriptor.IsValidKey(sourceKey))
        {
            throw new ArgumentException(
                "An app source key must be 2-31 lowercase ASCII letters, digits or hyphens, starting with a letter.",
                nameof(sourceKey));
        }

        return sourceKey!;
    }

    /// <summary>Resolves an app version from one named source; null when the source, app or version is unknown.</summary>
    private async ValueTask<(AppManifest Manifest, AppProvenance Provenance)?> ResolveAsync(
        string key,
        AppSlug slug,
        AppVersion? version,
        CancellationToken cancellationToken)
    {
        if (!_sources.TryGet(key, out _))
        {
            return null;
        }

        var resolved = await _sources.ResolveAsync(slug, version, key, cancellationToken).ConfigureAwait(false);
        if (resolved.Status is AppSourceStatus.NotFound or AppSourceStatus.VersionMismatch)
        {
            return null;
        }

        if (!resolved.IsResolved || resolved.Manifest is not { } manifest)
        {
            throw AppsControlFailures.SourceUnusable(slug, resolved);
        }

        return (manifest, resolved.Provenance ?? manifest.Identity.Provenance);
    }
}
