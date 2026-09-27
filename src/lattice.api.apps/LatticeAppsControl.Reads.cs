using System.Collections.Immutable;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

internal sealed partial class LatticeAppsControl
{
    private static readonly LatticeAppsCapabilities AllGranted = new()
    {
        CanInstall = true,
        CanEnable = true,
        CanDisable = true,
        CanUninstall = true,
        CanList = true,
        CanDescribe = true,
        CanGetConsent = true,
        CanUpdateConsent = true,
    };

    private static readonly LatticeAppsCapabilities NoneGranted = new();

    /// <inheritdoc />
    public async Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
            await AuthorizeAsync(cancellationToken).ConfigureAwait(false);

            var apps = ImmutableArray.CreateBuilder<AppSummary>();
            await foreach (var record in _registry.ListForTenantAsync(tenant, cancellationToken).ConfigureAwait(false))
            {
                // Defensive: a prefix scan is tenant-exact, but a record filed under another
                // tenant must never be echoed to this caller.
                if (record.Tenant != tenant)
                {
                    continue;
                }

                var status = record.State == AppRegistryLifecycleState.Uninstalled
                    ? null
                    : await _pipeline.GetStatusAsync(tenant, record.Slug, cancellationToken).ConfigureAwait(false);
                apps.Add(new AppSummary
                {
                    Slug = record.Slug.Value,
                    Version = record.Version.Value,
                    State = AppsControlMapping.ToWireState(record.State, status),
                    Provenance = AppsControlMapping.ToWireProvenance(record.Provenance),
                });
            }

            return new AppCatalog { Apps = apps.ToImmutable() };
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, ownApp: null, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppDescriptor?> DescribeAsync(
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
            AppVersion? requested = version is null ? null : AppsControlMapping.ParseVersion(version, nameof(version));
            var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
            await AuthorizeAsync(cancellationToken).ConfigureAwait(false);

            var record = await _registry.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
            var live = record is { State: not AppRegistryLifecycleState.Uninstalled };
            var selected = requested ?? (live ? record!.Version : null);

            var resolved = await _source.ResolveAsync(slug, selected, cancellationToken).ConfigureAwait(false);
            if (resolved.Status is AppSourceStatus.NotFound or AppSourceStatus.VersionMismatch)
            {
                return null;
            }

            if (!resolved.IsResolved || resolved.Manifest is not { } manifest)
            {
                throw AppsControlFailures.SourceUnusable(slug, resolved);
            }

            var matching = record is not null && record.Version == manifest.Identity.Version ? record : null;
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

            return AppsControlMapping.ToDescriptor(
                manifest,
                resolved.Provenance ?? manifest.Identity.Provenance,
                state,
                live ? matching : null);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        try
        {
            var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
            var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
            await AuthorizeAsync(cancellationToken).ConfigureAwait(false);

            var record = await _registry.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
            return record is null || record.State == AppRegistryLifecycleState.Uninstalled ? null : ToConsentReport(record);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeAsync(cancellationToken).ConfigureAwait(false);
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

    private static AppConsentReport ToConsentReport(AppRegistryRecord record) =>
        new()
        {
            Slug = record.Slug.Value,
            Version = record.Version.Value,
            Ceiling = AppsControlMapping.ToWireCeiling(record.Ceiling),
        };
}
