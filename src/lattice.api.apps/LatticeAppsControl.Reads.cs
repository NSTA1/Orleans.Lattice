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

                var (state, pending) = await AppsLiveState.ReadAsync(_pipeline, tenant, record, cancellationToken).ConfigureAwait(false);
                apps.Add(new AppSummary
                {
                    Slug = record.Slug.Value,
                    Version = record.Version.Value,
                    State = state,
                    PendingState = pending,
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

            // The installed version resolves from the source it was installed from, so another source offering
            // the slug cannot make it ambiguous; any other version resolves across every source.
            var sourceKey = live && selected == record!.Version ? record.Provenance.Source : null;
            var resolved = await _source.ResolveFromAsync(slug, selected, sourceKey, cancellationToken).ConfigureAwait(false);
            if (resolved.Status is AppSourceStatus.NotFound or AppSourceStatus.VersionMismatch)
            {
                return null;
            }

            if (resolved.Status == AppSourceStatus.Ambiguous)
            {
                throw AppsControlFailures.Ambiguous(slug, resolved);
            }

            if (!resolved.IsResolved || resolved.Manifest is not { } manifest)
            {
                throw AppsControlFailures.SourceUnusable(slug, resolved);
            }

            var matching = record is not null && record.Version == manifest.Identity.Version ? record : null;
            AppLifecycleState state;
            AppLifecycleState? pending = null;
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
                (state, pending) = await AppsLiveState.ReadAsync(_pipeline, tenant, matching, cancellationToken).ConfigureAwait(false);
            }

            var provenance = resolved.Provenance ?? manifest.Identity.Provenance;
            var owner = AppsControlMapping.OwnershipProbeProvenance(live ? matching : null, provenance);
            var conflicts = await _registry.GetTreeOwnershipConflictsAsync(tenant, manifest, owner, cancellationToken)
                .ConfigureAwait(false);
            return AppsControlMapping.ToDescriptor(
                manifest,
                provenance,
                state,
                live ? matching : null,
                conflicts) with { PendingState = pending };
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
            BridgeGrants = AppsPresentationMapping.ToWireConsent(record.ConsentedBridge),
        };
}
