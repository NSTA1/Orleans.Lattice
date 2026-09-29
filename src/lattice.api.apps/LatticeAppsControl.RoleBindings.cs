using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

internal sealed partial class LatticeAppsControl : ILatticeAppRoleBindings
{
    /// <inheritdoc />
    /// <remarks>
    /// The bindings are replaced through a same-version registry upgrade that keeps the
    /// installed identity, provenance, ceiling, bridge consent and lifecycle state. It is
    /// pinned to the version and the record revision just read, so any transition that lands
    /// in between - another re-binding, a consent update, an upgrade, an enable - refuses this
    /// one instead of being rolled back. The roles are checked against the installed version's
    /// manifest, resolved from the source its provenance names. An enabled app is then
    /// reconciled, which replaces its compiled role rules; when that fails, the failure is
    /// thrown noting that the bindings themselves were recorded.
    /// </remarks>
    public async Task<AppRoleBindingsReport> UpdateRoleBindingsAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken = default)
    {
        try
        {
            return await UpdateRoleBindingsCoreAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, request?.Slug, out var sanitized))
        {
            throw sanitized;
        }
    }

    private async Task<AppRoleBindingsReport> UpdateRoleBindingsCoreAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var slug = AppsControlMapping.ParseSlug(request.Slug, nameof(request));
        var version = AppsControlMapping.ParseVersion(request.Version, nameof(request));
        var bindings = AppsControlMapping.ToEngineBindings(request.RoleBindings);
        var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);

        await AuthorizeAsync(cancellationToken).ConfigureAwait(false);

        var current = await _registry.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
        if (current is null || current.State == AppRegistryLifecycleState.Uninstalled)
        {
            throw AppsControlFailures.NotInstalled(slug);
        }

        if (current.Version != version)
        {
            throw new InvalidOperationException(
                $"The role bindings name version '{version}' of app '{slug}', but version '{current.Version}' is installed; "
                + "role bindings are pinned to the installed version.");
        }

        // The upgrade below re-pins the kept ceiling to the version; a ceiling that was never
        // consented for this version must be re-consented, not adopted by a re-binding.
        if (!current.IsCeilingPinnedToVersion)
        {
            throw new InvalidOperationException(
                $"The consent of app '{slug}' was not recorded for the installed version; re-consent before changing its role bindings.");
        }

        var resolved = await _source.ResolveInstalledAsync(current, cancellationToken).ConfigureAwait(false);
        var manifest = AppsControlFailures.RequireResolved(slug, version, resolved);
        ThrowIfUndeclaredRoles(manifest, bindings);

        var transition = await _registry.UpgradeAsync(
            new AppRegistryInstallRequest
            {
                Tenant = tenant,
                Identity = new AppIdentity { Slug = slug, Version = current.Version, Provenance = current.Provenance },
                Ceiling = current.Ceiling,
                RoleBindings = bindings,
                ExpectedVersion = current.Version,
                ExpectedRevision = current.Revision,
            },
            cancellationToken).ConfigureAwait(false);
        if (!transition.Succeeded || transition.Record is not { } record)
        {
            throw AppsControlFailures.FromTransition(slug, "change the role bindings of", transition);
        }

        if (record.State == AppRegistryLifecycleState.Enabled)
        {
            await ReapplyAsync(tenant, slug, "The role bindings were recorded, but re-applying the enabled app failed. ", cancellationToken)
                .ConfigureAwait(false);
        }

        return ToRoleBindingsReport(record);
    }

    private static AppRoleBindingsReport ToRoleBindingsReport(AppRegistryRecord record) =>
        new()
        {
            Slug = record.Slug.Value,
            Version = record.Version.Value,
            RoleBindings = AppsControlMapping.ToWireBindings(record.RoleBindings),
            State = AppsControlMapping.ToWireState(record.State),
        };
}
