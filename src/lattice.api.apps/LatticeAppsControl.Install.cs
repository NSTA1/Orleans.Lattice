using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

internal sealed partial class LatticeAppsControl
{
    /// <inheritdoc />
    /// <remarks>
    /// A slug with no live installation is installed (state Installed, never enabled).
    /// A live installation at a different version is upgraded in place, keeping its
    /// state; an enabled app is then re-applied through the activation pipeline so the
    /// new version's trees and grants take effect. Installing the version already
    /// installed is refused: consent changes go through
    /// <see cref="UpdateConsentAsync"/>.
    /// </remarks>
    public async Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            return await InstallCoreAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, request?.Slug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// The ceiling is replaced through a same-version registry upgrade, which keeps the
    /// installed identity, provenance, role bindings and lifecycle state. An enabled app
    /// is then reconciled so a reduced ceiling cannot leave stale authority; when that
    /// re-application fails the pipeline withdraws the app's grants (fail closed) and the
    /// failure is thrown, noting that the consent itself was recorded.
    /// </remarks>
    public async Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)
    {
        try
        {
            return await UpdateConsentCoreAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, request?.Slug, out var sanitized))
        {
            throw sanitized;
        }
    }

    private async Task<AppLifecycleResult> InstallCoreAsync(AppInstallRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var slug = AppsControlMapping.ParseSlug(request.Slug, nameof(request));
        var version = AppsControlMapping.ParseVersion(request.Version, nameof(request));
        var bindings = AppsControlMapping.ToEngineBindings(request.RoleBindings);
        var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
        var ceiling = AppsControlMapping.ToEngineCeiling(request.Ceiling, tenant);

        await AuthorizeAsync(cancellationToken).ConfigureAwait(false);

        var resolved = await _source.ResolveAsync(slug, version, cancellationToken).ConfigureAwait(false);
        if (resolved.Status is AppSourceStatus.NotFound or AppSourceStatus.VersionMismatch)
        {
            throw AppsControlFailures.SourceNotFound(slug, version);
        }

        if (!resolved.IsResolved || resolved.Manifest is not { } manifest)
        {
            throw AppsControlFailures.SourceUnusable(slug, resolved);
        }

        ThrowIfUndeclaredRoles(manifest, bindings);

        var installRequest = new AppRegistryInstallRequest
        {
            Tenant = tenant,
            Identity = new AppIdentity
            {
                Slug = slug,
                Version = version,
                Provenance = resolved.Provenance ?? manifest.Identity.Provenance,
            },
            Ceiling = ceiling,
            RoleBindings = bindings,
        };

        var current = await _registry.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
        var upgrading = current is { State: not AppRegistryLifecycleState.Uninstalled } && current.Version != version;

        var transition = upgrading
            ? await _registry.UpgradeAsync(installRequest, cancellationToken).ConfigureAwait(false)
            : await _registry.InstallAsync(installRequest, cancellationToken).ConfigureAwait(false);
        if (!transition.Succeeded || transition.Record is not { } record)
        {
            throw AppsControlFailures.FromTransition(slug, upgrading ? "upgrade" : "install", transition);
        }

        if (upgrading && record.State == AppRegistryLifecycleState.Enabled)
        {
            await ReapplyAsync(tenant, slug, "The upgrade was recorded, but re-applying the enabled app failed. ", cancellationToken)
                .ConfigureAwait(false);
        }

        return ToLifecycleResult(record, transition.Changed);
    }

    private async Task<AppConsentReport> UpdateConsentCoreAsync(AppConsentUpdate request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var slug = AppsControlMapping.ParseSlug(request.Slug, nameof(request));
        var version = AppsControlMapping.ParseVersion(request.Version, nameof(request));
        var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
        var ceiling = AppsControlMapping.ToEngineCeiling(request.Ceiling, tenant);

        await AuthorizeAsync(cancellationToken).ConfigureAwait(false);

        var current = await _registry.GetAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
        if (current is null || current.State == AppRegistryLifecycleState.Uninstalled)
        {
            throw AppsControlFailures.NotInstalled(slug);
        }

        if (current.Version != version)
        {
            throw new InvalidOperationException(
                $"The consent names version '{version}' of app '{slug}', but version '{current.Version}' is installed; "
                + "consent is pinned to the installed version.");
        }

        // A same-version upgrade replaces the ceiling (re-pinning it to the version) and keeps
        // identity, bindings and state. The registry cannot compare the version atomically with
        // this read, so a concurrent upgrade landing in between is the one residual race.
        var transition = await _registry.UpgradeAsync(
            new AppRegistryInstallRequest
            {
                Tenant = tenant,
                Identity = new AppIdentity { Slug = slug, Version = current.Version, Provenance = current.Provenance },
                Ceiling = ceiling,
                RoleBindings = current.RoleBindings,
            },
            cancellationToken).ConfigureAwait(false);
        if (!transition.Succeeded || transition.Record is not { } record)
        {
            throw AppsControlFailures.FromTransition(slug, "update the consent of", transition);
        }

        if (record.State == AppRegistryLifecycleState.Enabled)
        {
            await ReapplyAsync(tenant, slug, "The consent was recorded, but re-applying the enabled app failed. ", cancellationToken)
                .ConfigureAwait(false);
        }

        return ToConsentReport(record);
    }

    private async Task ReapplyAsync(TenantId tenant, AppSlug slug, string preface, CancellationToken cancellationToken)
    {
        var outcome = await _pipeline.ReconcileAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
        if (!outcome.Succeeded)
        {
            throw AppsControlFailures.FromActivation(slug, outcome, preface);
        }
    }

    private static void ThrowIfUndeclaredRoles(AppManifest manifest, AppRoleBinding[] bindings)
    {
        var roles = manifest.Roles ?? [];
        for (var i = 0; i < bindings.Length; i++)
        {
            var declared = false;
            foreach (var role in roles)
            {
                if (string.Equals(role.Name, bindings[i].RoleName, StringComparison.Ordinal))
                {
                    declared = true;
                    break;
                }
            }

            if (!declared)
            {
                throw new ArgumentException(
                    $"Role binding {i} names a role the app's manifest does not declare.", "request");
            }
        }
    }
}
