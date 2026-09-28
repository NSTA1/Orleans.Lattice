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
    /// <para>
    /// The version resolves from the source <see cref="AppInstallRequest.SourceKey"/> names, or from the one
    /// source that offers the slug when it is null; a slug several sources offer is then refused rather than
    /// resolved from either, and the install's provenance records the answering source. A fresh install records
    /// consent to the bridge grants the manifest requests; an upgrade keeps the consented grants, so an upgrade
    /// that requests more cannot activate until <see cref="UpdateConsentAsync"/> re-consents them.
    /// </para>
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
    /// is then reconciled so a reduced ceiling cannot leave stale authority. When that
    /// re-application fails because the installed version can no longer be activated -
    /// for example its manifest is unavailable or invalid, or its roles exceed the new
    /// ceiling - the pipeline withdraws the app's grants (fail closed); a transient
    /// tree-provisioning or rule-write failure, or an unexpected fault, keeps the existing
    /// grants so a retry is not an outage. Either way the failure is thrown, noting that
    /// the consent itself was recorded.
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

        var resolved = await _source.ResolveFromAsync(slug, version, request.SourceKey, cancellationToken).ConfigureAwait(false);
        var manifest = AppsControlFailures.RequireResolved(slug, version, resolved);

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

        // A fresh install consents to the bridge grants the reviewed manifest requests; an upgrade keeps the
        // grants already consented, so one that requests more cannot activate until it is re-consented.
        if (!upgrading)
        {
            installRequest = installRequest with { BridgeConsent = AppUiBridgeRequest.FromManifest(manifest) };
        }

        // The upgrade is pinned to the version just read, so an upgrade racing this one is
        // refused rather than silently overwritten.
        var transition = upgrading
            ? await _registry.UpgradeAsync(installRequest with { ExpectedVersion = current!.Version }, cancellationToken).ConfigureAwait(false)
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
        var bridgeConsent = AppsPresentationMapping.ToEngineConsent(request.BridgeGrants);

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
        // identity, bindings and state. It is pinned to the version just read, so an upgrade that
        // lands in between is refused (ConcurrencyConflict) instead of being rolled back.
        var transition = await _registry.UpgradeAsync(
            new AppRegistryInstallRequest
            {
                Tenant = tenant,
                Identity = new AppIdentity { Slug = slug, Version = current.Version, Provenance = current.Provenance },
                Ceiling = ceiling,
                RoleBindings = current.RoleBindings,
                ExpectedVersion = current.Version,
                BridgeConsent = bridgeConsent,
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
