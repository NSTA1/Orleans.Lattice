namespace Orleans.Lattice.Apps;

/// <summary>
/// <see cref="IAppActivationPipeline"/> that hands each run to the app's
/// <see cref="IAppActivationGrain"/>, which serializes runs per tenant app and authorizes the
/// caller (whose credential flows on the request context) for
/// <see cref="LatticeOperation.AppInstall"/> before any side effect. A successful uninstall then
/// reconciles the tenant's enabled apps that depend on the uninstalled one across app boundaries.
/// </summary>
internal sealed class AppActivationPipeline : IAppActivationPipeline
{
    private readonly IGrainFactory _grainFactory;
    private readonly IAppActivationStatusStore _statusStore;
    private readonly IAppRegistry _registry;
    private readonly IAppSource _source;

    public AppActivationPipeline(
        IGrainFactory grainFactory,
        IAppActivationStatusStore statusStore,
        IAppRegistry registry,
        IAppSource source)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(statusStore);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(source);
        _grainFactory = grainFactory;
        _statusStore = statusStore;
        _registry = registry;
        _source = source;
    }

    public Task<AppActivationOutcome> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Enable, tenant, slug, cancellationToken);

    public Task<AppActivationOutcome> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Disable, tenant, slug, cancellationToken);

    public async Task<AppActivationOutcome> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)
    {
        var outcome = await RunAsync(AppActivationOperation.Uninstall, tenant, slug, cancellationToken).ConfigureAwait(false);
        if (outcome.Succeeded)
        {
            await ReconcileDependantsAsync(tenant, slug, cancellationToken).ConfigureAwait(false);
        }

        return outcome;
    }

    public Task<AppActivationOutcome> ReconcileAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Reconcile, tenant, slug, cancellationToken);

    public Task<AppActivationStatus?> GetStatusAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)
    {
        _ = AppRegistryTreeNames.ComposeKey(tenant, slug);
        return _statusStore.GetAsync(tenant, slug, cancellationToken);
    }

    private Task<AppActivationOutcome> RunAsync(
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        CancellationToken cancellationToken)
    {
        var key = AppRegistryTreeNames.ComposeKey(tenant, slug);
        return _grainFactory.GetGrain<IAppActivationGrain>(key).ExecuteAsync(operation, tenant, slug, cancellationToken);
    }

    /// <summary>
    /// Reconciles every enabled app in <paramref name="tenant"/> whose installed manifest reaches
    /// <paramref name="owner"/> through a cross-app role scope or subscription. A dependant whose
    /// manifest cannot be resolved is reconciled too, so it fails closed rather than keeping grants.
    /// </summary>
    private async Task ReconcileDependantsAsync(TenantId tenant, AppSlug owner, CancellationToken cancellationToken)
    {
        List<AppSlug>? dependants = null;
        await foreach (var record in _registry.ListForTenantAsync(tenant, cancellationToken).ConfigureAwait(false))
        {
            if (record.Slug == owner || record.Tenant != tenant || record.State != AppRegistryLifecycleState.Enabled)
            {
                continue;
            }

            var resolved = await _source.ResolveAsync(record.Slug, record.Version, cancellationToken).ConfigureAwait(false);
            if (resolved.Manifest is not { } manifest || DependsOn(manifest, owner))
            {
                (dependants ??= []).Add(record.Slug);
            }
        }

        if (dependants is null)
        {
            return;
        }

        foreach (var dependant in dependants)
        {
            await RunAsync(AppActivationOperation.Reconcile, tenant, dependant, cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool DependsOn(AppManifest manifest, AppSlug owner)
    {
        foreach (var role in manifest.Roles ?? [])
        {
            foreach (var template in role?.Scopes ?? [])
            {
                if (template?.App == owner)
                {
                    return true;
                }
            }
        }

        foreach (var subscription in manifest.Subscriptions ?? [])
        {
            if (subscription?.App == owner)
            {
                return true;
            }
        }

        return false;
    }
}
