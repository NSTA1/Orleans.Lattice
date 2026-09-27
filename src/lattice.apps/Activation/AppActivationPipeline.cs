namespace Orleans.Lattice.Apps;

/// <summary>
/// <see cref="IAppActivationPipeline"/> that hands each run to the app's
/// <see cref="IAppActivationGrain"/>, which serializes runs per tenant app and authorizes the
/// caller (whose credential flows on the request context) for
/// <see cref="LatticeOperation.AppInstall"/> before any side effect.
/// </summary>
internal sealed class AppActivationPipeline : IAppActivationPipeline
{
    private readonly IGrainFactory _grainFactory;
    private readonly IAppActivationStatusStore _statusStore;

    public AppActivationPipeline(IGrainFactory grainFactory, IAppActivationStatusStore statusStore)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(statusStore);
        _grainFactory = grainFactory;
        _statusStore = statusStore;
    }

    public Task<AppActivationOutcome> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Enable, tenant, slug, cancellationToken);

    public Task<AppActivationOutcome> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Disable, tenant, slug, cancellationToken);

    public Task<AppActivationOutcome> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Uninstall, tenant, slug, cancellationToken);

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
}
