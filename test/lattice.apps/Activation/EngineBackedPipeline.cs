namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An <see cref="IAppActivationPipeline"/> that runs the engine in-process with no grain hop, for
/// tests that exercise the startup reconcile over real activation logic.
/// </summary>
internal sealed class EngineBackedPipeline(AppActivationEngine engine, IAppActivationStatusStore status) : IAppActivationPipeline
{
    public Func<AppSlug, Exception?>? Throw { get; set; }

    public Task<AppActivationOutcome> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Enable, tenant, slug, cancellationToken);

    public Task<AppActivationOutcome> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Disable, tenant, slug, cancellationToken);

    public Task<AppActivationOutcome> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Uninstall, tenant, slug, cancellationToken);

    public Task<AppActivationOutcome> ReconcileAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        RunAsync(AppActivationOperation.Reconcile, tenant, slug, cancellationToken);

    public Task<AppActivationStatus?> GetStatusAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        status.GetAsync(tenant, slug, cancellationToken);

    private Task<AppActivationOutcome> RunAsync(AppActivationOperation operation, TenantId tenant, AppSlug slug, CancellationToken cancellationToken)
    {
        if (Throw?.Invoke(slug) is { } failure)
            throw failure;
        return engine.ExecuteAsync(operation, tenant, slug, cancellationToken);
    }
}
