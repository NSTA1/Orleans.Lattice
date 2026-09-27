namespace Orleans.Lattice.Apps;

/// <summary>
/// Stateless, non-reentrant grain that runs the activation pipeline for one tenant app,
/// serializing runs for that app across the cluster. See <see cref="AppActivationRunner"/> for
/// the authorization it enforces.
/// </summary>
internal sealed class AppActivationGrain(AppActivationRunner runner) : Grain, IAppActivationGrain
{
    public Task<AppActivationOutcome> ExecuteAsync(
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        CancellationToken cancellationToken = default) =>
        runner.RunAsync(this.GetPrimaryKeyString(), operation, tenant, slug, cancellationToken);
}
