namespace Orleans.Lattice.Apps;

/// <summary>
/// Stateless, non-reentrant grain that runs the activation pipeline for one tenant app,
/// serializing runs for that app across the cluster. See <see cref="AppActivationRunner"/> for
/// the authorization it enforces.
/// </summary>
internal sealed class AppActivationGrain(AppActivationRunner runner) : Grain, IAppActivationGrain
{
    private AppActivationOperation? _running;

    public async Task<AppActivationOutcome> ExecuteAsync(
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        CancellationToken cancellationToken = default)
    {
        // The grain is non-reentrant, so at most one run executes here at a time.
        _running = operation;
        try
        {
            return await runner.RunAsync(this.GetPrimaryKeyString(), operation, tenant, slug, cancellationToken);
        }
        finally
        {
            _running = null;
        }
    }

    public Task<AppActivationOperation?> GetRunningOperationAsync() => Task.FromResult(_running);
}
