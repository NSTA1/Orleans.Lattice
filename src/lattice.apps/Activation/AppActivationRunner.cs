namespace Orleans.Lattice.Apps;

/// <summary>
/// The body of an <see cref="AppActivationGrain"/> call: binds the run to the grain's key,
/// authorizes the caller for <see cref="LatticeOperation.AppInstall"/>, then runs the engine.
/// Authorization lives here, on the grain every run passes through, rather than in the pipeline
/// facade, so a caller that reaches the grain directly - an external Orleans client included,
/// whose forged system-origin marker the capability-stripping filter removes - is gated too.
/// </summary>
internal sealed class AppActivationRunner(AppInstallAuthorizer authorizer, AppActivationEngine engine)
{
    public async Task<AppActivationOutcome> RunAsync(
        string grainKey,
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        CancellationToken cancellationToken)
    {
        // Programmer errors surface before authorization; authorization precedes every side
        // effect, so a denied caller changes nothing and learns nothing.
        var expectedKey = AppRegistryTreeNames.ComposeKey(tenant, slug);
        if (!string.Equals(grainKey, expectedKey, StringComparison.Ordinal))
        {
            throw new ArgumentException($"Activation grain '{grainKey}' cannot run app '{expectedKey}'.", nameof(slug));
        }

        await authorizer.AuthorizeAsync(cancellationToken).ConfigureAwait(false);
        return await engine.ExecuteAsync(operation, tenant, slug, cancellationToken).ConfigureAwait(false);
    }
}
