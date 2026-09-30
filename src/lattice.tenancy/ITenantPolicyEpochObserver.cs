namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo callback the <see cref="ITenantPolicyEpochGrain"/> pushes a new
/// cluster epoch to. Implemented by each silo's
/// <see cref="TenantPolicyEpochSubscription"/>.
/// </summary>
[Alias(TenantTypeAliases.ITenantPolicyEpochObserver)]
internal interface ITenantPolicyEpochObserver : IGrainObserver
{
    /// <summary>
    /// Tells the silo the tenant registry has changed. The returned task completes
    /// only once the silo has marked its compiled snapshot out of date and
    /// scheduled a rebuild, so its completion is the acknowledgement the grain
    /// waits for.
    /// </summary>
    /// <param name="epoch">The advanced cluster epoch.</param>
    Task OnEpochAdvancedAsync(TenantPolicyEpoch epoch);
}
