namespace Orleans.Lattice.Apps;

/// <summary>
/// The per-silo callback the <see cref="IAppRegistryEpochGrain"/> pushes an advanced epoch to (#5713).
/// Implemented by each silo's <see cref="AppRegistryEpochSubscription"/>.
/// </summary>
[Alias(AppRegistryTypeAliases.IAppRegistryEpochObserver)]
internal interface IAppRegistryEpochObserver : IGrainObserver
{
    /// <summary>
    /// Tells the silo the app registry has changed. The returned task completes only once the silo has
    /// marked its compiled snapshot out of date, so its completion is the acknowledgement the grain
    /// waits for before the registry write is acknowledged.
    /// </summary>
    /// <param name="epoch">The advanced cluster epoch.</param>
    /// <returns>The acknowledgement.</returns>
    Task OnEpochAdvancedAsync(AppRegistryEpoch epoch);
}
