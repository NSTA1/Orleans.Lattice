namespace Orleans.Lattice.Apps;

/// <summary>
/// Reads and advances the cluster-wide <see cref="AppRegistryEpoch"/> (#5595). The production
/// implementation is <see cref="GrainAppRegistryEpochSource"/>; the seam exists so the cross-silo
/// currency of <see cref="CompiledAppRegistrySnapshotMaintainer"/> can be exercised deterministically
/// with several maintainers sharing one epoch.
/// </summary>
internal interface IAppRegistryEpochSource
{
    /// <summary>Returns the current cluster-wide epoch.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The current epoch.</returns>
    Task<AppRegistryEpoch> GetAsync(CancellationToken cancellationToken);

    /// <summary>Advances the cluster-wide epoch after a committed registry write.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The advanced epoch.</returns>
    Task<AppRegistryEpoch> AdvanceAsync(CancellationToken cancellationToken);
}
