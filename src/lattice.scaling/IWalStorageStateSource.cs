namespace Orleans.Lattice.Scaling;

/// <summary>
/// Internal seam over the cluster-aggregate WAL storage state the storage axis
/// reads each sample tick: per-tree retained bytes, backend saturation, and
/// per-partition provider placement, plus the registered catalogue keys. The
/// production implementation
/// (<see cref="LatticeWalStorageStateSource"/>) sources these from the core
/// <see cref="Orleans.Lattice.ILatticeAdmin"/> surface, the
/// <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/>, and the
/// <see cref="Orleans.Lattice.IWalSaturationSignal"/>. Tests substitute a
/// deterministic fake so the classification and recommendation logic in
/// <see cref="StoragePressureCollector"/> can be exercised without a cluster.
/// <para>
/// Sampled only on the facade's timer (off the scrape path), so the underlying
/// round-trips are amortised over the whole sample interval.
/// </para>
/// </summary>
internal interface IWalStorageStateSource
{
    /// <summary>
    /// Returns the current cluster-aggregate WAL storage sample.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task yielding the current <see cref="WalStorageSample"/>.</returns>
    ValueTask<WalStorageSample> SampleAsync(CancellationToken cancellationToken);
}
