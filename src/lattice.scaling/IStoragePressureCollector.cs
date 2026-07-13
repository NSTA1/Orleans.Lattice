namespace Orleans.Lattice.Scaling;

/// <summary>
/// Internal seam that collects the live, cluster-aggregate storage-axis pressure
/// (<see cref="StoragePressure"/>) for the current sample. Defined here (in the
/// compute-axis change, #1186) so the storage-axis change (#1187) can slot its
/// real collector in behind this interface <em>without touching any file owned by
/// the compute change</em>: it replaces the default no-op registration
/// (<see cref="NoOpStoragePressureCollector"/>) with its own implementation.
/// <para>
/// The real <see cref="ILatticeScalingSignal"/> facade composes an
/// <see cref="IComputePressureCollector"/> with one of these to build a full
/// <see cref="ScalingSignal"/>. Until #1187 lands, the default no-op returns a
/// zero <see cref="StoragePressure"/>, so the storage axis is inert.
/// </para>
/// </summary>
internal interface IStoragePressureCollector
{
    /// <summary>
    /// Returns the current cluster-aggregate storage-axis pressure snapshot.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task yielding the current <see cref="StoragePressure"/>.</returns>
    ValueTask<StoragePressure> CollectAsync(CancellationToken cancellationToken);
}
