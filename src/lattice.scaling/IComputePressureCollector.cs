namespace Orleans.Lattice.Scaling;

/// <summary>
/// Internal seam that collects the live, cluster-aggregate compute-axis pressure
/// (<see cref="ComputePressure"/>) for the current sample. The real
/// <see cref="ILatticeScalingSignal"/> facade composes one of these together with
/// an <see cref="IStoragePressureCollector"/> to build a full
/// <see cref="ScalingSignal"/>.
/// <para>
/// Implementations are expected to be cheap to call: the heavy sampling (a
/// cluster management round-trip, WAL-saturation reads) is performed off the
/// silo's sampling timer and cached, so a call here reads recent cached state
/// rather than fanning out per invocation.
/// </para>
/// </summary>
internal interface IComputePressureCollector
{
    /// <summary>
    /// Returns the current cluster-aggregate compute-axis pressure snapshot.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task yielding the current <see cref="ComputePressure"/>.</returns>
    ValueTask<ComputePressure> CollectAsync(CancellationToken cancellationToken);
}
