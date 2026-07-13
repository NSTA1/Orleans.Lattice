namespace Orleans.Lattice.Scaling;

/// <summary>
/// Read-only facade over the <c>Orleans.Lattice.Scaling</c> autoscaling signal.
/// Resolve it from the silo's service provider (registered by
/// <see cref="LatticeScalingServiceCollectionExtensions.AddLatticeScalingSignal(Orleans.Hosting.ISiloBuilder, System.Action{LatticeScalingSignalOptions})"/>)
/// and call <see cref="GetScalingSignalAsync(System.Threading.CancellationToken)"/>
/// to obtain a cluster-aggregate, two-axis pressure snapshot suitable for an
/// external autoscaler to scrape.
/// <para>
/// The scaffold implementation returns a well-formed zero/stub signal; live
/// pressure collection is added by later issues (#1186 compute, #1187 storage,
/// #1188 endpoint).
/// </para>
/// </summary>
public interface ILatticeScalingSignal
{
    /// <summary>
    /// Returns the current cluster-aggregate <see cref="ScalingSignal"/>. Cheap
    /// to call repeatedly - intended to back a per-scrape HTTP endpoint.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task yielding the current two-axis scaling snapshot.</returns>
    Task<ScalingSignal> GetScalingSignalAsync(CancellationToken cancellationToken = default);
}
