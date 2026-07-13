namespace Orleans.Lattice.Scaling;

/// <summary>
/// Configuration for the <c>Orleans.Lattice.Scaling</c> autoscaling signal,
/// bound through
/// <see cref="LatticeScalingServiceCollectionExtensions.AddLatticeScalingSignal(Orleans.Hosting.ISiloBuilder, System.Action{LatticeScalingSignalOptions})"/>.
/// <para>
/// This scaffold carries only the surface the endpoint issue (#1188) needs plus
/// a replica floor. The compute-collector (#1186), storage-axis (#1187), and
/// endpoint (#1188) issues extend this type with their own knobs (sampling
/// interval, thresholds, weighting, endpoint auth); keep additions backward
/// compatible so downstream hosts binding this options type are not broken.
/// </para>
/// </summary>
public sealed class LatticeScalingSignalOptions
{
    /// <summary>
    /// Default value for <see cref="EndpointPath"/>: <c>/lattice/scale</c>.
    /// </summary>
    public const string DefaultEndpointPath = "/lattice/scale";

    /// <summary>
    /// Default value for <see cref="MinReplicas"/>: <c>0</c> (no floor).
    /// </summary>
    public const int DefaultMinReplicas = 0;

    /// <summary>
    /// The HTTP path the scaling endpoint (#1188) serves the
    /// <see cref="ScalingSignal"/> from. Defaults to
    /// <see cref="DefaultEndpointPath"/>.
    /// </summary>
    public string EndpointPath { get; set; } = DefaultEndpointPath;

    /// <summary>
    /// Lower bound applied to <see cref="ScalingSignal.RecommendedReplicas"/>:
    /// the recommendation is never reported below this floor. Defaults to
    /// <see cref="DefaultMinReplicas"/>.
    /// </summary>
    public int MinReplicas { get; set; } = DefaultMinReplicas;
}
