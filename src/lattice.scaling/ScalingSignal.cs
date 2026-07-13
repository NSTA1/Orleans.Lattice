namespace Orleans.Lattice.Scaling;

/// <summary>
/// Cluster-aggregate, two-axis autoscaling snapshot returned by
/// <see cref="ILatticeScalingSignal.GetScalingSignalAsync(System.Threading.CancellationToken)"/>.
/// Combines a compute axis (<see cref="Compute"/>) and a storage axis
/// (<see cref="Storage"/>) into a single scale demand
/// (<see cref="ScaleValue"/>) plus a concrete replica recommendation
/// (<see cref="RecommendedReplicas"/>) that an external autoscaler can scrape.
/// <para>
/// This is a read-only point-in-time snapshot produced by the silo's compute
/// collector (#1186); the storage axis is populated by #1187. Before the first
/// sample completes the facade returns a warming-up signal
/// (<see cref="ScaleValue"/> zero, <see cref="Reason"/> naming the warm-up state).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ScalingTypeAliases.ScalingSignal)]
[Immutable]
public readonly record struct ScalingSignal
{
    /// <summary>
    /// Aggregate scale demand expressed in replica-units: the number of silo
    /// replicas the combined compute and storage pressure implies. <c>0.0</c>
    /// means no additional demand. Fractional values are permitted so callers
    /// can apply their own rounding or hysteresis.
    /// </summary>
    [Id(0)] public double ScaleValue { get; init; }

    /// <summary>
    /// Concrete recommended silo replica count derived from
    /// <see cref="ScaleValue"/> and any configured floor. An external
    /// autoscaler can consume this directly.
    /// </summary>
    [Id(1)] public int RecommendedReplicas { get; init; }

    /// <summary>The compute-axis pressure component of this signal.</summary>
    [Id(2)] public ComputePressure Compute { get; init; }

    /// <summary>The storage-axis pressure component of this signal.</summary>
    [Id(3)] public StoragePressure Storage { get; init; }

    /// <summary>
    /// Human-readable explanation of how the signal was derived (for example,
    /// which axis dominated the recommendation, or <c>"not yet collecting"</c>
    /// while the collector is not yet wired up).
    /// </summary>
    [Id(4)] public string Reason { get; init; }

    /// <summary>UTC instant at which this snapshot was sampled.</summary>
    [Id(5)] public DateTimeOffset SampledAt { get; init; }

    /// <summary>
    /// The un-smoothed replica-demand scalar for this sample: the dominant
    /// normalised compute dimension multiplied by the current replica count,
    /// before EWMA smoothing and scale-in gating are applied. <see cref="ScaleValue"/>
    /// carries the smoothed, gated value an autoscaler should act on; this field
    /// exposes the raw instantaneous demand for observability and debugging.
    /// </summary>
    [Id(6)] public double RawScaleValue { get; init; }
}
