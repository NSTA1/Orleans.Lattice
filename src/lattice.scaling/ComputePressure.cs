namespace Orleans.Lattice.Scaling;

/// <summary>
/// Normalised compute-axis pressure for the cluster, one of the two axes of a
/// <see cref="ScalingSignal"/>. Each ratio is a cluster-aggregate in the range
/// <c>0.0</c> (idle) to <c>1.0</c> (saturated); a value at or above <c>1.0</c>
/// indicates the corresponding resource is fully consumed and the cluster is
/// under-provisioned on that dimension.
/// <para>
/// This is a read-only point-in-time snapshot collected by the silo's compute
/// pressure collector. Before the first sample completes the facade returns an
/// all-zero, <see cref="Orleans.Lattice.WalSaturationState.Healthy"/> instance.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ScalingTypeAliases.ComputePressure)]
[Immutable]
public readonly record struct ComputePressure
{
    /// <summary>
    /// Normalised grain-activation pressure in the range <c>0.0</c> to
    /// <c>1.0</c>: how close the cluster is to its activation-working-set
    /// ceiling. <c>0.0</c> means negligible activation load.
    /// </summary>
    [Id(0)] public double Activation { get; init; }

    /// <summary>
    /// Normalised host-resource pressure in the range <c>0.0</c> to <c>1.0</c>:
    /// the worst-case of CPU and memory headroom across the silo pool.
    /// <c>0.0</c> means ample headroom.
    /// </summary>
    [Id(1)] public double Resource { get; init; }

    /// <summary>
    /// Normalised write-ahead-log dispatch pressure in the range <c>0.0</c> to
    /// <c>1.0</c>: how close the WAL append-dispatch pipeline is to its
    /// admission ceiling. <c>0.0</c> means dispatch is admitting without waiting.
    /// </summary>
    [Id(2)] public double WalDispatch { get; init; }

    /// <summary>
    /// Worst-case <see cref="Orleans.Lattice.WalSaturationState"/> observed
    /// across every tree and partition in the cluster. Callers should treat
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/> as a hard
    /// signal to scale out the compute axis.
    /// </summary>
    [Id(3)] public WalSaturationState WalSaturation { get; init; }
}
