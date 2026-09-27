namespace Orleans.Lattice.Scaling;

/// <summary>
/// Normalised compute-axis pressure for the cluster, one of the two axes of a
/// <see cref="ScalingSignal"/>. Each ratio is in the range <c>0.0</c> (idle) to
/// <c>1.0</c> (saturated); a value at or above <c>1.0</c> indicates the
/// corresponding resource is fully consumed on that dimension.
/// <see cref="Activation"/> and <see cref="Resource"/> are cluster aggregates
/// (the worst-case silo), whereas <see cref="WalDispatch"/> and
/// <see cref="WalSaturation"/> reflect only the silo that computed this snapshot
/// (the silo answering the scaling-signal request).
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
    /// <c>1.0</c>: a step mapping of <see cref="WalSaturation"/> - <c>0.0</c> for
    /// <see cref="Orleans.Lattice.WalSaturationState.Healthy"/> (dispatch is
    /// admitting without waiting), <c>0.5</c> for
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/>, and <c>1.0</c>
    /// for <see cref="Orleans.Lattice.WalSaturationState.Saturated"/> (the
    /// pipeline is at its admission ceiling) - so, like it, it reflects the
    /// answering silo only.
    /// </summary>
    [Id(2)] public double WalDispatch { get; init; }

    /// <summary>
    /// Worst-case <see cref="Orleans.Lattice.WalSaturationState"/> across every
    /// tree the answering silo's saturation sampler has observed so far - the
    /// silo's own view, not an aggregate across the cluster's silos. Callers
    /// should treat <see cref="Orleans.Lattice.WalSaturationState.Saturated"/> as
    /// a hard signal to scale out the compute axis.
    /// </summary>
    [Id(3)] public WalSaturationState WalSaturation { get; init; }
}
