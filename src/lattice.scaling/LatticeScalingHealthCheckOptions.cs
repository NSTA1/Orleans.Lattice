namespace Orleans.Lattice.Scaling;

/// <summary>
/// Configuration thresholds for <c>LatticeScalingHealthCheck</c>, the ASP.NET
/// Core / Kubernetes probe target that projects the cluster-aggregate
/// <see cref="ScalingSignal"/> onto a single
/// <see cref="Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus"/>.
/// Bound via the named-options instance whose name matches the health check's
/// registered name (default <see cref="DefaultName"/>); a host that registers
/// the check under a different name binds against that name.
/// </summary>
/// <remarks>
/// The check reads a point-in-time snapshot from
/// <see cref="ILatticeScalingSignal.GetScalingSignalAsync(System.Threading.CancellationToken)"/>
/// and derives a verdict from two independent inputs: the worst normalised
/// compute-axis pressure dimension (compared against the tiered
/// <see cref="ComputePressure"/> bound and the discrete
/// <see cref="ComputePressure.WalSaturation"/> classification) and the
/// storage-axis <see cref="StoragePressure.OverThreshold"/> flag. The storage
/// axis is advisory only - it is <b>not</b> wired to the replica
/// recommendation - so an over-threshold storage axis contributes at most
/// <see cref="Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Degraded"/>.
/// <para>
/// Setting <see cref="ComputePressure"/> to <see langword="null"/> disables the
/// tiered compute-pressure signal entirely; the boolean toggles disable the
/// WAL-saturation and storage signals independently, so a host can gate
/// readiness on exactly the axes it cares about without rebuilding the check.
/// </para>
/// </remarks>
public sealed class LatticeScalingHealthCheckOptions
{
    /// <summary>
    /// Tiered bound applied to the worst normalised compute-axis pressure
    /// dimension (the maximum of
    /// <see cref="Orleans.Lattice.Scaling.ComputePressure.Activation"/>,
    /// <see cref="Orleans.Lattice.Scaling.ComputePressure.Resource"/>, and
    /// <see cref="Orleans.Lattice.Scaling.ComputePressure.WalDispatch"/>): a
    /// snapshot whose worst dimension is at or above <see cref="DoubleTier.Degraded"/>
    /// reports <c>Degraded</c>; at or above <see cref="DoubleTier.Unhealthy"/>
    /// reports <c>Unhealthy</c>. Set to <see langword="null"/> to disable the
    /// signal. Defaults to <see cref="DefaultComputePressure"/>.
    /// </summary>
    public DoubleTier? ComputePressure { get; set; } = DefaultComputePressure;

    /// <summary>
    /// When <see langword="true"/> (the default), a
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/> worst-case WAL
    /// saturation on the compute axis reports <c>Unhealthy</c> irrespective of
    /// the <see cref="ComputePressure"/> ratios. A saturated WAL is a hard
    /// admission-ceiling signal to scale the compute axis out.
    /// </summary>
    public bool UnhealthyOnWalSaturated { get; set; } = true;

    /// <summary>
    /// When <see langword="true"/> (the default), a
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/> worst-case WAL
    /// saturation on the compute axis contributes <c>Degraded</c> to the
    /// aggregate verdict.
    /// </summary>
    public bool DegradeOnWalThrottled { get; set; } = true;

    /// <summary>
    /// When <see langword="true"/> (the default), an over-threshold storage axis
    /// (<see cref="StoragePressure.OverThreshold"/>) contributes <c>Degraded</c>
    /// to the aggregate verdict. The storage axis never escalates past
    /// <c>Degraded</c> because it is advisory and not wired to the replica
    /// recommendation.
    /// </summary>
    public bool DegradeOnStorageOverThreshold { get; set; } = true;

    /// <summary>
    /// Default for <see cref="ComputePressure"/>: <c>0.85</c> soft, <c>0.95</c>
    /// hard. Sized so a cluster running hot but with headroom degrades while a
    /// near-saturated cluster (within 5% of a ceiling on any compute dimension)
    /// reports unhealthy.
    /// </summary>
    public static readonly DoubleTier DefaultComputePressure = new(0.85d, 0.95d);

    /// <summary>
    /// Default registered name for the health check. Hosts that register it
    /// under a different name supply the alternative name to
    /// <see cref="LatticeScalingServiceCollectionExtensions.AddLatticeScalingHealthCheck(Microsoft.Extensions.Diagnostics.HealthChecks.IHealthChecksBuilder, string, Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus?, System.Collections.Generic.IEnumerable{string})"/>
    /// and bind named options under the same name.
    /// </summary>
    public const string DefaultName = "orleans.lattice.scaling";

    /// <summary>
    /// Tiered bound shape for a normalised <see cref="double"/> pressure ratio.
    /// An observed value at or above <see cref="Degraded"/> classifies as at
    /// least <c>Degraded</c>; one at or above <see cref="Unhealthy"/> classifies
    /// as <c>Unhealthy</c>.
    /// </summary>
    /// <param name="Degraded">Soft bound. Must be non-negative and less than or equal to <paramref name="Unhealthy"/>.</param>
    /// <param name="Unhealthy">Hard bound. Must be non-negative and greater than or equal to <paramref name="Degraded"/>.</param>
    public readonly record struct DoubleTier(double Degraded, double Unhealthy);
}
