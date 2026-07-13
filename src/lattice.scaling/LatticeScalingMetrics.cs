using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/>
/// instruments for <c>Orleans.Lattice.Scaling</c>. Every autoscaling instrument
/// is published on a single <see cref="Meter"/> named <see cref="MeterName"/> so
/// an OpenTelemetry pipeline can subscribe once and receive the whole two-axis
/// scaling signal. Mirrors the structure of
/// <c>Orleans.Lattice.Auth.LatticeAuthMetrics</c>.
/// </summary>
/// <remarks>
/// <para>
/// Every instrument on this meter is an <see cref="ObservableGauge{T}"/>: the
/// live <see cref="ILatticeScalingSignal"/> facade samples cluster-aggregate
/// pressure on its own timer and publishes a flat scalar snapshot
/// (see <see cref="ScalingSignalGaugeRegistry"/>), and each gauge's measurement
/// callback simply reads one already-computed scalar from that snapshot. The
/// callbacks therefore allocate nothing, recompute nothing, and never touch the
/// per-account list on the scrape path.
/// </para>
/// <para>
/// The gauges are created when the facade starts (see
/// <see cref="ScalingSignalGaugeRegistry.EnsureRegistered"/>), so a snapshot
/// <see cref="MeterListener"/> attached before any silo has started will not see
/// them; subscribers should enumerate by the published <c>...Name</c> constants.
/// </para>
/// </remarks>
public static class LatticeScalingMetrics
{
    /// <summary>
    /// The root meter name for all <c>Orleans.Lattice.Scaling</c> telemetry.
    /// Internal telemetry hooks and external subscribers must reference this
    /// constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice.scaling";

    /// <summary>
    /// Canonical name of the smoothed, scale-in-gated scale-value observable
    /// gauge - the replica-demand scalar (the KEDA value) an external autoscaler
    /// should act on. Mirrors <see cref="ScalingSignal.ScaleValue"/>.
    /// </summary>
    public const string ScaleValueName = "orleans.lattice.scaling.scale_value";

    /// <summary>
    /// Canonical name of the raw, un-smoothed scale-value observable gauge - the
    /// instantaneous replica demand before EWMA smoothing and scale-in gating.
    /// Mirrors <see cref="ScalingSignal.RawScaleValue"/>.
    /// </summary>
    public const string RawScaleValueName = "orleans.lattice.scaling.raw_scale_value";

    /// <summary>
    /// Canonical name of the normalised grain-activation compute-pressure
    /// observable gauge (0.0 idle to 1.0 saturated). Mirrors
    /// <see cref="ComputePressure.Activation"/>.
    /// </summary>
    public const string ComputeActivationPressureName = "orleans.lattice.scaling.compute.activation_pressure";

    /// <summary>
    /// Canonical name of the normalised host-resource compute-pressure
    /// observable gauge (0.0 idle to 1.0 saturated). Mirrors
    /// <see cref="ComputePressure.Resource"/>.
    /// </summary>
    public const string ComputeResourcePressureName = "orleans.lattice.scaling.compute.resource_pressure";

    /// <summary>
    /// Canonical name of the normalised WAL-dispatch compute-pressure observable
    /// gauge (0.0 idle to 1.0 saturated). Mirrors
    /// <see cref="ComputePressure.WalDispatch"/>.
    /// </summary>
    public const string ComputeWalDispatchPressureName = "orleans.lattice.scaling.compute.wal_dispatch_pressure";

    /// <summary>
    /// Canonical name of the recommended-replica-count observable gauge - the
    /// concrete silo replica count the signal recommends, honouring the replica
    /// floor. Mirrors <see cref="ScalingSignal.RecommendedReplicas"/>.
    /// </summary>
    public const string ComputeReplicasName = "orleans.lattice.scaling.compute.replicas";

    /// <summary>
    /// Canonical name of the storage-axis over-threshold-account observable
    /// gauge - the number of WAL catalogue keys whose retained bytes have crossed
    /// the advisory threshold. Derived from
    /// <see cref="StoragePressure.Accounts"/>.
    /// </summary>
    public const string StorageAccountsOverThresholdName = "orleans.lattice.scaling.storage.accounts_over_threshold";

    /// <summary>
    /// Canonical name of the storage-axis rebalance-recommendation observable
    /// gauge - <c>1</c> when the storage collector is recommending a WAL move,
    /// otherwise <c>0</c>. Derived from
    /// <see cref="StoragePressure.Recommendation"/>.
    /// </summary>
    public const string StorageRebalanceRecommendationsName = "orleans.lattice.scaling.storage.rebalance_recommendations";

    /// <summary>
    /// The meter that owns every autoscaling instrument. Exposed publicly so
    /// integration tests and custom OpenTelemetry exporters can subscribe by
    /// reference rather than by name.
    /// </summary>
    public static readonly Meter Meter = new(MeterName);
}
