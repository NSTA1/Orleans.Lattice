namespace Orleans.Lattice.Scaling;

/// <summary>
/// Configuration for the <c>Orleans.Lattice.Scaling</c> autoscaling signal,
/// bound through
/// <see cref="LatticeScalingServiceCollectionExtensions.AddLatticeScalingSignal(Orleans.Hosting.ISiloBuilder, System.Action{LatticeScalingSignalOptions})"/>.
/// <para>
/// The knobs are grouped by axis: the endpoint path and replica floor, the
/// compute-axis collector cadence and scale-in gating, and the storage-axis
/// retention and rebalance thresholds. Keep additions backward compatible so
/// downstream hosts binding this options type are not broken.
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

    // --- Compute axis (#1186) ---------------------------------------------
    // Knobs for the compute-axis pressure collector and the replica-demand
    // scalar (sampling cadence, EWMA smoothing, scale-in gating, per-dimension
    // scale-in thresholds, and the activation working-set target). Added as a
    // contiguous region so a merge with sibling option additions stays trivial.

    /// <summary>Default value for <see cref="SampleInterval"/>: 5 seconds.</summary>
    public static readonly TimeSpan DefaultSampleInterval = TimeSpan.FromSeconds(5);

    /// <summary>Default value for <see cref="EwmaHalfLife"/>: 30 seconds.</summary>
    public static readonly TimeSpan DefaultEwmaHalfLife = TimeSpan.FromSeconds(30);

    /// <summary>Default value for <see cref="ScaleInGateWindow"/>: 2 minutes.</summary>
    public static readonly TimeSpan DefaultScaleInGateWindow = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Default value for <see cref="ActivationScaleInThreshold"/>,
    /// <see cref="ResourceScaleInThreshold"/>, and
    /// <see cref="WalDispatchScaleInThreshold"/>: <c>0.25</c>.
    /// </summary>
    public const double DefaultScaleInThreshold = 0.25;

    /// <summary>
    /// Default value for <see cref="ActivationWorkingSetTarget"/>: <c>100000</c>
    /// activations per silo.
    /// </summary>
    public const int DefaultActivationWorkingSetTarget = 100_000;

    /// <summary>
    /// Default value for <see cref="SplitAwareScaleIn"/>: <see langword="true"/>.
    /// The scale-in gate is split-aware out of the box.
    /// </summary>
    public const bool DefaultSplitAwareScaleIn = true;

    /// <summary>
    /// How often the silo samples cluster-aggregate compute pressure and
    /// recomputes the scaling signal. The per-scrape facade reads the cached
    /// result, so this is the freshness bound, not the scrape cost. Defaults to
    /// <see cref="DefaultSampleInterval"/>.
    /// </summary>
    public TimeSpan SampleInterval { get; set; } = DefaultSampleInterval;

    /// <summary>
    /// Half-life of the exponentially-weighted moving average applied to the
    /// replica-demand scalar on the scale-in (release) side. A longer half-life
    /// damps per-tick noise more aggressively and makes scale-in more
    /// conservative; scale-out reacts immediately regardless. Defaults to
    /// <see cref="DefaultEwmaHalfLife"/>.
    /// </summary>
    public TimeSpan EwmaHalfLife { get; set; } = DefaultEwmaHalfLife;

    /// <summary>
    /// How long every scale-in precondition (all compute dimensions low, WAL
    /// healthy, no shard split in flight) must hold continuously before the
    /// scalar is allowed to fall toward scale-in. Any break resets the window.
    /// Defaults to <see cref="DefaultScaleInGateWindow"/>.
    /// </summary>
    public TimeSpan ScaleInGateWindow { get; set; } = DefaultScaleInGateWindow;

    /// <summary>
    /// Activation-pressure level (0..1) at or above which the activation
    /// dimension is considered too hot to permit scale-in. Defaults to
    /// <see cref="DefaultScaleInThreshold"/>.
    /// </summary>
    public double ActivationScaleInThreshold { get; set; } = DefaultScaleInThreshold;

    /// <summary>
    /// Resource-pressure level (0..1) at or above which the resource dimension is
    /// considered too hot to permit scale-in. Defaults to
    /// <see cref="DefaultScaleInThreshold"/>.
    /// </summary>
    public double ResourceScaleInThreshold { get; set; } = DefaultScaleInThreshold;

    /// <summary>
    /// WAL-dispatch-pressure level (0..1) at or above which the WAL-dispatch
    /// dimension is considered too hot to permit scale-in. Defaults to
    /// <see cref="DefaultScaleInThreshold"/>.
    /// </summary>
    public double WalDispatchScaleInThreshold { get; set; } = DefaultScaleInThreshold;

    /// <summary>
    /// Per-silo grain-activation count treated as full activation saturation when
    /// normalising the activation dimension: activation pressure is
    /// <c>activationCount / target</c>, clamped to 0..1. Defaults to
    /// <see cref="DefaultActivationWorkingSetTarget"/>.
    /// </summary>
    public int ActivationWorkingSetTarget { get; set; } = DefaultActivationWorkingSetTarget;

    /// <summary>
    /// Whether the scale-in safety gate is split-aware: while any adaptive shard
    /// split is in flight cluster-wide, scale-in is suppressed. Defaults to
    /// <see cref="DefaultSplitAwareScaleIn"/> (<see langword="true"/>).
    /// <para>
    /// The signal costs one call to the cluster's split-admission singleton per
    /// sample tick and never fans out. Set to <see langword="false"/> to make the
    /// split axis inert - appropriate for a deployment with autonomic splitting
    /// disabled, where the query would be pure overhead. Scale-<em>out</em> is
    /// never influenced by split activity either way.
    /// </para>
    /// </summary>
    public bool SplitAwareScaleIn { get; set; } = DefaultSplitAwareScaleIn;

    // --- Storage axis (#1187) ---------------------------------------------
    // Knobs for the storage-axis pressure collector: the retained-bytes advisory
    // ratio that classifies capacity pressure, the window a provider key must
    // stay saturated before it counts as throughput-bound, and a master toggle
    // for emitting rebalance recommendations. Kept as a contiguous region so a
    // merge with sibling option additions stays trivial. The storage axis is
    // report-only: none of these knobs affect the compute scale value.

    /// <summary>
    /// Default value for <see cref="RetainedBytesAdvisoryRatio"/>: <c>0.8</c>.
    /// </summary>
    public const double DefaultRetainedBytesAdvisoryRatio = 0.8;

    /// <summary>
    /// Default value for <see cref="StorageRecommendationsEnabled"/>:
    /// <see langword="true"/>.
    /// </summary>
    public const bool DefaultStorageRecommendationsEnabled = true;

    /// <summary>Default value for <see cref="AccountSaturationWindow"/>: 30 seconds.</summary>
    public static readonly TimeSpan DefaultAccountSaturationWindow = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Fraction of <see cref="Orleans.Lattice.LatticeOptions.WalMaxRetainedBytes"/>
    /// at or above which retained WAL bytes are treated as capacity pressure -
    /// both for the aggregate <see cref="StoragePressure.OverThreshold"/> flag and
    /// for per-account <see cref="WalAccountPressure.OverThreshold"/> /
    /// <see cref="WalPressureClassification.CapacityBound"/> classification. Clamped
    /// to the open-closed interval <c>(0, 1]</c> at evaluation time. Ignored when
    /// <see cref="Orleans.Lattice.LatticeOptions.WalMaxRetainedBytes"/> is
    /// <see langword="null"/> (no ceiling configured). Defaults to
    /// <see cref="DefaultRetainedBytesAdvisoryRatio"/>.
    /// </summary>
    public double RetainedBytesAdvisoryRatio { get; set; } = DefaultRetainedBytesAdvisoryRatio;

    /// <summary>
    /// How long a provider key must have been continuously observed saturated
    /// before the collector classifies it
    /// <see cref="WalPressureClassification.ThroughputBound"/> and recommends a
    /// move. Debounces a transient saturation blip into a durable signal. A
    /// non-positive value classifies on the first saturated sample (no debounce).
    /// Defaults to <see cref="DefaultAccountSaturationWindow"/>.
    /// </summary>
    public TimeSpan AccountSaturationWindow { get; set; } = DefaultAccountSaturationWindow;

    /// <summary>
    /// Master switch for emitting a <see cref="WalRebalanceRecommendation"/> on
    /// the storage axis. When <see langword="false"/> the collector still reports
    /// <see cref="StoragePressure.OverThreshold"/> and the per-account breakdown
    /// but leaves <see cref="StoragePressure.Recommendation"/> <see langword="null"/>.
    /// Defaults to <see cref="DefaultStorageRecommendationsEnabled"/>.
    /// </summary>
    public bool StorageRecommendationsEnabled { get; set; } = DefaultStorageRecommendationsEnabled;
}
