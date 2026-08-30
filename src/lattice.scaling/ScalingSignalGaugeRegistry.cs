using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Backs the <c>orleans.lattice.scaling</c> observable gauges. The live
/// <see cref="LatticeScalingSignal"/> facade calls <see cref="EnsureRegistered"/>
/// once when it starts to create the gauges (idempotent, process-wide), then
/// calls <see cref="Publish"/> on every sampling tick to publish the latest flat
/// scalar snapshot. Each gauge's measurement callback reads exactly one scalar
/// from the published fields with a single <see cref="Volatile"/> read, so the
/// scrape path allocates nothing and never recomputes the signal.
/// </summary>
/// <remarks>
/// The published state is a set of individual scalar fields rather than a single
/// struct so that each gauge callback can read its one field with an atomic,
/// tear-free <see cref="System.Threading.Volatile"/> read (<c>Volatile.Read(ref double)</c> / <c>Volatile.Read(ref long)</c>).
/// Cross-field consistency is not required (each gauge reports one dimension
/// independently), so no lock is taken on the scrape path. The facade is the
/// single writer; the last silo to sample in a shared process wins, which is the
/// intended behaviour for a per-process cluster-aggregate signal.
/// </remarks>
internal static class ScalingSignalGaugeRegistry
{
    private static readonly object Lock = new();
    private static bool _registered;

    private static double _scaleValue;
    private static double _rawScaleValue;
    private static double _activationPressure;
    private static double _resourcePressure;
    private static double _walDispatchPressure;
    private static long _recommendedReplicas;
    private static long _accountsOverThreshold;
    private static long _rebalanceRecommendations;

    /// <summary>
    /// Creates the observable gauges on <see cref="LatticeScalingMetrics.Meter"/>
    /// exactly once. Safe to call from every facade instance and every start.
    /// </summary>
    public static void EnsureRegistered()
    {
        if (Volatile.Read(ref _registered))
        {
            return;
        }

        lock (Lock)
        {
            if (_registered)
            {
                return;
            }

            var meter = LatticeScalingMetrics.Meter;

            meter.CreateObservableGauge(
                LatticeScalingMetrics.ScaleValueName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _scaleValue)),
                unit: "{replica}",
                description: "Smoothed, scale-in-gated replica-demand scalar an autoscaler should act on.");

            meter.CreateObservableGauge(
                LatticeScalingMetrics.RawScaleValueName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _rawScaleValue)),
                unit: "{replica}",
                description: "Raw, un-smoothed replica-demand scalar before smoothing and scale-in gating.");

            meter.CreateObservableGauge(
                LatticeScalingMetrics.ComputeActivationPressureName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _activationPressure)),
                unit: "1",
                description: "Normalised grain-activation compute pressure (0.0 idle to 1.0 saturated).");

            meter.CreateObservableGauge(
                LatticeScalingMetrics.ComputeResourcePressureName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _resourcePressure)),
                unit: "1",
                description: "Normalised host-resource compute pressure (0.0 idle to 1.0 saturated).");

            meter.CreateObservableGauge(
                LatticeScalingMetrics.ComputeWalDispatchPressureName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _walDispatchPressure)),
                unit: "1",
                description: "Normalised WAL-dispatch compute pressure (0.0 idle to 1.0 saturated).");

            meter.CreateObservableGauge(
                LatticeScalingMetrics.ComputeReplicasName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _recommendedReplicas)),
                unit: "{replica}",
                description: "Recommended silo replica count honouring the replica floor.");

            meter.CreateObservableGauge(
                LatticeScalingMetrics.StorageAccountsOverThresholdName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _accountsOverThreshold)),
                unit: "{account}",
                description: "WAL catalogue keys whose retained bytes are over the advisory threshold.");

            meter.CreateObservableGauge(
                LatticeScalingMetrics.StorageRebalanceRecommendationsName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _rebalanceRecommendations)),
                unit: "1",
                description: "1 when a WAL rebalance is recommended, otherwise 0.");

            Volatile.Write(ref _registered, true);
        }
    }

    /// <summary>
    /// Publishes <paramref name="snapshot"/> as the latest values the gauges
    /// observe. Called on the facade's sampling timer, off the scrape path.
    /// </summary>
    /// <param name="snapshot">The flat scalar snapshot to publish.</param>
    public static void Publish(in ScalingGaugeSnapshot snapshot)
    {
        Volatile.Write(ref _scaleValue, snapshot.ScaleValue);
        Volatile.Write(ref _rawScaleValue, snapshot.RawScaleValue);
        Volatile.Write(ref _activationPressure, snapshot.ActivationPressure);
        Volatile.Write(ref _resourcePressure, snapshot.ResourcePressure);
        Volatile.Write(ref _walDispatchPressure, snapshot.WalDispatchPressure);
        Volatile.Write(ref _recommendedReplicas, snapshot.RecommendedReplicas);
        Volatile.Write(ref _accountsOverThreshold, snapshot.AccountsOverThreshold);
        Volatile.Write(ref _rebalanceRecommendations, snapshot.RebalanceRecommendations);
    }

    /// <summary>
    /// The latest published snapshot, reassembled from the individual scalar
    /// fields. Exposed for tests; the gauges read the fields directly.
    /// </summary>
    public static ScalingGaugeSnapshot Latest => new()
    {
        ScaleValue = Volatile.Read(ref _scaleValue),
        RawScaleValue = Volatile.Read(ref _rawScaleValue),
        ActivationPressure = Volatile.Read(ref _activationPressure),
        ResourcePressure = Volatile.Read(ref _resourcePressure),
        WalDispatchPressure = Volatile.Read(ref _walDispatchPressure),
        RecommendedReplicas = Volatile.Read(ref _recommendedReplicas),
        AccountsOverThreshold = Volatile.Read(ref _accountsOverThreshold),
        RebalanceRecommendations = Volatile.Read(ref _rebalanceRecommendations),
    };
}
