namespace Orleans.Lattice.Scaling;

/// <summary>
/// Flat scalar projection of a <see cref="ScalingSignal"/> that backs the
/// <c>orleans.lattice.scaling</c> observable gauges. The live facade computes one
/// of these off its sampling timer (folding the per-account storage list down to
/// two counts) and publishes it through <see cref="ScalingSignalGaugeRegistry"/>,
/// so each gauge callback reads a single already-computed scalar on the scrape
/// path without recomputing anything or touching a list.
/// </summary>
internal readonly record struct ScalingGaugeSnapshot
{
    /// <summary>Smoothed, scale-in-gated scale value (<see cref="ScalingSignal.ScaleValue"/>).</summary>
    public double ScaleValue { get; init; }

    /// <summary>Raw, un-smoothed scale value (<see cref="ScalingSignal.RawScaleValue"/>).</summary>
    public double RawScaleValue { get; init; }

    /// <summary>Normalised grain-activation compute pressure (<see cref="ComputePressure.Activation"/>).</summary>
    public double ActivationPressure { get; init; }

    /// <summary>Normalised host-resource compute pressure (<see cref="ComputePressure.Resource"/>).</summary>
    public double ResourcePressure { get; init; }

    /// <summary>Normalised WAL-dispatch compute pressure (<see cref="ComputePressure.WalDispatch"/>).</summary>
    public double WalDispatchPressure { get; init; }

    /// <summary>Recommended silo replica count (<see cref="ScalingSignal.RecommendedReplicas"/>).</summary>
    public long RecommendedReplicas { get; init; }

    /// <summary>Number of WAL catalogue keys whose retained bytes are over the advisory threshold.</summary>
    public long AccountsOverThreshold { get; init; }

    /// <summary><c>1</c> when a WAL rebalance is recommended, otherwise <c>0</c>.</summary>
    public long RebalanceRecommendations { get; init; }

    /// <summary>
    /// Projects <paramref name="signal"/> down to the flat scalar snapshot the
    /// gauges observe. The per-account list is folded to a single count with a
    /// plain index loop (no LINQ, no allocation); this runs on the facade's
    /// sampling timer, off the scrape path.
    /// </summary>
    /// <param name="signal">The freshly computed scaling signal to project.</param>
    /// <returns>The flat scalar snapshot.</returns>
    public static ScalingGaugeSnapshot FromSignal(in ScalingSignal signal)
    {
        var accounts = signal.Storage.Accounts;
        var overThreshold = 0L;
        for (var i = 0; i < accounts.Count; i++)
        {
            if (accounts[i].OverThreshold)
            {
                overThreshold++;
            }
        }

        return new ScalingGaugeSnapshot
        {
            ScaleValue = signal.ScaleValue,
            RawScaleValue = signal.RawScaleValue,
            ActivationPressure = signal.Compute.Activation,
            ResourcePressure = signal.Compute.Resource,
            WalDispatchPressure = signal.Compute.WalDispatch,
            RecommendedReplicas = signal.RecommendedReplicas,
            AccountsOverThreshold = overThreshold,
            RebalanceRecommendations = signal.Storage.Recommendation is not null ? 1L : 0L,
        };
    }
}
