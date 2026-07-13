using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Turns a per-tick compute/storage pressure snapshot into a
/// <see cref="ScalingSignal"/>: it selects the dominant compute dimension,
/// computes the raw replica-demand scalar, applies asymmetric smoothing
/// (fast-attack on scale-out, EWMA slow-release on scale-in), enforces the
/// gated scale-in safety window and the replica floor, and names the dominant
/// dimension in <see cref="ScalingSignal.Reason"/>.
/// <para>
/// The scalar is driven only by scale-relievable compute pressure: adding silo
/// replicas spreads activation working set, host-resource load, and WAL dispatch
/// admission, so those three dimensions (and only those) feed the demand. The
/// storage axis is carried through for observability but does not inflate the
/// compute scalar. Stateful across ticks (EWMA + scale-in window); not
/// thread-safe - the owning facade calls it from a single sampling timer.
/// </para>
/// </summary>
internal sealed class ScalingSignalComputer(IOptions<LatticeScalingSignalOptions> options)
{
    private readonly IOptions<LatticeScalingSignalOptions> _options = options;
    private readonly Ewma _ewma = new();
    private readonly ScaleInGate _scaleInGate = new();

    private double _heldScalar;

    /// <summary>
    /// Computes the scaling signal for one sample. Updates the internal EWMA and
    /// scale-in window as a side effect, so successive calls model successive
    /// sample ticks.
    /// </summary>
    /// <param name="compute">This tick's cluster-aggregate compute pressure.</param>
    /// <param name="storage">This tick's cluster-aggregate storage pressure (carried through, not scaled).</param>
    /// <param name="replicaCount">The current active replica count.</param>
    /// <param name="splitInFlight">Whether any shard split is in flight (suppresses scale-in).</param>
    /// <param name="now">This tick's timestamp.</param>
    /// <returns>The computed <see cref="ScalingSignal"/>.</returns>
    internal ScalingSignal Compute(
        ComputePressure compute,
        StoragePressure storage,
        int replicaCount,
        bool splitInFlight,
        DateTimeOffset now)
    {
        var options = _options.Value;
        var replicas = Math.Max(1, replicaCount);

        var activation = ComputePressureMath.Clamp01(compute.Activation);
        var resource = ComputePressureMath.Clamp01(compute.Resource);
        var walDispatch = ComputePressureMath.Clamp01(compute.WalDispatch);

        // Dominant (max, not sum) dimension so one bottleneck is unambiguous.
        var dominantValue = activation;
        var dominantName = ActivationDimension;
        if (resource > dominantValue)
        {
            dominantValue = resource;
            dominantName = ResourceDimension;
        }

        if (walDispatch > dominantValue)
        {
            dominantValue = walDispatch;
            dominantName = WalDispatchDimension;
        }

        var rawScalar = dominantValue * replicas;

        var floor = Math.Max(0, options.MinReplicas);
        var eligibleForScaleIn =
            activation < options.ActivationScaleInThreshold &&
            resource < options.ResourceScaleInThreshold &&
            walDispatch < options.WalDispatchScaleInThreshold &&
            compute.WalSaturation == WalSaturationState.Healthy &&
            !splitInFlight;

        var scaleInAllowed = _scaleInGate.Evaluate(eligibleForScaleIn, now, options.ScaleInGateWindow);

        double finalScalar;
        var scaleInHeld = false;
        if (rawScalar >= _heldScalar)
        {
            // Scale-out (or steady): react immediately and snap the EWMA baseline
            // to the peak so any later release decays from here.
            finalScalar = rawScalar;
            _ewma.Set(rawScalar, now);
        }
        else
        {
            // Falling demand: only permit descent through the gated window,
            // otherwise hold at the previous level (conservative scale-in).
            var smoothed = _ewma.Update(rawScalar, now, options.EwmaHalfLife);
            if (scaleInAllowed)
            {
                finalScalar = smoothed;
            }
            else
            {
                finalScalar = _heldScalar;
                _ewma.Set(_heldScalar, now);
                scaleInHeld = true;
            }
        }

        // Respect the replica floor for scale-in safety.
        if (finalScalar < floor)
        {
            finalScalar = floor;
        }

        _heldScalar = finalScalar;

        var recommended = Math.Max(floor, (int)Math.Ceiling(finalScalar));

        return new ScalingSignal
        {
            ScaleValue = finalScalar,
            RawScaleValue = rawScalar,
            RecommendedReplicas = recommended,
            Compute = compute,
            Storage = storage,
            Reason = BuildReason(dominantName, dominantValue, replicas, scaleInHeld),
            SampledAt = now,
        };
    }

    private static string BuildReason(string dominantName, double dominantValue, int replicas, bool scaleInHeld)
    {
        var core = string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"{dominantName} pressure {dominantValue:0.00} across {replicas} replica(s)");
        return scaleInHeld ? core + "; scale-in held by safety gate" : core;
    }

    /// <summary>Dominant-dimension label for the activation axis.</summary>
    internal const string ActivationDimension = "activation";

    /// <summary>Dominant-dimension label for the host-resource axis.</summary>
    internal const string ResourceDimension = "resource";

    /// <summary>Dominant-dimension label for the WAL-dispatch axis.</summary>
    internal const string WalDispatchDimension = "wal-dispatch";
}
