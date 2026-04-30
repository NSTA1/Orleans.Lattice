using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains.Vehicles;

/// <summary>
/// Pure speed model:
/// <list type="bullet">
///   <item>Within the first or last <see cref="NearCityFraction"/> of the segment length, target
///         speed is sampled uniformly in <c>[1, 40] kph</c>.</item>
///   <item>Otherwise (cruise), target speed is sampled uniformly in <c>[50, 140] kph</c>.</item>
/// </list>
/// Targets are re-sampled at most once per <see cref="VehicleConfig.SpeedResampleInterval"/>; an
/// exponential moving average (alpha = <see cref="VehicleConfig.SpeedSmoothingAlpha"/>) smooths the
/// transition from the previous speed.
/// </summary>
public static class SpeedModel
{
    public const double ApproachMinKph = 1.0;
    public const double ApproachMaxKph = 40.0;
    public const double CruiseMinKph = 50.0;
    public const double CruiseMaxKph = 140.0;

    /// <summary>
    /// Fraction of the segment length on each side that counts as "near a city". Using a fraction
    /// rather than a fixed kilometre value makes the slow-down/ease-out behaviour scale naturally
    /// with segment distance — a 30 km hop has a 3 km approach window, a 500 km cruise has a 50 km
    /// one — so vehicles spend a similar proportion of every leg in the approach regime.
    /// </summary>
    public const double NearCityFraction = 0.10;

    /// <summary>Returns whether the given segment position is in the approach/departure regime.</summary>
    public static bool IsNearCity(double segmentProgressKm, double segmentLengthKm)
    {
        var window = segmentLengthKm * NearCityFraction;
        return segmentProgressKm < window
            || segmentLengthKm - segmentProgressKm < window;
    }

    /// <summary>Sample a fresh target speed for the given segment position.</summary>
    public static double SampleTarget(
        double segmentProgressKm,
        double segmentLengthKm,
        VehicleConfig config,
        Random random)
    {
        var (lo, hi) = IsNearCity(segmentProgressKm, segmentLengthKm)
            ? (ApproachMinKph, ApproachMaxKph)
            : (CruiseMinKph, CruiseMaxKph);
        var sample = lo + random.NextDouble() * (hi - lo);
        return Math.Clamp(sample, config.MinSpeedKph, config.MaxSpeedKph);
    }

    /// <summary>EMA smoothing: <c>new = previous + alpha * (target - previous)</c>.</summary>
    public static double Smooth(double previousSpeed, double targetSpeed, double alpha)
    {
        var clampedAlpha = Math.Clamp(alpha, 0.0, 1.0);
        return previousSpeed + clampedAlpha * (targetSpeed - previousSpeed);
    }
}
