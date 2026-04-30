using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains.Vehicles;

/// <summary>
/// Pure fuel consumption model. Consumption rate (litres per km) follows a U-shaped curve
/// of speed: a quadratic penalty either side of <see cref="VehicleConfig.OptimalSpeedKph"/>.
/// </summary>
public static class FuelModel
{
    /// <summary>Litres of fuel consumed per kilometre at the given speed.</summary>
    public static double LitresPerKm(double speedKph, VehicleConfig config)
    {
        if (speedKph <= 0) return double.PositiveInfinity;
        var delta = speedKph - config.OptimalSpeedKph;
        var penalty = delta < 0
            ? config.LowSpeedPenaltyCoefficient * delta * delta
            : config.HighSpeedPenaltyCoefficient * delta * delta;
        return config.LitresPerKmAtOptimal + penalty;
    }

    /// <summary>
    /// Estimate of the fuel needed to traverse <paramref name="distanceKm"/>, using the
    /// cruise-rate at <see cref="VehicleConfig.OptimalSpeedKph"/> plus
    /// <see cref="VehicleConfig.FuelSafetyMargin"/>. Vehicles settle around the optimal speed
    /// in steady-state (that is the basin of the U-shaped consumption curve), so budgeting at
    /// the max-speed rate would be massively over-conservative — its quadratic high-speed
    /// penalty would make typical inter-city segments unaffordable on a single tank and
    /// deadlock the refuel/depart loop. The safety margin absorbs deviations toward the
    /// extremes; low-speed rates near cities only apply over the small approach window.
    /// </summary>
    public static double FuelRequired(double distanceKm, VehicleConfig config)
    {
        var rateAtOptimal = LitresPerKm(config.OptimalSpeedKph, config);
        return distanceKm * rateAtOptimal * config.FuelSafetyMargin;
    }
}
