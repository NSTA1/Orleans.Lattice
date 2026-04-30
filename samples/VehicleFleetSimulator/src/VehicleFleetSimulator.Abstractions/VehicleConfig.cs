using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>Per-vehicle tunable parameters.</summary>
[GenerateSerializer, Immutable]
public sealed record VehicleConfig(
    [property: Id(0)] double FuelCapacityLitres,
    [property: Id(1)] double LitresPerKmAtOptimal,
    [property: Id(2)] double OptimalSpeedKph,
    [property: Id(3)] double LowSpeedPenaltyCoefficient,
    [property: Id(4)] double HighSpeedPenaltyCoefficient,
    [property: Id(5)] double FuelSafetyMargin,
    [property: Id(6)] double MinSpeedKph,
    [property: Id(7)] double MaxSpeedKph,
    [property: Id(8)] TimeSpan RefuelDelay,
    [property: Id(9)] double SpeedSmoothingAlpha,
    [property: Id(10)] TimeSpan SpeedResampleInterval)
{
    // Modelled on a 40-tonne articulated lorry (Volvo FH / Scania R envelope). Twin saddle
    // tanks of ~250 L each give the 500 L total; cruise burn of 32 L/100 km at the diesel
    // sweet spot of ~80 kph rises to ~50 L/100 km in town and is capped at the EU 100 kph
    // governor. The previous 120 L / 0.07 L-per-km / 0.0005 low-speed penalty values were
    // passenger-car-shaped *and* internally inconsistent: at 30 kph they implied 187 L/100 km,
    // which drained every vehicle to red within a single segment.
    public static VehicleConfig Default { get; } = new(
        FuelCapacityLitres: 500.0,
        LitresPerKmAtOptimal: 0.32,
        OptimalSpeedKph: 80.0,
        // Solved so 30 kph -> ~50 L/100 km and 100 kph -> ~33 L/100 km.
        LowSpeedPenaltyCoefficient: 0.00007,
        HighSpeedPenaltyCoefficient: 0.00002,
        // Realistic burn including approach/departure overhead is ~36 L per 100 km segment
        // versus a 32 L cruise budget, so widen the safety margin from 1.15 -> 1.30.
        FuelSafetyMargin: 1.30,
        MinSpeedKph: 1.0,
        MaxSpeedKph: 100.0,
        RefuelDelay: TimeSpan.FromSeconds(30),
        SpeedSmoothingAlpha: 0.2,
        SpeedResampleInterval: TimeSpan.FromSeconds(1));
}
