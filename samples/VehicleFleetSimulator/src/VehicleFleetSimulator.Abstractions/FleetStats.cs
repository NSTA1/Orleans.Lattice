namespace VehicleFleetSimulator.Abstractions;

/// <summary>Aggregate fleet statistics.</summary>
[GenerateSerializer, Immutable]
public sealed record FleetStats(
    [property: Id(0)] int Total,
    [property: Id(1)] int Driving,
    [property: Id(2)] int Refuelling,
    [property: Id(3)] int Idle,
    [property: Id(4)] int RouteCompleted);
