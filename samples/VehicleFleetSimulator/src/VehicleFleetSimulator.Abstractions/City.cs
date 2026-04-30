namespace VehicleFleetSimulator.Abstractions;

/// <summary>A node in the city graph.</summary>
[GenerateSerializer, Immutable]
public sealed record City(
    [property: Id(0)] string Id,
    [property: Id(1)] string Name);
