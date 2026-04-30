namespace VehicleFleetSimulator.Abstractions;

/// <summary>A directed edge between two cities. Bidirectional travel is modelled by adding both directions.</summary>
[GenerateSerializer, Immutable]
public sealed record RoadSegment(
    [property: Id(0)] string FromCityId,
    [property: Id(1)] string ToCityId,
    [property: Id(2)] double DistanceKm);
