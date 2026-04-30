using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>Specification used to create a new vehicle. All fields are optional.</summary>
[GenerateSerializer, Immutable]
public sealed record VehicleSpec(
    [property: Id(0)] Guid? VehicleId = null,
    [property: Id(1)] string? StartCityId = null,
    [property: Id(2)] ImmutableArray<string>? Route = null,
    [property: Id(3)] VehicleConfig? Config = null,
    [property: Id(4)] double? InitialFuelLitres = null);
