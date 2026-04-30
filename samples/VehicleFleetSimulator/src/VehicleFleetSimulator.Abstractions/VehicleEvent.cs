using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>Discrete events emitted by the simulator (in addition to per-tick telemetry).</summary>
[GenerateSerializer]
public abstract record VehicleEvent(
    [property: Id(0)] Guid VehicleId,
    [property: Id(1)] DateTimeOffset TimestampUtc);

[GenerateSerializer, Immutable]
public sealed record DepartedCity(Guid VehicleId, DateTimeOffset TimestampUtc,
    [property: Id(2)] string CityId,
    [property: Id(3)] string NextCityId)
    : VehicleEvent(VehicleId, TimestampUtc);

[GenerateSerializer, Immutable]
public sealed record ArrivedCity(Guid VehicleId, DateTimeOffset TimestampUtc,
    [property: Id(2)] string CityId)
    : VehicleEvent(VehicleId, TimestampUtc);

[GenerateSerializer, Immutable]
public sealed record RefuellingStarted(Guid VehicleId, DateTimeOffset TimestampUtc,
    [property: Id(2)] string CityId,
    [property: Id(3)] double FuelRequiredLitres,
    [property: Id(4)] double FuelAvailableLitres)
    : VehicleEvent(VehicleId, TimestampUtc);

[GenerateSerializer, Immutable]
public sealed record RefuellingCompleted(Guid VehicleId, DateTimeOffset TimestampUtc,
    [property: Id(2)] string CityId,
    [property: Id(3)] double FuelLitres)
    : VehicleEvent(VehicleId, TimestampUtc);

[GenerateSerializer, Immutable]
public sealed record RouteCompleted(Guid VehicleId, DateTimeOffset TimestampUtc,
    [property: Id(2)] ImmutableArray<string> CompletedRoute,
    [property: Id(3)] ImmutableArray<string> NewRoute)
    : VehicleEvent(VehicleId, TimestampUtc);

/// <summary>Operator-injected fault, raised exactly once at the moment <c>InjectFault</c> is invoked.
/// Subsequent simulator state transitions (e.g. an automatic refuel after an <c>OutOfFuel</c> fault)
/// produce their own normal events.</summary>
[GenerateSerializer, Immutable]
public sealed record VehicleFaulted(Guid VehicleId, DateTimeOffset TimestampUtc,
    [property: Id(2)] VehicleFault Fault,
    [property: Id(3)] string? Detail)
    : VehicleEvent(VehicleId, TimestampUtc);
