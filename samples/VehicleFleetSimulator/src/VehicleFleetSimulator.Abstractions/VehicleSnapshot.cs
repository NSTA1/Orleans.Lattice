using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>A read-only snapshot of a vehicle suitable for API responses.</summary>
[GenerateSerializer, Immutable]
public sealed record VehicleSnapshot(
    [property: Id(0)] Guid VehicleId,
    [property: Id(1)] ImmutableArray<string> Route,
    [property: Id(2)] int RouteIndex,
    [property: Id(3)] string FromCityId,
    [property: Id(4)] string ToCityId,
    [property: Id(5)] double SegmentProgressKm,
    [property: Id(6)] double SpeedKph,
    [property: Id(7)] double FuelLitres,
    [property: Id(8)] VehicleStatus Status,
    [property: Id(9)] DateTimeOffset LastUpdatedUtc,
    [property: Id(10)] bool IsRunning)
{
    public static VehicleSnapshot From(in VehicleState s, bool isRunning) => new(
        s.VehicleId, s.Route, s.RouteIndex, s.CurrentFromCityId, s.CurrentToCityId,
        s.SegmentProgressKm, s.SpeedKph, s.FuelLitres, s.Status, s.LastUpdatedUtc, isRunning);
}
