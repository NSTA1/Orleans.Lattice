using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>The full mutable-by-replacement state of a single vehicle.
/// Modelled as a <see langword="readonly record struct"/> so the per-tick
/// <c>state with { ... }</c> rewrites in <c>VehicleSimulator</c> stay on the stack
/// and don't pressure the GC under load.</summary>
[GenerateSerializer, Immutable]
public readonly record struct VehicleState(
    [property: Id(0)] Guid VehicleId,
    [property: Id(1)] ImmutableArray<string> Route,
    [property: Id(2)] int RouteIndex,
    [property: Id(3)] double SegmentProgressKm,
    [property: Id(4)] double SpeedKph,
    [property: Id(5)] double FuelLitres,
    [property: Id(6)] VehicleStatus Status,
    [property: Id(7)] DateTimeOffset LastUpdatedUtc,
    [property: Id(8)] DateTimeOffset? RefuellingUntilUtc,
    [property: Id(9)] TimeSpan SpeedTimeSinceResample)
{
    /// <summary>City id the vehicle most recently departed from (or is currently at).</summary>
    public string CurrentFromCityId => Route[RouteIndex];

    /// <summary>City id the vehicle is currently driving toward, or the same as <see cref="CurrentFromCityId"/> at route end.</summary>
    public string CurrentToCityId =>
        RouteIndex + 1 < Route.Length ? Route[RouteIndex + 1] : Route[RouteIndex];

    public bool IsOnFinalCity => RouteIndex + 1 >= Route.Length;
}
