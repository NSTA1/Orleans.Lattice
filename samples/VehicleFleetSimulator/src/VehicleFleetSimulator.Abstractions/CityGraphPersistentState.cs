using System.Collections.Generic;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>Persistent state for the singleton <c>CityGraphGrain</c>: holds operator-overridden
/// 2-D positions per city id so drags survive silo restarts.</summary>
[GenerateSerializer]
public sealed class CityGraphPersistentState
{
    [Id(0)] public Dictionary<string, CityPosition> Positions { get; set; } = new();
}
