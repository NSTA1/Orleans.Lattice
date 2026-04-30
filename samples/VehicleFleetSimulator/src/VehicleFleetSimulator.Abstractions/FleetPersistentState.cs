namespace VehicleFleetSimulator.Abstractions;

/// <summary>Persistent envelope for the singleton fleet grain.</summary>
[GenerateSerializer]
public sealed class FleetPersistentState
{
    [Id(0)] public HashSet<Guid> VehicleIds { get; set; } = [];
}
