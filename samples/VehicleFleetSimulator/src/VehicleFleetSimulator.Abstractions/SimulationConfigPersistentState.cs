namespace VehicleFleetSimulator.Abstractions;

/// <summary>Persistent envelope for <see cref="SimulationConfig"/>.</summary>
[GenerateSerializer]
public sealed class SimulationConfigPersistentState
{
    [Id(0)] public SimulationConfig? Config { get; set; }
}
