namespace VehicleFleetSimulator.Abstractions;

/// <summary>Persistent envelope for a vehicle grain.</summary>
[GenerateSerializer]
public sealed class VehiclePersistentState
{
    [Id(0)] public VehicleState? State { get; set; }
    [Id(1)] public VehicleConfig Config { get; set; } = VehicleConfig.Default;
    [Id(2)] public bool IsRunning { get; set; }

    public bool IsInitialized => State is not null;
}
