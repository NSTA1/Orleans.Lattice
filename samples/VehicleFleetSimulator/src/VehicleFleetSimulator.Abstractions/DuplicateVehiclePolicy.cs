namespace VehicleFleetSimulator.Abstractions;

/// <summary>
/// Behaviour when <see cref="IFleetGrain.AddVehicle"/> or <see cref="IFleetGrain.AddVehicleBatch"/> is
/// called with a <see cref="VehicleSpec.VehicleId"/> that already exists in the fleet.
/// </summary>
[GenerateSerializer]
public enum DuplicateVehiclePolicy
{
    /// <summary>Throw <see cref="InvalidOperationException"/> on duplicate (default).</summary>
    Throw = 0,

    /// <summary>Skip the duplicate silently and return the existing id.</summary>
    Skip = 1,
}
