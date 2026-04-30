namespace VehicleFleetSimulator.Abstractions;

/// <summary>Singleton grain holding the global <see cref="SimulationConfig"/> for the fleet.</summary>
public interface ISimulationConfigGrain : IGrainWithGuidKey
{
    public static readonly Guid Key = Guid.Empty;

    /// <summary>Return the current global simulation configuration.</summary>
    Task<SimulationConfig> GetConfig();

    /// <summary>Apply a partial update; returns the merged result.</summary>
    Task<SimulationConfig> UpdateConfig(SimulationConfigPatch patch);
}
