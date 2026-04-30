using Orleans;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>Round-trip verification grain for Milestone 0.</summary>
public interface IPingGrain : IGrainWithStringKey
{
    Task<string> Ping(string message);
}
