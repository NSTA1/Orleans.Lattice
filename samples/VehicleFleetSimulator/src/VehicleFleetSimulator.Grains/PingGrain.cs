using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains;

public sealed class PingGrain : Grain, IPingGrain
{
    public Task<string> Ping(string message) =>
        Task.FromResult($"pong: {message} (from grain {this.GetPrimaryKeyString()})");
}
