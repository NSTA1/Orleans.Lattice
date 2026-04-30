using Orleans.Runtime;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains;

/// <summary>Singleton grain that holds the global <see cref="SimulationConfig"/> with persistence.</summary>
public sealed class SimulationConfigGrain : Grain, ISimulationConfigGrain
{
    private readonly IPersistentState<SimulationConfigPersistentState> _persistent;
    private readonly SimulationRuntimeState _runtime;

    public SimulationConfigGrain(
        [PersistentState("simulation-config", "Default")] IPersistentState<SimulationConfigPersistentState> persistent,
        SimulationRuntimeState runtime)
    {
        _persistent = persistent;
        _runtime = runtime;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        // Hydrate the in-process cache from persistent state on activation so a silo restart
        // resumes with the last-saved TimeScale and IsPaused values, not the SimulationConfig.Default.
        _persistent.State.Config ??= SimulationConfig.Default;
        _runtime.TimeScale = _persistent.State.Config.TimeScale;
        _runtime.IsPaused = _persistent.State.Config.IsPaused;
        return base.OnActivateAsync(cancellationToken);
    }

    public Task<SimulationConfig> GetConfig()
    {
        _persistent.State.Config ??= SimulationConfig.Default;
        return Task.FromResult(_persistent.State.Config);
    }

    public async Task<SimulationConfig> UpdateConfig(SimulationConfigPatch patch)
    {
        ArgumentNullException.ThrowIfNull(patch);

        var current = _persistent.State.Config ?? SimulationConfig.Default;
        var next = current with
        {
            TickInterval = patch.TickInterval ?? current.TickInterval,
            DefaultVehicleConfig = patch.DefaultVehicleConfig ?? current.DefaultVehicleConfig,
            TimeScale = patch.TimeScale ?? current.TimeScale,
        };

        if (next.TickInterval <= TimeSpan.Zero)
            throw new ArgumentException("TickInterval must be positive.", nameof(patch));
        if (!double.IsFinite(next.TimeScale) || next.TimeScale <= 0)
            throw new ArgumentException("TimeScale must be a positive finite number.", nameof(patch));

        _persistent.State.Config = next;
        await _persistent.WriteStateAsync();

        // Publish the new TimeScale to the in-process cache before returning. Done after the write
        // so a failed persist doesn't leave the runtime cache out of sync with durable state.
        _runtime.TimeScale = next.TimeScale;
        return next;
    }
}
