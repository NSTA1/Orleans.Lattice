namespace VehicleFleetSimulator.Abstractions;

/// <summary>Global, fleet-wide simulation tunables (defaults applied to new vehicles, tick cadence, etc.).</summary>
/// <remarks>
/// <para>
/// <see cref="TimeScale"/> multiplies the wall-clock elapsed passed to <c>VehicleSimulator.Tick</c>
/// without changing the grain timer cadence: telemetry rate is unchanged but each message represents
/// <c>TimeScale</c>× more simulated travel. Useful when visualising tiny fleets where realtime pacing
/// makes a 360 km segment a 4-hour wait.
/// </para>
/// <para>
/// <see cref="IsPaused"/> is a global "freeze the simulation" flag - <c>VehicleGrain.TickAsync</c>
/// reads it via <c>SimulationRuntimeState</c> and short-circuits before touching the simulator
/// when set. Distinct from per-vehicle <c>Stop()</c>: pause keeps every vehicle's running flag set
/// so a single resume restarts everything in lockstep, while <c>Stop()</c> tears down the timer.
/// </para>
/// </remarks>
[GenerateSerializer, Immutable]
public sealed record SimulationConfig(
    [property: Id(0)] TimeSpan TickInterval,
    [property: Id(1)] VehicleConfig DefaultVehicleConfig,
    [property: Id(2)] double TimeScale,
    [property: Id(3)] bool IsPaused = false)
{
    public static SimulationConfig Default { get; } = new(
        TickInterval: TimeSpan.FromMilliseconds(200),
        DefaultVehicleConfig: VehicleConfig.Default,
        TimeScale: 9000.0,
        IsPaused: false);
}

/// <summary>Partial update for <see cref="SimulationConfig"/>; null fields leave the existing value untouched.</summary>
[GenerateSerializer, Immutable]
public sealed record SimulationConfigPatch(
    [property: Id(0)] TimeSpan? TickInterval = null,
    [property: Id(1)] VehicleConfig? DefaultVehicleConfig = null,
    [property: Id(2)] double? TimeScale = null,
    [property: Id(3)] bool? IsPaused = null);
