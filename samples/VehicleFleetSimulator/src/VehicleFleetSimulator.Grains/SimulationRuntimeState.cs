namespace VehicleFleetSimulator.Grains;

/// <summary>
/// Silo-scoped, in-process shared state for runtime simulation parameters that need to take effect
/// across thousands of grains the instant they change. Lives in DI as a singleton so both the
/// authoritative writer (<see cref="SimulationConfigGrain"/>) and the high-frequency readers
/// (<see cref="VehicleGrain"/>) share a single field load - no cross-grain calls, no polling, no
/// stream subscriptions.
/// </summary>
/// <remarks>
/// <para>
/// This is single-silo by construction: a different silo would have its own singleton instance,
/// which is why mutations are still routed through the persistent grain (so any future second
/// silo can pick up the value via <c>GetConfig</c> on activation, and so values survive process
/// restart by being persisted). The current docker-compose deployment is single-silo; if the
/// cluster ever grows to multi-silo, swap this out for an Orleans stream broadcast or a per-silo
/// cache invalidation pulse from the config grain.
/// </para>
/// <para>
/// <see cref="TimeScale"/> is a <c>volatile</c> <c>double</c> via <see cref="System.Threading.Volatile"/>
/// reads/writes so a writer on one thread is observed by a reader on any other thread without a
/// lock - torn 64-bit reads are not a correctness concern on x64/arm64 .NET, but the volatile
/// barrier guarantees the change is visible promptly to the grain timer threads.
/// </para>
/// </remarks>
public sealed class SimulationRuntimeState
{
    private double _timeScale = 1.0;
    private int _isPaused; // 0 = running, 1 = paused. Int because Volatile lacks a bool overload.

    public double TimeScale
    {
        get => System.Threading.Volatile.Read(ref _timeScale);
        set => System.Threading.Volatile.Write(ref _timeScale, value);
    }

    /// <summary>
    /// Global pause flag. When <c>true</c>, <see cref="VehicleGrain"/> tick handlers short-circuit
    /// before advancing the simulator or publishing telemetry. The grain timer keeps firing so
    /// resume is instant - no per-grain timer churn for thousands of activations.
    /// </summary>
    public bool IsPaused
    {
        get => System.Threading.Volatile.Read(ref _isPaused) != 0;
        set => System.Threading.Volatile.Write(ref _isPaused, value ? 1 : 0);
    }
}
