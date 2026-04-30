using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>
/// Single vehicle simulator grain. One grain instance per vehicle, keyed by <c>VehicleId</c> (Guid).
/// </summary>
public interface IVehicleGrain : IGrainWithGuidKey
{
    /// <summary>Initialize a freshly activated grain from a spec. Idempotent if called multiple times with the same spec.</summary>
    Task Initialize(VehicleSpec spec);

    /// <summary>Begin ticking on the configured interval and publishing telemetry.</summary>
    Task Start();

    /// <summary>Stop ticking. State is preserved.</summary>
    Task Stop();

    /// <summary>Stop ticking, purge all persisted state for this vehicle, and deactivate the grain.</summary>
    Task Clear();

    /// <summary>Replace per-vehicle configuration (fuel curve, speed bounds, refuel delay, etc.).</summary>
    Task UpdateConfig(VehicleConfig config);

    /// <summary>Replace the current route. The vehicle is re-positioned at the first city of the new route.</summary>
    Task SetRoute(ImmutableArray<string> route);

    /// <summary>Return the current snapshot, or null if not yet initialized.</summary>
    /// <remarks>Returns <see cref="ValueTask{T}"/> so the synchronous fast path (state already
    /// in memory) doesn't allocate a <see cref="Task{T}"/> via <c>Task.FromResult</c>.</remarks>
    ValueTask<VehicleSnapshot?> GetSnapshot();

    /// <summary>Inject a discrete malfunction. Returns <c>true</c> if the fault was applied
    /// (always true today, but reserved for future "fault not applicable in this state" cases).</summary>
    Task<bool> InjectFault(VehicleFault fault);
}
