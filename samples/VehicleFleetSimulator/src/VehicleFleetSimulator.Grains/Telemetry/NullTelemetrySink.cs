using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains.Telemetry;

/// <summary>
/// <see cref="ITelemetrySink"/> that discards every publish. Use this to isolate the simulator's
/// producer-side cost from any downstream sink — benchmark scenarios B-01 (no downstream) and B-12
/// (observer-off control) both run against this sink.
/// </summary>
public sealed class NullTelemetrySink : ITelemetrySink
{
    public static readonly NullTelemetrySink Instance = new();

    public ValueTask PublishTelemetryAsync(VehicleTelemetryEvent telemetry, CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;

    public ValueTask PublishEventAsync(VehicleEvent vehicleEvent, CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;
}
