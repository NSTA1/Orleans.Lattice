using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains.Telemetry;

/// <summary>
/// <see cref="ITelemetrySink"/> that discards every publish. Use this to isolate the simulator's
/// producer-side cost from any downstream sink - benchmark scenarios simulator-baseline (no downstream) and observer-no-peer
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
