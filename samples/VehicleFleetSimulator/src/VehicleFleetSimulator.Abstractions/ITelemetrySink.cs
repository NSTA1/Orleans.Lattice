namespace VehicleFleetSimulator.Abstractions;

/// <summary>
/// Sink seam that <see cref="IVehicleGrain"/> writes per-tick telemetry and discrete events to.
/// Introduced so the same simulator load can drive different downstream pipelines without
/// modifying grain code:
///
/// <list type="bullet">
///   <item><description><c>FanOutTelemetrySink</c> — current behavior, dispatches to the sharded
///     <see cref="IFleetFanOutGrain"/> relay (used by <c>FleetStreamHub</c> + load harness).</description></item>
///   <item><description><c>NullTelemetrySink</c> — discards every publish, for isolating producer-side
///     cost (benchmark scenario simulator-baseline baseline / observer-off control).</description></item>
///   <item><description>Future <c>LatticeSink</c> — writes telemetry to an <c>Orleans.Lattice</c> tree
///     (benchmark scenarios current-state-no-replication and onward).</description></item>
/// </list>
///
/// <para>Implementations must be safe to call from the <see cref="IVehicleGrain"/> turn at the
/// configured tick cadence across the entire fleet. Implementations that perform external I/O
/// should buffer off-turn so grain-tick latency is not coupled to the downstream's latency.</para>
/// </summary>
public interface ITelemetrySink
{
    /// <summary>
    /// Publish a per-tick telemetry sample. Called once per <see cref="IVehicleGrain"/> tick on the
    /// hot path; implementations should be allocation-free in steady state and must not throw.
    /// </summary>
    ValueTask PublishTelemetryAsync(VehicleTelemetryEvent telemetry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publish a discrete vehicle event (route start / stop, refuel, status transition).
    /// Called only on ticks that produce events, so cost matters less than the telemetry path.
    /// </summary>
    ValueTask PublishEventAsync(VehicleEvent vehicleEvent, CancellationToken cancellationToken = default);
}
