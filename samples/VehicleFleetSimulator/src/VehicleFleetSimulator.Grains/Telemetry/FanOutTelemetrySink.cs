using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains.Telemetry;

/// <summary>
/// Default <see cref="ITelemetrySink"/>: dispatches telemetry to the sharded
/// <see cref="IFleetFanOutGrain"/> relay (vehicle-id-bucketed) and discrete events to shard 0
/// (the shared events feed). Preserves the original <c>VehicleGrain</c> dispatch behavior verbatim.
///
/// <para>Per-shard grain references are cached in a fixed-size array sized to
/// <see cref="StreamConstants.TelemetryAllShardCount"/>, populated lazily on first use. Orleans
/// grain references are themselves cheap to resolve, so the cache is a minor optimization that
/// also keeps the hot publish path free of dictionary allocations.</para>
/// </summary>
public sealed class FanOutTelemetrySink : ITelemetrySink
{
    private readonly IGrainFactory _grainFactory;
    private readonly IFleetFanOutGrain?[] _shards = new IFleetFanOutGrain?[StreamConstants.TelemetryAllShardCount];
    private IFleetFanOutGrain? _eventsShard;

    public FanOutTelemetrySink(IGrainFactory grainFactory)
    {
        _grainFactory = grainFactory;
    }

    public ValueTask PublishTelemetryAsync(VehicleTelemetryEvent telemetry, CancellationToken cancellationToken = default)
    {
        var shardIndex = StreamConstants.ShardForVehicle(telemetry.VehicleId);
        var shard = _shards[shardIndex] ??= _grainFactory.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shardIndex));
        return new ValueTask(shard.PublishTelemetry(telemetry));
    }

    public ValueTask PublishEventAsync(VehicleEvent vehicleEvent, CancellationToken cancellationToken = default)
    {
        // Events go to their own dedicated activation, not a telemetry shard. Keeping the events
        // feed off the telemetry shards means a backlog on the (much hotter) events queue can
        // never starve the control-plane Subscribe/Unsubscribe path on a shard that's also
        // serving telemetry -- which previously manifested as the API hub timing out at the back
        // of a multi-million-message queue and the UI displaying "connected, 0 vehicles".
        var shard = _eventsShard ??= _grainFactory.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.EventsKey());
        return new ValueTask(shard.PublishEvent(vehicleEvent));
    }
}
