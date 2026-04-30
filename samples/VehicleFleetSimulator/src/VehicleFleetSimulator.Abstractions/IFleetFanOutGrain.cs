using Orleans.Concurrency;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>
/// Sharded silo-side relay that receives direct cross-grain publishes from <c>VehicleGrain</c>
/// and fans every item out to all registered <see cref="IFleetStreamObserver"/> client
/// observers. There are <see cref="StreamConstants.TelemetryAllShardCount"/> telemetry shard
/// instances; clients must register an observer with every shard to receive the full fleet
/// feed. A separate singleton activation, addressed by <see cref="EventsKey"/>, owns the
/// shared events feed -- distinct from any telemetry shard so a backlog on the (much hotter)
/// events path can't block control-plane Subscribe/Unsubscribe on a telemetry shard.
/// </summary>
public interface IFleetFanOutGrain : IGrainWithGuidKey
{
    /// <summary>Grain key for telemetry shard <paramref name="shard"/>.</summary>
    public static Guid ShardKey(int shard) => StreamConstants.GetTelemetryAllShardKey(shard);

    /// <summary>Grain key for the singleton events-feed activation. Distinct from every
    /// <see cref="ShardKey"/> so events live on their own message queue.</summary>
    public static Guid EventsKey() => StreamConstants.EventsGrainKey;

    // Control-plane methods are tagged [AlwaysInterleave] so they bypass the activation's
    // message queue rather than waiting in line behind potentially millions of [OneWay] publish
    // calls. Class-level [Reentrant] should already permit this, but [AlwaysInterleave] is a
    // per-method guarantee that holds even if reentrancy is degraded under load (we observed a
    // shard-0 activation hit ~2.5M queued messages with NumRunning=1 in production, which made
    // every Subscribe time out at the back of the queue and prevented the gRPC hub from ever
    // attaching its observer -- the UI then displayed "connected, 0 vehicles" indefinitely).
    // None of these methods share mutable state with the publish path, so the interleave is
    // strictly safe: Subscribe/Unsubscribe call ObserverManager (internally synchronised), and
    // Ping is a no-op.

    /// <summary>Register an observer. Idempotent: repeated subscriptions refresh the observer's lease.</summary>
    [AlwaysInterleave] Task Subscribe(IFleetStreamObserver observer);

    /// <summary>Unregister an observer if present.</summary>
    [AlwaysInterleave] Task Unsubscribe(IFleetStreamObserver observer);

    /// <summary>Heartbeat from the client to keep this grain activated and the observer's lease fresh.</summary>
    [AlwaysInterleave] Task Ping();

    /// <summary>Lightweight diagnostics: number of registered observers on this activation.
    /// Tagged <see cref="AlwaysInterleaveAttribute"/> so a wedged shard can still be queried.</summary>
    [AlwaysInterleave] Task<FanOutDiagnostics> GetDiagnostics();

    /// <summary>Fire-and-forget publish from a <c>VehicleGrain</c> on this shard. <see cref="OneWayAttribute"/>
    /// causes the caller's await to complete as soon as the message is enqueued, with no response on the wire.</summary>
    [OneWay] Task PublishTelemetry(VehicleTelemetryEvent telemetry);

    /// <summary>Fire-and-forget publish of a discrete event. Routed exclusively to the singleton
    /// events-feed activation (see <see cref="EventsKey"/>); never published to a telemetry shard
    /// so the events queue can't compete with high-volume telemetry on the same activation.</summary>
    [OneWay] Task PublishEvent(VehicleEvent vehicleEvent);
}

/// <summary>Lightweight diagnostics record returned by <see cref="IFleetFanOutGrain.GetDiagnostics"/>.</summary>
[GenerateSerializer, Immutable]
public sealed record FanOutDiagnostics(
    [property: Id(0)] int ObserverCount,
    [property: Id(1)] long PublishedCount);

