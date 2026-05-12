using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Utilities;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains;

/// <summary>
/// Silo-side relay that receives direct cross-grain publishes and fans every item out to all
/// registered <see cref="IFleetStreamObserver"/> client observers. Two activation roles share
/// this single grain class:
/// <list type="bullet">
///   <item>Telemetry shards: <see cref="StreamConstants.TelemetryAllShardCount"/> instances
///   addressed via <see cref="IFleetFanOutGrain.ShardKey"/>, each receiving the bucket of
///   <c>VehicleGrain</c>s whose ids hash to that shard.</item>
///   <item>Events feed: a single instance addressed via <see cref="IFleetFanOutGrain.EventsKey"/>,
///   receiving every discrete <see cref="VehicleEvent"/> from every vehicle. Lives on its own
///   activation so a backlog on the events queue can never block Subscribe / Unsubscribe on a
///   telemetry shard.</item>
/// </list>
///
/// Marked <see cref="ReentrantAttribute"/> so many publish calls can interleave concurrently - a
/// non-reentrant turn-based grain caps this dispatch path at ~1000 msg/sec per shard. The body
/// only sets a field and calls <see cref="ObserverManager{T}"/> (which is internally thread-safe)
/// fully synchronously, so reentrancy is benign here (no awaits → no turn boundary where another
/// invocation could clobber the field mid-Notify).
/// </summary>
[Reentrant]
public sealed class FleetFanOutGrain : Grain, IFleetFanOutGrain
{
    private static readonly TimeSpan ObserverExpiration = TimeSpan.FromMinutes(2);

    private readonly ILogger<FleetFanOutGrain> _logger;
    private readonly ObserverManager<IFleetStreamObserver> _observers;

    // Cached notifier delegates that read mutable per-grain state. ObserverManager.Notify takes
    // an Action<T>; passing a lambda that captures `item` allocates a closure object + delegate
    // per call. Stashing the current item on a field and reusing a single delegate eliminates that.
    private VehicleTelemetryEvent? _pendingTelemetry;
    private VehicleEvent? _pendingEvent;
    private readonly Action<IFleetStreamObserver> _telemetryNotifier;
    private readonly Action<IFleetStreamObserver> _eventNotifier;
    private long _publishedCount; // diagnostics counter, incremented on every Publish[Telemetry|Event]

    public FleetFanOutGrain(ILogger<FleetFanOutGrain> logger)
    {
        _logger = logger;
        _observers = new ObserverManager<IFleetStreamObserver>(ObserverExpiration, logger);
        _telemetryNotifier = o => { var t = _pendingTelemetry; if (t.HasValue) o.OnTelemetry(t.Value); };
        _eventNotifier = o => { var e = _pendingEvent; if (e is not null) o.OnEvent(e); };
    }

    public Task Subscribe(IFleetStreamObserver observer)
    {
        _observers.Subscribe(observer, observer);
        return Task.CompletedTask;
    }

    public Task Unsubscribe(IFleetStreamObserver observer)
    {
        _observers.Unsubscribe(observer);
        return Task.CompletedTask;
    }

    public Task Ping() => Task.CompletedTask;

    public Task<FanOutDiagnostics> GetDiagnostics() =>
        Task.FromResult(new FanOutDiagnostics(_observers.Count, Interlocked.Read(ref _publishedCount)));

    public Task PublishTelemetry(VehicleTelemetryEvent telemetry)
    {
        _pendingTelemetry = telemetry;
        _observers.Notify(_telemetryNotifier);
        Interlocked.Increment(ref _publishedCount);
        return Task.CompletedTask;
    }

    public Task PublishEvent(VehicleEvent vehicleEvent)
    {
        _pendingEvent = vehicleEvent;
        _observers.Notify(_eventNotifier);
        Interlocked.Increment(ref _publishedCount);
        return Task.CompletedTask;
    }
}
