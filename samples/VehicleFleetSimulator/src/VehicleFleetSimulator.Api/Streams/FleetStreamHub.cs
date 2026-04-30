using System.Threading.Channels;
using Google.Protobuf.WellKnownTypes;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Api.Grpc;
using AbsDeparted = VehicleFleetSimulator.Abstractions.DepartedCity;
using AbsArrived = VehicleFleetSimulator.Abstractions.ArrivedCity;
using AbsRefuelStart = VehicleFleetSimulator.Abstractions.RefuellingStarted;
using AbsRefuelDone = VehicleFleetSimulator.Abstractions.RefuellingCompleted;
using AbsRouteDone = VehicleFleetSimulator.Abstractions.RouteCompleted;
using AbsVehicleStatus = VehicleFleetSimulator.Abstractions.VehicleStatus;
using PbDeparted = VehicleFleetSimulator.Api.Grpc.DepartedCity;
using PbArrived = VehicleFleetSimulator.Api.Grpc.ArrivedCity;
using PbRefuelStart = VehicleFleetSimulator.Api.Grpc.RefuellingStarted;
using PbRefuelDone = VehicleFleetSimulator.Api.Grpc.RefuellingCompleted;
using PbRouteDone = VehicleFleetSimulator.Api.Grpc.RouteCompleted;
using PbVehicleStatus = VehicleFleetSimulator.Api.Grpc.VehicleStatus;

namespace VehicleFleetSimulator.Api.Streams;

/// <summary>
/// In-process fan-out for fleet telemetry and discrete events. Each subscriber owns a bounded
/// channel that drops oldest items on overflow, surfacing the dropped count for backpressure metrics.
/// </summary>
public interface IFleetStreamHub
{
    FleetSubscription<TelemetryMessage> SubscribeTelemetry(TelemetryFilter filter, int capacity = 1024);
    FleetSubscription<VehicleEventMessage> SubscribeEvents(EventFilter filter, int capacity = 1024);
}

/// <summary>A single subscriber's channel + drop counter. Dispose to unregister.</summary>
public sealed class FleetSubscription<T> : IDisposable
{
    private readonly Action _onDispose;
    private long _dropped;

    public FleetSubscription(Channel<T> channel, Action onDispose)
    {
        Channel = channel;
        _onDispose = onDispose;
    }

    public Channel<T> Channel { get; }
    public long DroppedCount => Interlocked.Read(ref _dropped);

    internal void TryWrite(T item)
    {
        // BoundedChannelFullMode.DropOldest will silently evict; we need to count those drops ourselves.
        while (!Channel.Writer.TryWrite(item))
        {
            if (!Channel.Reader.TryRead(out _)) break;
            Interlocked.Increment(ref _dropped);
        }
    }

    public void Dispose()
    {
        Channel.Writer.TryComplete();
        _onDispose();
    }
}

internal sealed class FleetStreamHub : IFleetStreamHub
{
    private readonly object _gate = new();
    private readonly List<TelemetrySubscriber> _telemetry = [];
    private readonly List<EventSubscriber> _events = [];
    // Copy-on-write snapshots read lock-free from the dispatch hot path. Refreshed under _gate
    // whenever the underlying subscriber list mutates (subscribe / unsubscribe).
    private TelemetrySubscriber[] _telemetrySnapshot = Array.Empty<TelemetrySubscriber>();
    private EventSubscriber[] _eventsSnapshot = Array.Empty<EventSubscriber>();

    /// <summary>
    /// Raised with <c>true</c> when the first subscriber arrives (0 → ≥1) and with <c>false</c>
    /// when the last subscriber leaves (≥1 → 0). Fired outside <c>_gate</c> so handlers may do work
    /// without risk of deadlock. <see cref="TelemetryFanOutService"/> uses this to attach the
    /// upstream Orleans observer only while at least one downstream client is connected — without
    /// it the silo would keep pushing 1000 msg/s to an empty hub indefinitely.
    /// </summary>
    public event Action<bool>? ActiveSubscribersChanged;

    public FleetSubscription<TelemetryMessage> SubscribeTelemetry(TelemetryFilter filter, int capacity = 1024)
    {
        var channel = Channel.CreateBounded<TelemetryMessage>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait, // we eject manually so we can count drops
            SingleReader = true,
            SingleWriter = false,
        });
        TelemetrySubscriber? sub = null;
        var subscription = new FleetSubscription<TelemetryMessage>(channel, () =>
        {
            bool wentIdle = false;
            lock (_gate)
            {
                if (sub is not null && _telemetry.Remove(sub))
                {
                    Volatile.Write(ref _telemetrySnapshot, _telemetry.ToArray());
                    wentIdle = _telemetry.Count == 0 && _events.Count == 0;
                }
            }
            if (wentIdle) ActiveSubscribersChanged?.Invoke(false);
        });
        sub = new TelemetrySubscriber(filter, subscription);
        bool wentActive;
        lock (_gate)
        {
            wentActive = _telemetry.Count == 0 && _events.Count == 0;
            _telemetry.Add(sub);
            Volatile.Write(ref _telemetrySnapshot, _telemetry.ToArray());
        }
        if (wentActive) ActiveSubscribersChanged?.Invoke(true);
        return subscription;
    }

    public FleetSubscription<VehicleEventMessage> SubscribeEvents(EventFilter filter, int capacity = 1024)
    {
        var channel = Channel.CreateBounded<VehicleEventMessage>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });
        EventSubscriber? sub = null;
        var subscription = new FleetSubscription<VehicleEventMessage>(channel, () =>
        {
            bool wentIdle = false;
            lock (_gate)
            {
                if (sub is not null && _events.Remove(sub))
                {
                    Volatile.Write(ref _eventsSnapshot, _events.ToArray());
                    wentIdle = _telemetry.Count == 0 && _events.Count == 0;
                }
            }
            if (wentIdle) ActiveSubscribersChanged?.Invoke(false);
        });
        sub = new EventSubscriber(filter, subscription);
        bool wentActive;
        lock (_gate)
        {
            wentActive = _telemetry.Count == 0 && _events.Count == 0;
            _events.Add(sub);
            Volatile.Write(ref _eventsSnapshot, _events.ToArray());
        }
        if (wentActive) ActiveSubscribersChanged?.Invoke(true);
        return subscription;
    }

    internal void DispatchTelemetry(VehicleTelemetryEvent ev)
    {
        var snapshot = Volatile.Read(ref _telemetrySnapshot);
        if (snapshot.Length == 0) return;
        var msg = MessageMapper.ToTelemetry(ev);
        foreach (var sub in snapshot)
        {
            if (!sub.Matches(ev)) continue;
            sub.Subscription.TryWrite(msg);
        }
    }

    internal void DispatchEvent(VehicleEvent ev)
    {
        var snapshot = Volatile.Read(ref _eventsSnapshot);
        if (snapshot.Length == 0) return;
        var msg = MessageMapper.ToEvent(ev);
        if (msg is null) return;
        foreach (var sub in snapshot)
        {
            if (!sub.Matches(ev, msg.Kind)) continue;
            sub.Subscription.TryWrite(msg);
        }
    }

    private sealed class TelemetrySubscriber
    {
        public TelemetrySubscriber(TelemetryFilter filter, FleetSubscription<TelemetryMessage> subscription)
        {
            Filter = filter;
            Subscription = subscription;
            HasVehicleId = filter.HasVehicleId && Guid.TryParse(filter.VehicleId, out var g) ? g : null;
            RouteContains = string.IsNullOrEmpty(filter.RouteContains) ? null : filter.RouteContains;
        }

        public TelemetryFilter Filter { get; }
        public FleetSubscription<TelemetryMessage> Subscription { get; }
        public Guid? HasVehicleId { get; }
        public string? RouteContains { get; }

        public bool Matches(VehicleTelemetryEvent ev)
        {
            if (HasVehicleId is { } id && id != ev.VehicleId) return false;
            if (RouteContains is { } rc &&
                !string.Equals(ev.FromCityId, rc, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(ev.ToCityId, rc, StringComparison.OrdinalIgnoreCase))
                return false;
            return true;
        }
    }

    private sealed class EventSubscriber
    {
        public EventSubscriber(EventFilter filter, FleetSubscription<VehicleEventMessage> subscription)
        {
            Filter = filter;
            Subscription = subscription;
            HasVehicleId = filter.HasVehicleId && Guid.TryParse(filter.VehicleId, out var g) ? g : null;
            Kinds = filter.EventTypes.Count == 0 ? null : new HashSet<EventKind>(filter.EventTypes);
        }

        public EventFilter Filter { get; }
        public FleetSubscription<VehicleEventMessage> Subscription { get; }
        public Guid? HasVehicleId { get; }
        public HashSet<EventKind>? Kinds { get; }

        public bool Matches(VehicleEvent ev, EventKind kind)
        {
            if (HasVehicleId is { } id && id != ev.VehicleId) return false;
            if (Kinds is not null && !Kinds.Contains(kind)) return false;
            return true;
        }
    }
}

internal static class MessageMapper
{
    // Guid.ToString() allocates a 36-char string on every call. Vehicle IDs are stable for the
    // lifetime of a vehicle, so cache the formatted form. With ~1000 vehicles the dictionary
    // tops out at ~36 KB; per-message lookup avoids ~36 B × 1000/sec = 36 KB/s of garbage.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, string> VehicleIdStrings = new();

    private static string GetVehicleIdString(Guid id)
        => VehicleIdStrings.TryGetValue(id, out var s) ? s : VehicleIdStrings.GetOrAdd(id, static g => g.ToString());

    public static TelemetryMessage ToTelemetry(VehicleTelemetryEvent ev) => new()
    {
        VehicleId = GetVehicleIdString(ev.VehicleId),
        Timestamp = Timestamp.FromDateTimeOffset(ev.TimestampUtc),
        FromCityId = ev.FromCityId,
        ToCityId = ev.ToCityId,
        SegmentProgressKm = ev.SegmentProgressKm,
        SegmentLengthKm = ev.SegmentLengthKm,
        SpeedKph = ev.SpeedKph,
        FuelLitres = ev.FuelLitres,
        TankCapacityLitres = ev.FuelCapacityLitres,
        Status = ev.Status switch
        {
            AbsVehicleStatus.Idle => PbVehicleStatus.Idle,
            AbsVehicleStatus.Driving => PbVehicleStatus.Driving,
            AbsVehicleStatus.Refuelling => PbVehicleStatus.Refuelling,
            AbsVehicleStatus.RouteCompleted => PbVehicleStatus.RouteCompleted,
            _ => PbVehicleStatus.Unspecified,
        },
    };

    public static VehicleEventMessage? ToEvent(VehicleEvent ev)
    {
        var msg = new VehicleEventMessage
        {
            VehicleId = GetVehicleIdString(ev.VehicleId),
            Timestamp = Timestamp.FromDateTimeOffset(ev.TimestampUtc),
        };

        switch (ev)
        {
            case AbsDeparted d:
                msg.Kind = EventKind.DepartedCity;
                msg.Departed = new PbDeparted { CityId = d.CityId, NextCityId = d.NextCityId };
                break;
            case AbsArrived a:
                msg.Kind = EventKind.ArrivedCity;
                msg.Arrived = new PbArrived { CityId = a.CityId };
                break;
            case AbsRefuelStart rs:
                msg.Kind = EventKind.RefuellingStarted;
                msg.RefuellingStarted = new PbRefuelStart
                {
                    CityId = rs.CityId,
                    FuelRequiredLitres = rs.FuelRequiredLitres,
                    FuelAvailableLitres = rs.FuelAvailableLitres,
                };
                break;
            case AbsRefuelDone rc:
                msg.Kind = EventKind.RefuellingCompleted;
                msg.RefuellingCompleted = new PbRefuelDone { CityId = rc.CityId, FuelLitres = rc.FuelLitres };
                break;
            case AbsRouteDone rcm:
                msg.Kind = EventKind.RouteCompleted;
                var completed = new PbRouteDone();
                completed.CompletedRoute.AddRange(rcm.CompletedRoute);
                completed.NewRoute.AddRange(rcm.NewRoute);
                msg.RouteCompleted = completed;
                break;
            default:
                return null;
        }
        return msg;
    }
}
