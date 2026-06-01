using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Api.Grpc;
using VehicleFleetSimulator.Api.Streams;
using AbsVehicleStatus = VehicleFleetSimulator.Abstractions.VehicleStatus;
using PbVehicleStatus = VehicleFleetSimulator.Api.Grpc.VehicleStatus;

namespace VehicleFleetSimulator.Api.Services;

/// <summary>
/// In-memory telemetry/event recorder. Records subscribe to <see cref="IFleetStreamHub"/> like any
/// other client (so the upstream Orleans observer attaches automatically via the existing
/// reference-counting in <see cref="FleetStreamHub.ActiveSubscribersChanged"/>) and stash messages
/// into a bounded ring per recording. Replay (see <c>POST /api/recording/{id}/replay</c>) reads
/// the stash to recreate the scene.
/// </summary>
/// <remarks>
/// Recordings live in process memory only - restarting the API loses them. That's an intentional
/// scope limitation - a persistent sink is a bigger lift left out of the demo.
/// </remarks>
public sealed class RecordingService : IHostedService, IAsyncDisposable
{
    private readonly IFleetStreamHub _hub;
    private readonly ILogger<RecordingService> _logger;
    private readonly ConcurrentDictionary<Guid, Recording> _recordings = new();
    private volatile bool _stopping;

    public RecordingService(IFleetStreamHub hub, ILogger<RecordingService> logger)
    {
        _hub = hub;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping = true;
        foreach (var rec in _recordings.Values)
        {
            try { rec.Dispose(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to close recording {Id} during shutdown.", rec.Id); }
        }
        _recordings.Clear();
        await Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => new(StopAsync(CancellationToken.None));

    /// <summary>Begin a new recording. Returns the recording id used in subsequent calls.</summary>
    /// <param name="capacity">Per-recording ring size (oldest entries are dropped on overflow).
    /// Applied independently to telemetry and events.</param>
    public Guid Start(int capacity)
    {
        if (_stopping) throw new InvalidOperationException("Recording service is stopping.");
        if (capacity <= 0) capacity = 100_000;

        var id = Guid.NewGuid();
        var rec = new Recording(id, capacity, _hub);
        _recordings[id] = rec;
        rec.Begin();
        _logger.LogInformation("Recording {Id} started (capacity {Capacity}).", id, capacity);
        return id;
    }

    /// <summary>Stop a recording. Returns a small summary, or <c>null</c> when unknown.</summary>
    public RecordingSummary? Stop(Guid id)
    {
        if (!_recordings.TryGetValue(id, out var rec)) return null;
        rec.Finish();
        return new RecordingSummary(
            Id: rec.Id,
            StartedUtc: rec.StartedUtc,
            StoppedUtc: rec.StoppedUtc,
            TelemetryCount: rec.TelemetryCount,
            EventCount: rec.EventCount,
            Capacity: rec.Capacity,
            Active: rec.Active);
    }

    public IReadOnlyList<RecordingSummary> List() =>
        _recordings.Values
            .OrderBy(r => r.StartedUtc)
            .Select(r => new RecordingSummary(
                Id: r.Id,
                StartedUtc: r.StartedUtc,
                StoppedUtc: r.StoppedUtc,
                TelemetryCount: r.TelemetryCount,
                EventCount: r.EventCount,
                Capacity: r.Capacity,
                Active: r.Active))
            .ToArray();

    public RecordingDump? Get(Guid id)
    {
        if (!_recordings.TryGetValue(id, out var rec)) return null;
        return rec.Snapshot();
    }

    private sealed class Recording : IDisposable
    {
        private readonly IFleetStreamHub _hub;
        private readonly object _gate = new();
        private FleetSubscription<TelemetryMessage>? _telemetrySub;
        private FleetSubscription<VehicleEventMessage>? _eventSub;
        private CancellationTokenSource? _cts;
        private Task? _telemetryLoop;
        private Task? _eventLoop;

        // Bounded ring lists - when the cap is reached we evict the oldest entry, which keeps
        // the list cost bounded in memory and gives "the most recent N samples" replay semantics.
        private readonly LinkedList<RecordedTelemetry> _telemetry = new();
        private readonly LinkedList<RecordedEvent> _events = new();

        public Recording(Guid id, int capacity, IFleetStreamHub hub)
        {
            Id = id;
            Capacity = capacity;
            _hub = hub;
            StartedUtc = DateTimeOffset.UtcNow;
        }

        public Guid Id { get; }
        public int Capacity { get; }
        public DateTimeOffset StartedUtc { get; }
        public DateTimeOffset? StoppedUtc { get; private set; }
        public bool Active { get; private set; }

        public int TelemetryCount { get { lock (_gate) return _telemetry.Count; } }
        public int EventCount { get { lock (_gate) return _events.Count; } }

        public void Begin()
        {
            // Fresh filters subscribe to everything; Capacity bounds memory rather than the hub
            // channel (we want to record the most recent N events, not let the hub drop them).
            _telemetrySub = _hub.SubscribeTelemetry(new TelemetryFilter(), capacity: 4096);
            _eventSub = _hub.SubscribeEvents(new EventFilter(), capacity: 1024);
            _cts = new CancellationTokenSource();
            Active = true;
            _telemetryLoop = Task.Run(() => DrainTelemetryAsync(_cts.Token));
            _eventLoop = Task.Run(() => DrainEventsAsync(_cts.Token));
        }

        public void Finish()
        {
            if (!Active) return;
            Active = false;
            StoppedUtc = DateTimeOffset.UtcNow;
            try { _cts?.Cancel(); } catch { }
            try { _telemetrySub?.Dispose(); } catch { }
            try { _eventSub?.Dispose(); } catch { }
        }

        public void Dispose() => Finish();

        public RecordingDump Snapshot()
        {
            RecordedTelemetry[] telem;
            RecordedEvent[] evts;
            lock (_gate)
            {
                telem = _telemetry.ToArray();
                evts = _events.ToArray();
            }
            return new RecordingDump(
                Id: Id,
                StartedUtc: StartedUtc,
                StoppedUtc: StoppedUtc,
                Active: Active,
                Capacity: Capacity,
                Telemetry: telem,
                Events: evts);
        }

        private async Task DrainTelemetryAsync(CancellationToken ct)
        {
            var sub = _telemetrySub;
            if (sub is null) return;
            try
            {
                await foreach (var msg in sub.Channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    var rec = Project(msg);
                    lock (_gate)
                    {
                        _telemetry.AddLast(rec);
                        if (_telemetry.Count > Capacity) _telemetry.RemoveFirst();
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (ChannelClosedException) { }
        }

        private async Task DrainEventsAsync(CancellationToken ct)
        {
            var sub = _eventSub;
            if (sub is null) return;
            try
            {
                await foreach (var msg in sub.Channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    var rec = Project(msg);
                    lock (_gate)
                    {
                        _events.AddLast(rec);
                        if (_events.Count > Capacity) _events.RemoveFirst();
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (ChannelClosedException) { }
        }

        private static RecordedTelemetry Project(TelemetryMessage m)
        {
            // Best-effort vehicle-id parse; non-Guid ids are theoretically possible if a future
            // change loosens the schema, so we fall back to Guid.Empty rather than throwing.
            _ = Guid.TryParse(m.VehicleId, out var vid);
            return new RecordedTelemetry(
                VehicleId: vid,
                TimestampUtc: m.Timestamp.ToDateTimeOffset(),
                FromCityId: m.FromCityId ?? string.Empty,
                ToCityId: m.ToCityId ?? string.Empty,
                SegmentProgressKm: m.SegmentProgressKm,
                SegmentLengthKm: m.SegmentLengthKm,
                SpeedKph: m.SpeedKph,
                FuelLitres: m.FuelLitres,
                Status: m.Status switch
                {
                    PbVehicleStatus.Idle => AbsVehicleStatus.Idle,
                    PbVehicleStatus.Driving => AbsVehicleStatus.Driving,
                    PbVehicleStatus.Refuelling => AbsVehicleStatus.Refuelling,
                    PbVehicleStatus.RouteCompleted => AbsVehicleStatus.RouteCompleted,
                    _ => AbsVehicleStatus.Idle,
                });
        }

        private static RecordedEvent Project(VehicleEventMessage m)
        {
            _ = Guid.TryParse(m.VehicleId, out var vid);
            return new RecordedEvent(
                VehicleId: vid,
                TimestampUtc: m.Timestamp.ToDateTimeOffset(),
                Kind: m.Kind.ToString());
        }
    }
}

/// <summary>Compact summary used by list/start/stop endpoints.</summary>
public sealed record RecordingSummary(
    Guid Id,
    DateTimeOffset StartedUtc,
    DateTimeOffset? StoppedUtc,
    int TelemetryCount,
    int EventCount,
    int Capacity,
    bool Active);

/// <summary>Full dump of recorded data; used by <c>GET /api/recording/{id}</c> and replay.</summary>
public sealed record RecordingDump(
    Guid Id,
    DateTimeOffset StartedUtc,
    DateTimeOffset? StoppedUtc,
    bool Active,
    int Capacity,
    IReadOnlyList<RecordedTelemetry> Telemetry,
    IReadOnlyList<RecordedEvent> Events);

/// <summary>One recorded telemetry sample (projected from the gRPC message). Replay only needs
/// VehicleId/TimestampUtc/FromCityId, but the rest is kept so the dump endpoint is useful for
/// inspection and later, richer replay modes.</summary>
public sealed record RecordedTelemetry(
    Guid VehicleId,
    DateTimeOffset TimestampUtc,
    string FromCityId,
    string ToCityId,
    double SegmentProgressKm,
    double SegmentLengthKm,
    double SpeedKph,
    double FuelLitres,
    AbsVehicleStatus Status);

public sealed record RecordedEvent(
    Guid VehicleId,
    DateTimeOffset TimestampUtc,
    string Kind);
