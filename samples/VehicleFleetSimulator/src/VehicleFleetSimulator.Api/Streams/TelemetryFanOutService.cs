using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Api.Streams;

/// <summary>
/// Hosted service that bridges the silo's <c>FleetFanOutGrain</c> shards into the in-process
/// <see cref="FleetStreamHub"/>. Subscriber-driven: only registers the upstream Orleans observer
/// while at least one downstream gRPC client is connected. When the last client disconnects the
/// observer is torn down so the silo stops shipping 1000 msg/s of telemetry to a hub that would
/// just throw it away - the post-disconnect CPU + GC pressure on the API came from exactly that.
/// </summary>
internal sealed class TelemetryFanOutService : IHostedService, IDisposable
{
    private static readonly TimeSpan PingInterval = TimeSpan.FromSeconds(30);

    private readonly IClusterClient _orleans;
    private readonly FleetStreamHub _hub;
    private readonly ILogger<TelemetryFanOutService> _logger;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private FanOutObserver? _observer;
    private IFleetStreamObserver? _observerRef;
    private CancellationTokenSource? _pingCts;
    private Task? _pingLoop;
    private bool _attached;
    private volatile bool _stopped;

    public TelemetryFanOutService(IClusterClient orleans, IFleetStreamHub hub, ILogger<TelemetryFanOutService> logger)
    {
        _orleans = orleans;
        _hub = (FleetStreamHub)hub;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _hub.ActiveSubscribersChanged += OnActiveSubscribersChanged;
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        _hub.ActiveSubscribersChanged -= OnActiveSubscribersChanged;
        await DetachAsync().ConfigureAwait(false);
    }

    private void OnActiveSubscribersChanged(bool hasSubscribers)
    {
        if (_stopped) return;
        // Subscribe / unsubscribe involves cross-grain calls - never run them on the publisher's
        // thread under FleetStreamHub._gate. Fire and forget; AttachAsync / DetachAsync are
        // serialised by _gate (the SemaphoreSlim) so transitions can't interleave.
        _ = Task.Run(async () =>
        {
            try
            {
                if (hasSubscribers) await AttachAsync().ConfigureAwait(false);
                else await DetachAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "TelemetryFanOutService transition failed (hasSubscribers={HasSubscribers}).", hasSubscribers);
            }
        });
    }

    private async Task AttachAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_attached || _stopped) return;

            _observer = new FanOutObserver(_hub);
            _observerRef = _orleans.CreateObjectReference<IFleetStreamObserver>(_observer);

            for (var shard = 0; shard < StreamConstants.TelemetryAllShardCount; shard++)
            {
                await _orleans.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shard))
                    .Subscribe(_observerRef).ConfigureAwait(false);
            }
            // Events live on their own activation (see IFleetFanOutGrain.EventsKey) so the
            // observer has to be registered there too, otherwise we'd see telemetry but no
            // discrete events on the gRPC stream.
            await _orleans.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.EventsKey())
                .Subscribe(_observerRef).ConfigureAwait(false);

            _pingCts = new CancellationTokenSource();
            _pingLoop = Task.Run(() => PingLoop(_pingCts.Token));
            _attached = true;

            _logger.LogInformation(
                "TelemetryFanOutService attached observer to {Shards} FleetFanOutGrain telemetry shards + 1 events activation (first client connected).",
                StreamConstants.TelemetryAllShardCount);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task DetachAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!_attached) return;

            _pingCts?.Cancel();
            if (_pingLoop is not null)
            {
                try { await _pingLoop.ConfigureAwait(false); } catch (OperationCanceledException) { }
            }
            _pingCts?.Dispose();
            _pingCts = null;
            _pingLoop = null;

            if (_observerRef is not null)
            {
                for (var shard = 0; shard < StreamConstants.TelemetryAllShardCount; shard++)
                {
                    try
                    {
                        await _orleans.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shard))
                            .Unsubscribe(_observerRef).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to unsubscribe fan-out observer from shard {Shard}.", shard);
                    }
                }
                try
                {
                    await _orleans.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.EventsKey())
                        .Unsubscribe(_observerRef).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to unsubscribe fan-out observer from events activation.");
                }
            }

            _observerRef = null;
            _observer = null;
            _attached = false;

            _logger.LogInformation("TelemetryFanOutService detached observer (last client disconnected).");
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task PingLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(PingInterval, ct).ConfigureAwait(false);
                var observerRef = _observerRef;
                if (observerRef is null) continue;
                for (var shard = 0; shard < StreamConstants.TelemetryAllShardCount; shard++)
                {
                    var grain = _orleans.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shard));
                    await grain.Subscribe(observerRef).ConfigureAwait(false); // refreshes the observer lease
                    await grain.Ping().ConfigureAwait(false);
                }
                // Refresh the lease + keep alive the events-feed activation as well; without this
                // ping the events grain would time out after ObserverExpiration and silently stop
                // delivering events while telemetry kept flowing.
                var eventsGrain = _orleans.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.EventsKey());
                await eventsGrain.Subscribe(observerRef).ConfigureAwait(false);
                await eventsGrain.Ping().ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Fan-out ping iteration failed; will retry.");
            }
        }
    }

    public void Dispose()
    {
        _pingCts?.Dispose();
        _gate.Dispose();
    }

    private sealed class FanOutObserver : IFleetStreamObserver
    {
        private readonly FleetStreamHub _hub;
        public FanOutObserver(FleetStreamHub hub) => _hub = hub;

        public Task OnTelemetry(VehicleTelemetryEvent telemetry)
        {
            _hub.DispatchTelemetry(telemetry);
            return Task.CompletedTask;
        }

        public Task OnEvent(VehicleEvent vehicleEvent)
        {
            _hub.DispatchEvent(vehicleEvent);
            return Task.CompletedTask;
        }
    }
}
