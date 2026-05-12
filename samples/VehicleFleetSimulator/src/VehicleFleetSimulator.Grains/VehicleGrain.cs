using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using System.Collections.Immutable;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Cities;
using VehicleFleetSimulator.Grains.Vehicles;

namespace VehicleFleetSimulator.Grains;

/// <summary>
/// One Orleans grain per vehicle. Owns a timer that drives <see cref="VehicleSimulator"/> and publishes
/// telemetry + discrete events to the configured Orleans streams.
/// </summary>
public sealed class VehicleGrain : Grain, IVehicleGrain
{
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(200);

    private readonly IPersistentState<VehiclePersistentState> _persistent;
    private readonly ICityGraphProvider _graphProvider;
    private readonly ITelemetrySink _telemetrySink;
    private readonly ILogger<VehicleGrain> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly SimulationRuntimeState _runtime;
    private readonly Random _random = new();

    private RouteGenerator _routeGenerator = null!;
    private IGrainTimer? _timer;

    public VehicleGrain(
        [PersistentState("vehicle", "Default")] IPersistentState<VehiclePersistentState> persistent,
        ICityGraphProvider graphProvider,
        ITelemetrySink telemetrySink,
        SimulationRuntimeState runtime,
        ILogger<VehicleGrain> logger,
        TimeProvider? timeProvider = null)
    {
        _persistent = persistent;
        _graphProvider = graphProvider;
        _telemetrySink = telemetrySink;
        _runtime = runtime;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _routeGenerator = new RouteGenerator(_graphProvider.Graph, _random);

        // Telemetry / event publish is routed through ITelemetrySink (DI singleton). The default
        // FanOutTelemetrySink preserves the original direct-cross-grain dispatch to the sharded
        // IFleetFanOutGrain relay; alternative sinks (NullTelemetrySink, future LatticeSink) can be
        // swapped in via DI to drive different benchmark scenarios without changing this grain.

        if (_persistent.State.IsInitialized && _persistent.State.IsRunning)
        {
            ScheduleTimer();
        }

        return Task.CompletedTask;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_timer is not null)
        {
            _timer.Dispose();
            _timer = null;
        }
        // No persistence flush: this grain runs against memory grain storage, so writes don't
        // survive silo restart anyway. _persistent.State is the live in-memory state for the
        // grain's lifetime and isn't read back from the provider during normal operation.
        return Task.CompletedTask;
    }

    public Task Initialize(VehicleSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);

        if (_persistent.State.IsInitialized)
        {
            _logger.LogDebug("Vehicle {VehicleId} already initialized; skipping.", this.GetPrimaryKey());
            return Task.CompletedTask;
        }

        var grainKey = this.GetPrimaryKey();
        var effectiveSpec = spec.VehicleId is null ? spec with { VehicleId = grainKey } : spec;
        if (effectiveSpec.VehicleId != grainKey)
            throw new ArgumentException($"VehicleSpec.VehicleId {effectiveSpec.VehicleId} does not match grain key {grainKey}.", nameof(spec));

        var config = effectiveSpec.Config ?? VehicleConfig.Default;
        var initialState = VehicleSimulator.CreateInitialState(
            effectiveSpec, _graphProvider.Graph, _routeGenerator, DateTimeOffset.UtcNow);

        _persistent.State.State = initialState;
        _persistent.State.Config = config;
        _persistent.State.IsRunning = false;
        return Task.CompletedTask;
    }

    public Task Start()
    {
        if (!_persistent.State.IsInitialized)
            throw new InvalidOperationException("Vehicle has not been initialized.");

        if (_persistent.State.IsRunning) return Task.CompletedTask;

        _persistent.State.IsRunning = true;
        ScheduleTimer();
        return Task.CompletedTask;
    }

    public Task Stop()
    {
        if (!_persistent.State.IsRunning) return Task.CompletedTask;

        _timer?.Dispose();
        _timer = null;
        _persistent.State.IsRunning = false;
        return Task.CompletedTask;
    }

    public async Task Clear()
    {
        _timer?.Dispose();
        _timer = null;
        await _persistent.ClearStateAsync();
        DeactivateOnIdle();
    }

    public Task UpdateConfig(VehicleConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        _persistent.State.Config = config;
        return Task.CompletedTask;
    }

    public Task SetRoute(ImmutableArray<string> route)
    {
        if (route.IsDefault || route.Length < 2)
            throw new ArgumentException("Route must contain at least two cities.", nameof(route));
        if (!_graphProvider.Graph.IsRouteValid(route))
            throw new ArgumentException("Route is invalid for the city graph.", nameof(route));
        if (_persistent.State.State is null)
            throw new InvalidOperationException("Vehicle has not been initialized.");

        _persistent.State.State = _persistent.State.State.Value with
        {
            Route = route,
            RouteIndex = 0,
            SegmentProgressKm = 0,
            SpeedKph = 0,
            SpeedTimeSinceResample = TimeSpan.Zero,
            Status = VehicleStatus.Driving,
            LastUpdatedUtc = DateTimeOffset.UtcNow,
        };
        return Task.CompletedTask;
    }

    public ValueTask<VehicleSnapshot?> GetSnapshot()
    {
        var state = _persistent.State.State;
        return ValueTask.FromResult(state is null
            ? null
            : VehicleSnapshot.From(state.Value, _persistent.State.IsRunning));
    }

    public async Task<bool> InjectFault(VehicleFault fault)
    {
        if (_persistent.State.State is not { } current) return false;

        var now = DateTimeOffset.UtcNow;
        string? detail = null;
        switch (fault)
        {
            case VehicleFault.OutOfFuel:
                // Drain the tank. The simulator's existing low-fuel handler will trigger refuel
                // on the next tick exactly as it does for an organically empty tank.
                _persistent.State.State = current with { FuelLitres = 0, LastUpdatedUtc = now };
                detail = "tank drained to 0 L";
                break;

            case VehicleFault.EngineStall:
                // Park the vehicle and clear the running flag. Resume requires Start() - same
                // contract as a manual stop, but the operator sees a discrete event in the feed.
                _timer?.Dispose();
                _timer = null;
                _persistent.State.IsRunning = false;
                _persistent.State.State = current with
                {
                    Status = VehicleStatus.Idle,
                    SpeedKph = 0,
                    LastUpdatedUtc = now,
                };
                detail = "vehicle stalled and stopped";
                break;

            case VehicleFault.RouteBlock:
                // Replace the remaining route with a new one starting from the current "from"
                // city. We keep RouteIndex at 0 because the new route has the current city at
                // position 0; SegmentProgressKm resets so the vehicle starts the new first leg
                // from its current city centre. Status flips to Driving so a stalled vehicle
                // doesn't silently get a route it won't use until manually restarted.
                var newRoute = _routeGenerator.Generate(current.CurrentFromCityId);
                _persistent.State.State = current with
                {
                    Route = newRoute,
                    RouteIndex = 0,
                    SegmentProgressKm = 0,
                    SpeedKph = 0,
                    SpeedTimeSinceResample = TimeSpan.Zero,
                    Status = VehicleStatus.Driving,
                    LastUpdatedUtc = now,
                };
                detail = $"route reissued ({newRoute.Length} cities)";
                break;
        }

        try
        {
            await _telemetrySink.PublishEventAsync(new VehicleFaulted(this.GetPrimaryKey(), now, fault, detail));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to publish fault event for vehicle {VehicleId}.", this.GetPrimaryKey());
        }
        return true;
    }

    private void ScheduleTimer()
    {
        if (_timer is not null) return;
        _timer = this.RegisterGrainTimer(TickAsync, new GrainTimerCreationOptions
        {
            DueTime = TickInterval,
            Period = TickInterval,
            Interleave = false,
        });
    }

    private Task TickAsync(CancellationToken cancellationToken)
    {
        if (_persistent.State.State is not { } current) return Task.CompletedTask;

        // Global pause: skip the simulator advance entirely. We don't tear down the timer or flip
        // IsRunning so a single SimulationConfig.IsPaused=false resume restarts every vehicle in
        // lockstep without any per-grain re-Schedule work. We also don't republish the existing
        // state as telemetry -- it would duplicate the last-frame packet at full cadence and
        // wedge the UI's exp-smoothing on a stationary point. Skipping is correct: the UI's
        // FleetState already shows the last position; clients reconnecting see it via the standard
        // fan-out path on the next non-paused tick.
        if (_runtime.IsPaused) return Task.CompletedTask;

        var now = DateTimeOffset.UtcNow;
        var elapsed = now - current.LastUpdatedUtc;
        if (elapsed > TimeSpan.FromMinutes(5)) elapsed = TickInterval; // skip large gaps after silo restart

        // Apply the global simulation TimeScale: 1.0 = realtime, >1 advances the sim N× faster
        // without changing telemetry rate. Read straight from the silo-shared SimulationRuntimeState
        // so slider changes propagate to every vehicle on the next tick - single volatile load, no
        // grain calls, no staleness window.
        var timeScale = _runtime.TimeScale;
        var effectiveConfig = _persistent.State.Config;
        if (timeScale != 1.0)
        {
            elapsed = TimeSpan.FromTicks((long)(elapsed.Ticks * timeScale));
            // RefuelDelay is a sim-time duration ("30 seconds at the pump"); without scaling, a
            // 30 wall-second wait at TimeScale=2000 would freeze a vehicle for ~16 sim-hours and
            // make the refuel look permanent. Compress it into wall-clock so the vehicle resumes
            // driving in real time consistent with the rest of the sim cadence.
            var scaledRefuel = TimeSpan.FromTicks((long)Math.Max(1, effectiveConfig.RefuelDelay.Ticks / timeScale));
            effectiveConfig = effectiveConfig with { RefuelDelay = scaledRefuel };
        }

        var result = VehicleSimulator.Tick(
            current, effectiveConfig, _graphProvider.Graph, _routeGenerator,
            elapsed, now, _random);

        _persistent.State.State = result.State;

        // Steady-state fast path: vast majority of ticks have zero discrete events, so we can
        // return the publish ValueTask directly without allocating an async state-machine box.
        if (result.Events.Length == 0)
        {
            try { return PublishTelemetry(result.State).AsTask(); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish telemetry for vehicle {VehicleId}", this.GetPrimaryKey());
                return Task.CompletedTask;
            }
        }

        return TickWithEventsAsync(result);
    }

    private async Task TickWithEventsAsync(TickResult result)
    {
        try
        {
            await PublishTelemetry(result.State);
            foreach (var ev in result.Events)
            {
                await _telemetrySink.PublishEventAsync(ev);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish telemetry for vehicle {VehicleId}", this.GetPrimaryKey());
        }

        // No WriteStateAsync: under memory grain storage every write is a pointless deep-copy
        // into a dictionary that doesn't survive silo restart. The grain's _persistent.State is
        // already the live in-memory state for its activation lifetime. If durable storage is
        // ever reinstated, reintroduce a throttled write here (e.g. on result.Events.Length > 0).
    }

    private ValueTask PublishTelemetry(in VehicleState state)
    {
        var graph = _graphProvider.Graph;
        graph.TryGetDistance(state.CurrentFromCityId, state.CurrentToCityId, out var segLen);
        var telemetry = new VehicleTelemetryEvent(
            VehicleId: state.VehicleId,
            TimestampUtc: state.LastUpdatedUtc,
            FromCityId: state.CurrentFromCityId,
            ToCityId: state.CurrentToCityId,
            SegmentProgressKm: state.SegmentProgressKm,
            SegmentLengthKm: segLen,
            SpeedKph: state.SpeedKph,
            FuelLitres: state.FuelLitres,
            Status: state.Status,
            FuelCapacityLitres: _persistent.State.Config.FuelCapacityLitres);
        // Return the sink's ValueTask directly - no async modifier, no extra state-machine box per
        // tick. The default FanOutTelemetrySink targets a [OneWay] grain method so the underlying
        // await completes once the message is enqueued, with no response round-trip on the wire.
        return _telemetrySink.PublishTelemetryAsync(telemetry);
    }
}
