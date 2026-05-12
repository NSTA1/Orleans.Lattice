using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains;
using VehicleFleetSimulator.Grains.Cities;

namespace VehicleFleetSimulator.Tests;

/// <summary>
/// Locks in the central contract every Lattice-targeted benchmark scenario depends on:
/// registering a custom <see cref="ITelemetrySink"/> in the silo's DI container is the *only*
/// thing required to redirect <c>VehicleGrain</c> telemetry. No grain-side or stream-side change
/// is needed to add a <c>LatticeSink</c>; the seam is sufficient on its own.
/// </summary>
public sealed class TelemetrySinkSwappabilityTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            _cluster.Dispose();
        }
    }

    [Fact]
    public async Task A_custom_sink_registered_in_DI_receives_every_vehicle_tick()
    {
        var fleet = _cluster.GrainFactory.GetGrain<IFleetGrain>(IFleetGrain.Key);
        var vehicleId = Guid.NewGuid();

        await fleet.AddVehicle(new VehicleSpec { VehicleId = vehicleId, StartCityId = "A" });

        // Under load each VehicleGrain ticks at 1 Hz; wait for at least a couple of telemetry
        // samples to arrive at the custom sink so the test isn't satisfied by a single race-y
        // first publish.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < deadline && CapturingTelemetrySink.Telemetry.Count(t => t.VehicleId == vehicleId) < 2)
        {
            await Task.Delay(100);
        }

        var captured = CapturingTelemetrySink.Telemetry.Where(t => t.VehicleId == vehicleId).ToArray();
        Assert.True(captured.Length >= 2,
            $"Expected at least 2 telemetry samples on the custom sink for vehicle {vehicleId}; got {captured.Length}.");

        // Stopping the vehicle through the same DI-routed path must keep working - the swap
        // doesn't affect anything other than the sink target.
        await fleet.RemoveVehicle(vehicleId);
    }

    [Fact]
    public async Task The_default_FanOutTelemetrySink_is_overridden_not_chained()
    {
        // If both sinks were active, the FleetFanOutGrain would still see telemetry. The
        // benchmark plan assumes the swap is exclusive (current-state-no-replication/current-state-single-peer measure Lattice cost only),
        // so verify the fan-out shards see *no* telemetry for our vehicle.
        var fleet = _cluster.GrainFactory.GetGrain<IFleetGrain>(IFleetGrain.Key);
        var vehicleId = Guid.NewGuid();

        var fanOutSeen = new ConcurrentBag<Guid>();
        var observer = new EverythingTelemetryObserver(fanOutSeen);
        var observerRef = _cluster.Client.CreateObjectReference<IFleetStreamObserver>(observer);
        var shards = new List<IFleetFanOutGrain>();
        for (var shard = 0; shard < StreamConstants.TelemetryAllShardCount; shard++)
        {
            var grain = _cluster.GrainFactory.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shard));
            await grain.Subscribe(observerRef);
            shards.Add(grain);
        }

        try
        {
            await fleet.AddVehicle(new VehicleSpec { VehicleId = vehicleId, StartCityId = "A" });

            // Give the vehicle a few ticks. The custom sink should fill; the fan-out shards must not.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (DateTime.UtcNow < deadline && CapturingTelemetrySink.Telemetry.Count(t => t.VehicleId == vehicleId) < 2)
            {
                await Task.Delay(100);
            }

            Assert.Contains(CapturingTelemetrySink.Telemetry, t => t.VehicleId == vehicleId);
            Assert.DoesNotContain(vehicleId, fanOutSeen);

            await fleet.RemoveVehicle(vehicleId);
        }
        finally
        {
            foreach (var s in shards)
            {
                try { await s.Unsubscribe(observerRef); } catch { /* best effort */ }
            }
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder silo)
        {
            silo.AddMemoryGrainStorage("Default");
            silo.ConfigureServices(services =>
            {
                services.AddSingleton<ICityGraphProvider>(new StaticCityGraphProvider(TestGraph.BuildSimple()));
                services.AddSingleton<SimulationRuntimeState>();
                // Custom sink only - no FanOutTelemetrySink registration. This is exactly the
                // swap pattern a benchmark run will use to redirect to a LatticeSink.
                services.AddSingleton<ITelemetrySink, CapturingTelemetrySink>();
            });
        }
    }

    private sealed class EverythingTelemetryObserver(ConcurrentBag<Guid> seen) : IFleetStreamObserver
    {
        public Task OnTelemetry(VehicleTelemetryEvent telemetry)
        {
            seen.Add(telemetry.VehicleId);
            return Task.CompletedTask;
        }
        public Task OnEvent(VehicleEvent vehicleEvent) => Task.CompletedTask;
    }
}

/// <summary>
/// Test sink that captures every publish into a static collection. Static so the test method can
/// inspect what the silo-side sink saw without round-tripping a grain reference.
/// </summary>
internal sealed class CapturingTelemetrySink : ITelemetrySink
{
    public static ConcurrentBag<VehicleTelemetryEvent> Telemetry { get; } = [];
    public static ConcurrentBag<VehicleEvent> Events { get; } = [];

    public ValueTask PublishTelemetryAsync(VehicleTelemetryEvent telemetry, CancellationToken cancellationToken = default)
    {
        Telemetry.Add(telemetry);
        return ValueTask.CompletedTask;
    }

    public ValueTask PublishEventAsync(VehicleEvent vehicleEvent, CancellationToken cancellationToken = default)
    {
        Events.Add(vehicleEvent);
        return ValueTask.CompletedTask;
    }
}
