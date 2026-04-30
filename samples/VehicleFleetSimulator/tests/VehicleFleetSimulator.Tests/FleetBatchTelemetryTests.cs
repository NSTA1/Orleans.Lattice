using System.Collections.Concurrent;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Tests;

/// <summary>
/// Milestone 8: batch-add N vehicles and assert that telemetry is observed for every one of them
/// on the silo's sharded fan-out grains within a bounded time window.
/// </summary>
[Collection(ClusterCollection.Name)]
public class FleetBatchTelemetryTests(ClusterFixture fixture)
{
    [Fact]
    public async Task Batch_add_emits_telemetry_for_every_vehicle_within_time_budget()
    {
        const int vehicleCount = 8;
        var timeout = TimeSpan.FromSeconds(15);

        var fleet = fixture.Cluster.GrainFactory.GetGrain<IFleetGrain>(IFleetGrain.Key);
        var ids = Enumerable.Range(0, vehicleCount).Select(_ => Guid.NewGuid()).ToArray();
        var specs = ids.Select(id => new VehicleSpec { VehicleId = id, StartCityId = "A" }).ToArray();

        var seen = new ConcurrentDictionary<Guid, byte>();
        var allSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Telemetry-all is sharded — register an observer with every shard so we observe the whole fleet.
        var observer = new TelemetryAllObserver(seen, ids, allSeen);
        var observerRef = fixture.Cluster.Client.CreateObjectReference<IFleetStreamObserver>(observer);
        var shards = new List<IFleetFanOutGrain>();
        for (var shard = 0; shard < StreamConstants.TelemetryAllShardCount; shard++)
        {
            var grain = fixture.Cluster.GrainFactory.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shard));
            await grain.Subscribe(observerRef);
            shards.Add(grain);
        }

        try
        {
            await fleet.AddVehicleBatch(specs);

            var completed = await Task.WhenAny(allSeen.Task, Task.Delay(timeout));
            Assert.True(ReferenceEquals(completed, allSeen.Task),
                $"Only {seen.Count}/{vehicleCount} vehicles emitted telemetry within {timeout.TotalSeconds:n0}s.");
        }
        finally
        {
            foreach (var s in shards)
            {
                try { await s.Unsubscribe(observerRef); } catch { /* best effort */ }
            }
            foreach (var id in ids)
            {
                try { await fleet.RemoveVehicle(id); } catch { /* best effort */ }
            }
        }
    }

    private sealed class TelemetryAllObserver(
        ConcurrentDictionary<Guid, byte> seen,
        Guid[] expected,
        TaskCompletionSource done) : IFleetStreamObserver
    {
        public Task OnTelemetry(VehicleTelemetryEvent telemetry)
        {
            seen.TryAdd(telemetry.VehicleId, 0);
            if (expected.All(seen.ContainsKey)) done.TrySetResult();
            return Task.CompletedTask;
        }

        public Task OnEvent(VehicleEvent vehicleEvent) => Task.CompletedTask;
    }
}
