using System.Collections.Concurrent;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Tests;

/// <summary>
/// A subscriber observing the sharded telemetry-all fan-out grain that owns a given vehicle sees
/// that vehicle's telemetry events with monotonically non-decreasing timestamps. Per-vehicle
/// ordering must hold even though many vehicles share the shard, because each vehicle is
/// turn-based and publishes sequentially.
/// </summary>
[Collection(ClusterCollection.Name)]
public class StreamSubscriberOrderTests(ClusterFixture fixture)
{
    [Fact]
    public async Task Telemetry_events_are_ordered_per_vehicle()
    {
        const int targetCount = 4;
        var timeout = TimeSpan.FromSeconds(15);

        var fleet = fixture.Cluster.GrainFactory.GetGrain<IFleetGrain>(IFleetGrain.Key);
        var id = Guid.NewGuid();
        await fleet.AddVehicle(new VehicleSpec { VehicleId = id, StartCityId = "A" });

        var shard = fixture.Cluster.GrainFactory.GetGrain<IFleetFanOutGrain>(
            IFleetFanOutGrain.ShardKey(StreamConstants.ShardForVehicle(id)));

        var collected = new ConcurrentQueue<VehicleTelemetryEvent>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Filter to just the vehicle under test — the shard carries traffic for many vehicles.
        var observer = new OrderObserver(collected, targetCount, done, id);
        var observerRef = fixture.Cluster.Client.CreateObjectReference<IFleetStreamObserver>(observer);
        await shard.Subscribe(observerRef);

        try
        {
            var completed = await Task.WhenAny(done.Task, Task.Delay(timeout));
            Assert.True(ReferenceEquals(completed, done.Task),
                $"Only collected {collected.Count} telemetry events for vehicle {id} within {timeout.TotalSeconds:n0}s.");

            var ordered = collected.ToArray();
            for (int i = 1; i < ordered.Length; i++)
            {
                Assert.True(ordered[i].TimestampUtc >= ordered[i - 1].TimestampUtc,
                    $"Telemetry timestamps regressed at index {i}: {ordered[i - 1].TimestampUtc:O} -> {ordered[i].TimestampUtc:O}");
                Assert.Equal(id, ordered[i].VehicleId);
            }
        }
        finally
        {
            try { await shard.Unsubscribe(observerRef); } catch { /* best effort */ }
            try { await fleet.RemoveVehicle(id); } catch { /* best effort */ }
        }
    }

    private sealed class OrderObserver(
        ConcurrentQueue<VehicleTelemetryEvent> sink,
        int target,
        TaskCompletionSource done,
        Guid vehicleId) : IFleetStreamObserver
    {
        public Task OnTelemetry(VehicleTelemetryEvent telemetry)
        {
            if (telemetry.VehicleId != vehicleId) return Task.CompletedTask;
            sink.Enqueue(telemetry);
            if (sink.Count >= target) done.TrySetResult();
            return Task.CompletedTask;
        }

        public Task OnEvent(VehicleEvent vehicleEvent) => Task.CompletedTask;
    }
}
