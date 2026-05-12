using System.Collections.Concurrent;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Telemetry;

namespace VehicleFleetSimulator.Tests;

/// <summary>
/// Locks in <see cref="FanOutTelemetrySink"/>'s routing contract, which the benchmark plan
/// depends on:
/// <list type="bullet">
///   <item><description>Telemetry is dispatched to the shard chosen by
///     <see cref="StreamConstants.ShardForVehicle"/> - never to a different shard.</description></item>
///   <item><description>Discrete events are dispatched to the singleton events-feed activation
///     (<see cref="IFleetFanOutGrain.EventsKey"/>) and never to any telemetry shard, so a
///     backed-up events queue can't block control-plane Subscribe / Unsubscribe on a telemetry
///     shard that's also serving live publishes.</description></item>
/// </list>
/// Together these guarantee that benchmark scenarios driven through this sink (simulator-baseline, current-state-no-replication, current-state-single-peer
/// when not yet swapped) observe the same shard distribution the load harness was designed against.
/// </summary>
[Collection(ClusterCollection.Name)]
public class FanOutTelemetrySinkRoutingTests(ClusterFixture fixture)
{
    [Fact]
    public async Task Telemetry_lands_on_the_shard_chosen_by_ShardForVehicle()
    {
        var sink = new FanOutTelemetrySink(fixture.Cluster.GrainFactory);

        // Pick a deterministic vehicle id per shard so we can assert exact routing rather than
        // statistical distribution. Bruteforcing Guid.NewGuid until it falls in the target shard
        // is fast enough (each shard absorbs 1/8 of guids) and avoids leaking ShardForVehicle's
        // internals into the test.
        var idsByShard = new Guid[StreamConstants.TelemetryAllShardCount];
        for (var shard = 0; shard < idsByShard.Length; shard++)
        {
            Guid candidate;
            do { candidate = Guid.NewGuid(); }
            while (StreamConstants.ShardForVehicle(candidate) != shard);
            idsByShard[shard] = candidate;
        }

        // Subscribe a per-shard observer so we can attribute each delivery to a specific shard.
        var perShardSeen = new ConcurrentDictionary<Guid, int>();
        var observers = new ShardAttributingObserver[idsByShard.Length];
        var observerRefs = new IFleetStreamObserver[idsByShard.Length];
        var shards = new IFleetFanOutGrain[idsByShard.Length];
        for (var shard = 0; shard < idsByShard.Length; shard++)
        {
            observers[shard] = new ShardAttributingObserver(shard, perShardSeen);
            observerRefs[shard] = fixture.Cluster.Client.CreateObjectReference<IFleetStreamObserver>(observers[shard]);
            shards[shard] = fixture.Cluster.GrainFactory.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shard));
            await shards[shard].Subscribe(observerRefs[shard]);
        }

        try
        {
            for (var shard = 0; shard < idsByShard.Length; shard++)
            {
                var id = idsByShard[shard];
                await sink.PublishTelemetryAsync(new VehicleTelemetryEvent(
                    VehicleId: id,
                    TimestampUtc: DateTimeOffset.UtcNow,
                    FromCityId: "A",
                    ToCityId: "B",
                    SegmentProgressKm: 1,
                    SegmentLengthKm: 10,
                    SpeedKph: 50,
                    FuelLitres: 30,
                    Status: VehicleStatus.Driving));
            }

            // Allow ObserverManager.Notify to flush; observers run on the silo, not in-line with
            // the publish caller. A short bounded wait is sufficient - every shard is independent.
            await WaitUntil(() => perShardSeen.Count == idsByShard.Length, TimeSpan.FromSeconds(10));

            for (var shard = 0; shard < idsByShard.Length; shard++)
            {
                Assert.True(
                    perShardSeen.TryGetValue(idsByShard[shard], out var observingShard),
                    $"Vehicle for shard {shard} produced no telemetry observation.");
                Assert.Equal(shard, observingShard);
            }
        }
        finally
        {
            for (var shard = 0; shard < shards.Length; shard++)
            {
                try { await shards[shard].Unsubscribe(observerRefs[shard]); } catch { /* best effort */ }
            }
        }
    }

    [Fact]
    public async Task Events_land_on_the_dedicated_events_activation_and_never_on_a_telemetry_shard()
    {
        var sink = new FanOutTelemetrySink(fixture.Cluster.GrainFactory);

        var seenOnEventsGrain = new TaskCompletionSource<VehicleEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var leakedShards = new ConcurrentBag<int>();

        // Subscribe a per-shard observer to every telemetry shard plus the dedicated events-feed
        // activation. A correctly-routed event must fire only the events-grain observer; any
        // delivery to a telemetry shard's observer is a leak that breaks the isolation guarantee.
        var shards = new IFleetFanOutGrain[StreamConstants.TelemetryAllShardCount];
        var shardRefs = new IFleetStreamObserver[StreamConstants.TelemetryAllShardCount];
        for (var shard = 0; shard < shards.Length; shard++)
        {
            var s = shard;
            var observer = new EventCapturingObserver(_ => leakedShards.Add(s));
            shardRefs[shard] = fixture.Cluster.Client.CreateObjectReference<IFleetStreamObserver>(observer);
            shards[shard] = fixture.Cluster.GrainFactory.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.ShardKey(shard));
            await shards[shard].Subscribe(shardRefs[shard]);
        }

        var eventsObserver = new EventCapturingObserver(ev => seenOnEventsGrain.TrySetResult(ev));
        var eventsRef = fixture.Cluster.Client.CreateObjectReference<IFleetStreamObserver>(eventsObserver);
        var eventsGrain = fixture.Cluster.GrainFactory.GetGrain<IFleetFanOutGrain>(IFleetFanOutGrain.EventsKey());
        await eventsGrain.Subscribe(eventsRef);

        try
        {
            // Vehicle id is irrelevant for events routing now (events bypass ShardForVehicle
            // entirely), but pick one whose telemetry shard isn't zero anyway -- that way a
            // regression that re-routes events through ShardForVehicle would also be caught.
            Guid id;
            do { id = Guid.NewGuid(); } while (StreamConstants.ShardForVehicle(id) == 0);

            VehicleEvent ev = new ArrivedCity(id, DateTimeOffset.UtcNow, "B");
            await sink.PublishEventAsync(ev);

            var completed = await Task.WhenAny(seenOnEventsGrain.Task, Task.Delay(TimeSpan.FromSeconds(10)));
            Assert.Same(seenOnEventsGrain.Task, completed);
            Assert.True(leakedShards.IsEmpty, $"Event leaked to telemetry shard(s): [{string.Join(",", leakedShards)}].");
            var observed = await seenOnEventsGrain.Task;
            Assert.Equal(id, observed.VehicleId);
        }
        finally
        {
            for (var shard = 0; shard < shards.Length; shard++)
            {
                try { await shards[shard].Unsubscribe(shardRefs[shard]); } catch { /* best effort */ }
            }
            try { await eventsGrain.Unsubscribe(eventsRef); } catch { /* best effort */ }
        }
    }

    private static async Task WaitUntil(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(25);
        }
    }

    private sealed class ShardAttributingObserver(int shard, ConcurrentDictionary<Guid, int> seen) : IFleetStreamObserver
    {
        public Task OnTelemetry(VehicleTelemetryEvent telemetry)
        {
            seen[telemetry.VehicleId] = shard;
            return Task.CompletedTask;
        }

        public Task OnEvent(VehicleEvent vehicleEvent) => Task.CompletedTask;
    }

    private sealed class EventCapturingObserver(Action<VehicleEvent> onEvent) : IFleetStreamObserver
    {
        public Task OnTelemetry(VehicleTelemetryEvent telemetry) => Task.CompletedTask;
        public Task OnEvent(VehicleEvent vehicleEvent)
        {
            onEvent(vehicleEvent);
            return Task.CompletedTask;
        }
    }
}
