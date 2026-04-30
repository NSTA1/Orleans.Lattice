using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Tests;

/// <summary>
/// Milestone 8: end-to-end lifecycle of a single vehicle through the Orleans <see cref="ClusterFixture"/>:
/// add → tick → snapshot reflects driving state → stop preserves state.
/// </summary>
[Collection(ClusterCollection.Name)]
public class VehicleLifecycleTests(ClusterFixture fixture)
{
    [Fact]
    public async Task Vehicle_lifecycle_add_tick_snapshot_stop()
    {
        var fleet = fixture.Cluster.GrainFactory.GetGrain<IFleetGrain>(IFleetGrain.Key);
        var id = Guid.NewGuid();

        await fleet.AddVehicle(new VehicleSpec { VehicleId = id, StartCityId = "A" });

        var grain = fixture.Cluster.GrainFactory.GetGrain<IVehicleGrain>(id);

        // Wait for the simulator to tick and update the snapshot's timestamp.
        VehicleSnapshot? snapshot = null;
        var initial = await grain.GetSnapshot();
        Assert.NotNull(initial);
        var initialTs = initial!.LastUpdatedUtc;

        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            snapshot = await grain.GetSnapshot();
            if (snapshot is not null && snapshot.LastUpdatedUtc > initialTs) break;
            await Task.Delay(250);
        }

        Assert.NotNull(snapshot);
        Assert.True(snapshot!.IsRunning);
        Assert.NotEmpty(snapshot.Route);
        Assert.True(snapshot.LastUpdatedUtc > initialTs, "Expected at least one tick to have advanced the snapshot timestamp.");
        Assert.Contains(snapshot.Status, new[] { VehicleStatus.Driving, VehicleStatus.Refuelling, VehicleStatus.RouteCompleted });

        await grain.Stop();
        var stopped = await grain.GetSnapshot();
        Assert.NotNull(stopped);
        Assert.False(stopped!.IsRunning);

        await fleet.RemoveVehicle(id);
    }
}
