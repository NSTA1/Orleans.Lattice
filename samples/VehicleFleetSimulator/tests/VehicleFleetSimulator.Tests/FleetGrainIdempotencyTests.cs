using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Tests;

[Collection(ClusterCollection.Name)]
public class FleetGrainIdempotencyTests(ClusterFixture fixture)
{
    private IFleetGrain Fleet() =>
        fixture.Cluster.GrainFactory.GetGrain<IFleetGrain>(IFleetGrain.Key);

    private static VehicleSpec SpecWith(Guid id, string startCity = "A") =>
        new() { VehicleId = id, StartCityId = startCity };

    [Fact]
    public async Task AddVehicle_with_existing_id_throws_under_default_policy()
    {
        var fleet = Fleet();
        var id = Guid.NewGuid();

        var first = await fleet.AddVehicle(SpecWith(id));
        Assert.Equal(id, first);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fleet.AddVehicle(SpecWith(id)));

        var listed = await fleet.ListVehicles();
        Assert.Single(listed, x => x == id);
    }

    [Fact]
    public async Task AddVehicle_with_existing_id_is_noop_under_skip_policy()
    {
        var fleet = Fleet();
        var id = Guid.NewGuid();

        await fleet.AddVehicle(SpecWith(id));
        var second = await fleet.AddVehicle(SpecWith(id), DuplicateVehiclePolicy.Skip);

        Assert.Equal(id, second);
        var listed = await fleet.ListVehicles();
        Assert.Single(listed, x => x == id);
    }

    [Fact]
    public async Task AddVehicleBatch_throws_when_any_id_already_exists()
    {
        var fleet = Fleet();
        var existing = Guid.NewGuid();
        await fleet.AddVehicle(SpecWith(existing));

        var batch = new VehicleSpec[]
        {
            SpecWith(Guid.NewGuid()),
            SpecWith(existing), // collides
            SpecWith(Guid.NewGuid()),
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fleet.AddVehicleBatch(batch));
    }

    [Fact]
    public async Task AddVehicleBatch_throws_on_intra_batch_duplicates()
    {
        var fleet = Fleet();
        var dup = Guid.NewGuid();

        var batch = new VehicleSpec[]
        {
            SpecWith(dup),
            SpecWith(dup),
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fleet.AddVehicleBatch(batch));
    }

    [Fact]
    public async Task AddVehicleBatch_skips_duplicates_under_skip_policy()
    {
        var fleet = Fleet();
        var existing = Guid.NewGuid();
        var fresh = Guid.NewGuid();
        await fleet.AddVehicle(SpecWith(existing));

        var before = await fleet.ListVehicles();
        var beforeCount = before.Count;

        var batch = new VehicleSpec[]
        {
            SpecWith(existing),
            SpecWith(fresh),
            SpecWith(existing),
        };

        var ids = await fleet.AddVehicleBatch(batch, DuplicateVehiclePolicy.Skip);

        // Returned ids preserve input order, including the resolved duplicates.
        Assert.Equal(3, ids.Count);
        Assert.Equal(existing, ids[0]);
        Assert.Equal(fresh, ids[1]);
        Assert.Equal(existing, ids[2]);

        var after = await fleet.ListVehicles();
        Assert.Equal(beforeCount + 1, after.Count);
        Assert.Contains(fresh, after);
    }
}
