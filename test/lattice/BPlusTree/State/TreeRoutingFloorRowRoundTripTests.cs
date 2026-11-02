using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// The tree id's routing-generation floor row (epic #5605, #5668) round trips through
/// Lattice's binary grain-storage serializer and through the JSON grain-storage
/// serializer a host falls back to when it bypasses Lattice's (#5579). It is a new
/// type under a new alias, so 10.2.0 never reads one, and an absent row reads as floor
/// 0, so a tree id never purged starts its generations at 1, as before.
/// </summary>
[TestFixture]
public sealed class TreeRoutingFloorRowRoundTripTests
{
    private static ServiceProvider Services()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddSingleton<OrleansJsonSerializer>();
        services.AddSingleton<JsonGrainStorageSerializer>();
        return services.BuildServiceProvider();
    }

    [Test]
    public void Routing_floor_round_trips_in_json_and_binary()
    {
        using var services = Services();
        var json = services.GetRequiredService<JsonGrainStorageSerializer>();
        var binary = new LatticeGrainStorageSerializer(services.GetRequiredService<Serializer>(), json);
        var row = new TreeRoutingFloorState { Floors = { ["s0"] = 17, ["t0"] = 4 } };

        var payload = binary.Serialize(row);
        var fromBinary = binary.Deserialize<TreeRoutingFloorState>(payload);
        var fromJson = json.Deserialize<TreeRoutingFloorState>(json.Serialize(row));

        Assert.Multiple(() =>
        {
            Assert.That(payload.ToArray().AsSpan(0, 4).ToArray(), Is.EqualTo("LGB1"u8.ToArray()),
                "the floor is marked ILatticeBinaryPersistedState, so it is written in binary");
            Assert.That(fromBinary.Floors, Is.EquivalentTo(row.Floors));
            Assert.That(fromJson.Floors, Is.EquivalentTo(row.Floors));
            Assert.That(json.Deserialize<TreeRoutingFloorState>(json.Serialize(new TreeRoutingFloorState())).Floors, Is.Empty);
        });
    }
}
