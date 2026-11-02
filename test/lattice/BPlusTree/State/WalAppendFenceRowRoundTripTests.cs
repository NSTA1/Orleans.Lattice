using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Every row type the WAL append fence adds (epic #5605) round trips through
/// Lattice's binary grain-storage serializer and through the JSON grain-storage
/// serializer a host falls back to when it bypasses Lattice's (#5579). Each is
/// a new type under a new alias, so 10.2.0 never reads one, and an absent row
/// reads as zero, the value that holds every fence closed.
/// </summary>
[TestFixture]
public sealed class WalAppendFenceRowRoundTripTests
{
    private static ServiceProvider Services()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddSingleton<OrleansJsonSerializer>();
        services.AddSingleton<JsonGrainStorageSerializer>();
        return services.BuildServiceProvider();
    }

    private static T JsonRoundTrip<T>(T value)
    {
        using var services = Services();
        var json = services.GetRequiredService<JsonGrainStorageSerializer>();
        return json.Deserialize<T>(json.Serialize(value));
    }

    private static T BinaryRoundTrip<T>(T value)
    {
        using var services = Services();
        var binary = new LatticeGrainStorageSerializer(
            services.GetRequiredService<Serializer>(),
            services.GetRequiredService<JsonGrainStorageSerializer>());
        var payload = binary.Serialize(value);
        Assert.That(payload.ToArray().AsSpan(0, 4).ToArray(), Is.EqualTo("LGB1"u8.ToArray()),
            $"{typeof(T).Name} is marked ILatticeBinaryPersistedState, so it is written in binary");
        return binary.Deserialize<T>(payload);
    }

    [Test]
    public void Leaf_append_epoch_round_trips_in_json_and_binary()
    {
        var row = new LeafAppendEpochState { Epoch = 7 };

        Assert.Multiple(() =>
        {
            Assert.That(JsonRoundTrip(row).Epoch, Is.EqualTo(7));
            Assert.That(BinaryRoundTrip(row).Epoch, Is.EqualTo(7));
        });
    }

    [Test]
    public void Wal_append_epoch_entry_round_trips_in_json_and_binary()
    {
        var row = new WalAppendEpochEntryState { Epoch = 11 };

        Assert.Multiple(() =>
        {
            Assert.That(JsonRoundTrip(row).Epoch, Is.EqualTo(11));
            Assert.That(BinaryRoundTrip(row).Epoch, Is.EqualTo(11));
        });
    }

    [Test]
    public void Wal_append_epoch_floor_round_trips_in_json_and_binary()
    {
        var row = new WalAppendEpochFloorState { Floor = 13 };

        Assert.Multiple(() =>
        {
            Assert.That(JsonRoundTrip(row).Floor, Is.EqualTo(13));
            Assert.That(BinaryRoundTrip(row).Floor, Is.EqualTo(13));
        });
    }

    [Test]
    public void Wal_routing_generation_round_trips_in_json_and_binary()
    {
        var row = new WalRoutingGenerationState { Generation = 17 };

        Assert.Multiple(() =>
        {
            Assert.That(JsonRoundTrip(row).Generation, Is.EqualTo(17));
            Assert.That(BinaryRoundTrip(row).Generation, Is.EqualTo(17));
        });
    }

    [Test]
    public void Wal_append_epoch_directory_page_round_trips_in_json_and_binary()
    {
        var leaves = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() };
        var row = new WalAppendEpochDirectoryPageState { Leaves = new List<Guid>(leaves), AddedUtcTicks = new List<long> { 19, 23 } };

        var json = JsonRoundTrip(row);
        var binary = BinaryRoundTrip(row);

        Assert.Multiple(() =>
        {
            Assert.That(json.Leaves, Is.EqualTo(leaves));
            Assert.That(json.AddedUtcTicks, Is.EqualTo(new[] { 19L, 23L }));
            Assert.That(binary.Leaves, Is.EqualTo(leaves));
            Assert.That(binary.AddedUtcTicks, Is.EqualTo(new[] { 19L, 23L }));
            Assert.That(JsonRoundTrip(new WalAppendEpochDirectoryPageState()).Leaves, Is.Empty, "an empty page reads back empty, never null");
        });
    }

    [Test]
    public void Wal_append_fence_sweep_round_trips_in_json_and_binary()
    {
        var row = new WalAppendFenceSweepState { LastStartedUtcTicks = 29 };

        Assert.Multiple(() =>
        {
            Assert.That(JsonRoundTrip(row).LastStartedUtcTicks, Is.EqualTo(29));
            Assert.That(BinaryRoundTrip(row).LastStartedUtcTicks, Is.EqualTo(29));
            Assert.That(JsonRoundTrip(new WalAppendFenceSweepState()).LastStartedUtcTicks, Is.Zero);
        });
    }

    [Test]
    public void A_zero_valued_row_round_trips_as_zero_in_json()
    {
        // The JSON grain-storage serializer omits members equal to the type default
        // (ShardRootStateStorageRoundTripTests); zero must still read back as zero,
        // which is the value that holds every fence closed.
        Assert.Multiple(() =>
        {
            Assert.That(JsonRoundTrip(new LeafAppendEpochState()).Epoch, Is.Zero);
            Assert.That(JsonRoundTrip(new WalAppendEpochEntryState()).Epoch, Is.Zero);
            Assert.That(JsonRoundTrip(new WalAppendEpochFloorState()).Floor, Is.Zero);
            Assert.That(JsonRoundTrip(new WalRoutingGenerationState()).Generation, Is.Zero);
        });
    }
}
