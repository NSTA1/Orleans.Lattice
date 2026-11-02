using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Every state type member 2 of epic #5605 adds - the hand-off marker rows and the
/// tree range-delete fence's state - round trips through Lattice's binary
/// grain-storage serializer and through the JSON grain-storage serializer a host
/// falls back to when it bypasses Lattice's (#5579): every member is a public
/// property of a concrete type. Each is a new type under a new alias, so 10.2.0 never
/// reads one, and an absent row reads as uninitialised with nothing open, which holds.
/// </summary>
[TestFixture]
public sealed class HandOffMarkerRowRoundTripTests
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

    private static ShardHandOffState SampleShard() => new()
    {
        Initialised = true,
        Open = { ["rd:1"] = new HandOffMarkerRange { Token = "rd:1", LowInclusive = "a", HighExclusive = "m" } },
        Closes = { new HandOffClose { Token = "fold:2", LowInclusive = null, HighExclusive = "z", Heads = [3, 4], Sequence = 5 } },
        CompactedHeads = [1, 2],
        RoutingGeneration = 7,
        BulkBirthHints = { ["bulk-op"] = [10, 11] },
        Sequence = 9,
    };

    private static void AssertShard(ShardHandOffState round)
    {
        Assert.Multiple(() =>
        {
            Assert.That(round.Initialised, Is.True);
            Assert.That(round.Open["rd:1"].LowInclusive, Is.EqualTo("a"));
            Assert.That(round.Open["rd:1"].HighExclusive, Is.EqualTo("m"));
            Assert.That(round.Closes.Single().Token, Is.EqualTo("fold:2"));
            Assert.That(round.Closes.Single().LowInclusive, Is.Null);
            Assert.That(round.Closes.Single().Heads, Is.EqualTo(new long[] { 3, 4 }));
            Assert.That(round.Closes.Single().Sequence, Is.EqualTo(5));
            Assert.That(round.CompactedHeads, Is.EqualTo(new long[] { 1, 2 }));
            Assert.That(round.RoutingGeneration, Is.EqualTo(7));
            Assert.That(round.BulkBirthHints["bulk-op"], Is.EqualTo(new long[] { 10, 11 }));
            Assert.That(round.Sequence, Is.EqualTo(9));
        });
    }

    [Test]
    public void Shard_hand_off_marker_round_trips_in_json_and_binary()
    {
        AssertShard(JsonRoundTrip(SampleShard()));
        AssertShard(BinaryRoundTrip(SampleShard()));
    }

    [Test]
    public void Leaf_hand_off_marker_round_trips_in_json_and_binary()
    {
        var row = new LeafHandOffState
        {
            Initialised = true,
            Open = { ["split:x"] = new HandOffMarkerRange { Token = "split:x" } },
            Closes = { new HandOffClose { Token = "reclaim:y", Heads = [6] } },
            CompactedHeads = [2],
            Sequence = 3,
        };

        foreach (var round in new[] { JsonRoundTrip(row), BinaryRoundTrip(row) })
        {
            Assert.Multiple(() =>
            {
                Assert.That(round.Initialised, Is.True);
                Assert.That(round.Open.Keys, Is.EquivalentTo(new[] { "split:x" }));
                Assert.That(round.Closes.Single().Heads, Is.EqualTo(new long[] { 6 }));
                Assert.That(round.CompactedHeads, Is.EqualTo(new long[] { 2 }));
                Assert.That(round.Sequence, Is.EqualTo(3));
            });
        }
    }

    [Test]
    public void Tree_range_delete_fence_state_round_trips_in_json_and_binary()
    {
        var intentId = Guid.NewGuid();
        var row = new TreeRangeDeleteFenceState
        {
            Generation = 4,
            ClaimedGeneration = 3,
            Intents =
            {
                [intentId] = new TreeRangeDeleteIntent
                {
                    IntentId = intentId,
                    StartInclusive = "a",
                    EndExclusive = "b",
                    DeadlineUtcTicks = 42,
                    Shards = [0, 2],
                    Closing = true,
                },
            },
        };

        foreach (var round in new[] { JsonRoundTrip(row), BinaryRoundTrip(row) })
        {
            var intent = round.Intents[intentId];
            Assert.Multiple(() =>
            {
                Assert.That(round.Generation, Is.EqualTo(4));
                Assert.That(round.ClaimedGeneration, Is.EqualTo(3));
                Assert.That(intent.StartInclusive, Is.EqualTo("a"));
                Assert.That(intent.EndExclusive, Is.EqualTo("b"));
                Assert.That(intent.DeadlineUtcTicks, Is.EqualTo(42));
                Assert.That(intent.Shards, Is.EqualTo(new[] { 0, 2 }));
                Assert.That(intent.Closing, Is.True);
            });
        }
    }

    [Test]
    public void An_absent_marker_reads_as_uninitialised_with_nothing_open()
    {
        var shard = new ShardHandOffState();
        var leaf = new LeafHandOffState();

        Assert.Multiple(() =>
        {
            Assert.That(shard.Initialised, Is.False, "absence never means closed (B2)");
            Assert.That(leaf.Initialised, Is.False);
            Assert.That(shard.RoutingGeneration, Is.Zero);
            Assert.That(shard.Open, Is.Empty);
            Assert.That(leaf.Open, Is.Empty);
        });
    }
}
