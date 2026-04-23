using System.Text.Json;
using Microsoft.Extensions.Configuration;
using MultiSiteManufacturing.Host.Replication;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Replication;

/// <summary>
/// Unit tests for the deterministic pieces of the M13 cross-cluster
/// replication subsystem — replog key codec, topology config loader,
/// and wire-type JSON round-trip. These run without an Orleans
/// cluster and are the first line of defence before the integration
/// (two-cluster) walkthrough.
/// </summary>
[TestFixture]
public sealed class ReplogKeyCodecTests
{
    [Test]
    public void Encode_and_decode_round_trip_preserves_components()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 1234567890123456789, Counter = 42 };
        var encoded = ReplogKeyCodec.Encode(hlc, "forge", ReplicationOp.Set, "mfg-facts/HPT-PART-2028-00001/0001");

        Assert.That(ReplogKeyCodec.TryDecode(encoded, out var decodedHlc, out var cluster, out var op, out var key), Is.True);
        Assert.That(decodedHlc, Is.EqualTo(hlc));
        Assert.That(cluster, Is.EqualTo("forge"));
        Assert.That(op, Is.EqualTo(ReplicationOp.Set));
        Assert.That(key, Is.EqualTo("mfg-facts/HPT-PART-2028-00001/0001"));
    }

    [Test]
    public void Encode_sorts_by_hlc_then_cluster_id()
    {
        var early = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var later = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 };

        var forgeEarly = ReplogKeyCodec.Encode(early, "forge", ReplicationOp.Set, "k");
        var heatEarly = ReplogKeyCodec.Encode(early, "heattreat", ReplicationOp.Set, "k");
        var forgeLater = ReplogKeyCodec.Encode(later, "forge", ReplicationOp.Set, "k");

        Assert.That(string.CompareOrdinal(forgeEarly, heatEarly), Is.LessThan(0),
            "Same HLC: 'forge' must sort before 'heattreat'");
        Assert.That(string.CompareOrdinal(heatEarly, forgeLater), Is.LessThan(0),
            "Later HLC must sort above earlier HLC regardless of cluster");
    }

    [Test]
    public void Encode_rejects_cluster_id_containing_separator()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        Assert.That(() => ReplogKeyCodec.Encode(hlc, "bad|cluster", ReplicationOp.Set, "k"),
            Throws.ArgumentException);
    }

    [Test]
    public void StartAfter_is_strictly_above_cursor()
    {
        var cursor = new HybridLogicalClock { WallClockTicks = 100, Counter = 5 };
        var atCursor = ReplogKeyCodec.Encode(cursor, "forge", ReplicationOp.Set, "k");
        var start = ReplogKeyCodec.StartAfter(cursor, "forge");
        Assert.That(string.CompareOrdinal(start, atCursor), Is.GreaterThan(0));
    }

    [Test]
    public void ReplogEndBound_sorts_above_every_valid_key()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = long.MaxValue, Counter = int.MaxValue };
        var max = ReplogKeyCodec.Encode(hlc, "zzzzzz", ReplicationOp.Set, new string('~', 64));
        Assert.That(string.CompareOrdinal(max, ReplogKeyCodec.ReplogEndBound), Is.LessThan(0));
    }

    [Test]
    public void TryDecode_returns_false_for_malformed_input()
    {
        Assert.That(ReplogKeyCodec.TryDecode("not-a-valid-key", out _, out _, out _, out _), Is.False);
        Assert.That(ReplogKeyCodec.TryDecode("", out _, out _, out _, out _), Is.False);
    }
}

/// <summary>
/// Unit tests for <see cref="ReplicationTopology.Load"/> — config
/// binding, peer URL validation, default-trees fallback.
/// </summary>
[TestFixture]
public sealed class ReplicationTopologyTests
{
    [Test]
    public void Load_binds_all_sections_from_memory_config()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Replication:LocalCluster"] = "forge",
                ["Replication:SharedSecret"] = "s",
                ["Replication:Peers:0:Name"] = "heattreat",
                ["Replication:Peers:0:BaseUrl"] = "http://localhost:5003",
                ["Replication:ReplicatedTrees:0"] = "mfg-facts",
                ["Replication:ReplicatedTrees:1"] = "mfg-site-activity-index",
            })
            .Build();

        var topo = ReplicationTopology.Load(config);

        Assert.Multiple(() =>
        {
            Assert.That(topo.LocalCluster, Is.EqualTo("forge"));
            Assert.That(topo.SharedSecret, Is.EqualTo("s"));
            Assert.That(topo.Peers, Has.Count.EqualTo(1));
            Assert.That(topo.Peers[0].Name, Is.EqualTo("heattreat"));
            Assert.That(topo.Peers[0].BaseUrls, Has.Count.EqualTo(1));
            Assert.That(topo.Peers[0].BaseUrls[0].ToString(), Is.EqualTo("http://localhost:5003/"));
            Assert.That(topo.ReplicatedTrees, Is.EquivalentTo(new[] { "mfg-facts", "mfg-site-activity-index" }));
            Assert.That(topo.IsEnabled, Is.True);
            Assert.That(topo.IsReplicated("mfg-facts"), Is.True);
            Assert.That(topo.IsReplicated("mfg-part-crdt"), Is.False);
        });
    }

    [Test]
    public void Load_falls_back_to_default_trees_when_config_omits_list()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Replication:LocalCluster"] = "forge",
                ["Replication:SharedSecret"] = "s",
                ["Replication:Peers:0:Name"] = "heattreat",
                ["Replication:Peers:0:BaseUrl"] = "http://localhost:5003",
            })
            .Build();

        var topo = ReplicationTopology.Load(config);
        Assert.That(topo.ReplicatedTrees, Is.EquivalentTo(ReplicationTopology.DefaultReplicatedTrees));
    }

    [Test]
    public void Load_rejects_malformed_peer_url()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Replication:LocalCluster"] = "forge",
                ["Replication:SharedSecret"] = "s",
                ["Replication:Peers:0:Name"] = "heattreat",
                ["Replication:Peers:0:BaseUrl"] = "not-a-url",
            })
            .Build();

        Assert.That(() => ReplicationTopology.Load(config), Throws.InvalidOperationException);
    }

    [Test]
    public void IsEnabled_is_false_when_no_peers_configured()
    {
        var topo = new ReplicationTopology
        {
            LocalCluster = "forge",
            SharedSecret = "s",
            Peers = [],
            ReplicatedTrees = ["mfg-facts"],
        };
        Assert.That(topo.IsEnabled, Is.False);
    }
}

/// <summary>
/// Verifies <see cref="ReplicationBatch"/> / <see cref="ReplicationEntry"/>
/// / <see cref="ReplicationAck"/> JSON round-trip with the default
/// System.Text.Json web defaults used by the HTTP transport.
/// </summary>
[TestFixture]
public sealed class ReplicationWireJsonTests
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    [Test]
    public void Batch_round_trips_through_json()
    {
        var batch = new ReplicationBatch
        {
            SourceCluster = "forge",
            Tree = "mfg-facts",
            Entries =
            [
                new ReplicationEntry
                {
                    Key = "HPT-PART-2028-00001/0001",
                    Op = ReplicationOp.Set,
                    Value = [1, 2, 3, 4],
                    SourceHlc = new HybridLogicalClock { WallClockTicks = 42, Counter = 7 },
                },
                new ReplicationEntry
                {
                    Key = "HPT-PART-2028-00002/0002",
                    Op = ReplicationOp.Delete,
                    Value = null,
                    SourceHlc = new HybridLogicalClock { WallClockTicks = 43, Counter = 0 },
                },
            ],
        };

        var json = JsonSerializer.Serialize(batch, Options);
        var round = JsonSerializer.Deserialize<ReplicationBatch>(json, Options);

        Assert.That(round, Is.Not.Null);
        Assert.That(round!.SourceCluster, Is.EqualTo(batch.SourceCluster));
        Assert.That(round.Tree, Is.EqualTo(batch.Tree));
        Assert.That(round.Entries, Has.Count.EqualTo(2));
        Assert.That(round.Entries[0].Value, Is.EqualTo(batch.Entries[0].Value));
        Assert.That(round.Entries[0].SourceHlc, Is.EqualTo(batch.Entries[0].SourceHlc));
        Assert.That(round.Entries[1].Op, Is.EqualTo(ReplicationOp.Delete));
        Assert.That(round.Entries[1].Value, Is.Null);
    }

    [Test]
    public void Ack_round_trips_through_json()
    {
        var ack = new ReplicationAck
        {
            Applied = 17,
            HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 99, Counter = 3 },
        };
        var json = JsonSerializer.Serialize(ack, Options);
        var round = JsonSerializer.Deserialize<ReplicationAck>(json, Options);
        Assert.That(round, Is.Not.Null);
        Assert.That(round!.Applied, Is.EqualTo(17));
        Assert.That(round.HighestAppliedHlc, Is.EqualTo(ack.HighestAppliedHlc));
    }
}
