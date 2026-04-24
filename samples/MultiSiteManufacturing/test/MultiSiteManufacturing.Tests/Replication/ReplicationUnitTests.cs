using System.Text.Json;
using Microsoft.Extensions.Configuration;
using MultiSiteManufacturing.Host.Replication;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Replication;

/// <summary>
/// Unit tests for the deterministic pieces of the cross-cluster
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
        var encoded = ReplogKeyCodec.Encode(hlc, "us", ReplicationOp.Set, "mfg-facts/HPT-PART-2028-00001/0001");

        Assert.That(ReplogKeyCodec.TryDecode(encoded, out var decodedHlc, out var cluster, out var op, out var key), Is.True);
        Assert.That(decodedHlc, Is.EqualTo(hlc));
        Assert.That(cluster, Is.EqualTo("us"));
        Assert.That(op, Is.EqualTo(ReplicationOp.Set));
        Assert.That(key, Is.EqualTo("mfg-facts/HPT-PART-2028-00001/0001"));
    }

    [Test]
    public void Encode_sorts_by_hlc_then_cluster_id()
    {
        var early = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var later = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 };

        var euEarly = ReplogKeyCodec.Encode(early, "eu", ReplicationOp.Set, "k");
        var usEarly = ReplogKeyCodec.Encode(early, "us", ReplicationOp.Set, "k");
        var euLater = ReplogKeyCodec.Encode(later, "eu", ReplicationOp.Set, "k");

        Assert.That(string.CompareOrdinal(euEarly, usEarly), Is.LessThan(0),
            "Same HLC: 'eu' must sort before 'us'");
        Assert.That(string.CompareOrdinal(usEarly, euLater), Is.LessThan(0),
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
        var atCursor = ReplogKeyCodec.Encode(cursor, "us", ReplicationOp.Set, "k");
        var start = ReplogKeyCodec.StartAfter(cursor, "us");
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
                ["Replication:LocalCluster"] = "us",
                ["Replication:SharedSecret"] = "s",
                ["Replication:Peers:0:Name"] = "eu",
                ["Replication:Peers:0:BaseUrl"] = "http://localhost:5003",
                ["Replication:ReplicatedTrees:0"] = "mfg-facts",
                ["Replication:ReplicatedTrees:1"] = "mfg-site-activity-index",
            })
            .Build();

        var topo = ReplicationTopology.Load(config);

        Assert.Multiple(() =>
        {
            Assert.That(topo.LocalCluster, Is.EqualTo("us"));
            Assert.That(topo.SharedSecret, Is.EqualTo("s"));
            Assert.That(topo.Peers, Has.Count.EqualTo(1));
            Assert.That(topo.Peers[0].Name, Is.EqualTo("eu"));
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
                ["Replication:LocalCluster"] = "us",
                ["Replication:SharedSecret"] = "s",
                ["Replication:Peers:0:Name"] = "eu",
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
                ["Replication:LocalCluster"] = "us",
                ["Replication:SharedSecret"] = "s",
                ["Replication:Peers:0:Name"] = "eu",
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
            LocalCluster = "us",
            SharedSecret = "s",
            Peers = [],
            ReplicatedTrees = ["mfg-facts"],
        };
        Assert.That(topo.IsEnabled, Is.False);
    }

    // ---- IsKeyReplicated: per-tree per-key filter --------------------

    private static ReplicationTopology TopologyWith(params string[] trees) => new()
    {
        LocalCluster = "us",
        SharedSecret = "s",
        Peers = [new ReplicationPeer("eu", [new Uri("http://localhost:5003")])],
        ReplicatedTrees = trees,
    };

    [Test]
    public void IsKeyReplicated_returns_true_for_opted_in_tree_with_no_filter()
    {
        var topo = TopologyWith("mfg-facts");
        Assert.That(
            topo.IsKeyReplicated("mfg-facts", "HPT-PART-2028-00001/0001"),
            Is.True);
    }

    [Test]
    public void IsKeyReplicated_returns_false_for_tree_not_opted_in()
    {
        var topo = TopologyWith("mfg-facts");
        Assert.That(topo.IsKeyReplicated("mfg-part-crdt", "HPT/labels/hot"), Is.False);
    }

    [Test]
    public void IsKeyReplicated_mfg_part_crdt_replicates_labels_only()
    {
        // Opt the tree in and verify the per-key split: labels pass
        // (G-Set, safe under cross-cluster replay); operator register
        // is filtered at origin because the inbound apply path stamps
        // a fresh HLC and would lose the source HLC ordering.
        var topo = TopologyWith("mfg-part-crdt");

        Assert.Multiple(() =>
        {
            Assert.That(
                topo.IsKeyReplicated("mfg-part-crdt", "HPT-PART-2028-00001/labels/hot"),
                Is.True,
                "Label keys (G-Set) must replicate.");

            Assert.That(
                topo.IsKeyReplicated("mfg-part-crdt", "HPT-PART-2028-00001/operator"),
                Is.False,
                "Operator register (LWW) must NOT replicate.");

            // Shadow-prefixed writes (set under partition) still
            // contain /labels/ or /operator - the same rule applies.
            Assert.That(
                topo.IsKeyReplicated("mfg-part-crdt", "shadow/a/HPT-PART-2028-00001/labels/hot"),
                Is.True);
            Assert.That(
                topo.IsKeyReplicated("mfg-part-crdt", "shadow/a/HPT-PART-2028-00001/operator"),
                Is.False);
        });
    }

    [Test]
    public void IsKeyReplicated_throws_on_null_arguments()
    {
        var topo = TopologyWith("mfg-facts");
        Assert.Multiple(() =>
        {
            Assert.That(() => topo.IsKeyReplicated(null!, "k"), Throws.ArgumentNullException);
            Assert.That(() => topo.IsKeyReplicated("mfg-facts", null!), Throws.ArgumentNullException);
        });
    }
}
