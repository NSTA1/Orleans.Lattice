using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalMaterialiserPinRouting"/>: the stateless helper
/// that maps a leaf-materialiser consumer id to one of
/// <see cref="LatticeOptions.WalMaterialiserPinShards"/> durable pin grain keys
/// and enumerates the read keys (every shard plus the legacy key) the WAL GC
/// fans in over (issue #1030).
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinRoutingTests
{
    private const string Tree = "tree-1";

    private static IOptionsMonitor<LatticeOptions> Options(int shards)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalMaterialiserPinShards = shards });
        return monitor;
    }

    [Test]
    public void ResolveShardCount_null_options_defaults_to_one()
    {
        Assert.That(WalMaterialiserPinRouting.ResolveShardCount(null), Is.EqualTo(1));
    }

    [Test]
    public void ResolveShardCount_clamps_below_one_to_one()
    {
        Assert.That(WalMaterialiserPinRouting.ResolveShardCount(Options(0)), Is.EqualTo(1));
        Assert.That(WalMaterialiserPinRouting.ResolveShardCount(Options(-5)), Is.EqualTo(1));
    }

    [Test]
    public void ShardKey_single_shard_returns_legacy_unsuffixed_key()
    {
        Assert.That(WalMaterialiserPinRouting.ShardKey(Tree, "consumer-a", 1), Is.EqualTo(Tree));
    }

    [Test]
    public void ShardKey_is_stable_across_calls()
    {
        var a = WalMaterialiserPinRouting.ShardKey(Tree, "_lattice_materialiser_tree-1_leaf-7", 8);
        var b = WalMaterialiserPinRouting.ShardKey(Tree, "_lattice_materialiser_tree-1_leaf-7", 8);
        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Does.StartWith("tree-1#s"));
    }

    [Test]
    public void ShardKey_distributes_consumers_across_shards()
    {
        var shards = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < 200; i++)
        {
            shards.Add(WalMaterialiserPinRouting.ShardKey(Tree, $"_lattice_materialiser_tree-1_leaf-{i}", 8));
        }

        // A stable hash over 200 distinct ids must cover more than one shard.
        Assert.That(shards.Count, Is.GreaterThan(1));
        Assert.That(shards.Count, Is.LessThanOrEqualTo(8));
    }

    [Test]
    public void ShardKey_lands_in_range()
    {
        var valid = new HashSet<string>(StringComparer.Ordinal);
        for (var s = 0; s < 4; s++)
        {
            valid.Add($"{Tree}#s{s}");
        }

        for (var i = 0; i < 50; i++)
        {
            var key = WalMaterialiserPinRouting.ShardKey(Tree, $"consumer-{i}", 4);
            Assert.That(valid, Does.Contain(key));
        }
    }

    [Test]
    public void EnumerateReadKeys_single_shard_yields_only_legacy_key()
    {
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(Tree, 1);
        Assert.That(keys, Is.EqualTo(new[] { Tree }));
    }

    [Test]
    public void EnumerateReadKeys_includes_every_shard_and_legacy_key()
    {
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(Tree, 3);
        Assert.That(keys, Is.EquivalentTo(new[] { "tree-1#s0", "tree-1#s1", "tree-1#s2", "tree-1" }));
    }

    [Test]
    public void EnumerateReadKeys_covers_every_shardkey_target()
    {
        const int shards = 6;
        var readKeys = new HashSet<string>(WalMaterialiserPinRouting.EnumerateReadKeys(Tree, shards), StringComparer.Ordinal);

        // Every key a write could route to must be in the GC's read set,
        // otherwise a pin would be silently dropped from the trim floor.
        for (var i = 0; i < 100; i++)
        {
            var writeKey = WalMaterialiserPinRouting.ShardKey(Tree, $"_lattice_materialiser_tree-1_leaf-{i}", shards);
            Assert.That(readKeys, Does.Contain(writeKey));
        }
    }
}
