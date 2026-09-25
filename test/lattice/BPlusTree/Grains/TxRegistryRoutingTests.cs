using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="TxRegistryRouting"/>, the txid-to-registry-shard
/// routing of the sharded saga decision registry (issue #3501).
/// </summary>
[TestFixture]
public class TxRegistryRoutingTests
{
    private const string TreeId = "tree-route";

    [Test]
    public void MintTransactionId_with_count_one_returns_an_unsharded_v4_id()
    {
        var id = TxRegistryRouting.MintTransactionId(1);

        Assert.Multiple(() =>
        {
            Assert.That(id.Version, Is.EqualTo(4));
            Assert.That(TxRegistryRouting.IsSharded(id), Is.False);
            Assert.That(TxRegistryRouting.ShardOf(id), Is.EqualTo(-1));
        });
    }

    [TestCase(2)]
    [TestCase(8)]
    [TestCase(256)]
    public void MintTransactionId_with_count_above_one_returns_a_v8_id_with_an_in_range_shard(int count)
    {
        for (var i = 0; i < 200; i++)
        {
            var id = TxRegistryRouting.MintTransactionId(count);
            var shard = TxRegistryRouting.ShardOf(id);
            Assert.Multiple(() =>
            {
                Assert.That(id.Version, Is.EqualTo(TxRegistryRouting.ShardedTransactionIdVersion));
                Assert.That(id.Variant & 0xC, Is.EqualTo(0x8), "The RFC 9562 variant bits (10xx) must be preserved.");
                Assert.That(TxRegistryRouting.IsSharded(id), Is.True);
                Assert.That(shard, Is.InRange(0, count - 1));
            });
        }
    }

    [TestCase(8)]
    [TestCase(64)]
    [TestCase(128)]
    [TestCase(255)]
    [TestCase(256)]
    public void MintTransactionId_spreads_ids_across_every_shard(int count)
    {
        // The index must come from bytes with no fixed bits: drawing it from the
        // RFC variant byte reached only 64 distinct shards at counts above 64.
        var seen = new HashSet<int>();
        for (var i = 0; i < 100_000 && seen.Count < count; i++)
        {
            seen.Add(TxRegistryRouting.ShardOf(TxRegistryRouting.MintTransactionId(count)));
        }

        Assert.That(seen, Has.Count.EqualTo(count));
    }

    [Test]
    public void MintTransactionId_produces_unique_ids()
    {
        var ids = Enumerable.Range(0, 1000).Select(_ => TxRegistryRouting.MintTransactionId(8)).ToHashSet();
        Assert.That(ids, Has.Count.EqualTo(1000));
    }

    [Test]
    public void ShardOf_is_stable_for_the_same_id()
    {
        var id = TxRegistryRouting.MintTransactionId(8);
        Assert.That(TxRegistryRouting.ShardOf(id), Is.EqualTo(TxRegistryRouting.ShardOf(id)));
    }

    [Test]
    public void ShardOf_returns_the_stamped_index_unreduced()
    {
        // An id minted on a cluster with more shards (a replicated saga, or a
        // silo configured with a larger count) keeps its own shard: routing is
        // never reduced by the reading silo's count.
        var id = WithStampedShard(200);

        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.ShardOf(id), Is.EqualTo(200));
            Assert.That(TxRegistryRouting.ShardKey(TreeId, id), Is.EqualTo("_lattice_txshard_200_tree-route"));
        });
    }

    [Test]
    public void ShardKey_is_identical_on_silos_configured_with_different_counts()
    {
        // Mixed-count silos during a rolling reconfiguration: routing takes no
        // count, so the owner is a pure function of the id. Minting under 8 and
        // then "lowering" the count to 1 must leave every id on its original key.
        var ids = Enumerable.Range(0, 64).Select(_ => TxRegistryRouting.MintTransactionId(8)).ToArray();
        var before = ids.Select(id => TxRegistryRouting.ShardKey(TreeId, id)).ToArray();
        var lowered = TxRegistryRouting.MintTransactionId(1);

        Assert.Multiple(() =>
        {
            Assert.That(ids.Select(id => TxRegistryRouting.ShardKey(TreeId, id)), Is.EqualTo(before));
            Assert.That(before, Has.All.StartsWith(TxRegistryRouting.ShardKeyPrefix).And.All.EndsWith("_" + TreeId));
            Assert.That(TxRegistryRouting.ShardKey(TreeId, lowered), Is.EqualTo(TreeId));
        });
    }

    private static Guid WithStampedShard(byte shard)
    {
        Span<byte> bytes = stackalloc byte[16];
        TxRegistryRouting.MintTransactionId(2).TryWriteBytes(bytes);
        bytes[15] = shard;
        return new Guid(bytes);
    }

    [Test]
    public void IsSharded_is_false_for_empty_and_random_ids()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.IsSharded(Guid.Empty), Is.False);
            Assert.That(TxRegistryRouting.IsSharded(Guid.NewGuid()), Is.False);
        });
    }

    [Test]
    public void ShardKey_routes_a_v4_id_to_the_bare_tree_id()
    {
        Assert.That(TxRegistryRouting.ShardKey(TreeId, Guid.NewGuid()), Is.EqualTo(TreeId));
    }

    [Test]
    public void ShardKey_routes_a_sharded_id_to_its_suffixed_key()
    {
        var id = TxRegistryRouting.MintTransactionId(8);
        var shard = TxRegistryRouting.ShardOf(id);

        Assert.That(TxRegistryRouting.ShardKey(TreeId, id), Is.EqualTo($"_lattice_txshard_{shard}_{TreeId}"));
    }

    [Test]
    public void ShardKeyAt_formats_the_shard_suffix()
    {
        Assert.That(TxRegistryRouting.ShardKeyAt(TreeId, 12), Is.EqualTo("_lattice_txshard_12_tree-route"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void EnumerateKeys_with_no_shard_high_water_returns_only_the_legacy_key(int highWater)
    {
        Assert.That(TxRegistryRouting.EnumerateKeys(TreeId, highWater), Is.EqualTo(new[] { TreeId }));
    }

    [Test]
    public void EnumerateKeys_with_high_water_one_covers_shard_zero_and_the_legacy_key()
    {
        Assert.That(TxRegistryRouting.EnumerateKeys(TreeId, 1), Is.EqualTo(new[] { "_lattice_txshard_0_tree-route", TreeId }));
    }

    [Test]
    public void EnumerateKeys_clamps_the_high_water_to_the_maximum_shard_count()
    {
        var keys = TxRegistryRouting.EnumerateKeys(TreeId, 10_000);
        Assert.Multiple(() =>
        {
            Assert.That(keys, Has.Length.EqualTo(LatticeOptions.MaxTxRegistryShardCount + 1));
            Assert.That(keys[^2], Is.EqualTo("_lattice_txshard_255_tree-route"));
            Assert.That(keys[^1], Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void EnumerateKeys_returns_every_shard_then_the_legacy_key()
    {
        var keys = TxRegistryRouting.EnumerateKeys(TreeId, 3);
        Assert.That(keys, Is.EqualTo(new[] { "_lattice_txshard_0_tree-route", "_lattice_txshard_1_tree-route", "_lattice_txshard_2_tree-route", TreeId }));
    }

    [TestCase("_lattice_txshard_0_tree-route", "tree-route", false)]
    [TestCase("_lattice_txshard_255_tree-route", "tree-route", false)]
    [TestCase("_lattice_txshard_3_a_b_7", "a_b_7", false)]
    [TestCase("_lattice_txshard_3_orders~s4", "orders~s4", false)]
    [TestCase("tree-route", "tree-route", true)]
    [TestCase("_lattice_txshard_256_tree-route", "_lattice_txshard_256_tree-route", true)]
    [TestCase("_lattice_txshard_1000_tree-route", "_lattice_txshard_1000_tree-route", true)]
    [TestCase("_lattice_txshard_03_tree-route", "_lattice_txshard_03_tree-route", true)]
    [TestCase("_lattice_txshard_x_tree-route", "_lattice_txshard_x_tree-route", true)]
    [TestCase("_lattice_txshard__tree-route", "_lattice_txshard__tree-route", true)]
    [TestCase("_lattice_txshard_-1_tree-route", "_lattice_txshard_-1_tree-route", true)]
    [TestCase("_lattice_txshard_3_", "_lattice_txshard_3_", true)]
    [TestCase("_lattice_txshard_3", "_lattice_txshard_3", true)]
    [TestCase("_lattice_trees", "_lattice_trees", true)]
    [TestCase("x_lattice_txshard_3_tree", "x_lattice_txshard_3_tree", true)]
    public void TreeIdFromKey_and_IsLegacyKey_parse_only_a_well_formed_shard_key(string key, string expectedTree, bool expectedLegacy)
    {
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.TreeIdFromKey(key), Is.EqualTo(expectedTree));
            Assert.That(TxRegistryRouting.IsLegacyKey(key), Is.EqualTo(expectedLegacy));
        });
    }

    [TestCase("orders~s3")]
    [TestCase("orders~s0")]
    [TestCase("orders_3")]
    [TestCase("3_orders")]
    [TestCase("t/acme/orders~s12")]
    public void A_tree_named_like_a_shard_key_keeps_a_legacy_key_distinct_from_every_shard_key(string treeId)
    {
        // Tree ids are arbitrary strings, so a user tree may carry the old
        // "~s{n}" suffix or digits and underscores. Its legacy key is itself,
        // it parses back to itself, and it never equals any shard key of any
        // tree - so two trees can never share a registry row.
        var shardKeys = TxRegistryRouting.EnumerateKeys("orders", LatticeOptions.MaxTxRegistryShardCount)
            .Concat(TxRegistryRouting.EnumerateKeys(treeId, LatticeOptions.MaxTxRegistryShardCount))
            .Where(k => !TxRegistryRouting.IsLegacyKey(k))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.ShardKey(treeId, Guid.NewGuid()), Is.EqualTo(treeId));
            Assert.That(TxRegistryRouting.IsLegacyKey(treeId), Is.True);
            Assert.That(TxRegistryRouting.TreeIdFromKey(treeId), Is.EqualTo(treeId));
            Assert.That(shardKeys, Has.None.EqualTo(treeId));
            Assert.That(
                TxRegistryRouting.EnumerateKeys(treeId, 4).Select(TxRegistryRouting.TreeIdFromKey),
                Has.All.EqualTo(treeId));
        });
    }

    [Test]
    public void Shard_keys_live_in_the_reserved_system_namespace_that_no_public_tree_can_use()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.ShardKeyPrefix, Does.StartWith(LatticeConstants.SystemTreePrefix));
            Assert.That(TxRegistryRouting.ShardKeyAt("orders", 3), Does.StartWith(LatticeConstants.SystemTreePrefix));
            Assert.That(LatticeConstants.RegistryTreeId, Does.Not.StartWith(TxRegistryRouting.ShardKeyPrefix));
            Assert.That(LatticeConstants.WalTreePrefix, Does.Not.StartWith(TxRegistryRouting.ShardKeyPrefix));
            Assert.That(LatticeConstants.QueueTreePrefix, Does.Not.StartWith(TxRegistryRouting.ShardKeyPrefix));
        });
    }

    [Test]
    public void TreeIdFromKey_round_trips_a_system_tree_shard_key()
    {
        var key = TxRegistryRouting.ShardKeyAt(LatticeConstants.RegistryTreeId, 7);

        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.TryParseShardKey(key, out var treeId, out var shard), Is.True);
            Assert.That(treeId, Is.EqualTo(LatticeConstants.RegistryTreeId));
            Assert.That(shard, Is.EqualTo(7));
        });
    }
    [Test]
    public void TreeIdFromKey_round_trips_every_shard_key()
    {
        foreach (var key in TxRegistryRouting.EnumerateKeys(TreeId, 16))
        {
            Assert.That(TxRegistryRouting.TreeIdFromKey(key), Is.EqualTo(TreeId));
        }
    }

    [Test]
    public void ResolveShardCount_with_null_monitor_returns_one()
    {
        Assert.That(TxRegistryRouting.ResolveShardCount(null), Is.EqualTo(1));
    }

    [Test]
    public void ResolveShardCount_with_null_options_returns_one()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns((LatticeOptions)null!);
        Assert.That(TxRegistryRouting.ResolveShardCount(monitor), Is.EqualTo(1));
    }

    [TestCase(0, 1)]
    [TestCase(-5, 1)]
    [TestCase(1, 1)]
    [TestCase(8, 8)]
    [TestCase(256, 256)]
    [TestCase(9999, 256)]
    public void ResolveShardCount_clamps_the_configured_value(int configured, int expected)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(string.Empty).Returns(new LatticeOptions { TxRegistryShardCount = configured });
        Assert.That(TxRegistryRouting.ResolveShardCount(monitor), Is.EqualTo(expected));
    }

    [Test]
    public void ResolveShardCount_uses_the_default_options_count()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(string.Empty).Returns(new LatticeOptions());
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.ResolveShardCount(monitor), Is.EqualTo(LatticeOptions.DefaultTxRegistryShardCount));
            Assert.That(LatticeOptions.DefaultTxRegistryShardCount, Is.EqualTo(1),
                "Sharding is opt-in: a default-configured silo must keep minting legacy ids a pre-sharding silo can resolve.");
        });
    }

    [Test]
    public void ResolveShardCountFromServices_with_null_or_empty_provider_returns_one()
    {
        using var empty = new ServiceCollection().BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.ResolveShardCountFromServices(null), Is.EqualTo(1));
            Assert.That(TxRegistryRouting.ResolveShardCountFromServices(empty), Is.EqualTo(1));
        });
    }

    [Test]
    public void ResolveShardCountFromServices_reads_the_registered_monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(string.Empty).Returns(new LatticeOptions { TxRegistryShardCount = 5 });
        using var services = new ServiceCollection().AddSingleton(monitor).BuildServiceProvider();

        Assert.That(TxRegistryRouting.ResolveShardCountFromServices(services), Is.EqualTo(5));
    }

    [Test]
    public void GetRegistry_resolves_the_owning_key_through_the_factory()
    {
        var factory = Substitute.For<IGrainFactory>();
        var expected = Substitute.For<ITxRegistryGrain>();
        var id = TxRegistryRouting.MintTransactionId(8);
        var key = TxRegistryRouting.ShardKey(TreeId, id);
        factory.GetGrain<ITxRegistryGrain>(key).Returns(expected);

        Assert.That(TxRegistryRouting.GetRegistry(factory, TreeId, id), Is.SameAs(expected));
    }

    [Test]
    public void GetRegistry_resolves_a_legacy_id_to_the_bare_tree_key()
    {
        var factory = Substitute.For<IGrainFactory>();
        var expected = Substitute.For<ITxRegistryGrain>();
        factory.GetGrain<ITxRegistryGrain>(TreeId).Returns(expected);

        Assert.That(TxRegistryRouting.GetRegistry(factory, TreeId, Guid.NewGuid()), Is.SameAs(expected));
    }
}
