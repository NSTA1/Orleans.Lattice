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
            Assert.That(TxRegistryRouting.ShardOf(id, 8), Is.EqualTo(-1));
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
            var shard = TxRegistryRouting.ShardOf(id, count);
            Assert.Multiple(() =>
            {
                Assert.That(id.Version, Is.EqualTo(TxRegistryRouting.ShardedTransactionIdVersion));
                Assert.That(id.Variant & 0xC, Is.EqualTo(0x8), "The RFC 9562 variant bits (10xx) must be preserved.");
                Assert.That(TxRegistryRouting.IsSharded(id), Is.True);
                Assert.That(shard, Is.InRange(0, count - 1));
            });
        }
    }

    [Test]
    public void MintTransactionId_spreads_ids_across_every_shard()
    {
        const int count = 8;
        var seen = new HashSet<int>();
        for (var i = 0; i < 2000 && seen.Count < count; i++)
        {
            seen.Add(TxRegistryRouting.ShardOf(TxRegistryRouting.MintTransactionId(count), count));
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
        Assert.That(TxRegistryRouting.ShardOf(id, 8), Is.EqualTo(TxRegistryRouting.ShardOf(id, 8)));
    }

    [Test]
    public void ShardOf_reduces_an_id_minted_under_a_larger_count_into_range()
    {
        // A replicated saga's id may have been minted on a peer with more shards.
        for (var i = 0; i < 200; i++)
        {
            var id = TxRegistryRouting.MintTransactionId(256);
            Assert.That(TxRegistryRouting.ShardOf(id, 3), Is.InRange(0, 2));
        }
    }

    [Test]
    public void ShardOf_with_count_one_routes_a_sharded_id_to_the_legacy_registry()
    {
        var id = TxRegistryRouting.MintTransactionId(8);
        Assert.That(TxRegistryRouting.ShardOf(id, 1), Is.EqualTo(-1));
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
        Assert.That(TxRegistryRouting.ShardKey(TreeId, Guid.NewGuid(), 8), Is.EqualTo(TreeId));
    }

    [Test]
    public void ShardKey_routes_a_sharded_id_to_its_suffixed_key()
    {
        var id = TxRegistryRouting.MintTransactionId(8);
        var shard = TxRegistryRouting.ShardOf(id, 8);

        Assert.That(TxRegistryRouting.ShardKey(TreeId, id, 8), Is.EqualTo($"{TreeId}~s{shard}"));
    }

    [Test]
    public void ShardKeyAt_formats_the_shard_suffix()
    {
        Assert.That(TxRegistryRouting.ShardKeyAt(TreeId, 12), Is.EqualTo("tree-route~s12"));
    }

    [Test]
    public void EnumerateKeys_with_count_one_returns_only_the_legacy_key()
    {
        Assert.That(TxRegistryRouting.EnumerateKeys(TreeId, 1), Is.EqualTo(new[] { TreeId }));
    }

    [Test]
    public void EnumerateKeys_returns_every_shard_then_the_legacy_key()
    {
        var keys = TxRegistryRouting.EnumerateKeys(TreeId, 3);
        Assert.That(keys, Is.EqualTo(new[] { "tree-route~s0", "tree-route~s1", "tree-route~s2", TreeId }));
    }

    [TestCase("tree-route~s0", "tree-route", false)]
    [TestCase("tree-route~s255", "tree-route", false)]
    [TestCase("tree-route", "tree-route", true)]
    [TestCase("tree-route~s256", "tree-route~s256", true)]
    [TestCase("tree-route~s1000", "tree-route~s1000", true)]
    [TestCase("tree-route~sx", "tree-route~sx", true)]
    [TestCase("tree-route~s", "tree-route~s", true)]
    [TestCase("tree-route~s-1", "tree-route~s-1", true)]
    [TestCase("~s3", "~s3", true)]
    [TestCase("a~sb~s4", "a~sb", false)]
    [TestCase("a~s4~sb", "a~s4~sb", true)]
    public void TreeIdFromKey_and_IsLegacyKey_parse_only_a_trailing_numeric_suffix(string key, string expectedTree, bool expectedLegacy)
    {
        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryRouting.TreeIdFromKey(key), Is.EqualTo(expectedTree));
            Assert.That(TxRegistryRouting.IsLegacyKey(key), Is.EqualTo(expectedLegacy));
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
        Assert.That(TxRegistryRouting.ResolveShardCount(monitor), Is.EqualTo(LatticeOptions.DefaultTxRegistryShardCount));
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
        var key = TxRegistryRouting.ShardKey(TreeId, id, 8);
        factory.GetGrain<ITxRegistryGrain>(key).Returns(expected);

        Assert.That(TxRegistryRouting.GetRegistry(factory, TreeId, id, 8), Is.SameAs(expected));
    }

    [Test]
    public void GetRegistry_resolves_a_legacy_id_to_the_bare_tree_key()
    {
        var factory = Substitute.For<IGrainFactory>();
        var expected = Substitute.For<ITxRegistryGrain>();
        factory.GetGrain<ITxRegistryGrain>(TreeId).Returns(expected);

        Assert.That(TxRegistryRouting.GetRegistry(factory, TreeId, Guid.NewGuid(), 8), Is.SameAs(expected));
    }
}
