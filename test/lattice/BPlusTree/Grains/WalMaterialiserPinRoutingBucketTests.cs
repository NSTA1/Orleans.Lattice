using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the durable pin <b>bucket</b> routing added for issue #2014.
/// Buckets split the persistence of one pin shard across several durable state
/// slots so a single leaf's advance rewrites only its own bucket rather than
/// every consumer pinned to the shard, which is the write amplification that
/// made the pin store the bottleneck in issue #2012.
/// <para>
/// The load-bearing property here is the <b>default</b>: a bucket count of one
/// must resolve to the historical unsuffixed slot name, so an existing
/// deployment that has not opted in persists byte-for-byte what it always did.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinRoutingBucketTests
{
    private const string ConsumerA = "_lattice_materialiser_tree-2014_leaf-A";
    private const string ConsumerB = "_lattice_materialiser_tree-2014_leaf-B";

    private static IOptionsMonitor<LatticeOptions> Options(int buckets)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalMaterialiserPinBuckets = buckets });
        return monitor;
    }

    [Test]
    public void ResolveBucketCount_defaults_to_one_when_options_are_absent()
    {
        Assert.That(WalMaterialiserPinRouting.ResolveBucketCount(null), Is.EqualTo(1),
            "a host with no options monitor must get the un-bucketed legacy layout");
    }

    [Test]
    public void ResolveBucketCount_defaults_to_one_on_a_default_options_instance()
    {
        Assert.That(WalMaterialiserPinRouting.ResolveBucketCount(Options(new LatticeOptions().WalMaterialiserPinBuckets)),
            Is.EqualTo(1),
            "bucketing must ship disabled so existing deployments are unaffected until an operator opts in");
    }

    [Test]
    public void ResolveBucketCount_clamps_non_positive_values_to_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMaterialiserPinRouting.ResolveBucketCount(Options(0)), Is.EqualTo(1));
            Assert.That(WalMaterialiserPinRouting.ResolveBucketCount(Options(-4)), Is.EqualTo(1));
        });
    }

    [Test]
    public void BucketStateName_single_bucket_returns_the_legacy_unsuffixed_slot()
    {
        Assert.That(
            WalMaterialiserPinRouting.BucketStateName(ConsumerA, 1),
            Is.EqualTo(WalMaterialiserPinState.StateName),
            "the default layout must write the exact slot every pre-bucketing build wrote");
    }

    [Test]
    public void BucketStateName_multi_bucket_suffixes_the_legacy_slot()
    {
        var name = WalMaterialiserPinRouting.BucketStateName(ConsumerA, 8);

        Assert.Multiple(() =>
        {
            Assert.That(name, Does.StartWith(WalMaterialiserPinState.StateName + WalMaterialiserPinRouting.BucketSeparator));
            Assert.That(name, Is.Not.EqualTo(WalMaterialiserPinState.StateName));
        });
    }

    [Test]
    public void BucketOf_is_stable_across_calls()
    {
        var first = WalMaterialiserPinRouting.BucketOf(ConsumerA, 16);
        var second = WalMaterialiserPinRouting.BucketOf(ConsumerA, 16);

        Assert.That(second, Is.EqualTo(first),
            "routing must be a pure function of the consumer id so a pin lands in the same bucket after every restart");
    }

    [Test]
    public void BucketOf_stays_within_range()
    {
        for (var count = 1; count <= 32; count++)
        {
            var bucket = WalMaterialiserPinRouting.BucketOf(ConsumerA, count);
            Assert.That(bucket, Is.InRange(0, count - 1));
        }
    }

    [Test]
    public void BucketOf_collapses_to_zero_when_bucketing_is_disabled()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMaterialiserPinRouting.BucketOf(ConsumerA, 1), Is.EqualTo(0));
            Assert.That(WalMaterialiserPinRouting.BucketOf(ConsumerB, 1), Is.EqualTo(0));
        });
    }

    [Test]
    public void BucketOf_spreads_consumers_across_buckets()
    {
        var seen = new HashSet<int>();
        for (var i = 0; i < 256; i++)
        {
            seen.Add(WalMaterialiserPinRouting.BucketOf($"_lattice_materialiser_tree-2014_leaf-{i}", 8));
        }

        Assert.That(seen, Has.Count.GreaterThan(1),
            "a hash that mapped every consumer to one bucket would not reduce the per-write blob at all");
    }

    [Test]
    public void BucketOf_spreads_the_consumers_of_a_single_shard_across_buckets()
    {
        // The regression that matters. A shard activation only ever writes the
        // consumers routed to IT, so the distribution that reduces the per-write
        // blob is the one CONDITIONED on a shard - not the global spread the test
        // above measures, which stays healthy even when bucketing does nothing.
        // Deriving the bucket from the unmixed shard hash makes the two
        // selections dependent: with equal counts, every consumer in shard N can
        // only land in bucket N, so the shard's whole map sits in one bucket. A
        // live deployment showed exactly that - one 1.08 MB bucket, seven empty.
        const int shards = 8;
        const int buckets = 8;
        const string tree = "tree-2014";

        var perShard = new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        for (var i = 0; i < 4096; i++)
        {
            var consumer = $"_lattice_materialiser_tree-2014_leaf-{i}";
            var shardKey = WalMaterialiserPinRouting.ShardKey(tree, consumer, shards);
            if (!perShard.TryGetValue(shardKey, out var seen))
            {
                seen = new HashSet<int>();
                perShard[shardKey] = seen;
            }

            seen.Add(WalMaterialiserPinRouting.BucketOf(consumer, buckets));
        }

        Assert.That(perShard, Has.Count.EqualTo(shards), "the sample must reach every shard");
        Assert.Multiple(() =>
        {
            foreach (var (shardKey, seen) in perShard)
            {
                Assert.That(seen, Has.Count.EqualTo(buckets),
                    $"shard '{shardKey}' must spread its consumers over every bucket; it used {seen.Count} of {buckets}");
            }
        });
    }

    [Test]
    public void BucketOf_is_not_a_function_of_the_shard_ordinal()
    {
        // Stated directly as a property, so the dependency cannot creep back in
        // under a different bucket count. With 4 buckets and 8 shards a correlated
        // hash makes bucket == shard % 4 for every consumer.
        var pairs = new HashSet<(string Shard, int Bucket)>();
        for (var i = 0; i < 2048; i++)
        {
            var consumer = $"_lattice_materialiser_tree-2014_leaf-{i}";
            pairs.Add((WalMaterialiserPinRouting.ShardKey("tree-2014", consumer, 8),
                WalMaterialiserPinRouting.BucketOf(consumer, 4)));
        }

        Assert.That(pairs, Has.Count.EqualTo(8 * 4),
            "every (shard, bucket) combination must be reachable; a correlated hash reaches only 8 of the 32");
    }

    [Test]
    public void EnumerateBucketStateNames_single_bucket_yields_only_the_legacy_slot()
    {
        Assert.That(
            WalMaterialiserPinRouting.EnumerateBucketStateNames(1),
            Is.EqualTo(new[] { WalMaterialiserPinState.StateName }),
            "the default layout must not issue extra durable reads on activation");
    }

    [Test]
    public void EnumerateBucketStateNames_multi_bucket_includes_every_bucket_and_the_legacy_slot()
    {
        var names = WalMaterialiserPinRouting.EnumerateBucketStateNames(4);

        Assert.Multiple(() =>
        {
            Assert.That(names, Has.Count.EqualTo(5), "four buckets plus the legacy slot");
            Assert.That(names, Is.Unique);
            for (var bucket = 0; bucket < 4; bucket++)
            {
                Assert.That(names, Does.Contain(WalMaterialiserPinRouting.BucketStateName(bucket)));
            }

            Assert.That(names, Does.Contain(WalMaterialiserPinState.StateName),
                "the legacy slot must always be read so a pin written before bucketing was enabled keeps counting toward the trim floor");
        });
    }
}
