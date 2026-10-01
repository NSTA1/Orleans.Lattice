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
        Assert.That(a, Does.StartWith("tree-1~s"));
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
            valid.Add($"{Tree}~s{s}");
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
        Assert.That(keys, Is.EquivalentTo(new[]{"tree-1~s0","tree-1~s1","tree-1~s2","tree-1#s0","tree-1#s1","tree-1#s2","tree-1"}),
            "the GC must read both separators so a pin written by an earlier build still holds the trim floor");
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

    [Test]
    public void AuthoritativeKeyIndex_points_at_the_key_a_write_would_land_on()
    {
        // The WAL GC classifies a read result by INDEX rather than by composing
        // and comparing a key string per consumer per shard, so the two must
        // agree exactly. If EnumerateReadKeys ever reordered its output - putting
        // the legacy keys first, say - the index would silently address a stale
        // shape and the GC would treat a stranded pin as authoritative, which is
        // the defect in issue #2433 with the sign flipped. Nothing else observes
        // the ordering, so only this assertion catches it.
        foreach (var shards in new[] { 2, 3, 6, 8 })
        {
            var keys = WalMaterialiserPinRouting.EnumerateReadKeys(Tree, shards);
            for (var i = 0; i < 100; i++)
            {
                var consumerId = $"_lattice_materialiser_tree-1_leaf-{i}";
                var index = WalMaterialiserPinRouting.AuthoritativeKeyIndex(consumerId, shards);

                Assert.That(index, Is.InRange(0, keys.Count - 1));
                Assert.That(
                    keys[index],
                    Is.EqualTo(WalMaterialiserPinRouting.ShardKey(Tree, consumerId, shards)),
                    $"Read-key index {index} at {shards} shards must be consumer '{consumerId}'s write key.");
            }
        }
    }

    [Test]
    public void AuthoritativeKeyIndex_is_always_a_current_separator_shard()
    {
        // The GC skips the authoritative test entirely for indices at or above
        // the shard count, on the strength of the legacy and bare-tree keys
        // occupying that tail. That is an optimisation resting on a layout claim,
        // so the claim is asserted rather than assumed.
        const int shards = 8;
        for (var i = 0; i < 200; i++)
        {
            var index = WalMaterialiserPinRouting.AuthoritativeKeyIndex($"consumer-{i}", shards);
            Assert.That(index, Is.LessThan(shards));
        }
    }

    // ----- Storage safety and the self-healing separator migration -----

    [Test]
    public void A_composed_shard_key_is_storage_safe()
    {
        // The pin grain is persistent, so its key reaches the Partition/Row key
        // columns and the request URL of a keyed storage backend, which reject
        // these characters. The composer must not introduce one.
        var key = WalMaterialiserPinRouting.ShardKey("tree-1", "consumer-a", shardCount: 8);

        Assert.Multiple(() =>
        {
            Assert.That(key.IndexOfAny(['/', '\\', '#', '?']), Is.LessThan(0));
            Assert.That(key.Any(char.IsControl), Is.False);
        });
    }

    [Test]
    public void A_pin_written_under_the_legacy_separator_is_still_read()
    {
        // The migration is self-healing precisely because the GC keeps reading the
        // old key: an existing pin continues to hold the WAL trim floor with no
        // operator action, so upgrading strands no WAL segment.
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(Tree, shardCount: 4);

        for (var shard = 0; shard < 4; shard++)
        {
            Assert.That(keys, Does.Contain($"{Tree}{WalMaterialiserPinRouting.LegacyShardSeparator}{shard}"));
        }
    }

    [Test]
    public void The_legacy_separator_is_never_written()
    {
        for (var i = 0; i < 50; i++)
        {
            var key = WalMaterialiserPinRouting.ShardKey(Tree, $"consumer-{i}", shardCount: 8);
            Assert.That(key, Does.Not.Contain(WalMaterialiserPinRouting.LegacyShardSeparator));
        }
    }

    [TestCase("tree-1~s3", "tree-1")]
    [TestCase("tree-1#s3", "tree-1")]
    [TestCase("tree-1", "tree-1")]
    [TestCase("t/acme/orders~s2", "t/acme/orders")]
    [TestCase("t/acme/orders", "t/acme/orders")]
    public void TreeNameFromKey_strips_either_separator(string key, string expected)
        => Assert.That(WalMaterialiserPinRouting.TreeNameFromKey(key), Is.EqualTo(expected));

    [TestCase("tree~sname")]
    [TestCase("tree#sname")]
    public void TreeNameFromKey_does_not_truncate_a_non_numeric_suffix(string key)
        => Assert.That(
            WalMaterialiserPinRouting.TreeNameFromKey(key),
            Is.EqualTo(key),
            "only a genuine all-digit shard suffix is a suffix; anything else belongs to the tree name");

    [Test]
    public void TreeNameFromKey_anchors_on_the_last_separator()
        => Assert.That(
            WalMaterialiserPinRouting.TreeNameFromKey("tree~s1~s2"),
            Is.EqualTo("tree~s1"),
            "the suffix is appended, so an earlier occurrence belongs to the tree name");

    // ----------------------------------------------------------------------
    // Degenerate and boundary inputs. Every arm below is one the WAL GC can
    // reach from stored state rather than from a call site under this file's
    // control: a key read back from storage can be absent or malformed, and a
    // consumer id is an arbitrary string. They were all cold.
    // ----------------------------------------------------------------------

    [TestCase(1)]
    [TestCase(0)]
    [TestCase(-3)]
    public void AuthoritativeKeyIndex_is_zero_when_there_is_only_one_key(int shardCount)
        => Assert.That(
            WalMaterialiserPinRouting.AuthoritativeKeyIndex("consumer-a", shardCount),
            Is.Zero,
            "an unsharded tree enumerates only the legacy key, so index 0 is the only valid answer");

    [Test]
    public void AuthoritativeKeyIndex_agrees_with_EnumerateReadKeys_when_unsharded()
    {
        // The same coupling the sharded case already pins, asserted on the arm
        // that returns the constant: the index must address the key a write
        // would actually land on, or the WAL GC classifies a live pin as a
        // stranded duplicate.
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(Tree, 1);
        var index = WalMaterialiserPinRouting.AuthoritativeKeyIndex("consumer-a", 1);

        Assert.That(keys[index], Is.EqualTo(WalMaterialiserPinRouting.ShardKey(Tree, "consumer-a", 1)));
    }

    [TestCase(null)]
    [TestCase("")]
    public void TreeNameFromKey_is_empty_for_an_absent_key(string? key)
        => Assert.That(
            WalMaterialiserPinRouting.TreeNameFromKey(key),
            Is.Empty,
            "a null or empty row key must degrade to the empty tree name rather than throw inside a GC sweep");

    [TestCase(null)]
    [TestCase("")]
    public void ShardIndexFromKey_is_zero_for_an_absent_key(string? key)
        => Assert.That(
            WalMaterialiserPinRouting.ShardIndexFromKey(key),
            Is.Zero,
            "per-shard attribution must fall back to shard 0 rather than throw on a malformed key");

    [TestCase("tree-1~s3", 3)]
    [TestCase("tree-1#s3", 3)]
    [TestCase("tree-1~s0", 0)]
    [TestCase("tree-1~s12", 12)]
    [TestCase("tree-1", 0)]
    [TestCase("tree~sname", 0)]
    public void ShardIndexFromKey_parses_either_separator(string key, int expected)
        => Assert.That(WalMaterialiserPinRouting.ShardIndexFromKey(key), Is.EqualTo(expected));

    [TestCase("tree-1~s")]
    [TestCase("tree-1#s")]
    public void A_key_whose_shard_suffix_is_empty_is_not_a_sharded_key(string key)
    {
        // The separator is present but nothing follows it. An empty span is not
        // "all digits", so the whole string is the tree name and the ordinal is
        // zero - the alternative would silently attribute the pin to shard 0 of
        // a truncated tree.
        Assert.That(WalMaterialiserPinRouting.TreeNameFromKey(key), Is.EqualTo(key));
        Assert.That(WalMaterialiserPinRouting.ShardIndexFromKey(key), Is.Zero);
    }

    [Test]
    public void A_key_whose_shard_suffix_is_only_partly_numeric_is_not_a_sharded_key()
    {
        Assert.That(WalMaterialiserPinRouting.TreeNameFromKey("tree-1~s1a"), Is.EqualTo("tree-1~s1a"));
        Assert.That(WalMaterialiserPinRouting.TreeNameFromKey("tree-1~sa1"), Is.EqualTo("tree-1~sa1"));
    }

    // ----------------------------------------------------------------------
    // StableHash transcode paths. The hash picks one of three bodies by the
    // string's UTF-8 size, and only the short stack path had ever run. The
    // other two produce the routing for any consumer id past ~84 characters,
    // which is the normal shape here ("_lattice_materialiser_<tree>_<leaf>"),
    // so a divergence between them would silently re-route live pins.
    // ----------------------------------------------------------------------

    [Test]
    public void StableHash_of_the_empty_string_is_the_FNV_offset_basis()
        => Assert.That(
            WalMaterialiserPinRouting.StableHash(string.Empty),
            Is.EqualTo(2166136261u),
            "an empty value must fold nothing and return the basis unchanged");

    [Test]
    public void StableHash_is_identical_across_all_three_transcode_paths()
    {
        // GetMaxByteCount(n) is 3n + 3, so an ASCII string of 85 characters is
        // the first that cannot use the tight stack buffer while its true byte
        // count still fits the 256-byte budget; past 256 characters it must
        // rent. Folding the same bytes through three different buffers has to
        // give the same hash, because the shard a consumer routes to is that
        // hash modulo the shard count.
        foreach (var length in new[] { 1, 84, 85, 100, 256, 257, 1024 })
        {
            var value = new string('a', length);
            var expected = ReferenceFnv1a(value);

            Assert.That(
                WalMaterialiserPinRouting.StableHash(value),
                Is.EqualTo(expected),
                $"the transcode path chosen for a {length}-character value must not change the hash");
        }
    }

    [Test]
    public void StableHash_is_stable_for_a_long_multibyte_consumer_id()
    {
        // A multibyte string reaches the rented path at a quarter of the
        // character count an ASCII one does, so it exercises the same body with
        // a byte count that is not the character count.
        var value = string.Concat(Enumerable.Repeat("\u00e9\u00e8\u00ea", 120));

        Assert.That(value.Length, Is.EqualTo(360));
        Assert.That(
            WalMaterialiserPinRouting.StableHash(value),
            Is.EqualTo(ReferenceFnv1a(value)));
    }

    [Test]
    public void A_long_consumer_id_still_routes_into_range_and_stably()
    {
        const int shards = 8;
        var consumer = "_lattice_materialiser_" + new string('t', 300) + "_leaf-7";

        var first = WalMaterialiserPinRouting.ShardKey(Tree, consumer, shards);
        var second = WalMaterialiserPinRouting.ShardKey(Tree, consumer, shards);

        Assert.That(first, Is.EqualTo(second));
        Assert.That(
            WalMaterialiserPinRouting.EnumerateReadKeys(Tree, shards),
            Does.Contain(first));
    }

    /// <summary>
    /// An independent FNV-1a 32-bit fold over the value's UTF-8 bytes, written
    /// without any of the buffer selection the subject performs, so the three
    /// paths are compared against the definition rather than against each
    /// other.
    /// </summary>
    private static uint ReferenceFnv1a(string value)
    {
        var hash = 2166136261u;
        foreach (var b in System.Text.Encoding.UTF8.GetBytes(value))
        {
            hash ^= b;
            hash *= 16777619u;
        }

        return hash;
    }
}
