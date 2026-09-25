using System.Collections.Immutable;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalKeyFilter"/>, the ownership a WAL replay read
/// pushes down to storage (issue #3565). The filter is only ever an
/// optimisation - storage drops what it excludes - so the property every test
/// here protects is that it excludes exactly the key-scoped records its owner
/// would reject, and never one it would keep.
/// </summary>
[TestFixture]
public sealed class WalKeyFilterTests
{
    private const int VirtualSlots = 64;
    private const int PhysicalShards = 4;
    private const int OwnedShard = 2;

    private static readonly ShardMap Map = ShardMap.CreateDefault(VirtualSlots, PhysicalShards);

    /// <summary>
    /// Keys spanning ASCII, the range bounds themselves, multi-byte UTF-8,
    /// surrogate pairs, a lone surrogate (which UTF-8 encoding replaces) and a
    /// key long enough to leave the stack-decode buffer.
    /// </summary>
    private static readonly string[] Keys =
    [
        string.Empty, "a", "l", "m", "m-owned", "mz", "n", "n-after", "z",
        "\u00FCber", "\u65E5\u672C", "m\uD83D\uDE00", "\uD800lone", new string('m', 300),
        .. Enumerable.Range(0, 64).Select(i => $"k{i:D3}"),
    ];

    private static readonly MutationKind[] KeyScopedKinds =
        [MutationKind.Set, MutationKind.Delete, MutationKind.Tombstone];

    [Test]
    public void Default_filter_is_unbounded_and_excludes_nothing()
    {
        var filter = default(WalKeyFilter);

        Assert.Multiple(() =>
        {
            Assert.That(filter.IsUnbounded, Is.True);
            Assert.That(filter.HasShardConstraint, Is.False);
            Assert.That(Keys.All(filter.Owns), Is.True, "An unbounded filter owns every key.");
            Assert.That(Keys.Any(k => filter.Excludes(MutationKind.Set, k)), Is.False);
        });
    }

    [Test]
    public void Range_constructor_owns_the_half_open_range_with_null_meaning_no_bound()
    {
        var bounded = new WalKeyFilter("m", "n");
        var openLow = new WalKeyFilter(null, "n");
        var openHigh = new WalKeyFilter("m", null);

        Assert.Multiple(() =>
        {
            Assert.That(bounded.IsUnbounded, Is.False);
            Assert.That(bounded.HasShardConstraint, Is.False);
            Assert.That(bounded.Owns("m"), Is.True, "The low bound is inclusive.");
            Assert.That(bounded.Owns("mz"), Is.True);
            Assert.That(bounded.Owns("n"), Is.False, "The high bound is exclusive.");
            Assert.That(bounded.Owns("l"), Is.False);
            Assert.That(openLow.Owns(string.Empty), Is.True);
            Assert.That(openLow.Owns("n"), Is.False);
            Assert.That(openHigh.Owns("z"), Is.True);
            Assert.That(openHigh.Owns("l"), Is.False);
        });
    }

    [Test]
    public void Shard_constructor_owns_exactly_the_keys_the_map_routes_to_the_shard()
    {
        var filter = new WalKeyFilter(null, null, Map, OwnedShard);

        Assert.Multiple(() =>
        {
            Assert.That(filter.HasShardConstraint, Is.True);
            Assert.That(filter.IsUnbounded, Is.False);
            Assert.That(filter.VirtualShardCount, Is.EqualTo(VirtualSlots));
            foreach (var key in Keys)
            {
                Assert.That(
                    filter.Owns(key),
                    Is.EqualTo(Map.Resolve(key) == OwnedShard),
                    $"Key '{key}' must be owned exactly when the map routes it to shard {OwnedShard}.");
            }
        });
    }

    [Test]
    public void Shard_and_range_axes_are_conjoined()
    {
        var filter = new WalKeyFilter("k010", "k040", Map, OwnedShard);

        Assert.Multiple(() =>
        {
            foreach (var key in Keys)
            {
                var expected = string.CompareOrdinal(key, "k010") >= 0
                    && string.CompareOrdinal(key, "k040") < 0
                    && Map.Resolve(key) == OwnedShard;
                Assert.That(filter.Owns(key), Is.EqualTo(expected), $"Key '{key}'.");
            }
        });
    }

    [Test]
    public void Shard_constructor_rejects_a_null_or_empty_map()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new WalKeyFilter(null, null, null!, 0), Throws.ArgumentNullException);
            Assert.That(
                () => new WalKeyFilter(null, null, new ShardMap { Slots = [] }, 0),
                Throws.ArgumentException.With.Message.Contains("no slots"));
        });
    }

    [Test]
    public void Shard_constructor_over_a_map_routing_every_slot_to_the_shard_carries_no_constraint()
    {
        // A single-shard tree's leaf owns every slot, so the shard axis can
        // exclude nothing and must not stop an unbounded leaf reading unfiltered.
        var singleShard = ShardMap.CreateDefault(VirtualSlots, 1);

        var unboundedRange = new WalKeyFilter(null, null, singleShard, 0);
        var boundedRange = new WalKeyFilter("k010", "k040", singleShard, 0);

        Assert.Multiple(() =>
        {
            Assert.That(unboundedRange.HasShardConstraint, Is.False);
            Assert.That(unboundedRange.IsUnbounded, Is.True);
            Assert.That(unboundedRange, Is.EqualTo(default(WalKeyFilter)));
            Assert.That(boundedRange, Is.EqualTo(new WalKeyFilter("k010", "k040")));
        });
    }

    [Test]
    public void Owns_rejects_a_null_key()
    {
        var filter = new WalKeyFilter("m", "n");

        Assert.That(() => filter.Owns(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Excludes_only_key_scoped_records_whose_key_is_not_owned()
    {
        var filter = new WalKeyFilter("m", "n");

        Assert.Multiple(() =>
        {
            foreach (var kind in KeyScopedKinds)
            {
                Assert.That(filter.Excludes(kind, "z"), Is.True, $"{kind} outside the range is excluded.");
                Assert.That(filter.Excludes(kind, "m1"), Is.False, $"{kind} inside the range is kept.");
            }

            // Ownership of these is not a property of one key, so they always
            // reach the reader.
            Assert.That(filter.Excludes(MutationKind.DeleteRange, "z"), Is.False);
            Assert.That(filter.Excludes(MutationKind.TxCommit, "z"), Is.False);
            Assert.That(filter.Excludes(MutationKind.TxAbort, "z"), Is.False);
            Assert.That(filter.Excludes((MutationKind)99, "z"), Is.False, "An unknown kind is never excluded.");
            Assert.That(filter.Excludes(MutationKind.Set, null), Is.False, "A record with no key is never excluded.");
        });
    }

    [Test]
    public void ExcludesUtf8_agrees_with_Excludes_over_the_decoded_key_for_every_key_shape()
    {
        // The pushed-down verdict is computed from the stored UTF-8 bytes, and
        // the reader's own judgement from the key those bytes decode to. They
        // must agree on every shape, including a lone surrogate that encoding
        // replaced and a key long enough to leave the stack buffer.
        WalKeyFilter[] filters =
        [
            new WalKeyFilter("m", "n"),
            new WalKeyFilter(null, null, Map, OwnedShard),
            new WalKeyFilter("k010", "k040", Map, OwnedShard),
            new WalKeyFilter("\u00FC", null),
            default,
        ];

        Assert.Multiple(() =>
        {
            foreach (var filter in filters)
            {
                foreach (var key in Keys)
                {
                    var bytes = Encoding.UTF8.GetBytes(key);
                    var decoded = Encoding.UTF8.GetString(bytes);
                    foreach (var kind in KeyScopedKinds.Append(MutationKind.DeleteRange))
                    {
                        Assert.That(
                            filter.ExcludesUtf8(kind, bytes),
                            Is.EqualTo(filter.Excludes(kind, decoded)),
                            $"{kind} '{key}' under {filter}.");
                    }
                }
            }
        });
    }

    [Test]
    public void A_malformed_shard_constraint_does_not_constrain_the_shard_axis()
    {
        // A bitmap that does not cover the slot count cannot prove anything
        // about a slot, so the filter must fall back to the range alone rather
        // than guess.
        var malformed = new WalKeyFilter("m", "n")
        {
            VirtualShardCount = 4096,
            OwnedSlots = ImmutableArray.Create(0UL),
        };

        Assert.Multiple(() =>
        {
            Assert.That(malformed.HasShardConstraint, Is.False);
            Assert.That(malformed.Owns("m1"), Is.True, "Only the range axis applies.");
            Assert.That(malformed.Owns("z"), Is.False);
        });
    }

    [Test]
    public void Equality_is_structural_over_bounds_and_owned_slots()
    {
        var a = new WalKeyFilter("m", "n", Map, OwnedShard);
        var b = new WalKeyFilter("m", "n", ShardMap.CreateDefault(VirtualSlots, PhysicalShards), OwnedShard);
        var otherShard = new WalKeyFilter("m", "n", Map, OwnedShard + 1);
        var otherRange = new WalKeyFilter("m", "o", Map, OwnedShard);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b), "Two filters built from equal maps own the same keys.");
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
            Assert.That(a, Is.Not.EqualTo(otherShard));
            Assert.That(a != otherRange, Is.True);
            Assert.That(new WalKeyFilter("m", "n"), Is.Not.EqualTo(a), "A shard constraint is part of the identity.");
            Assert.That(default(WalKeyFilter), Is.EqualTo(new WalKeyFilter(null, null)));
        });
    }

    [Test]
    public void A_filter_round_trips_through_the_Orleans_serializer()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalKeyFilter>>();
        WalKeyFilter[] filters =
        [
            default,
            new WalKeyFilter("m", null),
            new WalKeyFilter("m", "n", Map, OwnedShard),
        ];

        Assert.Multiple(() =>
        {
            foreach (var filter in filters)
            {
                var copy = serializer.Deserialize(serializer.SerializeToArray(filter));
                Assert.That(copy, Is.EqualTo(filter), $"{filter} must survive a cross-silo hop unchanged.");
                Assert.That(Keys.All(k => copy.Owns(k) == filter.Owns(k)), Is.True);
            }
        });
    }
}
