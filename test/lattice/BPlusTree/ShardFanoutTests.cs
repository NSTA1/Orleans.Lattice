using System.Collections.Generic;
using System.Linq;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins <see cref="ShardFanout"/> against the hashed <c>Dictionary&lt;int, List&lt;T&gt;&gt;</c>
/// bucketing it replaces, across the map geometries the batch read and write
/// paths actually see plus the pathological ones that must fall back.
/// </summary>
/// <remarks>
/// The optimisation is only safe because
/// <see cref="ShardMap.GetPhysicalShardIndices"/> returns distinct ascending
/// indices and every value <see cref="ShardMap.Resolve"/> can return is drawn
/// from that set, so the last element bounds the dense array exactly. These
/// tests assert that invariant holds for hand-constructed maps as well as
/// generated ones, and that the fallback triggers rather than throwing when it
/// does not.
/// </remarks>
public class ShardFanoutTests
{
    private static Dictionary<int, List<string>> BucketKeysHashed(
        IReadOnlyList<string> keys, ShardMap map, int bucketCapacity)
    {
        var buckets = new Dictionary<int, List<string>>();
        foreach (var key in keys)
        {
            var idx = map.Resolve(key);
            if (!buckets.TryGetValue(idx, out var bucket))
            {
                bucket = new List<string>(bucketCapacity);
                buckets[idx] = bucket;
            }

            bucket.Add(key);
        }

        return buckets;
    }

    private static Dictionary<int, List<KeyValuePair<string, byte[]>>> BucketEntriesHashed(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, ShardMap map, int bucketCapacity)
    {
        var buckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        foreach (var entry in entries)
        {
            var idx = map.Resolve(entry.Key);
            if (!buckets.TryGetValue(idx, out var bucket))
            {
                bucket = new List<KeyValuePair<string, byte[]>>(bucketCapacity);
                buckets[idx] = bucket;
            }

            bucket.Add(entry);
        }

        return buckets;
    }

    private static string[] SampleKeys(int count) =>
        Enumerable.Range(0, count).Select(i => $"tenant-a/orders/{i:D6}").ToArray();

    [TestCase(4096, 1)]
    [TestCase(4096, 2)]
    [TestCase(4096, 8)]
    [TestCase(4096, 16)]
    [TestCase(64, 7)]
    [TestCase(1, 1)]
    public void BucketKeys_matches_hashed_bucketing_for_default_maps(
        int virtualShards, int physicalShards)
    {
        var map = ShardMap.CreateDefault(virtualShards, physicalShards);
        var indices = map.GetPhysicalShardIndices();
        var keys = SampleKeys(500);

        var expected = BucketKeysHashed(keys, map, bucketCapacity: 4);
        var actual = ShardFanout.BucketKeys(keys, map, indices, bucketCapacity: 4);

        AssertSameGrouping(expected, actual);
    }

    [Test]
    public void BucketEntries_matches_hashed_bucketing()
    {
        var map = ShardMap.CreateDefault(4096, 8);
        var indices = map.GetPhysicalShardIndices();
        var entries = SampleKeys(500)
            .Select(k => new KeyValuePair<string, byte[]>(k, [1, 2, 3]))
            .ToArray();

        var expected = BucketEntriesHashed(entries, map, bucketCapacity: 4);
        var actual = ShardFanout.BucketEntries(entries, map, indices, bucketCapacity: 4);

        Assert.That(actual.Count, Is.EqualTo(expected.Count));
        foreach (var (shardIndex, bucket) in actual)
        {
            Assert.That(expected.ContainsKey(shardIndex), Is.True, $"unexpected shard {shardIndex}");
            Assert.That(bucket.Select(e => e.Key), Is.EqualTo(expected[shardIndex].Select(e => e.Key)));
        }
    }

    [Test]
    public void BucketKeys_handles_a_sparse_hand_built_map()
    {
        // A hand-built map whose owner values are far above the slot count -
        // the shape ShardMap.GetPhysicalShardIndices and
        // LatticeGrain.BuildOwnedSlotMap already guard for.
        var map = new ShardMap { Slots = [9, 9, 3, 9] };
        var indices = map.GetPhysicalShardIndices();
        var keys = SampleKeys(200);

        var expected = BucketKeysHashed(keys, map, bucketCapacity: 4);
        var actual = ShardFanout.BucketKeys(keys, map, indices, bucketCapacity: 4);

        AssertSameGrouping(expected, actual);
    }

    [Test]
    public void BucketKeys_falls_back_when_an_owner_index_is_negative()
    {
        // TryGetDenseLength must refuse a negative index rather than index out
        // of an array; the hashed path still answers correctly.
        Assert.That(ShardFanout.TryGetDenseLength([-1, 0, 2], out _), Is.False);
    }

    // --- The hashed (sparse) fallback path ---
    //
    // The dense array is only safe when the physical index domain is small and
    // non-negative. When it is not, BucketKeys/BucketEntries must fall back to the
    // hashed Dictionary they replaced and still produce byte-identical grouping,
    // ordering, and enumeration - otherwise a pathological map would silently
    // mis-route a batch instead of merely running slower.

    [Test]
    public void BucketKeys_falls_back_to_hashed_bucketing_for_a_negative_index_domain()
    {
        var map = ShardMap.CreateDefault(4096, 8);
        var keys = SampleKeys(500);

        // A negative entry forces TryGetDenseLength to refuse, so the hashed
        // fallback runs while the map itself still resolves normally.
        var expected = BucketKeysHashed(keys, map, bucketCapacity: 4);
        var actual = ShardFanout.BucketKeys(keys, map, [-1, .. map.GetPhysicalShardIndices()], bucketCapacity: 4);

        AssertSameGrouping(expected, actual);
        Assert.That(actual.Count, Is.GreaterThan(1),
            "The fixture must actually spread over several shards, or the fallback proves nothing.");
    }

    [Test]
    public void BucketKeys_falls_back_to_hashed_bucketing_for_a_pathologically_large_index_domain()
    {
        var map = ShardMap.CreateDefault(4096, 4);
        var keys = SampleKeys(300);

        var expected = BucketKeysHashed(keys, map, bucketCapacity: 4);
        var actual = ShardFanout.BucketKeys(
            keys, map, [.. map.GetPhysicalShardIndices(), ShardFanout.DenseOwnerLimit], bucketCapacity: 4);

        AssertSameGrouping(expected, actual);
    }

    [Test]
    public void BucketEntries_falls_back_to_hashed_bucketing_for_an_unsuitable_index_domain()
    {
        var map = ShardMap.CreateDefault(4096, 8);
        var entries = SampleKeys(400)
            .Select(k => new KeyValuePair<string, byte[]>(k, [1, 2, 3]))
            .ToArray();

        var expected = BucketEntriesHashed(entries, map, bucketCapacity: 4);
        var actual = ShardFanout.BucketEntries(
            entries, map, [-1, .. map.GetPhysicalShardIndices()], bucketCapacity: 4);

        Assert.That(actual.Count, Is.EqualTo(expected.Count));
        var seen = new HashSet<int>();
        foreach (var (shardIndex, bucket) in actual)
        {
            Assert.That(seen.Add(shardIndex), Is.True, $"shard {shardIndex} enumerated twice");
            Assert.That(expected.ContainsKey(shardIndex), Is.True, $"unexpected shard {shardIndex}");

            // Order within a bucket matters: the batch paths stitch per-shard
            // results back by position.
            Assert.That(bucket.Select(e => e.Key).ToArray(),
                Is.EqualTo(expected[shardIndex].Select(e => e.Key).ToArray()));
            Assert.That(bucket.Select(e => e.Value).ToArray(),
                Is.EqualTo(expected[shardIndex].Select(e => e.Value).ToArray()));
        }

        Assert.That(seen, Is.EquivalentTo(expected.Keys));
    }

    [Test]
    public void The_hashed_fallback_enumerates_every_bucket_exactly_once_and_then_stops()
    {
        // Pins the sparse enumerator itself: it must surface each (shardIndex,
        // bucket) pair once, and MoveNext must stay false once drained rather
        // than restarting or throwing.
        var map = ShardMap.CreateDefault(4096, 3);
        var keys = SampleKeys(60);
        var buckets = ShardFanout.BucketKeys(keys, map, [-1, 0, 1, 2], bucketCapacity: 4);

        var enumerator = buckets.GetEnumerator();
        var observed = new List<int>();
        var total = 0;
        while (enumerator.MoveNext())
        {
            observed.Add(enumerator.Current.ShardIndex);
            total += enumerator.Current.Bucket.Count;
        }

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.Unique);
            Assert.That(observed, Has.Count.EqualTo(buckets.Count));
            Assert.That(total, Is.EqualTo(keys.Length),
                "Every key must land in exactly one bucket on the fallback path too.");
            Assert.That(enumerator.MoveNext(), Is.False,
                "A drained sparse enumerator must stay drained.");
        });
    }

    [Test]
    public void The_hashed_fallback_over_an_empty_batch_yields_no_buckets()
    {
        var map = ShardMap.CreateDefault(4096, 8);
        var buckets = ShardFanout.BucketKeys([], map, [-1, 0, 1], bucketCapacity: 4);

        Assert.That(buckets.Count, Is.Zero);

        var enumerated = 0;
        foreach (var _ in buckets)
            enumerated++;

        Assert.That(enumerated, Is.Zero);
    }

    [Test]
    public void The_hashed_fallback_over_an_empty_entry_batch_yields_no_buckets()
    {
        var map = ShardMap.CreateDefault(4096, 8);
        var buckets = ShardFanout.BucketEntries([], map, [-1, 0, 1], bucketCapacity: 4);

        Assert.That(buckets.Count, Is.Zero);

        var enumerated = 0;
        foreach (var _ in buckets)
            enumerated++;

        Assert.That(enumerated, Is.Zero);
    }

    [Test]
    public void TryGetDenseLength_refuses_a_pathologically_large_index()
    {
        Assert.That(
            ShardFanout.TryGetDenseLength([0, ShardFanout.DenseOwnerLimit], out _),
            Is.False);
    }

    [Test]
    public void TryGetDenseLength_sizes_to_the_largest_index_plus_one()
    {
        Assert.That(ShardFanout.TryGetDenseLength([0, 3, 7], out var length), Is.True);
        Assert.That(length, Is.EqualTo(8));
    }

    [Test]
    public void TryGetDenseLength_refuses_an_empty_index_set()
    {
        Assert.That(ShardFanout.TryGetDenseLength([], out _), Is.False);
    }

    [Test]
    public void BucketKeys_over_an_empty_batch_yields_no_buckets()
    {
        var map = ShardMap.CreateDefault(4096, 8);
        var buckets = ShardFanout.BucketKeys(
            [], map, map.GetPhysicalShardIndices(), bucketCapacity: 4);

        Assert.That(buckets.Count, Is.Zero);

        var enumerated = 0;
        foreach (var _ in buckets)
            enumerated++;

        Assert.That(enumerated, Is.Zero);
    }

    [Test]
    public void A_default_constructed_bucket_set_enumerates_empty()
    {
        // The batch paths declare the bucket set before deciding whether the
        // batch is worth fanning out, so the default value must be safe to
        // enumerate rather than throwing.
        ShardFanoutBuckets<string> buckets = default;

        Assert.That(buckets.Count, Is.Zero);

        var enumerated = 0;
        foreach (var _ in buckets)
            enumerated++;

        Assert.That(enumerated, Is.Zero);
    }

    [Test]
    public void BucketCapacity_is_shard_fair_and_bounded()
    {
        // A 1000-key batch over 8 shards reserves the fair share, not the
        // whole batch per shard, and never less than a small floor.
        Assert.That(ShardFanout.BucketCapacity(1000, 8), Is.EqualTo(125));
        Assert.That(ShardFanout.BucketCapacity(1, 8), Is.EqualTo(4));
        Assert.That(ShardFanout.BucketCapacity(0, 8), Is.EqualTo(4));
        Assert.That(ShardFanout.BucketCapacity(1_000_000, 1), Is.EqualTo(256));
        Assert.That(ShardFanout.BucketCapacity(1000, 0), Is.EqualTo(256));
    }

    private static void AssertSameGrouping(
        Dictionary<int, List<string>> expected, ShardFanoutBuckets<string> actual)
    {
        Assert.That(actual.Count, Is.EqualTo(expected.Count));

        var seen = new HashSet<int>();
        foreach (var (shardIndex, bucket) in actual)
        {
            Assert.That(seen.Add(shardIndex), Is.True, $"shard {shardIndex} enumerated twice");
            Assert.That(expected.ContainsKey(shardIndex), Is.True, $"unexpected shard {shardIndex}");

            // Order within a bucket must match too: the batch paths stitch
            // per-shard results back by position.
            Assert.That(bucket, Is.EqualTo(expected[shardIndex]));
        }

        Assert.That(seen, Is.EquivalentTo(expected.Keys));
    }
}
