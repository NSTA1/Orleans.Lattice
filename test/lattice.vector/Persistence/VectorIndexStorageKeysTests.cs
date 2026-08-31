using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorIndexStorageKeysTests
{
    private const string Prefix = "vidx/";

    [Test]
    public void Manifest_and_build_state_sit_outside_every_generation()
    {
        var generation = VectorIndexStorageKeys.AllGenerationsPrefix(Prefix);

        Assert.Multiple(() =>
        {
            Assert.That(VectorIndexStorageKeys.Manifest(Prefix), Does.StartWith(Prefix));
            Assert.That(VectorIndexStorageKeys.Manifest(Prefix), Does.Not.StartWith(generation));
            Assert.That(VectorIndexStorageKeys.BuildState(Prefix), Does.Not.StartWith(generation));
            Assert.That(VectorIndexStorageKeys.RetirementPrefix(Prefix), Does.Not.StartWith(generation));
            Assert.That(VectorIndexStorageKeys.KeyMapPrefix(Prefix), Does.Not.StartWith(generation));
        });
    }

    [Test]
    public void Chunk_keys_sort_in_sequence_order()
    {
        var keys = new List<string>();
        for (var sequence = 0; sequence < 12; sequence++)
        {
            keys.Add(VectorIndexStorageKeys.VectorChunk(Prefix, 3, 5, 9, sequence));
        }

        var sorted = keys.Order(StringComparer.Ordinal).ToList();
        Assert.That(sorted, Is.EqualTo(keys), "Zero padding is what makes ordinal key order chunk order.");
    }

    [Test]
    public void Generation_and_epoch_keys_sort_numerically_past_a_digit_boundary()
    {
        var nine = VectorIndexStorageKeys.GenerationPrefix(Prefix, 9);
        var ten = VectorIndexStorageKeys.GenerationPrefix(Prefix, 10);

        Assert.That(string.CompareOrdinal(nine, ten), Is.LessThan(0));
    }

    [Test]
    public void A_partition_prefix_covers_only_that_partition()
    {
        var first = VectorIndexStorageKeys.PartitionVectorPrefix(Prefix, 1, 1);
        var eleventh = VectorIndexStorageKeys.VectorChunk(Prefix, 1, 11, 0, 0);

        Assert.That(eleventh, Does.Not.StartWith(first),
            "A fixed-width partition component stops partition 1's prefix from swallowing partition 11.");
    }

    [Test]
    public void An_epoch_prefix_covers_only_that_epoch()
    {
        var epochOne = VectorIndexStorageKeys.PartitionEpochPrefix(Prefix, 1, 2, 1);
        var epochEleven = VectorIndexStorageKeys.VectorChunk(Prefix, 1, 2, 11, 0);

        Assert.That(epochEleven, Does.Not.StartWith(epochOne));
    }

    [Test]
    public void Centroid_chunk_keys_live_under_the_generation_and_epoch_prefix()
    {
        var chunk = VectorIndexStorageKeys.CentroidChunk(Prefix, 2, 7, 3);

        Assert.Multiple(() =>
        {
            Assert.That(chunk, Does.StartWith(VectorIndexStorageKeys.CentroidPrefix(Prefix, 2, 7)));
            Assert.That(chunk, Does.StartWith(VectorIndexStorageKeys.GenerationPrefix(Prefix, 2)));
        });
    }

    [Test]
    public void Partition_state_keys_live_under_the_generation_state_prefix()
    {
        var state = VectorIndexStorageKeys.PartitionState(Prefix, 4, 17);

        Assert.That(state, Does.StartWith(VectorIndexStorageKeys.PartitionStatePrefix(Prefix, 4)));
    }

    [Test]
    public void A_retirement_key_round_trips_the_index_key_it_names()
    {
        foreach (var key in new[] { 0L, 1L, -1L, long.MaxValue, long.MinValue })
        {
            var encoded = VectorIndexStorageKeys.Retirement(Prefix, key);

            Assert.That(VectorIndexStorageKeys.TryReadRetirementKey(Prefix, encoded, out var decoded), Is.True);
            Assert.That(decoded, Is.EqualTo(key));
        }
    }

    [Test]
    public void A_key_from_another_range_is_not_read_as_a_retirement()
    {
        var other = VectorIndexStorageKeys.KeyMap(Prefix, "doc-1");

        Assert.That(VectorIndexStorageKeys.TryReadRetirementKey(Prefix, other, out _), Is.False);
    }

    [Test]
    public void A_malformed_retirement_suffix_is_refused_rather_than_guessed_at()
    {
        var malformed = VectorIndexStorageKeys.RetirementPrefix(Prefix) + "not-hex";

        Assert.That(VectorIndexStorageKeys.TryReadRetirementKey(Prefix, malformed, out _), Is.False);
    }

    [Test]
    public void A_key_map_key_round_trips_the_identifier_it_names()
    {
        foreach (var id in new[] { "doc-1", "a/b/c.cs", "with spaces", "unicode-\u00e9\u4e2d" })
        {
            var encoded = VectorIndexStorageKeys.KeyMap(Prefix, id);

            Assert.That(VectorIndexStorageKeys.TryReadKeyMapId(Prefix, encoded, out var decoded), Is.True);
            Assert.That(decoded, Is.EqualTo(id));
        }
    }

    [Test]
    public void A_key_outside_the_mapping_range_is_not_read_as_a_mapping()
    {
        Assert.That(
            VectorIndexStorageKeys.TryReadKeyMapId(Prefix, VectorIndexStorageKeys.Manifest(Prefix), out _),
            Is.False);
    }

    [Test]
    public void The_key_watermark_sits_outside_the_mapping_scan_range()
    {
        Assert.That(
            VectorIndexStorageKeys.KeyWatermark(Prefix),
            Does.Not.StartWith(VectorIndexStorageKeys.KeyMapPrefix(Prefix)),
            "A watermark caught by the mapping scan would be decoded as an identifier.");
    }

    [Test]
    public void A_null_prefix_is_refused_by_every_key_builder()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => VectorIndexStorageKeys.Manifest(null!), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.BuildState(null!), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.RetirementPrefix(null!), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.Retirement(null!, 1), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.KeyWatermark(null!), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.KeyMapPrefix(null!), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.KeyMap(null!, "a"), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.KeyMap(Prefix, null!), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.GenerationPrefix(null!, 0), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.AllGenerationsPrefix(null!), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.TryReadKeyMapId(Prefix, null!, out _), Throws.ArgumentNullException);
            Assert.That(() => VectorIndexStorageKeys.TryReadRetirementKey(Prefix, null!, out _), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_negative_component_is_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => VectorIndexStorageKeys.GenerationPrefix(Prefix, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => VectorIndexStorageKeys.CentroidPrefix(Prefix, 0, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => VectorIndexStorageKeys.CentroidChunk(Prefix, 0, 0, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => VectorIndexStorageKeys.PartitionState(Prefix, 0, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => VectorIndexStorageKeys.PartitionVectorPrefix(Prefix, 0, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => VectorIndexStorageKeys.PartitionEpochPrefix(Prefix, 0, 0, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => VectorIndexStorageKeys.VectorChunk(Prefix, 0, 0, 0, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => VectorIndexStorageKeys.PartitionStatePrefix(Prefix, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void The_padding_widths_cover_the_ranges_they_are_used_for()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                VectorIndexStorageKeys.PartitionWidth,
                Is.GreaterThanOrEqualTo(VectorIndexOptions.MaximumPartitionCount.ToString().Length),
                "A partition identifier must never overflow its fixed-width component.");
            Assert.That(
                VectorIndexStorageKeys.CounterWidth,
                Is.GreaterThanOrEqualTo(long.MaxValue.ToString().Length));
        });
    }
}
