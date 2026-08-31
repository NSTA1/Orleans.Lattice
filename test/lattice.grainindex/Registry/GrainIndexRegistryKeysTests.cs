using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexRegistryKeys"/>: the key layout that lets one
/// tree hold the persisted definitions, the activation-path seen markers, and
/// the backfill checkpoints without any of the three colliding.
/// </summary>
[TestFixture]
public sealed class GrainIndexRegistryKeysTests
{
    [Test]
    public void A_definition_key_is_the_definition_segment_plus_the_index_name()
    {
        Assert.That(
            GrainIndexRegistryKeys.Definition("users"),
            Is.EqualTo(GrainIndexRegistryKeys.DefinitionSegment + "users"));
    }

    [Test]
    public void A_seen_key_nests_the_encoded_grain_key_under_the_index()
    {
        Assert.That(
            GrainIndexRegistryKeys.Seen("users", "alice"),
            Is.EqualTo(GrainIndexRegistryKeys.SeenSegment + "users/alice"));
    }

    [Test]
    public void A_checkpoint_key_is_the_checkpoint_segment_plus_the_index_name()
    {
        Assert.That(
            GrainIndexRegistryKeys.Checkpoint("users"),
            Is.EqualTo(GrainIndexRegistryKeys.CheckpointSegment + "users"));
    }

    [Test]
    public void The_three_segments_are_distinct_so_no_kind_can_prefix_match_another()
    {
        var segments = new[]
        {
            GrainIndexRegistryKeys.DefinitionSegment,
            GrainIndexRegistryKeys.SeenSegment,
            GrainIndexRegistryKeys.CheckpointSegment,
        };

        Assert.Multiple(() =>
        {
            Assert.That(segments, Is.Unique);
            foreach (var outer in segments)
            {
                foreach (var inner in segments.Where(s => !string.Equals(s, outer, StringComparison.Ordinal)))
                {
                    Assert.That(outer.StartsWith(inner, StringComparison.Ordinal), Is.False,
                        $"'{outer}' must not begin with '{inner}', or a scan of one kind would "
                        + "sweep up the other.");
                }
            }
        });
    }

    [Test]
    public void A_key_of_one_kind_never_falls_inside_another_kinds_scan_range()
    {
        var definition = GrainIndexRegistryKeys.Definition("users");
        var seen = GrainIndexRegistryKeys.Seen("users", "alice");
        var checkpoint = GrainIndexRegistryKeys.Checkpoint("users");

        Assert.Multiple(() =>
        {
            Assert.That(InDefinitionRange(definition), Is.True);
            Assert.That(InDefinitionRange(seen), Is.False);
            Assert.That(InDefinitionRange(checkpoint), Is.False);
            Assert.That(InSeenRange("users", seen), Is.True);
            Assert.That(InSeenRange("users", definition), Is.False);
            Assert.That(InSeenRange("users", checkpoint), Is.False);
        });
    }

    [Test]
    public void A_seen_scan_range_excludes_a_sibling_index_whose_name_is_a_prefix()
    {
        // 'user' is a prefix of 'users', so a naive prefix scan would sweep up
        // the sibling's markers. The trailing separator is what stops it.
        var sibling = GrainIndexRegistryKeys.Seen("users", "alice");

        Assert.That(InSeenRange("user", sibling), Is.False,
            "One index's seen-marker scan must never return another index's markers.");
    }

    [Test]
    public void A_definition_scan_range_covers_every_declared_index()
    {
        Assert.Multiple(() =>
        {
            Assert.That(InDefinitionRange(GrainIndexRegistryKeys.Definition("a")), Is.True);
            Assert.That(InDefinitionRange(GrainIndexRegistryKeys.Definition("zzzz")), Is.True);
            Assert.That(InDefinitionRange(GrainIndexRegistryKeys.Definition(string.Empty)), Is.True);
        });
    }

    [Test]
    public void Scan_bounds_are_ordered_low_to_high()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                string.CompareOrdinal(
                    GrainIndexRegistryKeys.DefinitionPrefix(),
                    GrainIndexRegistryKeys.DefinitionPrefixEnd()),
                Is.LessThan(0));
            Assert.That(
                string.CompareOrdinal(
                    GrainIndexRegistryKeys.SeenPrefix("users"),
                    GrainIndexRegistryKeys.SeenPrefixEnd("users")),
                Is.LessThan(0),
                "A half-open range whose end did not sort above its start would scan nothing.");
        });
    }

    [Test]
    public void The_seen_scan_end_bound_increments_only_the_final_character()
    {
        var start = GrainIndexRegistryKeys.SeenPrefix("users");
        var end = GrainIndexRegistryKeys.SeenPrefixEnd("users");

        Assert.Multiple(() =>
        {
            Assert.That(end, Has.Length.EqualTo(start.Length));
            Assert.That(end[..^1], Is.EqualTo(start[..^1]));
            Assert.That(end[^1], Is.EqualTo((char)(start[^1] + 1)));
        });
    }

    [Test]
    public void Every_builder_rejects_a_null_index_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => GrainIndexRegistryKeys.Definition(null!), Throws.ArgumentNullException);
            Assert.That(() => GrainIndexRegistryKeys.Seen(null!, "alice"), Throws.ArgumentNullException);
            Assert.That(() => GrainIndexRegistryKeys.SeenPrefix(null!), Throws.ArgumentNullException);
            Assert.That(() => GrainIndexRegistryKeys.SeenPrefixEnd(null!), Throws.ArgumentNullException);
            Assert.That(() => GrainIndexRegistryKeys.Checkpoint(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_seen_key_rejects_a_null_encoded_grain_key()
    {
        Assert.That(
            () => GrainIndexRegistryKeys.Seen("users", null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void An_empty_encoded_grain_key_still_produces_a_key_inside_the_index_range()
    {
        var key = GrainIndexRegistryKeys.Seen("users", string.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(key, Is.EqualTo(GrainIndexRegistryKeys.SeenPrefix("users")));
            Assert.That(InSeenRange("users", key), Is.True,
                "An empty grain key is legal in Orleans, so its marker must still be scannable.");
        });
    }

    private static bool InDefinitionRange(string key) =>
        string.CompareOrdinal(key, GrainIndexRegistryKeys.DefinitionPrefix()) >= 0
        && string.CompareOrdinal(key, GrainIndexRegistryKeys.DefinitionPrefixEnd()) < 0;

    private static bool InSeenRange(string indexName, string key) =>
        string.CompareOrdinal(key, GrainIndexRegistryKeys.SeenPrefix(indexName)) >= 0
        && string.CompareOrdinal(key, GrainIndexRegistryKeys.SeenPrefixEnd(indexName)) < 0;
}
