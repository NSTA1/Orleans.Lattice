namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="SchemaDeadLetterKey"/>: the time-ordered composite
/// key encoding and the prefix-range bounds used by the dead-letter store.
/// </summary>
public class SchemaDeadLetterKeyTests
{
    private const char Sep = '\u001f';

    [Test]
    public void Encode_produces_separated_composite_key()
    {
        var ts = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var key = SchemaDeadLetterKey.Encode("orders", ts, "k1", "abcd1234");

        Assert.That(key, Does.StartWith($"orders{Sep}"));
        Assert.That(key, Does.EndWith($"{Sep}k1{Sep}abcd1234"));
    }

    [Test]
    public void Encode_pads_ticks_to_nineteen_digits_for_lexical_time_order()
    {
        var early = SchemaDeadLetterKey.Encode("t", DateTimeOffset.UnixEpoch, "k", "u");
        var late = SchemaDeadLetterKey.Encode("t", DateTimeOffset.UnixEpoch.AddTicks(1), "k", "u");

        // Lexical comparison must agree with time order.
        Assert.That(string.CompareOrdinal(early, late), Is.LessThan(0));
    }

    [Test]
    public void IdentitySuffix_is_stable_per_source_write_and_distinct_across_writes()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 1_000, Counter = 2 };
        var suffix = SchemaDeadLetterKey.IdentitySuffix("peer", hlc);

        Assert.Multiple(() =>
        {
            Assert.That(suffix, Has.Length.EqualTo(SchemaDeadLetterKey.UniqueSuffixLength));
            Assert.That(suffix, Does.Match("^[0-9a-f]{8}$"));
            Assert.That(SchemaDeadLetterKey.IdentitySuffix("peer", hlc), Is.EqualTo(suffix));
            Assert.That(SchemaDeadLetterKey.IdentitySuffix("other", hlc), Is.Not.EqualTo(suffix));
            Assert.That(SchemaDeadLetterKey.IdentitySuffix("peer", hlc with { Counter = 3 }), Is.Not.EqualTo(suffix));
            Assert.That(SchemaDeadLetterKey.IdentitySuffix("peer", hlc with { WallClockTicks = 1_001 }), Is.Not.EqualTo(suffix));
        });
    }

    [Test]
    public void IdentityTimestamp_is_the_source_wall_clock_clamped_into_range()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaDeadLetterKey.IdentityTimestamp(new HybridLogicalClock { WallClockTicks = 1_000 }).UtcTicks, Is.EqualTo(1_000));
            Assert.That(SchemaDeadLetterKey.IdentityTimestamp(new HybridLogicalClock { WallClockTicks = -5 }).UtcTicks, Is.Zero);
            Assert.That(SchemaDeadLetterKey.IdentityTimestamp(new HybridLogicalClock { WallClockTicks = long.MaxValue }),
                Is.EqualTo(DateTimeOffset.MaxValue));
        });
    }

    [Test]
    public void PrefixStart_is_tree_id_plus_separator()
    {
        Assert.That(SchemaDeadLetterKey.PrefixStart("orders"), Is.EqualTo($"orders{Sep}"));
    }

    [Test]
    public void PrefixEnd_is_exclusive_upper_bound_of_the_range()
    {
        var start = SchemaDeadLetterKey.PrefixStart("orders");
        var end = SchemaDeadLetterKey.PrefixEnd("orders");
        var sample = SchemaDeadLetterKey.Encode("orders", DateTimeOffset.UtcNow, "k", "u");

        Assert.That(string.CompareOrdinal(start, sample), Is.LessThanOrEqualTo(0));
        Assert.That(string.CompareOrdinal(sample, end), Is.LessThan(0));
    }

    [Test]
    public void PrefixEnd_excludes_a_different_tree_with_a_longer_name()
    {
        // "orders" range must not swallow "orders2" entries.
        var end = SchemaDeadLetterKey.PrefixEnd("orders");
        var otherTree = SchemaDeadLetterKey.Encode("orders2", DateTimeOffset.UtcNow, "k", "u");
        Assert.That(string.CompareOrdinal(otherTree, end), Is.GreaterThan(0));
    }

    [Test]
    public void Encode_null_arguments_throw()
    {
        var ts = DateTimeOffset.UtcNow;
        Assert.That(() => SchemaDeadLetterKey.Encode(null!, ts, "k", "u"), Throws.ArgumentNullException);
        Assert.That(() => SchemaDeadLetterKey.Encode("t", ts, null!, "u"), Throws.ArgumentNullException);
        Assert.That(() => SchemaDeadLetterKey.Encode("t", ts, "k", null!), Throws.ArgumentNullException);
    }
}
