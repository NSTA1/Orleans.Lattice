using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="HistoryKey"/>: the fixed-width, chronologically
/// sortable re-key encoding for durable history revisions.
/// </summary>
[TestFixture]
public sealed class HistoryKeyTests
{
    private static HybridLogicalClock Clock(long wall, int counter) =>
        new() { WallClockTicks = wall, Counter = counter };

    [Test]
    public void Encode_produces_source_key_slash_fixed_width_suffix()
    {
        var key = HistoryKey.Encode("orders/42", Clock(0x1A2B, 7));

        Assert.That(key, Is.EqualTo("orders/42/0000000000001a2b.00000007"));
    }

    [Test]
    public void Encode_suffix_is_always_25_chars()
    {
        var key = HistoryKey.Encode("k", Clock(0, 0));

        // "k" + "/" + 25-char suffix
        Assert.That(key.Length, Is.EqualTo(1 + 1 + 25));
        Assert.That(key, Is.EqualTo("k/0000000000000000.00000000"));
    }

    [Test]
    public void Encode_orders_by_wall_then_counter_lexicographically()
    {
        var a = HistoryKey.Encode("k", Clock(10, 0));
        var b = HistoryKey.Encode("k", Clock(10, 1));
        var c = HistoryKey.Encode("k", Clock(11, 0));

        Assert.That(string.CompareOrdinal(a, b), Is.LessThan(0));
        Assert.That(string.CompareOrdinal(b, c), Is.LessThan(0));
    }

    [Test]
    public void Encode_keeps_all_revisions_of_a_key_under_its_prefix()
    {
        var key = HistoryKey.Encode("user:7", Clock(99, 3));

        Assert.That(key, Does.StartWith("user:7/"));
    }

    [Test]
    public void Encode_null_source_key_throws()
    {
        Assert.That(() => HistoryKey.Encode(null!, Clock(1, 0)), Throws.ArgumentNullException);
    }
}
