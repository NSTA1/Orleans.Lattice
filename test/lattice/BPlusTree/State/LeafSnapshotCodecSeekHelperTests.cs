using System.Text;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Direct coverage for the read helpers a bounded leaf hydration (issue #1839)
/// added to <see cref="LeafSnapshotCodec"/>: the one-pass aggregate walk, the
/// row-extent lookup, the ascending-order check, and the byte-accounting
/// overload of the random-access row read.
/// <para>
/// All four exist so a reader can learn what a frame holds, and how much of it
/// a partial read consumed, without decoding rows it does not want.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotCodecSeekHelperTests
{
    private static LeafSnapshotRow Row(string key, string? value, bool tombstone = false)
        => new(
            key,
            new LwwValue<byte[]>
            {
                Value = value is null ? null : Encoding.UTF8.GetBytes(value),
                Timestamp = new HybridLogicalClock { WallClockTicks = 5L },
                IsTombstone = tombstone,
            });

    [Test]
    public void TryComputeCacheAggregates_totals_the_footprint_and_the_live_rows()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("a", "12345"),
            Row("b", "xy"),
            Row("c", null, tombstone: true),
        });

        Assert.That(LeafSnapshotCodec.TryComputeCacheAggregates(frame, out var stateBytes, out var liveRows), Is.True);
        Assert.That(liveRows, Is.EqualTo(2), "a tombstone is not a live row");
        Assert.That(stateBytes, Is.EqualTo(1 + 5 + 1 + 2 + 1),
            "utf8 key length plus stored value length, with a tombstone contributing its key only");
    }

    [Test]
    public void TryComputeCacheAggregates_agrees_with_TryComputeStateBytes()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("a", "12345"),
            Row("b", null, tombstone: true),
            Row("c", "z"),
        });

        Assert.That(LeafSnapshotCodec.TryComputeStateBytes(frame, out var expected), Is.True);
        Assert.That(LeafSnapshotCodec.TryComputeCacheAggregates(frame, out var actual, out _), Is.True);
        Assert.That(actual, Is.EqualTo(expected),
            "the two walks must not drift apart; one is the other plus a tombstone tally");
    }

    [Test]
    public void TryComputeCacheAggregates_reports_zero_for_an_empty_frame()
    {
        Assert.That(
            LeafSnapshotCodec.TryComputeCacheAggregates(LeafSnapshotCodec.Encode([]), out var bytes, out var live),
            Is.True);
        Assert.That(bytes, Is.Zero);
        Assert.That(live, Is.Zero);
    }

    [Test]
    public void TryComputeCacheAggregates_rejects_a_truncated_frame()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1") });

        Assert.That(
            LeafSnapshotCodec.TryComputeCacheAggregates(frame.AsSpan(0, frame.Length - 4), out _, out _),
            Is.False);
    }

    [Test]
    public void TryGetRowExtent_covers_the_row_region_exactly()
    {
        var rows = new[] { Row("a", "1"), Row("b", "22"), Row("c", "333") };
        var frame = LeafSnapshotCodec.Encode(rows);
        Assert.That(LeafSnapshotCodec.TryReadHeader(frame, out var rowCount, out var indexOffset), Is.True);

        var expectedStart = LeafSnapshotCodec.HeaderLength;
        long totalLength = 0;
        for (var i = 0; i < rowCount; i++)
        {
            Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, i, out var start, out var length), Is.True);
            Assert.That(start, Is.EqualTo(expectedStart), "row extents must be contiguous from the header");
            Assert.That(length, Is.GreaterThan(0));
            expectedStart += length;
            totalLength += length;
        }

        Assert.That(expectedStart, Is.EqualTo(indexOffset));
        Assert.That(totalLength, Is.EqualTo(indexOffset - LeafSnapshotCodec.HeaderLength),
            "the extents must tile the row region with no gap and no overlap");
    }

    [Test]
    public void TryGetRowExtent_rejects_an_index_outside_the_frame()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1") });

        Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, 1, out _, out _), Is.False);
        Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, -1, out _, out _), Is.False);
        Assert.That(LeafSnapshotCodec.TryGetRowExtent([], 0, out _, out _), Is.False);
    }

    [Test]
    public void TryReadRowAt_reports_the_bytes_it_consumed_and_they_match_the_extent()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1"), Row("b", "22") });

        Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, 1, out var row, out var consumed), Is.True);
        Assert.That(row.Key, Is.EqualTo("b"));
        Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, 1, out _, out var length), Is.True);
        Assert.That(consumed, Is.EqualTo(length));
    }

    [Test]
    public void TryReadRowAt_with_byte_accounting_rejects_an_out_of_range_index()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1") });

        Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, 5, out _, out var consumed), Is.False);
        Assert.That(consumed, Is.Zero);
    }

    [Test]
    public void IsAscendingByKey_accepts_a_frame_encoded_in_ordinal_order()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1"), Row("b", "2"), Row("c", "3") });

        Assert.That(LeafSnapshotCodec.IsAscendingByKey(frame), Is.True);
    }

    [Test]
    public void IsAscendingByKey_accepts_a_frame_of_zero_or_one_row()
    {
        Assert.That(LeafSnapshotCodec.IsAscendingByKey(LeafSnapshotCodec.Encode([])), Is.True);
        Assert.That(LeafSnapshotCodec.IsAscendingByKey(LeafSnapshotCodec.Encode(new[] { Row("a", "1") })), Is.True);
    }

    [Test]
    public void IsAscendingByKey_rejects_an_unsorted_or_duplicate_bearing_frame()
    {
        Assert.That(
            LeafSnapshotCodec.IsAscendingByKey(LeafSnapshotCodec.Encode(new[] { Row("b", "1"), Row("a", "2") })),
            Is.False);
        Assert.That(
            LeafSnapshotCodec.IsAscendingByKey(LeafSnapshotCodec.Encode(new[] { Row("a", "1"), Row("a", "2") })),
            Is.False,
            "a duplicate key breaks the lower-bound seek's assumption that a hit is unique");
    }

    [Test]
    public void IsAscendingByKey_rejects_an_unreadable_frame()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1") });

        Assert.That(LeafSnapshotCodec.IsAscendingByKey(frame.AsSpan(0, frame.Length - 4)), Is.False);
        Assert.That(LeafSnapshotCodec.IsAscendingByKey([]), Is.False);
    }

    [Test]
    public void IsAscendingByKey_agrees_with_ordinal_string_order_across_astral_characters()
    {
        // A raw byte compare would call this frame unsorted: a surrogate pair
        // sorts below U+E000..U+FFFF under ordinal comparison but above them in
        // UTF-8 bytes. The check must follow the same order the leaf cache is
        // sorted by, or it would refuse a perfectly seekable frame.
        var keys = new[] { "a", "\ue000private", "\U0001F600emoji", "zz" };
        Array.Sort(keys, StringComparer.Ordinal);

        var frame = LeafSnapshotCodec.Encode(keys.Select(k => Row(k, "v")).ToArray());

        Assert.That(LeafSnapshotCodec.IsAscendingByKey(frame), Is.True);
    }
}
