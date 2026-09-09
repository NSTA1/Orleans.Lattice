using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Malformed-frame coverage for <see cref="LeafSnapshotCodec"/>: every decode
/// entry point must reject a corrupt or truncated frame by returning
/// <see langword="false"/>, never by reading past the row region, throwing, or -
/// worst of all - returning a partially decoded row.
/// </summary>
/// <remarks>
/// <para>
/// This matters more than an ordinary robustness test. A leaf snapshot frame is
/// read back from durable storage, and the coverage-gated WAL GC trims a
/// checkpointed prefix precisely <em>because</em> a snapshot covers it. A decoder
/// that accepted a damaged frame would rehydrate a leaf from partial rows over a
/// prefix that no longer exists in the WAL; a decoder that threw would fault the
/// activation instead of falling back to "no snapshot". Both are data loss where
/// a clean <see langword="false"/> is merely a re-capture.
/// </para>
/// <para>
/// The bulk of the coverage comes from a truncation sweep: one row carrying every
/// optional field (merge mode, origin cluster id, a multi-entry vector clock and a
/// value) is encoded, and then a frame is rebuilt around every proper prefix of
/// that row's bytes. Each prefix cuts a different field in half, so the sweep
/// walks every bounds check in the row parser without hand-picking offsets. The
/// remaining tests target the structural guards a truncation cannot reach: an
/// index table that disagrees with the rows it indexes, trailing junk inside the
/// row region, and a declared vector-clock entry count that could not possibly
/// fit.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LeafSnapshotCodecMalformedFrameTests
{
    private const int HeaderLength = 24;
    private const int TrailerLength = 8;

    private const byte RowFlagHasValue = 0x01;
    private const byte RowFlagHasOriginClusterId = 0x04;
    private const byte RowFlagHasVectorClock = 0x08;
    private const byte RowFlagHasMergeMode = 0x20;

    /// <summary>A row that exercises every optional field the row parser understands.</summary>
    private static LeafSnapshotRow RichRow(string key = "k") => new(
        key,
        new LwwValue<byte[]>
        {
            Value = [1, 2, 3, 4],
            Timestamp = new HybridLogicalClock { WallClockTicks = 77L, Counter = 3 },
            IsTombstone = false,
            ExpiresAtTicks = 99L,
            OriginClusterId = "cluster-eu",
            VectorClock = BuildVectorClock(("r1", 10L, 1), ("r2", 20L, 2)),
            IsMigrated = true,
        },
        LatticeMergeMode.OrSet);

    private static VersionVector BuildVectorClock(params (string Replica, long Ticks, int Counter)[] entries)
    {
        var vector = new VersionVector();
        foreach (var (replica, ticks, counter) in entries)
        {
            vector.Entries[replica] = new HybridLogicalClock { WallClockTicks = ticks, Counter = counter };
        }

        return vector;
    }

    /// <summary>Extracts the encoded row-record bytes (the region between the header and the index table).</summary>
    private static byte[] RowRegionOf(LeafSnapshotRow row)
    {
        var frame = LeafSnapshotCodec.Encode([row]);
        Assert.That(LeafSnapshotCodec.TryReadHeader(frame, out var rowCount, out var indexOffset), Is.True);
        Assert.That(rowCount, Is.EqualTo(1));
        return frame[HeaderLength..indexOffset];
    }

    /// <summary>
    /// Assembles a header-consistent frame around an arbitrary row region and
    /// index table. The header arithmetic is kept valid on purpose so the frame
    /// clears <c>TryReadHeader</c> and the failure that follows is attributable to
    /// the row bytes rather than to the header guard.
    /// </summary>
    private static byte[] Frame(byte[] rowRegion, int[] indexEntries, bool checksum = true)
    {
        var indexOffset = HeaderLength + rowRegion.Length;
        var total = indexOffset + (indexEntries.Length * sizeof(int)) + TrailerLength;
        var frame = new byte[total];
        var span = frame.AsSpan();

        span[0] = 0x4C;
        span[1] = 0x53;
        span[2] = 0x4E;
        span[3] = 0x01;
        span[4] = 1; // format version
        BinaryPrimitives.WriteInt32LittleEndian(span[8..], indexEntries.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span[12..], indexOffset);
        BinaryPrimitives.WriteInt32LittleEndian(span[16..], total);

        rowRegion.CopyTo(span[HeaderLength..]);
        for (var i = 0; i < indexEntries.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[(indexOffset + (i * sizeof(int)))..], indexEntries[i]);
        }

        if (checksum)
        {
            var body = span[..(total - TrailerLength)];
            BinaryPrimitives.WriteUInt64LittleEndian(span[(total - TrailerLength)..], XxHash64.HashToUInt64(body));
        }

        return frame;
    }

    /// <summary>Every reader that walks row bytes, so one malformed frame can be asserted against all of them.</summary>
    private static void AssertEveryReaderRejects(byte[] frame, string because)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCodec.Validate(frame), Is.False, because);
            Assert.That(LeafSnapshotCodec.TryComputeStateBytes(frame, out _), Is.False, because);
            Assert.That(LeafSnapshotCodec.TryComputeCacheAggregates(frame, out _, out _), Is.False, because);
        });
    }

    // ----- The truncation sweep -----

    [Test]
    public void Every_truncation_of_a_rich_row_is_rejected_by_every_walking_reader()
    {
        var rowRegion = RowRegionOf(RichRow());
        Assert.That(rowRegion.Length, Is.GreaterThan(40), "the probe row must carry every optional field");

        for (var prefix = 0; prefix < rowRegion.Length; prefix++)
        {
            var frame = Frame(rowRegion[..prefix], [HeaderLength]);

            Assert.That(LeafSnapshotCodec.Validate(frame), Is.False,
                $"a row truncated to {prefix} of {rowRegion.Length} bytes must not validate");
            Assert.That(LeafSnapshotCodec.TryComputeStateBytes(frame, out _), Is.False,
                $"the footprint walk must reject a row truncated to {prefix} bytes");
            Assert.That(LeafSnapshotCodec.TryComputeCacheAggregates(frame, out _, out _), Is.False,
                $"the aggregate walk must reject a row truncated to {prefix} bytes");
            Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, 0, out _), Is.False,
                $"the random-access read must reject a row truncated to {prefix} bytes");
        }
    }

    [Test]
    public void A_truncated_row_never_yields_a_partially_decoded_row()
    {
        var rowRegion = RowRegionOf(RichRow());

        for (var prefix = 1; prefix < rowRegion.Length; prefix++)
        {
            var frame = Frame(rowRegion[..prefix], [HeaderLength]);

            Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, 0, out var row, out var consumed), Is.False);
            Assert.Multiple(() =>
            {
                Assert.That(row.Key, Is.Null,
                    $"prefix {prefix}: a failed read must leave the out row at default, never half-populated");
                Assert.That(consumed, Is.Zero);
            });
        }
    }

    [Test]
    public void A_full_rich_row_still_round_trips_so_the_sweep_is_not_vacuous()
    {
        // Guards the sweep above: if the assembled frame were rejected for a
        // reason unrelated to truncation, every iteration would pass trivially.
        var original = RichRow();
        var frame = Frame(RowRegionOf(original), [HeaderLength]);

        Assert.That(LeafSnapshotCodec.Validate(frame), Is.True, "the untruncated control frame must validate");
        Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, 0, out var row), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(row.Key, Is.EqualTo(original.Key));
            Assert.That(row.Value.Value, Is.EqualTo(original.Value.Value));
            Assert.That(row.Value.OriginClusterId, Is.EqualTo("cluster-eu"));
            Assert.That(row.MergeMode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(row.Value.IsMigrated, Is.True);
            Assert.That(row.Value.VectorClock!.Entries, Has.Count.EqualTo(2));
            Assert.That(LeafSnapshotCodec.TryComputeStateBytes(frame, out var bytes), Is.True);
            Assert.That(bytes, Is.EqualTo(Encoding.UTF8.GetByteCount(original.Key) + 4));
        });
    }

    // ----- Structural corruption a truncation cannot express -----

    [Test]
    public void Validate_rejects_an_index_entry_that_disagrees_with_the_row_start()
    {
        // The index table is the seek structure: a binary search over a table
        // whose offsets do not land on real row starts would decode garbage, so
        // Validate cross-checks every entry against the walked position.
        var rowRegion = RowRegionOf(RichRow());
        var frame = Frame(rowRegion, [HeaderLength + 1]);

        Assert.That(LeafSnapshotCodec.Validate(frame), Is.False,
            "an index entry that does not equal the walked row start must fail validation");
    }

    [Test]
    public void Validate_rejects_trailing_junk_inside_the_row_region()
    {
        // The row walk must end exactly at the index table. Extra bytes mean the
        // declared row count under-reports what the region holds.
        var rowRegion = RowRegionOf(RichRow());
        var padded = new byte[rowRegion.Length + 5];
        rowRegion.CopyTo(padded, 0);

        var frame = Frame(padded, [HeaderLength]);

        AssertEveryReaderRejects(frame, "the row walk must finish exactly at the index-table offset");
    }

    [Test]
    public void An_impossible_vector_clock_entry_count_is_rejected_before_anything_is_allocated()
    {
        // A hostile or corrupt frame can declare an entry count whose entries
        // could not fit in the remaining row region. The parser must reject it on
        // the arithmetic rather than loop allocating until it runs out.
        var frame = Frame(RowWithDeclaredVectorClockEntries(int.MaxValue), [HeaderLength]);

        AssertEveryReaderRejects(frame, "an entry count that cannot fit the row region must be rejected up front");
    }

    [Test]
    public void A_negative_vector_clock_entry_count_is_rejected()
    {
        var frame = Frame(RowWithDeclaredVectorClockEntries(-1), [HeaderLength]);

        AssertEveryReaderRejects(frame, "a negative entry count must be rejected");
    }

    /// <summary>
    /// Hand-builds a row whose vector-clock section declares
    /// <paramref name="declaredEntries"/> entries but carries none, so the parser
    /// meets the count before it meets any entry bytes.
    /// </summary>
    private static byte[] RowWithDeclaredVectorClockEntries(int declaredEntries)
    {
        var row = new List<byte>();
        var key = Encoding.UTF8.GetBytes("k");

        AppendInt32(row, key.Length);
        row.AddRange(key);
        row.Add(RowFlagHasVectorClock);
        AppendInt64(row, 1L);   // wall-clock ticks
        AppendInt32(row, 0);    // counter
        AppendInt64(row, 0L);   // expires-at ticks
        AppendInt32(row, declaredEntries);
        return [.. row];
    }

    private static void AppendInt32(List<byte> target, int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        target.AddRange(buffer);
    }

    private static void AppendInt64(List<byte> target, long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(buffer, value);
        target.AddRange(buffer);
    }

    // ----- Seek helpers on an unreadable or out-of-range frame -----

    [Test]
    public void Seek_helpers_reject_a_payload_that_is_not_a_frame()
    {
        var notAFrame = Encoding.UTF8.GetBytes("{\"Rows\":[]}");

        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCodec.IsAscendingByKey(notAFrame), Is.False);
            Assert.That(LeafSnapshotCodec.TryFindFirstRowAtOrAfter(notAFrame, "a"u8, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryGetRowExtent(notAFrame, 0, out _, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryReadRowAt(notAFrame, 0, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryReadRowKeyUtf8At(notAFrame, 0, out _), Is.False);
        });
    }

    [Test]
    public void Random_access_helpers_reject_an_out_of_range_row_index()
    {
        var frame = LeafSnapshotCodec.Encode([RichRow("a"), RichRow("b")]);

        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, 2, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, -1, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, 2, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, -1, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, 2, out _, out _), Is.False);
            Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, -1, out _, out _), Is.False);
        });
    }

    [Test]
    public void TryGetRowExtent_rejects_a_frame_whose_next_index_entry_is_out_of_bounds()
    {
        // Extent of row i is derived from the start of row i+1, so a corrupt
        // successor entry must fail the extent lookup rather than yield a length
        // computed from a bogus offset.
        var rowRegion = RowRegionOf(RichRow());
        var frame = Frame(rowRegion, [HeaderLength, 0]);

        Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, 0, out _, out _), Is.False,
            "an unreadable successor offset must fail the extent lookup");
    }

    [Test]
    public void IsAscendingByKey_rejects_a_frame_whose_key_probe_fails()
    {
        // Row 0 is intact but row 1's index entry points inside row 0's payload,
        // so the second key probe reads a bogus length.
        var frame = Frame(RowRegionOf(RichRow()), [HeaderLength, HeaderLength + 2]);

        Assert.That(LeafSnapshotCodec.IsAscendingByKey(frame), Is.False);
    }

    [Test]
    public void IsAscendingByKey_rejects_a_frame_whose_very_first_key_probe_fails()
    {
        // The first probe is read before the loop, so it needs its own guard: an
        // index entry that lands too close to the index table for even the key
        // length prefix to be read.
        var rowRegion = RowRegionOf(RichRow());
        var lastByteOfRowRegion = HeaderLength + rowRegion.Length - 1;
        var frame = Frame(rowRegion, [lastByteOfRowRegion, lastByteOfRowRegion]);

        Assert.That(LeafSnapshotCodec.IsAscendingByKey(frame), Is.False,
            "a first-row probe that cannot even read the key length must fail the order check, "
            + "not be treated as an empty key that trivially sorts first");
    }

    [Test]
    public void TryFindFirstRowAtOrAfter_rejects_a_frame_whose_probe_fails_mid_search()
    {
        // The binary search probes keys through the index table; a corrupt entry
        // must abort the search rather than steer it with a garbage comparison.
        var rowRegion = RowRegionOf(RichRow());
        var lastByteOfRowRegion = HeaderLength + rowRegion.Length - 1;
        var frame = Frame(rowRegion, [lastByteOfRowRegion]);

        Assert.That(LeafSnapshotCodec.TryFindFirstRowAtOrAfter(frame, "a"u8, out _), Is.False,
            "an unreadable probe must fail the seek rather than mis-steer the bisection");
    }

    [Test]
    public void TryGetRowExtent_rejects_a_descending_index_table()
    {
        // Both entries are individually in range, so only the end-before-start
        // check catches the inversion. Without it the extent length would go
        // negative and a bounded hydration would account nonsense.
        var rowRegion = RowRegionOf(RichRow());
        Assert.That(rowRegion.Length, Is.GreaterThan(8));

        var frame = Frame(rowRegion, [HeaderLength + 6, HeaderLength + 2]);

        Assert.That(LeafSnapshotCodec.TryGetRowExtent(frame, 0, out _, out _), Is.False,
            "row i+1 starting before row i must fail the extent lookup, not yield a negative length");
    }
}
