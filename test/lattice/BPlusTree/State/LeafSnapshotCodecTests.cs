using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Unit coverage for <see cref="LeafSnapshotCodec"/>, the versioned binary
/// encoding of a leaf snapshot's row set.
/// <para>
/// Two properties are load-bearing and are asserted rather than assumed. The
/// codec must round-trip every shape a leaf row can hold - because the frame
/// is the sole durable copy of a WAL prefix the coverage-gated GC has been
/// authorised to trim, so a field silently lost in encoding is lost data. And
/// a truncated or corrupted frame must be rejected outright rather than
/// decoded partially, because a snapshot that under-reports its rows while
/// still claiming coverage is the same loss with a quieter failure mode.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotCodecTests
{
    private static LeafSnapshotRow Row(
        string key,
        byte[]? value,
        long ticks = 1_000L,
        int counter = 0,
        bool tombstone = false,
        long expiresAtTicks = 0L,
        string? originClusterId = null,
        VersionVector? vectorClock = null,
        bool migrated = false,
        LatticeMergeMode? mergeMode = null)
        => new(
            key,
            new LwwValue<byte[]>
            {
                Value = value,
                Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = counter },
                IsTombstone = tombstone,
                ExpiresAtTicks = expiresAtTicks,
                OriginClusterId = originClusterId,
                VectorClock = vectorClock,
                IsMigrated = migrated,
            },
            mergeMode);

    private static List<LeafSnapshotRow> Decode(byte[] frame)
    {
        var decoded = new List<LeafSnapshotRow>();
        foreach (var row in LeafSnapshotRowSequence.FromFrame(frame))
        {
            decoded.Add(row);
        }

        return decoded;
    }

    private static void AssertRowsEqual(LeafSnapshotRow expected, LeafSnapshotRow actual)
    {
        Assert.That(actual.Key, Is.EqualTo(expected.Key));
        Assert.That(actual.MergeMode, Is.EqualTo(expected.MergeMode));
        Assert.That(actual.Value.Value, Is.EqualTo(expected.Value.Value));
        Assert.That(actual.Value.Timestamp, Is.EqualTo(expected.Value.Timestamp));
        Assert.That(actual.Value.IsTombstone, Is.EqualTo(expected.Value.IsTombstone));
        Assert.That(actual.Value.ExpiresAtTicks, Is.EqualTo(expected.Value.ExpiresAtTicks));
        Assert.That(actual.Value.OriginClusterId, Is.EqualTo(expected.Value.OriginClusterId));
        Assert.That(actual.Value.IsMigrated, Is.EqualTo(expected.Value.IsMigrated));

        if (expected.Value.VectorClock is null)
        {
            Assert.That(actual.Value.VectorClock, Is.Null);
        }
        else
        {
            Assert.That(actual.Value.VectorClock, Is.Not.Null);
            Assert.That(actual.Value.VectorClock!.Entries, Is.EquivalentTo(expected.Value.VectorClock.Entries));
        }
    }

    private static void AssertRoundTrips(params LeafSnapshotRow[] rows)
    {
        var frame = LeafSnapshotCodec.Encode(rows);
        Assert.That(LeafSnapshotCodec.Validate(frame), Is.True, "a freshly encoded frame must validate");

        var decoded = Decode(frame);
        Assert.That(decoded, Has.Count.EqualTo(rows.Length));
        for (var i = 0; i < rows.Length; i++)
        {
            AssertRowsEqual(rows[i], decoded[i]);
        }
    }

    [Test]
    public void Encode_then_decode_round_trips_an_empty_leaf()
    {
        var frame = LeafSnapshotCodec.Encode(ReadOnlySpan<LeafSnapshotRow>.Empty);

        Assert.That(frame, Has.Length.EqualTo(LeafSnapshotCodec.MinimumFrameLength));
        Assert.That(LeafSnapshotCodec.HasFrameMagic(frame), Is.True);
        Assert.That(LeafSnapshotCodec.Validate(frame), Is.True);
        Assert.That(LeafSnapshotCodec.TryGetRowCount(frame, out var count), Is.True);
        Assert.That(count, Is.Zero);
        Assert.That(Decode(frame), Is.Empty);
    }

    [Test]
    public void Encode_then_decode_round_trips_a_single_row_leaf()
        => AssertRoundTrips(Row("only", Encoding.UTF8.GetBytes("payload")));

    [Test]
    public void Encode_then_decode_round_trips_a_value_with_embedded_nulls()
    {
        // A length-prefixed frame must be transparent to NUL bytes anywhere in
        // the payload, including as the first and last byte.
        var value = new byte[] { 0x00, 0x01, 0x00, 0x00, 0xFF, 0x00, 0x7F, 0x00 };
        AssertRoundTrips(Row("nulls", value));
    }

    [Test]
    public void Encode_then_decode_distinguishes_an_empty_value_from_a_null_value()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("a-empty", Array.Empty<byte>()),
            Row("b-null", null),
        });

        var decoded = Decode(frame);
        Assert.That(decoded[0].Value.Value, Is.Not.Null);
        Assert.That(decoded[0].Value.Value, Is.Empty);
        Assert.That(decoded[1].Value.Value, Is.Null,
            "a null value must not decode as an empty array; the two are distinct durable states");
    }

    [Test]
    public void Encode_then_decode_round_trips_a_tombstone_row()
        => AssertRoundTrips(Row("gone", null, ticks: 42L, counter: 7, tombstone: true));

    [Test]
    public void Encode_then_decode_round_trips_every_lww_metadata_field()
    {
        var vectorClock = new VersionVector();
        vectorClock.Entries["replica-a"] = new HybridLogicalClock { WallClockTicks = 11L, Counter = 2 };
        vectorClock.Entries["replica-b"] = new HybridLogicalClock { WallClockTicks = 22L, Counter = 0 };

        AssertRoundTrips(Row(
            "full",
            Encoding.UTF8.GetBytes("v"),
            ticks: 987_654_321L,
            counter: 13,
            tombstone: false,
            expiresAtTicks: 555_666_777L,
            originClusterId: "cluster-west",
            vectorClock: vectorClock,
            migrated: true,
            mergeMode: LatticeMergeMode.OrSet));
    }

    [Test]
    public void Encode_then_decode_round_trips_an_empty_vector_clock_distinctly_from_a_null_one()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("a", Encoding.UTF8.GetBytes("x"), vectorClock: new VersionVector()),
            Row("b", Encoding.UTF8.GetBytes("y")),
        });

        var decoded = Decode(frame);
        Assert.That(decoded[0].Value.VectorClock, Is.Not.Null);
        Assert.That(decoded[0].Value.VectorClock!.Entries, Is.Empty);
        Assert.That(decoded[1].Value.VectorClock, Is.Null);
    }

    [Test]
    public void Encode_then_decode_round_trips_every_crdt_merge_mode_a_leaf_can_hold()
    {
        // The per-key merge-mode discriminator is the durable record of which
        // CRDT shape a value carries; a mode lost in encoding silently
        // downgrades the key to last-writer-wins on the next capture.
        var modes = Enum.GetValues<LatticeMergeMode>();
        Assert.That(modes, Is.Not.Empty);

        var rows = new List<LeafSnapshotRow>(modes.Length + 1)
        {
            Row("key-00-none", Encoding.UTF8.GetBytes("plain-lww")),
        };
        for (var i = 0; i < modes.Length; i++)
        {
            rows.Add(Row(
                $"key-{i + 1:D2}-{modes[i]}",
                Encoding.UTF8.GetBytes($"state-{modes[i]}"),
                mergeMode: modes[i]));
        }

        var frame = LeafSnapshotCodec.Encode(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(rows));
        var decoded = Decode(frame);

        Assert.That(decoded, Has.Count.EqualTo(rows.Count));
        Assert.That(decoded[0].MergeMode, Is.Null);
        for (var i = 0; i < modes.Length; i++)
        {
            Assert.That(decoded[i + 1].MergeMode, Is.EqualTo(modes[i]));
        }
    }

    [Test]
    public void Encode_then_decode_round_trips_non_ascii_and_supplementary_plane_keys()
        => AssertRoundTrips(
            Row("ascii", Encoding.UTF8.GetBytes("1")),
            Row("k-\u00e9\u00e8-latin", Encoding.UTF8.GetBytes("2")),
            Row("k-\u4e2d\u6587-han", Encoding.UTF8.GetBytes("3")),
            Row("k-\ud83d\ude00-astral", Encoding.UTF8.GetBytes("4")),
            Row("k-\ue000-private", Encoding.UTF8.GetBytes("5")));

    [Test]
    public void Encode_then_decode_round_trips_a_multi_megabyte_leaf()
    {
        // A payload leaf holding packed vectors: many rows, each several KB.
        // Exercises the exact-size single-allocation encode and the streaming
        // decode at a realistic frame size rather than a toy one.
        const int rowCount = 512;
        const int valueBytes = 4_096;
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var value = new byte[valueBytes];
            // Deterministic, non-constant fill so a mis-sliced value cannot
            // accidentally compare equal to its neighbour.
            for (var b = 0; b < valueBytes; b++)
            {
                value[b] = (byte)((i * 31) + b);
            }

            rows[i] = Row($"vpay/{i:D6}", value, ticks: 1_000L + i);
        }

        var frame = LeafSnapshotCodec.Encode(rows);
        Assert.That(frame.Length, Is.GreaterThan(2 * 1024 * 1024));
        Assert.That(LeafSnapshotCodec.Validate(frame), Is.True);

        var decoded = Decode(frame);
        Assert.That(decoded, Has.Count.EqualTo(rowCount));
        for (var i = 0; i < rowCount; i++)
        {
            AssertRowsEqual(rows[i], decoded[i]);
        }
    }

    [Test]
    public void Encode_preserves_the_supplied_row_order()
    {
        var rows = new[]
        {
            Row("a", Encoding.UTF8.GetBytes("1")),
            Row("b", Encoding.UTF8.GetBytes("2")),
            Row("c", Encoding.UTF8.GetBytes("3")),
        };

        var decoded = Decode(LeafSnapshotCodec.Encode(rows));

        Assert.That(decoded.Select(r => r.Key).ToArray(), Is.EqualTo(new[] { "a", "b", "c" }).AsCollection);
    }

    [Test]
    public void Encode_rejects_a_row_with_a_null_key()
    {
        var rows = new[] { new LeafSnapshotRow(null!, LwwValue<byte[]>.Create([1], default)) };

        Assert.That(() => LeafSnapshotCodec.Encode(rows), Throws.ArgumentException);
    }

    [Test]
    public void HasFrameMagic_is_false_for_a_legacy_json_document_and_for_short_buffers()
    {
        // The dual-read sniff must never mistake persisted JSON for a frame.
        Assert.That(LeafSnapshotCodec.HasFrameMagic(Encoding.UTF8.GetBytes("{\"Rows\":[]}")), Is.False);
        Assert.That(LeafSnapshotCodec.HasFrameMagic(Encoding.UTF8.GetBytes("LSN")), Is.False);
        Assert.That(LeafSnapshotCodec.HasFrameMagic(ReadOnlySpan<byte>.Empty), Is.False);
        Assert.That(LeafSnapshotCodec.HasFrameMagic(LeafSnapshotCodec.Encode(ReadOnlySpan<LeafSnapshotRow>.Empty)), Is.True);
    }

    [Test]
    public void Validate_rejects_a_frame_truncated_at_any_length()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("k1", Encoding.UTF8.GetBytes("value-one")),
            Row("k2", Encoding.UTF8.GetBytes("value-two")),
        });

        for (var length = 0; length < frame.Length; length++)
        {
            var truncated = frame.AsSpan(0, length).ToArray();
            Assert.That(LeafSnapshotCodec.Validate(truncated), Is.False,
                $"a frame truncated to {length} of {frame.Length} bytes must be rejected outright, " +
                "never decoded to a short row set that still claims coverage");
        }

        Assert.That(LeafSnapshotCodec.Validate(frame), Is.True);
    }

    [Test]
    public void Validate_rejects_a_frame_with_trailing_bytes_appended()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("k", Encoding.UTF8.GetBytes("v")) });
        var extended = new byte[frame.Length + 1];
        frame.CopyTo(extended, 0);

        Assert.That(LeafSnapshotCodec.Validate(extended), Is.False);
    }

    [Test]
    public void Validate_rejects_a_single_flipped_bit_anywhere_in_the_frame()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("alpha", Encoding.UTF8.GetBytes("first"), mergeMode: LatticeMergeMode.PnCounter),
            Row("beta", Encoding.UTF8.GetBytes("second"), originClusterId: "west"),
        });

        for (var i = 0; i < frame.Length; i++)
        {
            var corrupt = (byte[])frame.Clone();
            corrupt[i] ^= 0x01;
            Assert.That(LeafSnapshotCodec.Validate(corrupt), Is.False,
                $"a single flipped bit at byte {i} must fail validation");
        }
    }

    [Test]
    public void Validate_rejects_an_unsupported_format_version()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("k", Encoding.UTF8.GetBytes("v")) });
        frame[4] = LeafSnapshotCodec.FormatVersion + 1;

        Assert.That(LeafSnapshotCodec.Validate(frame), Is.False,
            "a frame written by a newer codec must be rejected rather than decoded on a guess");
        Assert.That(LeafSnapshotCodec.TryGetRowCount(frame, out _), Is.False);
    }

    [Test]
    public void Validate_rejects_a_buffer_that_is_not_a_frame_at_all()
    {
        Assert.That(LeafSnapshotCodec.Validate(Encoding.UTF8.GetBytes("{\"SnapshotOffset\":3}")), Is.False);
        Assert.That(LeafSnapshotCodec.Validate(ReadOnlySpan<byte>.Empty), Is.False);
        Assert.That(LeafSnapshotCodec.Validate(new byte[LeafSnapshotCodec.MinimumFrameLength]), Is.False);
    }

    [Test]
    public void Validate_rejects_a_tampered_index_table_even_when_the_checksum_is_recomputed()
    {
        // Models a hostile or badly-recovered blob rather than bit rot: the
        // index table is repointed and the checksum rewritten to match. The
        // structural walk is what catches it, so the offsets a seek trusts
        // cannot be made to lie.
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("k1", Encoding.UTF8.GetBytes("one")),
            Row("k2", Encoding.UTF8.GetBytes("two")),
        });

        Assert.That(LeafSnapshotCodec.TryReadHeader(frame, out var rowCount, out var indexOffset), Is.True);
        Assert.That(rowCount, Is.EqualTo(2));

        var tampered = (byte[])frame.Clone();
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
            tampered.AsSpan(indexOffset + sizeof(int)),
            LeafSnapshotCodec.HeaderLength);
        var body = tampered.AsSpan(0, tampered.Length - LeafSnapshotCodec.TrailerLength);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(
            tampered.AsSpan(tampered.Length - LeafSnapshotCodec.TrailerLength),
            System.IO.Hashing.XxHash64.HashToUInt64(body));

        Assert.That(LeafSnapshotCodec.Validate(tampered), Is.False);
    }

    [Test]
    public void TryGetRowCount_reports_the_encoded_row_count_without_decoding()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("a", Encoding.UTF8.GetBytes("1")),
            Row("b", Encoding.UTF8.GetBytes("2")),
            Row("c", Encoding.UTF8.GetBytes("3")),
        });

        Assert.That(LeafSnapshotCodec.TryGetRowCount(frame, out var count), Is.True);
        Assert.That(count, Is.EqualTo(3));
        Assert.That(LeafSnapshotCodec.TryGetRowCount(Encoding.UTF8.GetBytes("not a frame"), out var missing), Is.False);
        Assert.That(missing, Is.Zero);
    }

    [Test]
    public void TryComputeStateBytes_matches_the_leaf_entry_cache_footprint_formula()
    {
        var rows = new[]
        {
            Row("plain", Encoding.UTF8.GetBytes("abcdef")),
            Row("k-\u4e2d\u6587", new byte[] { 1, 2, 3 }),
            Row("dead", null, tombstone: true),
            Row("nullvalue", null),
            Row("rich", Encoding.UTF8.GetBytes("xyz"), originClusterId: "west", mergeMode: LatticeMergeMode.GSet),
        };

        long expected = 0;
        foreach (var row in rows)
        {
            expected += LeafEntryCache.EntryBytes(row.Key, row.Value.IsTombstone ? null : row.Value.Value);
        }

        Assert.That(LeafSnapshotCodec.TryComputeStateBytes(LeafSnapshotCodec.Encode(rows), out var actual), Is.True);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public void TryComputeStateBytes_reports_failure_for_an_unreadable_frame()
    {
        Assert.That(LeafSnapshotCodec.TryComputeStateBytes(Encoding.UTF8.GetBytes("nope"), out var bytes), Is.False);
        Assert.That(bytes, Is.Zero);
    }

    [Test]
    public void TryReadHeader_reports_the_row_count_and_row_region_end()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("k", Encoding.UTF8.GetBytes("v")) });

        Assert.That(LeafSnapshotCodec.TryReadHeader(frame, out var rowCount, out var indexOffset), Is.True);
        Assert.That(rowCount, Is.EqualTo(1));
        Assert.That(indexOffset, Is.GreaterThan(LeafSnapshotCodec.HeaderLength));
        Assert.That(indexOffset + (rowCount * sizeof(int)) + LeafSnapshotCodec.TrailerLength, Is.EqualTo(frame.Length));
    }

    [Test]
    public void TryReadRow_walks_the_row_region_and_stops_at_its_end()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            Row("a", Encoding.UTF8.GetBytes("1")),
            Row("b", Encoding.UTF8.GetBytes("2")),
        });

        Assert.That(LeafSnapshotCodec.TryReadHeader(frame, out _, out var limit), Is.True);
        var pos = LeafSnapshotCodec.HeaderLength;

        Assert.That(LeafSnapshotCodec.TryReadRow(frame, limit, ref pos, out var first), Is.True);
        Assert.That(first.Key, Is.EqualTo("a"));
        Assert.That(LeafSnapshotCodec.TryReadRow(frame, limit, ref pos, out var second), Is.True);
        Assert.That(second.Key, Is.EqualTo("b"));
        Assert.That(pos, Is.EqualTo(limit));
        Assert.That(LeafSnapshotCodec.TryReadRow(frame, limit, ref pos, out _), Is.False,
            "reading past the row region must fail rather than run into the index table");
    }

    // --- Bounded partial-read seam (the primitives a key-range-scoped
    // --- hydration is built from: seek to a row by index without decoding
    // --- its predecessors, and probe a row's key without its payload).

    private static byte[] SeekFixture() => LeafSnapshotCodec.Encode(new[]
    {
        Row("vec/aaa", Encoding.UTF8.GetBytes("1")),
        Row("vec/ccc", Encoding.UTF8.GetBytes("2")),
        Row("vec/eee", Encoding.UTF8.GetBytes("3")),
        Row("vec/ggg", Encoding.UTF8.GetBytes("4")),
    });

    [Test]
    public void TryReadRowAt_decodes_any_row_directly_without_walking_its_predecessors()
    {
        var frame = SeekFixture();
        var expected = new[] { "vec/aaa", "vec/ccc", "vec/eee", "vec/ggg" };

        for (var i = 0; i < expected.Length; i++)
        {
            Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, i, out var row), Is.True);
            Assert.That(row.Key, Is.EqualTo(expected[i]));
            Assert.That(row.Value.Value, Is.EqualTo(Encoding.UTF8.GetBytes((i + 1).ToString())));
        }
    }

    [Test]
    public void TryReadRowAt_rejects_an_out_of_range_index()
    {
        var frame = SeekFixture();

        Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, -1, out _), Is.False);
        Assert.That(LeafSnapshotCodec.TryReadRowAt(frame, 4, out _), Is.False);
        Assert.That(LeafSnapshotCodec.TryReadRowAt(Encoding.UTF8.GetBytes("not a frame"), 0, out _), Is.False);
    }

    [Test]
    public void TryReadRowKeyUtf8At_returns_the_key_slice_without_the_payload()
    {
        var frame = SeekFixture();

        Assert.That(LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, 2, out var key), Is.True);
        Assert.That(Encoding.UTF8.GetString(key), Is.EqualTo("vec/eee"));
        Assert.That(LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, 9, out _), Is.False);
    }

    [Test]
    public void TryFindFirstRowAtOrAfter_returns_the_lower_bound_index_for_a_key_range_seek()
    {
        var frame = SeekFixture();

        static int Seek(byte[] frame, string key)
        {
            Assert.That(
                LeafSnapshotCodec.TryFindFirstRowAtOrAfter(frame, Encoding.UTF8.GetBytes(key), out var index),
                Is.True);
            return index;
        }

        Assert.That(Seek(frame, "vec/"), Is.EqualTo(0), "a key before every row seeks to the first row");
        Assert.That(Seek(frame, "vec/aaa"), Is.EqualTo(0), "an exact hit seeks to that row");
        Assert.That(Seek(frame, "vec/bbb"), Is.EqualTo(1), "a gap seeks to the next row");
        Assert.That(Seek(frame, "vec/eee"), Is.EqualTo(2));
        Assert.That(Seek(frame, "vec/ggg"), Is.EqualTo(3));
        Assert.That(Seek(frame, "vec/zzz"), Is.EqualTo(4), "a key past every row seeks to the row count");
    }

    [Test]
    public void TryFindFirstRowAtOrAfter_handles_an_empty_frame_and_rejects_a_non_frame()
    {
        var empty = LeafSnapshotCodec.Encode(ReadOnlySpan<LeafSnapshotRow>.Empty);

        Assert.That(LeafSnapshotCodec.TryFindFirstRowAtOrAfter(empty, "anything"u8, out var index), Is.True);
        Assert.That(index, Is.Zero);
        Assert.That(
            LeafSnapshotCodec.TryFindFirstRowAtOrAfter(Encoding.UTF8.GetBytes("junk"), "k"u8, out _),
            Is.False);
    }

    [Test]
    public void TryFindFirstRowAtOrAfter_agrees_with_a_linear_scan_over_a_large_sorted_frame()
    {
        // Property check on the binary search itself: for every probe, the
        // index it returns must equal the first index whose key is not less
        // than the probe under ordinal order.
        var keys = new List<string>();
        for (var i = 0; i < 200; i++)
        {
            keys.Add($"k/{i * 2:D4}");
        }

        keys.Sort(StringComparer.Ordinal);
        var rows = keys.Select(k => Row(k, Encoding.UTF8.GetBytes(k))).ToArray();
        var frame = LeafSnapshotCodec.Encode(rows);

        for (var i = 0; i < 400; i++)
        {
            var probe = $"k/{i:D4}";
            var expected = 0;
            while (expected < keys.Count && string.CompareOrdinal(keys[expected], probe) < 0)
            {
                expected++;
            }

            Assert.That(
                LeafSnapshotCodec.TryFindFirstRowAtOrAfter(frame, Encoding.UTF8.GetBytes(probe), out var actual),
                Is.True);
            Assert.That(actual, Is.EqualTo(expected), $"lower bound for '{probe}'");
        }
    }

    [Test]
    public void CompareKeysUtf8_reproduces_ordinal_string_order_including_supplementary_planes()
    {
        // A raw byte compare is not equivalent to ordinal string order: a
        // surrogate pair sorts below U+E000..U+FFFF in UTF-16 but above them in
        // UTF-8. A seek that used a byte compare would therefore miss rows in
        // exactly the keyspace an emoji or CJK-extension key lands in.
        var keys = new[]
        {
            string.Empty,
            "a",
            "ab",
            "b",
            "k/0001",
            "k/0002",
            "\u00e9",
            "\u4e2d\u6587",
            "\ud800\udc00",
            "\ud83d\ude00",
            "\udbff\udfff",
            "\ue000",
            "\uffff",
        };

        foreach (var left in keys)
        {
            foreach (var right in keys)
            {
                var expected = Math.Sign(string.CompareOrdinal(left, right));
                var actual = Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(
                    Encoding.UTF8.GetBytes(left),
                    Encoding.UTF8.GetBytes(right)));

                Assert.That(actual, Is.EqualTo(expected),
                    $"ordinal order of '{Escape(left)}' vs '{Escape(right)}'");
            }
        }

        static string Escape(string value) => string.Concat(value.Select(c => $"U+{(int)c:X4}"));
    }

    [Test]
    public void CompareKeysUtf8_falls_back_to_a_byte_compare_for_malformed_utf8()
    {
        // Never throws, and stays a total order, so a corrupt key can only
        // misplace a seek rather than fault the caller.
        ReadOnlySpan<byte> malformed = [0xFF, 0xFE];
        ReadOnlySpan<byte> valid = "a"u8;

        Assert.That(LeafSnapshotCodec.CompareKeysUtf8(malformed, valid), Is.Not.Zero);
        Assert.That(
            Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(malformed, valid)),
            Is.EqualTo(-Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(valid, malformed))));
        Assert.That(LeafSnapshotCodec.CompareKeysUtf8(malformed, malformed), Is.Zero);
    }
}
