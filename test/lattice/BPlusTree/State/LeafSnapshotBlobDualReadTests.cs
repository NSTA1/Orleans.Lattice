using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Dual-read coverage for <see cref="LeafSnapshotBlob"/>: a blob must read
/// back identically whether its rows arrived as the legacy serialised row
/// graph or as a <see cref="LeafSnapshotCodec"/> binary frame.
/// <para>
/// Legacy blobs cannot be discarded or lazily "upgraded" by a migration pass.
/// The coverage-gated WAL GC trims a checkpointed prefix precisely because a
/// snapshot covers it, so a legacy blob that stopped being readable over an
/// already-trimmed prefix is real data loss rather than a slow start. The
/// centrepiece here is therefore the byte-for-byte legacy JSON fixture: a blob
/// serialised by the same Newtonsoft path the grain-storage serializer uses
/// (see <c>InternalNodeStateJsonRoundTripTests</c> for the precedent), read
/// back through the new dual-read surface and asserted to produce an identical
/// row set and identical coverage offsets.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotBlobDualReadTests
{
    private const string LegacyFixtureResource =
        "Orleans.Lattice.Tests.BPlusTree.State.Fixtures.leaf-snapshot-legacy.json";

    private static string ReadLegacyFixture()
    {
        using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(LegacyFixtureResource);
        Assert.That(stream, Is.Not.Null, $"embedded fixture '{LegacyFixtureResource}' is missing");
        using var reader = new StreamReader(stream!, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static List<LeafSnapshotRow> Materialize(LeafSnapshotBlob blob)
    {
        var rows = new List<LeafSnapshotRow>(blob.GetRowCount());
        foreach (var row in blob.EnumerateRows())
        {
            rows.Add(row);
        }

        return rows;
    }

    /// <summary>
    /// The exact row set the committed legacy fixture encodes. Rebuilt in code
    /// so a binary re-encoding of the same rows can be compared against it.
    /// </summary>
    private static LeafSnapshotRow[] ExpectedFixtureRows() =>
    [
        new(
            "vec/0001",
            new LwwValue<byte[]>
            {
                Value = [0x00, 0x11, 0x22, 0x33, 0x00, 0xFF],
                Timestamp = new HybridLogicalClock { WallClockTicks = 638_500_000_000_000_001L, Counter = 3 },
                IsTombstone = false,
                ExpiresAtTicks = 0L,
                OriginClusterId = null,
                VectorClock = null,
                IsMigrated = false,
            },
            null),
        new(
            "vec/0002",
            new LwwValue<byte[]>
            {
                Value = [0x41, 0x42, 0x43],
                Timestamp = new HybridLogicalClock { WallClockTicks = 638_500_000_000_000_002L, Counter = 0 },
                IsTombstone = false,
                ExpiresAtTicks = 638_600_000_000_000_000L,
                OriginClusterId = "cluster-west",
                VectorClock = null,
                IsMigrated = true,
            },
            LatticeMergeMode.OrSet),
        new(
            "vec/0003",
            new LwwValue<byte[]>
            {
                Value = null,
                Timestamp = new HybridLogicalClock { WallClockTicks = 638_500_000_000_000_003L, Counter = 1 },
                IsTombstone = true,
                ExpiresAtTicks = 0L,
                OriginClusterId = null,
                VectorClock = null,
                IsMigrated = false,
            },
            null),
    ];

    private static void AssertRowsEqual(IReadOnlyList<LeafSnapshotRow> expected, IReadOnlyList<LeafSnapshotRow> actual)
    {
        Assert.That(actual, Has.Count.EqualTo(expected.Count));
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.That(actual[i].Key, Is.EqualTo(expected[i].Key));
            Assert.That(actual[i].MergeMode, Is.EqualTo(expected[i].MergeMode));
            Assert.That(actual[i].Value.Value, Is.EqualTo(expected[i].Value.Value));
            Assert.That(actual[i].Value.Timestamp, Is.EqualTo(expected[i].Value.Timestamp));
            Assert.That(actual[i].Value.IsTombstone, Is.EqualTo(expected[i].Value.IsTombstone));
            Assert.That(actual[i].Value.ExpiresAtTicks, Is.EqualTo(expected[i].Value.ExpiresAtTicks));
            Assert.That(actual[i].Value.OriginClusterId, Is.EqualTo(expected[i].Value.OriginClusterId));
            Assert.That(actual[i].Value.IsMigrated, Is.EqualTo(expected[i].Value.IsMigrated));
        }
    }

    [Test]
    public void Committed_legacy_fixture_is_a_pre_codec_shape_carrying_no_binary_slot()
    {
        var json = ReadLegacyFixture();

        Assert.That(json, Does.Contain("\"Rows\""));
        Assert.That(json, Does.Not.Contain("EncodedRows"),
            "the fixture must be a genuine pre-codec blob; if it carries the binary slot it no longer " +
            "proves that blobs persisted before the codec existed are still readable");
        Assert.That(json, Does.Contain("\"SnapshotOffsetsByPartition\""));
    }

    [Test]
    public void Legacy_json_blob_deserialises_and_rehydrates_through_the_dual_read_surface()
    {
        var blob = JsonConvert.DeserializeObject<LeafSnapshotBlob>(ReadLegacyFixture());

        Assert.That(blob, Is.Not.Null);
        Assert.That(blob!.EncodedRows, Is.Null, "a legacy blob carries no frame");
        Assert.That(blob.HasBinaryRowPayload(), Is.False);
        Assert.That(blob.ValidateRowPayload(), Is.True);
        Assert.That(blob.GetRowCount(), Is.EqualTo(3));

        AssertRowsEqual(ExpectedFixtureRows(), Materialize(blob));
    }

    [Test]
    public void Legacy_json_blob_round_trips_its_coverage_offsets_identically()
    {
        var blob = JsonConvert.DeserializeObject<LeafSnapshotBlob>(ReadLegacyFixture())!;

        Assert.That(blob.SnapshotOffset, Is.EqualTo(41L));
        Assert.That(blob.SnapshotOffsetsByPartition, Is.EqualTo(new[] { 41L, 77L, -1L, -1L }).AsCollection);
        Assert.That(blob.CapturedAtTicks, Is.EqualTo(638_500_000_000_000_009L));
        Assert.That(blob.SnapshotBytes, Is.Zero,
            "the fixture predates the precomputed-footprint slot, so it must decode to the 0 back-fill sentinel");
    }

    [Test]
    public void Binary_re_encoding_of_the_legacy_fixture_rehydrates_to_an_identical_row_set()
    {
        // The dual-read safety property, stated as an equality rather than
        // asserted in prose: the two encodings of the same rows are
        // indistinguishable to every reader.
        var legacy = JsonConvert.DeserializeObject<LeafSnapshotBlob>(ReadLegacyFixture())!;
        var legacyRows = Materialize(legacy);

        var rewritten = new LeafSnapshotBlob
        {
            SnapshotOffset = legacy.SnapshotOffset,
            Rows = Array.Empty<LeafSnapshotRow>(),
            EncodedRows = LeafSnapshotCodec.Encode(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(legacyRows)),
            CapturedAtTicks = legacy.CapturedAtTicks,
            SnapshotBytes = legacy.SnapshotBytes,
            SnapshotOffsetsByPartition = legacy.SnapshotOffsetsByPartition,
        };

        Assert.That(rewritten.HasBinaryRowPayload(), Is.True);
        Assert.That(rewritten.ValidateRowPayload(), Is.True);
        Assert.That(rewritten.GetRowCount(), Is.EqualTo(legacy.GetRowCount()));
        AssertRowsEqual(legacyRows, Materialize(rewritten));
        Assert.That(rewritten.SnapshotOffsetsByPartition, Is.EqualTo(legacy.SnapshotOffsetsByPartition).AsCollection);
    }

    [Test]
    public void Binary_blob_survives_a_grain_storage_json_round_trip()
    {
        // The frame is persisted through the same serializer as the legacy row
        // graph, so it must survive that round trip byte-for-byte.
        var rows = ExpectedFixtureRows();
        var original = new LeafSnapshotBlob
        {
            SnapshotOffset = 41L,
            EncodedRows = LeafSnapshotCodec.Encode(rows),
            CapturedAtTicks = 638_500_000_000_000_009L,
            SnapshotBytes = 123L,
            SnapshotOffsetsByPartition = [41L, 77L, -1L, -1L],
        };

        var json = JsonConvert.SerializeObject(original);
        var roundTripped = JsonConvert.DeserializeObject<LeafSnapshotBlob>(json);

        Assert.That(roundTripped, Is.Not.Null);
        Assert.That(roundTripped!.EncodedRows, Is.EqualTo(original.EncodedRows));
        Assert.That(roundTripped.ValidateRowPayload(), Is.True);
        AssertRowsEqual(rows, Materialize(roundTripped));
        Assert.That(roundTripped.SnapshotOffsetsByPartition, Is.EqualTo(original.SnapshotOffsetsByPartition).AsCollection);
        Assert.That(roundTripped.SnapshotOffset, Is.EqualTo(41L));
        Assert.That(roundTripped.SnapshotBytes, Is.EqualTo(123L));
    }

    [Test]
    public void ValidateRowPayload_rejects_a_truncated_frame_and_a_payload_that_is_not_a_frame()
    {
        var frame = LeafSnapshotCodec.Encode(ExpectedFixtureRows());

        Assert.That(new LeafSnapshotBlob { EncodedRows = frame }.ValidateRowPayload(), Is.True);
        Assert.That(
            new LeafSnapshotBlob { EncodedRows = frame.AsSpan(0, frame.Length - 3).ToArray() }.ValidateRowPayload(),
            Is.False);
        Assert.That(
            new LeafSnapshotBlob { EncodedRows = Encoding.UTF8.GetBytes("{\"Rows\":[]}") }.ValidateRowPayload(),
            Is.False,
            "a non-empty payload that is not a frame can only mean corruption, and must not silently fall " +
            "back to the (empty) legacy row slot");
    }

    [Test]
    public void ValidateRowPayload_rejects_a_legacy_row_with_a_null_key()
    {
        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 3L,
            Rows = new List<LeafSnapshotRow> { new(null!, LwwValue<byte[]>.Create([1], default)) },
        };

        Assert.That(blob.ValidateRowPayload(), Is.False);
    }

    [Test]
    public void ValidateRowPayload_accepts_a_blob_with_no_rows_in_either_encoding()
    {
        Assert.That(new LeafSnapshotBlob().ValidateRowPayload(), Is.True);
        Assert.That(new LeafSnapshotBlob { Rows = null! }.ValidateRowPayload(), Is.True);
        Assert.That(new LeafSnapshotBlob { EncodedRows = Array.Empty<byte>() }.ValidateRowPayload(), Is.True);
    }

    [Test]
    public void GetRowCount_and_EnumerateRows_agree_across_both_encodings_and_the_empty_case()
    {
        var rows = ExpectedFixtureRows();

        var legacy = new LeafSnapshotBlob { Rows = rows };
        var binary = new LeafSnapshotBlob { EncodedRows = LeafSnapshotCodec.Encode(rows) };
        var empty = new LeafSnapshotBlob();

        Assert.That(legacy.GetRowCount(), Is.EqualTo(3));
        Assert.That(binary.GetRowCount(), Is.EqualTo(3));
        Assert.That(empty.GetRowCount(), Is.Zero);
        Assert.That(Materialize(empty), Is.Empty);
        AssertRowsEqual(Materialize(legacy), Materialize(binary));
    }

    [Test]
    public void GetRowCount_reports_zero_for_an_unreadable_frame_rather_than_guessing()
    {
        var blob = new LeafSnapshotBlob { EncodedRows = Encoding.UTF8.GetBytes("not a frame") };

        Assert.That(blob.GetRowCount(), Is.Zero);
    }

    [Test]
    public void Enumerating_an_unvalidated_malformed_frame_throws_rather_than_yielding_a_short_row_set()
    {
        // Silently stopping early would hand a caller a truncated snapshot that
        // still looks complete, which is precisely the shape that loses data
        // once the WAL prefix it claims to cover has been trimmed.
        var frame = LeafSnapshotCodec.Encode(ExpectedFixtureRows());
        Assert.That(LeafSnapshotCodec.TryReadHeader(frame, out _, out var indexOffset), Is.True);

        // Corrupt a row-region length prefix without touching the header, so
        // the header still advertises three rows.
        var corrupt = (byte[])frame.Clone();
        corrupt[indexOffset - 1] ^= 0xFF;
        corrupt[LeafSnapshotCodec.HeaderLength] = 0xFF;
        corrupt[LeafSnapshotCodec.HeaderLength + 1] = 0xFF;
        corrupt[LeafSnapshotCodec.HeaderLength + 2] = 0xFF;
        corrupt[LeafSnapshotCodec.HeaderLength + 3] = 0x7F;

        var blob = new LeafSnapshotBlob { EncodedRows = corrupt };
        Assert.That(blob.ValidateRowPayload(), Is.False, "precondition: the frame is invalid");

        Assert.That(
            () =>
            {
                foreach (var _ in blob.EnumerateRows())
                {
                    // Draining the sequence is the point; the enumerator must fault.
                }
            },
            Throws.InstanceOf<InvalidDataException>());
    }

    // --- Storage-grain level dual read: an unreadable blob must present as
    // --- "no snapshot", never as a snapshot with fewer rows.

    private static LeafSnapshotStorageGrain CreateStore(out FakePersistentState<LeafSnapshotBlob> state)
    {
        state = new FakePersistentState<LeafSnapshotBlob>();
        return new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), state);
    }

    [Test]
    public async Task Storage_grain_reports_a_corrupt_binary_blob_as_absent_rather_than_as_coverage()
    {
        var store = CreateStore(out var state);
        var frame = LeafSnapshotCodec.Encode(ExpectedFixtureRows());
        state.State = new LeafSnapshotBlob
        {
            SnapshotOffset = 41L,
            EncodedRows = frame.AsSpan(0, frame.Length - 5).ToArray(),
            SnapshotOffsetsByPartition = [41L, 77L],
        };

        Assert.That(await store.LoadAsync(default), Is.Null,
            "an unreadable snapshot must be reported as absent so the leaf falls back to WAL replay; " +
            "returning it would let the coverage-gated GC trim a prefix nothing can reproduce");
        Assert.That(await store.GetSnapshotByteSizeAsync(default), Is.Zero);
    }

    [Test]
    public async Task Storage_grain_reports_a_corrupt_legacy_blob_as_absent_rather_than_as_coverage()
    {
        var store = CreateStore(out var state);
        state.State = new LeafSnapshotBlob
        {
            SnapshotOffset = 41L,
            Rows = new List<LeafSnapshotRow> { new(null!, LwwValue<byte[]>.Create([1], default)) },
            SnapshotOffsetsByPartition = [41L],
        };

        Assert.That(await store.LoadAsync(default), Is.Null);
        Assert.That(await store.GetSnapshotByteSizeAsync(default), Is.Zero);
    }

    [Test]
    public async Task Storage_grain_refuses_to_overwrite_a_good_snapshot_with_an_unreadable_one()
    {
        var store = CreateStore(out _);
        var rows = ExpectedFixtureRows();
        await store.SaveAsync(
            new LeafSnapshotBlob
            {
                SnapshotOffset = 41L,
                EncodedRows = LeafSnapshotCodec.Encode(rows),
                SnapshotOffsetsByPartition = [41L],
            },
            default);

        var good = LeafSnapshotCodec.Encode(rows);
        await store.SaveAsync(
            new LeafSnapshotBlob
            {
                SnapshotOffset = 99L,
                EncodedRows = good.AsSpan(0, good.Length - 2).ToArray(),
                SnapshotOffsetsByPartition = [99L],
            },
            default);

        var loaded = await store.LoadAsync(default);
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.SnapshotOffset, Is.EqualTo(41L),
            "the durable snapshot that already authorised a trim must survive an unreadable overwrite");
        AssertRowsEqual(rows, Materialize(loaded));
    }

    [Test]
    public async Task Storage_grain_round_trips_a_binary_blob_and_computes_its_footprint_without_decoding_values()
    {
        var store = CreateStore(out _);
        var rows = ExpectedFixtureRows();
        await store.SaveAsync(
            new LeafSnapshotBlob
            {
                SnapshotOffset = 41L,
                EncodedRows = LeafSnapshotCodec.Encode(rows),
                SnapshotBytes = 0L,
                SnapshotOffsetsByPartition = [41L, 77L],
            },
            default);

        long expected = 0;
        foreach (var row in rows)
        {
            expected += LeafEntryCache.EntryBytes(row.Key, row.Value.IsTombstone ? null : row.Value.Value);
        }

        Assert.That(await store.GetSnapshotByteSizeAsync(default), Is.EqualTo(expected));

        var loaded = await store.LoadAsync(default);
        Assert.That(loaded, Is.Not.Null);
        AssertRowsEqual(rows, Materialize(loaded!));
        Assert.That(loaded!.SnapshotOffsetsByPartition, Is.EqualTo(new[] { 41L, 77L }).AsCollection);
    }

    [Test]
    public async Task Storage_grain_merge_preserves_the_binary_encoding_and_keeps_coverage_monotonic()
    {
        // The coverage-regression merge path must not silently downgrade a
        // frame-encoded blob back to the legacy row graph, and the merged rows
        // must stay ordinal-sorted so the frame's index table remains seekable.
        var store = CreateStore(out _);

        await store.SaveAsync(
            new LeafSnapshotBlob
            {
                SnapshotOffset = -1L,
                EncodedRows = LeafSnapshotCodec.Encode(new[]
                {
                    new LeafSnapshotRow("zeta", LwwValue<byte[]>.Create(
                        Encoding.UTF8.GetBytes("high"), new HybridLogicalClock { WallClockTicks = 900L })),
                }),
                SnapshotOffsetsByPartition = [-1L, 5L],
            },
            default);

        await store.SaveAsync(
            new LeafSnapshotBlob
            {
                SnapshotOffset = -1L,
                EncodedRows = LeafSnapshotCodec.Encode(new[]
                {
                    new LeafSnapshotRow("alpha", LwwValue<byte[]>.Create(
                        Encoding.UTF8.GetBytes("new"), new HybridLogicalClock { WallClockTicks = 100L })),
                }),
                SnapshotOffsetsByPartition = [-1L, 2L],
            },
            default);

        var loaded = await store.LoadAsync(default);
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.SnapshotOffsetsByPartition![1], Is.EqualTo(5L), "coverage must not regress");
        Assert.That(loaded.HasBinaryRowPayload(), Is.True, "the merge must not downgrade the encoding");

        var merged = Materialize(loaded);
        Assert.That(merged.Select(r => r.Key).ToArray(), Is.EqualTo(new[] { "alpha", "zeta" }).AsCollection,
            "merged rows must stay in ascending ordinal key order so the frame index table remains seekable");
    }
}
