using System.Text;
using Newtonsoft.Json;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// The two quantitative claims the binary leaf-snapshot codec exists to make,
/// asserted rather than asserted-in-prose.
/// <para>
/// <b>Allocation.</b> The decode path must allocate no more than the
/// rehydrated payload itself - one key string and one value array per row,
/// which the entry cache genuinely needs - with no per-row intermediate. The
/// legacy shape allocates a base64 string and a scratch buffer per row on top
/// of that, and it is the decode cost, not just the byte count, that makes a
/// cold start expensive. The bound here is calibrated inside the test against
/// the cost of materialising the same keys and values directly, so it is a
/// statement about the codec rather than about a particular runtime's object
/// header sizes.
/// </para>
/// <para>
/// <b>Size.</b> The comparison is made end-to-end against the JSON the
/// grain-storage serializer actually writes, so it accounts for the base64
/// inflation the serializer applies to the frame as well as the per-row
/// envelope the frame removes.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotCodecAllocationTests
{
    private static LeafSnapshotRow[] BuildRows(int rowCount, int valueBytes, string keyPrefix)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var value = new byte[valueBytes];
            for (var b = 0; b < valueBytes; b++)
            {
                value[b] = (byte)((i * 7) + b);
            }

            rows[i] = new LeafSnapshotRow(
                $"{keyPrefix}{i:D8}",
                LwwValue<byte[]>.Create(value, new HybridLogicalClock { WallClockTicks = 1_000L + i, Counter = i & 7 }));
        }

        return rows;
    }

    private static long DecodeAllocations(byte[] frame, out int decodedRows)
    {
        var count = 0;
        var before = GC.GetAllocatedBytesForCurrentThread();
        foreach (var row in LeafSnapshotRowSequence.FromFrame(frame))
        {
            // Touch both allocated members so nothing can be optimised away.
            count += row.Key.Length + (row.Value.Value?.Length ?? 0);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        decodedRows = count;
        return allocated;
    }

    private static long PayloadAllocations(LeafSnapshotRow[] rows, out int touched)
    {
        // The floor: exactly what the rehydrated cache needs - one key string
        // and one value array per row - and nothing else.
        var sink = new object[rows.Length * 2];
        var count = 0;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < rows.Length; i++)
        {
            var key = new string(rows[i].Key.AsSpan());
            var value = rows[i].Value.Value!.AsSpan().ToArray();
            sink[i * 2] = key;
            sink[(i * 2) + 1] = value;
            count += key.Length + value.Length;
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(sink);
        touched = count;
        return allocated;
    }

    [Test]
    public void Decode_allocates_no_more_than_the_rehydrated_payload_itself()
    {
        const int rowCount = 512;
        const int valueBytes = 512;
        var rows = BuildRows(rowCount, valueBytes, "vec/");
        var frame = LeafSnapshotCodec.Encode(rows);

        // Warm up both measured paths so first-call JIT and static
        // initialisation are not counted. The measurement itself is a
        // deterministic per-thread allocation counter, not a GC observation, so
        // it does not depend on collection timing.
        _ = DecodeAllocations(frame, out _);
        _ = PayloadAllocations(rows, out _);

        var payload = PayloadAllocations(rows, out var payloadTouched);
        var decode = DecodeAllocations(frame, out var decodeTouched);

        Assert.That(decodeTouched, Is.EqualTo(payloadTouched), "precondition: both paths materialised the same bytes");
        Assert.That(payload, Is.GreaterThan(0L));
        Assert.That(decode, Is.LessThanOrEqualTo(payload + 1024L),
            $"decode allocated {decode} bytes against a {payload}-byte payload floor; the decode path must " +
            "materialise the key string and value array and nothing else - a per-row intermediate buffer " +
            "would roughly double this");
    }

    [Test]
    public void Decode_allocation_does_not_grow_with_a_repeated_decode_of_the_same_frame()
    {
        // Guards against a hidden per-decode cache or a growing scratch buffer:
        // decoding the same frame twice must cost the same.
        var rows = BuildRows(128, 256, "vec/");
        var frame = LeafSnapshotCodec.Encode(rows);
        _ = DecodeAllocations(frame, out _);

        var first = DecodeAllocations(frame, out _);
        var second = DecodeAllocations(frame, out _);

        Assert.That(second, Is.EqualTo(first));
    }

    [Test]
    public void Encode_allocates_exactly_one_buffer_the_size_of_the_frame()
    {
        // The encode path measures first and sizes the frame exactly, so it
        // never grows a buffer and never copies a string through a scratch
        // array. The pooled row buffer the capture path uses is rented, not
        // allocated, so it does not appear here either.
        var rows = BuildRows(256, 256, "vec/");
        _ = LeafSnapshotCodec.Encode(rows);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var frame = LeafSnapshotCodec.Encode(rows);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.LessThanOrEqualTo(frame.Length + 512L),
            $"encode allocated {allocated} bytes for a {frame.Length}-byte frame; only the frame itself " +
            "should be allocated");
    }

    [Test]
    public void Validate_and_state_bytes_walk_a_frame_without_allocating()
    {
        var frame = LeafSnapshotCodec.Encode(BuildRows(256, 256, "vec/"));
        _ = LeafSnapshotCodec.Validate(frame);
        _ = LeafSnapshotCodec.TryComputeStateBytes(frame, out _);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var valid = LeafSnapshotCodec.Validate(frame);
        var summed = LeafSnapshotCodec.TryComputeStateBytes(frame, out _);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(valid, Is.True);
        Assert.That(summed, Is.True);
        Assert.That(allocated, Is.Zero,
            "the validation and footprint walks are pure span reads and must not allocate at all - they run " +
            "on every snapshot load");
    }

    [Test]
    public void Seek_primitives_walk_a_frame_without_allocating()
    {
        var frame = LeafSnapshotCodec.Encode(BuildRows(512, 128, "vec/"));
        var probe = Encoding.UTF8.GetBytes("vec/00000256");
        _ = LeafSnapshotCodec.TryFindFirstRowAtOrAfter(frame, probe, out _);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var found = LeafSnapshotCodec.TryFindFirstRowAtOrAfter(frame, probe, out var index);
        var gotKey = LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, index, out _);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(found, Is.True);
        Assert.That(gotKey, Is.True);
        Assert.That(index, Is.EqualTo(256));
        Assert.That(allocated, Is.Zero,
            "a bounded key-range seek must cost no allocation at all, so a partial hydration pays only for " +
            "the rows it actually materialises");
    }

    // --- Persisted-size comparison, measured through the serializer that
    // --- actually writes the blob.

    private static int PersistedLegacyBytes(LeafSnapshotRow[] rows) =>
        JsonConvert.SerializeObject(new LeafSnapshotBlob
        {
            SnapshotOffset = 1_000L,
            Rows = rows,
            CapturedAtTicks = 638_500_000_000_000_000L,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = [1_000L],
        }).Length;

    private static int PersistedBinaryBytes(LeafSnapshotRow[] rows) =>
        JsonConvert.SerializeObject(new LeafSnapshotBlob
        {
            SnapshotOffset = 1_000L,
            EncodedRows = LeafSnapshotCodec.Encode(rows),
            CapturedAtTicks = 638_500_000_000_000_000L,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = [1_000L],
        }).Length;

    [Test]
    public void Persisted_size_falls_materially_for_a_vector_metadata_leaf()
    {
        // The shape a cold semantic query rehydrates first and in bulk: many
        // rows of vector metadata, where the legacy per-row envelope (property
        // names, a nested timestamp object, five always-emitted metadata
        // fields) dwarfs the value.
        var rows = BuildRows(rowCount: 128, valueBytes: 192, keyPrefix: "vec/meta/");

        var legacy = PersistedLegacyBytes(rows);
        var binary = PersistedBinaryBytes(rows);
        var ratio = (double)binary / legacy;

        TestContext.Out.WriteLine(
            $"metadata leaf: legacy {legacy} bytes, binary {binary} bytes, {(1 - ratio) * 100:F1}% smaller");

        Assert.That(ratio, Is.LessThan(0.75),
            $"the binary encoding must be materially smaller for a metadata-shaped leaf " +
            $"(legacy {legacy}, binary {binary})");
    }

    [Test]
    public void Persisted_size_falls_for_a_packed_vector_payload_leaf()
    {
        // The payload shape: 1024-dimension float32 vectors. Here the value
        // bytes dominate, and the grain-storage serializer still base64s the
        // frame, so the per-row envelope the frame removes is the whole of the
        // available win. The threshold pins the measured figure as a regression
        // guard; removing the residual base64 inflation needs a binary
        // grain-storage serializer, which is outside this codec.
        var rows = BuildRows(rowCount: 96, valueBytes: 4_096, keyPrefix: "vpay/");

        var legacy = PersistedLegacyBytes(rows);
        var binary = PersistedBinaryBytes(rows);
        var ratio = (double)binary / legacy;

        TestContext.Out.WriteLine(
            $"vector payload leaf: legacy {legacy} bytes, binary {binary} bytes, {(1 - ratio) * 100:F1}% smaller");

        Assert.That(binary, Is.LessThan(legacy),
            $"the binary encoding must not be larger for a packed-vector leaf (legacy {legacy}, binary {binary})");
        Assert.That(ratio, Is.LessThan(0.99));
    }

    [Test]
    public void Persisted_size_falls_furthest_for_a_leaf_of_many_small_rows()
    {
        // Membership and index rows: the per-row envelope is several times the
        // payload, which is where the encoding wins hardest.
        var rows = BuildRows(rowCount: 256, valueBytes: 16, keyPrefix: "vmem/");

        var legacy = PersistedLegacyBytes(rows);
        var binary = PersistedBinaryBytes(rows);
        var ratio = (double)binary / legacy;

        TestContext.Out.WriteLine(
            $"membership leaf: legacy {legacy} bytes, binary {binary} bytes, {(1 - ratio) * 100:F1}% smaller");

        Assert.That(ratio, Is.LessThan(0.45));
    }
}
