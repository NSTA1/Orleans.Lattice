using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// The allocation claim <see cref="LatticeGrainStorageSerializer"/> makes when
/// it writes a <see cref="LeafSnapshotBlob"/> (issue #2733).
/// <para>
/// This is not a micro-optimisation. On the deployment that motivated the
/// issue, every snapshot capture on the affected tree died here:
/// <c>ArrayBufferWriter{T}.CheckAndResizeBuffer</c> ->
/// <c>Writer{T}.Allocate</c> -> <c>Serializer.Serialize</c> ->
/// <c>LatticeGrainStorageSerializer.Serialize</c>, throwing
/// <see cref="OutOfMemoryException"/> 2,761 times in a 25-minute window across
/// 32 distinct leaves. A capture that cannot complete never advances the
/// leaf's durable coverage, so the leaf keeps a block pin and its tree's
/// entire WAL stays retained.
/// </para>
/// <para>
/// <b>Why total thread allocation is the right instrument for a claim about
/// PEAK CONTIGUOUS allocation.</b> The two are not the same quantity in
/// general, but they are tightly coupled for this specific code, because every
/// byte of the old path's excess came from one mechanism:
/// <c>ArrayBufferWriter</c> is backed by a single array that it grows by
/// doubling, and each growth holds the old array and the new one at once.
/// Excess total allocation is therefore exactly a count of superseded
/// contiguous arrays, and driving the total to ~1x payload is only achievable
/// by never allocating a second full-size array. A segmented pooled writer
/// gets there because its pages are individually small, pooled, and never
/// copied into one another.
/// </para>
/// <para>
/// The bound is calibrated inside the test against the serialized length the
/// serializer actually produced, so it states a property of this code rather
/// than of any particular runtime's array-growth constants or object headers.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeGrainStorageSerializerAllocationTests
{
    private ServiceProvider provider = null!;
    private Serializer serializer = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        this.provider = services.BuildServiceProvider();
        this.serializer = this.provider.GetRequiredService<Serializer>();
    }

    [TearDown]
    public void TearDown() => this.provider?.Dispose();

    private LatticeGrainStorageSerializer CreateSerializer()
        => new(this.serializer, new ThrowingFallbackSerializer());

    /// <summary>
    /// A fallback that must never be reached. Every type these fixtures write is
    /// marked <c>ILatticeBinaryPersistedState</c>, so a delegation to the
    /// fallback would mean the test measured the JSON path instead of the
    /// binary one - which would pass the allocation bound for entirely the
    /// wrong reason. Throwing turns that into a failure rather than a false
    /// green.
    /// </summary>
    private sealed class ThrowingFallbackSerializer : IGrainStorageSerializer
    {
        public BinaryData Serialize<T>(T value)
            => throw new InvalidOperationException(
                $"{typeof(T)} was routed to the fallback serializer, so it is not marked "
                + "ILatticeBinaryPersistedState and this fixture is not measuring the binary path.");

        public T Deserialize<T>(BinaryData input)
            => throw new InvalidOperationException(
                $"{typeof(T)} was routed to the fallback serializer on read.");
    }

    /// <summary>
    /// Builds a blob whose payload is dominated by one large contiguous value,
    /// which is the shape of a real leaf snapshot: row keys are short and row
    /// values are the bulk.
    /// </summary>
    private static LeafSnapshotBlob BuildBlob(int rowCount, int valueBytes)
    {
        var rows = new List<LeafSnapshotRow>(rowCount);
        for (var i = 0; i < rowCount; i++)
        {
            var value = new byte[valueBytes];

            // Vary the bytes so nothing downstream can dedupe or compress the
            // payload into something unrepresentative of a real embedding.
            for (var b = 0; b < valueBytes; b++)
            {
                value[b] = (byte)((i * 31) + b);
            }

            rows.Add(new LeafSnapshotRow(
                $"k{i:D8}",
                LwwValue<byte[]>.Create(
                    value,
                    new HybridLogicalClock { WallClockTicks = 1_000L + i, Counter = i & 7 })));
        }

        return new LeafSnapshotBlob
        {
            SnapshotOffset = 0L,
            Rows = rows,
            CapturedAtTicks = 1_234_567L,
            SnapshotBytes = (long)rowCount * valueBytes,
            SnapshotOffsetsByPartition = [0L],
        };
    }

    /// <summary>
    /// THE acceptance assertion for the serializer half of issue #2733, and the
    /// one that is RED on the pre-fix implementation.
    /// <para>
    /// Measured at ~2.75x on the <c>ArrayBufferWriter</c> shape this replaced
    /// and ~1.00x on the two-pass shape, so the 1.5x bound below sits well clear
    /// of both and is not a threshold tuned to pass by a hair.
    /// </para>
    /// </summary>
    [Test]
    public void Serialize_allocates_about_one_payload_for_a_large_leaf_snapshot()
    {
        var sut = this.CreateSerializer();
        var blob = BuildBlob(rowCount: 4, valueBytes: 1024 * 1024);

        // Warm up: the first call through this path JITs the generated
        // serializer for the type and populates the buffer pool, and charging
        // that one-off cost to the measurement would swamp the signal.
        var warm = sut.Serialize(blob);
        var payloadBytes = warm.ToMemory().Length;

        var before = GC.GetAllocatedBytesForCurrentThread();
        var written = sut.Serialize(blob);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(
                written.ToMemory().Length, Is.EqualTo(payloadBytes),
                "the two calls did not produce the same payload, so the ratio below would be "
                + "measured against the wrong denominator.");
            Assert.That(
                allocated, Is.LessThan(payloadBytes * 1.5),
                $"serializing a {payloadBytes:N0}-byte payload allocated {allocated:N0} bytes "
                + $"({(double)allocated / payloadBytes:F2}x). Every byte above ~1x is a superseded "
                + "contiguous array from a growth-by-doubling writer, and it is exactly that "
                + "second full-size array that threw OutOfMemoryException in production. Serialize "
                + "into a segmented pooled writer first, then allocate ONE exact-size array.");
        });
    }

    /// <summary>
    /// The two-pass rewrite must not change a single byte on the wire.
    /// <para>
    /// Load-bearing, and the reason it is a separate fixture: the switch to a
    /// segmented writer is only safe if the bytes are identical, since rows
    /// already on disk stay readable and rows written now must stay readable by
    /// an older build. An allocation win that silently altered the encoding
    /// would be a data-loss bug wearing a performance-improvement label.
    /// </para>
    /// </summary>
    [Test]
    public void Serialize_round_trips_a_large_blob_unchanged()
    {
        var sut = this.CreateSerializer();
        var blob = BuildBlob(rowCount: 4, valueBytes: 256 * 1024);

        var round = sut.Deserialize<LeafSnapshotBlob>(sut.Serialize(blob));

        Assert.Multiple(() =>
        {
            Assert.That(round.Rows, Has.Count.EqualTo(blob.Rows!.Count));
            Assert.That(round.SnapshotBytes, Is.EqualTo(blob.SnapshotBytes));
            Assert.That(round.SnapshotOffset, Is.EqualTo(blob.SnapshotOffset));
            for (var i = 0; i < blob.Rows!.Count; i++)
            {
                Assert.That(round.Rows![i].Key, Is.EqualTo(blob.Rows[i].Key));
                Assert.That(
                    round.Rows![i].Value.Value, Is.EqualTo(blob.Rows[i].Value.Value),
                    $"row {i} did not survive the round trip byte-for-byte.");
            }
        });
    }

    /// <summary>
    /// The payload the two-pass path writes must be byte-identical to what the
    /// single-pass <c>ArrayBufferWriter</c> shape wrote, not merely
    /// round-trippable by this build.
    /// <para>
    /// Round-tripping proves the pair of methods agree with each other, which a
    /// changed encoding would also satisfy. This compares against the ORIGINAL
    /// construction - magic prefix, then
    /// <c>Serializer.Serialize</c> into a contiguous writer - so it would catch
    /// an encoding change that both halves of this build agreed on and an older
    /// build could not read.
    /// </para>
    /// </summary>
    [Test]
    public void Serialize_matches_the_bytes_the_contiguous_writer_produced()
    {
        var sut = this.CreateSerializer();
        var blob = BuildBlob(rowCount: 3, valueBytes: 64 * 1024);

        var expected = new ArrayBufferWriter<byte>();
        LatticeGrainStorageSerializer.BinaryMagic.CopyTo(
            expected.GetSpan(LatticeGrainStorageSerializer.BinaryMagic.Length));
        expected.Advance(LatticeGrainStorageSerializer.BinaryMagic.Length);
        this.serializer.Serialize(blob, expected);

        Assert.That(
            sut.Serialize(blob).ToArray(), Is.EqualTo(expected.WrittenMemory.ToArray()),
            "the two-pass path produced different bytes from the contiguous path it replaced. "
            + "That is a wire-format change, not an optimisation: rows already on disk must stay "
            + "readable and rows written now must stay readable by an older build.");
    }
}
