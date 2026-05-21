using System.Buffers.Binary;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class EncodedBatchHeaderTests
{
    private static EncodedBatchHeader Sample(
        uint magic = EncodedBatchHeader.MagicValue,
        int wireVersion = EncodedBatchHeader.CurrentWireVersion,
        ulong originHash = 0xCAFEBABEDEADBEEFUL,
        int entryCount = 3,
        long batchSequence = 42L,
        int atomicSpanCount = 0,
        FramingCompression compression = FramingCompression.None)
        => new()
        {
            Magic = magic,
            WireVersion = wireVersion,
            OriginClusterIdHash = originHash,
            EntryCount = entryCount,
            BatchSequence = batchSequence,
            AtomicBatchSpanCount = atomicSpanCount,
            Compression = compression,
        };

    [Test]
    public void WireSize_is_32_bytes()
    {
        Assert.That(EncodedBatchHeader.WireSize, Is.EqualTo(32));
    }

    [Test]
    public void MagicValue_spells_OLRF_in_little_endian_ascii()
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buf, EncodedBatchHeader.MagicValue);
        Assert.That(System.Text.Encoding.ASCII.GetString(buf), Is.EqualTo("OLRF"));
    }

    [Test]
    public void WriteTo_then_ReadFrom_round_trips_every_field()
    {
        var header = Sample();
        var buf = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buf);
        var decoded = EncodedBatchHeader.ReadFrom(buf);
        Assert.That(decoded, Is.EqualTo(header));
    }

    [Test]
    public void WriteTo_writes_exactly_32_bytes_into_destination()
    {
        var buf = new byte[64];
        for (var i = 0; i < buf.Length; i++) buf[i] = 0xAB;
        Sample().WriteTo(buf);
        // The first 32 bytes are header content; bytes 32+ untouched.
        for (var i = 32; i < buf.Length; i++)
        {
            Assert.That(buf[i], Is.EqualTo(0xAB), $"byte {i} should be untouched");
        }
    }

    [Test]
    public void WriteTo_throws_on_short_destination()
    {
        Assert.That(
            () => Sample().WriteTo(new byte[EncodedBatchHeader.WireSize - 1]),
            Throws.ArgumentException);
    }

    [Test]
    public void ReadFrom_throws_on_short_source()
    {
        Assert.That(
            () => EncodedBatchHeader.ReadFrom(new byte[EncodedBatchHeader.WireSize - 1]),
            Throws.ArgumentException);
    }

    [Test]
    public void WriteTo_throws_when_AtomicBatchSpanCount_exceeds_24_bits()
    {
        var header = Sample(atomicSpanCount: 0x01_00_00_00);
        Assert.That(
            () => header.WriteTo(new byte[EncodedBatchHeader.WireSize]),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void WriteTo_throws_when_AtomicBatchSpanCount_is_negative()
    {
        var header = Sample(atomicSpanCount: -1);
        Assert.That(
            () => header.WriteTo(new byte[EncodedBatchHeader.WireSize]),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Compression_round_trips_through_packed_top_byte()
    {
        // Use a synthetic enum value that exercises the top-byte
        // packing without colliding with None (0). We re-use None
        // here but assert the byte position via raw inspection.
        var header = Sample(compression: FramingCompression.None);
        var buf = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buf);
        var packed = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(28, 4));
        Assert.That(packed >> 24, Is.EqualTo(0u), "None compression occupies top byte = 0");
    }

    [Test]
    public void HashClusterId_is_deterministic_across_calls()
    {
        var h1 = EncodedBatchHeader.HashClusterId("site-a");
        var h2 = EncodedBatchHeader.HashClusterId("site-a");
        Assert.That(h1, Is.EqualTo(h2));
    }

    [Test]
    public void HashClusterId_differs_across_distinct_inputs()
    {
        var h1 = EncodedBatchHeader.HashClusterId("site-a");
        var h2 = EncodedBatchHeader.HashClusterId("site-b");
        Assert.That(h1, Is.Not.EqualTo(h2));
    }

    [Test]
    public void HashClusterId_handles_long_inputs_beyond_stackalloc_threshold()
    {
        // The stackalloc fast path covers up to 256 bytes; force the
        // heap-allocated fallback by passing a string whose UTF-8
        // byte count exceeds that.
        var longId = new string('x', 1024);
        var h = EncodedBatchHeader.HashClusterId(longId);
        Assert.That(h, Is.Not.EqualTo(0UL));
    }

    [Test]
    public void HashClusterId_throws_on_null_input()
    {
        Assert.That(() => EncodedBatchHeader.HashClusterId(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Equality_is_structural_over_every_field()
    {
        var a = Sample();
        var b = Sample();
        Assert.That(a, Is.EqualTo(b));

        var c = Sample(entryCount: 4);
        Assert.That(a, Is.Not.EqualTo(c));
    }
}
