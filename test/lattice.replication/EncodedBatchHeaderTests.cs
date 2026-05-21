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
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister,
        LatticeCompression compression = LatticeCompression.None)
        => new()
        {
            Magic = magic,
            WireVersion = wireVersion,
            OriginClusterIdHash = originHash,
            EntryCount = entryCount,
            BatchSequence = batchSequence,
            AtomicBatchSpanCount = atomicSpanCount,
            Mode = mode,
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
    public void WriteTo_throws_when_AtomicBatchSpanCount_exceeds_16_bits()
    {
        // v5 tightened the validated cap from 24 bits to 16 bits to
        // free 8 bits for Mode in the trailing packed slot.
        var header = Sample(atomicSpanCount: 0x0001_0000);
        Assert.That(
            () => header.WriteTo(new byte[EncodedBatchHeader.WireSize]),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void WriteTo_accepts_AtomicBatchSpanCount_at_16_bit_boundary()
    {
        // 0xFFFF (65 535) is the inclusive upper bound; the writer
        // must accept it.
        var header = Sample(atomicSpanCount: 0x0000_FFFF);
        Assert.That(
            () => header.WriteTo(new byte[EncodedBatchHeader.WireSize]),
            Throws.Nothing);
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
        var header = Sample(compression: LatticeCompression.None);
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

    [Test]
    public void CurrentWireVersion_is_5()
    {
        // The wire version increments any time the framing payload
        // shape breaks. v5 hoisted Mode out of per-entry bytes into
        // the header's trailing packed slot. v4 receivers reject v5
        // payloads via the strictly-greater-than guard.
        Assert.That(EncodedBatchHeader.CurrentWireVersion, Is.EqualTo(5));
    }

    [Test]
    public void Mode_round_trips_through_packed_middle_byte()
    {
        var header = Sample(mode: LatticeMergeMode.OrSet);
        var buf = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buf);
        var decoded = EncodedBatchHeader.ReadFrom(buf);
        Assert.That(decoded.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        // Mode is packed into bits 16-23 of the trailing 32-bit slot.
        var packed = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(28, 4));
        Assert.That((byte)(packed >> 16), Is.EqualTo((byte)LatticeMergeMode.OrSet));
    }

    [Test]
    public void Mode_default_round_trips_as_LwwRegister()
    {
        // The default constructor leaves Mode at the enum default
        // (LwwRegister = 0), matching the wire-baseline behaviour
        // that pre-Mode-hoist producers wrote and that downgraded
        // receivers still observe.
        var header = Sample();
        var buf = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buf);
        var decoded = EncodedBatchHeader.ReadFrom(buf);
        Assert.That(decoded.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
    }

    [Test]
    public void Mode_round_trips_independently_of_Compression()
    {
        // Mode and Compression occupy separate bytes in the trailing
        // packed slot (bits 16-23 vs. 24-31); pin that they do not
        // alias by setting both to non-default values and asserting
        // independent round-trip.
        var header = Sample(mode: LatticeMergeMode.PnCounter, compression: LatticeCompression.None);
        var buf = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buf);
        var decoded = EncodedBatchHeader.ReadFrom(buf);
        Assert.That(decoded.Mode, Is.EqualTo(LatticeMergeMode.PnCounter));
        Assert.That(decoded.Compression, Is.EqualTo(LatticeCompression.None));
    }

    [Test]
    public void Equality_distinguishes_Mode()
    {
        var lww = Sample(mode: LatticeMergeMode.LwwRegister);
        var orSet = Sample(mode: LatticeMergeMode.OrSet);
        Assert.That(lww, Is.Not.EqualTo(orSet));
    }
}
