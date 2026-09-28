using System.Buffers.Binary;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression tests for decoding a hostile or corrupt aggregation row.
/// <para>
/// These rows are not purely local. Under <c>LatticeViewReplicationMode.ShipView</c>
/// the <c>view-{name}</c> tree is itself replicated, the inbound apply path
/// deliberately bypasses the protected-view write guard so a consumer receives its
/// tree, and no key-character validation rejects the NUL-prefixed reserved row keys
/// there. A replication peer can therefore plant an arbitrary byte payload under
/// an inverse or fold-inverse row key, which the producing cluster's view maintainer
/// then decodes.
/// </para>
/// <para>
/// So every field a row declares about itself - entry counts and byte lengths - is
/// attacker-controlled and must be bounded against the bytes actually present
/// before anything is sized from it.
/// </para>
/// </summary>
public partial class AggregationRowCodecTests
{
    /// <summary>Builds a row whose leading int32 entry count is <paramref name="declaredCount"/>, followed by <paramref name="trailingBytes"/> bytes of body.</summary>
    private static byte[] RowDeclaring(int declaredCount, int trailingBytes)
    {
        var bytes = new byte[sizeof(int) + trailingBytes];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, declaredCount);
        return bytes;
    }

    // --- The entry count must be bounded before it pre-sizes a dictionary ---

    [Test]
    public void DecodeInverse_rejects_an_entry_count_the_row_cannot_hold()
    {
        // 100 entries claimed, but 20 bytes of body can hold at most 2.
        var hostile = RowDeclaring(declaredCount: 100, trailingBytes: 20);

        Assert.That(
            () => AggregationRowCodec.DecodeInverse(hostile),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeFoldInverse_rejects_an_entry_count_the_row_cannot_hold()
    {
        var hostile = RowDeclaring(declaredCount: 100, trailingBytes: 20);

        Assert.That(
            () => AggregationRowCodec.DecodeFoldInverse(hostile),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeInverse_rejects_a_maximal_entry_count_without_allocating()
    {
        // The security case: four bytes of wire declaring int.MaxValue entries once
        // pre-sized a Dictionary, turning a tiny peer-planted row into a
        // multi-gigabyte allocation on the view maintainer. The bound must reject
        // it before the dictionary is constructed.
        var hostile = RowDeclaring(declaredCount: int.MaxValue, trailingBytes: 16);

        Assert.That(
            () => AggregationRowCodec.DecodeInverse(hostile),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeFoldInverse_rejects_a_maximal_entry_count_without_allocating()
    {
        var hostile = RowDeclaring(declaredCount: int.MaxValue, trailingBytes: 16);

        Assert.That(
            () => AggregationRowCodec.DecodeFoldInverse(hostile),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeInverse_rejects_a_negative_entry_count()
    {
        var hostile = RowDeclaring(declaredCount: -1, trailingBytes: 16);

        Assert.That(
            () => AggregationRowCodec.DecodeInverse(hostile),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeFoldInverse_rejects_a_negative_entry_count()
    {
        var hostile = RowDeclaring(declaredCount: -1, trailingBytes: 16);

        Assert.That(
            () => AggregationRowCodec.DecodeFoldInverse(hostile),
            Throws.InstanceOf<InvalidDataException>());
    }

    // --- A truncated or sentinel row is a framing fault, not an index fault ---

    [Test]
    public void DecodeInverse_rejects_the_empty_sentinel_row()
    {
        // EmptyRow() is a single 0x00 byte, which is too short even for the count.
        Assert.That(
            () => AggregationRowCodec.DecodeInverse(AggregationRowCodec.EmptyRow()),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeFoldInverse_rejects_the_empty_sentinel_row()
    {
        Assert.That(
            () => AggregationRowCodec.DecodeFoldInverse(AggregationRowCodec.EmptyRow()),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeInverse_rejects_a_row_truncated_mid_entry()
    {
        // One entry declared, then a source key but no bool/double body.
        var bytes = new byte[] { 0x01, 0x00, 0x00, 0x00, 0x01, (byte)'s' };

        Assert.That(
            () => AggregationRowCodec.DecodeInverse(bytes),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeInverse_rejects_a_string_longer_than_the_row()
    {
        // One entry, whose source key declares 200 bytes that are not present.
        var bytes = new byte[] { 0x01, 0x00, 0x00, 0x00, 200, 0x01 };

        Assert.That(
            () => AggregationRowCodec.DecodeInverse(bytes),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeMembership_rejects_a_truncated_row()
    {
        Assert.That(
            () => AggregationRowCodec.DecodeMembership(AggregationRowCodec.EmptyRow()),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void DecodeFoldInverse_rejects_a_value_length_longer_than_the_row()
    {
        // One entry: empty source key, HLC, then a declared 0x7FFFFFFF-byte value.
        var bytes = new byte[sizeof(int) + 1 + sizeof(long) + sizeof(int) + sizeof(int)];
        var span = bytes.AsSpan();
        BinaryPrimitives.WriteInt32LittleEndian(span, 1);
        span[4] = 0x00;
        BinaryPrimitives.WriteInt64LittleEndian(span[5..], 1L);
        BinaryPrimitives.WriteInt32LittleEndian(span[13..], 1);
        BinaryPrimitives.WriteInt32LittleEndian(span[17..], int.MaxValue);

        Assert.That(
            () => AggregationRowCodec.DecodeFoldInverse(bytes),
            Throws.InstanceOf<InvalidDataException>());
    }

    // --- Hardening must not change what a well-formed row decodes to ---

    [Test]
    public void A_well_formed_inverse_row_at_the_bound_still_round_trips()
    {
        var entries = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal);
        for (var i = 0; i < 64; i++)
        {
            entries[$"source-{i}"] = new AggregationRowCodec.MemberEntry(i * 1.5, i % 2 == 0 ? null : $"m{i}");
        }

        var decoded = AggregationRowCodec.DecodeInverse(AggregationRowCodec.EncodeInverse(entries));

        Assert.That(decoded, Is.EquivalentTo(entries));
    }

    [Test]
    public void A_well_formed_fold_inverse_row_with_empty_values_still_round_trips()
    {
        // The minimum-size entry: an empty source key and a zero-length value. This
        // is the shape the entry-count bound is computed from, so it must survive.
        var entries = new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
        {
            [string.Empty] = new([], new HybridLogicalClock { WallClockTicks = 5, Counter = 1 }),
        };

        var decoded = AggregationRowCodec.DecodeFoldInverse(AggregationRowCodec.EncodeFoldInverse(entries));

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Has.Count.EqualTo(1));
            Assert.That(decoded[string.Empty].Value, Is.Empty);
            Assert.That(decoded[string.Empty].Timestamp.WallClockTicks, Is.EqualTo(5));
        });
    }
}
