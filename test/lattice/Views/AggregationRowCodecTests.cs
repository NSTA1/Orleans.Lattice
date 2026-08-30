using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for the aggregation view internal row codec.</summary>
[TestFixture]
public class AggregationRowCodecTests
{
    [Test]
    public void IsReservedGroupKey_true_for_empty_and_nul_prefixed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AggregationRowCodec.IsReservedGroupKey(string.Empty), Is.True);
            Assert.That(AggregationRowCodec.IsReservedGroupKey("\u0000abc"), Is.True);
        });
    }

    [Test]
    public void IsReservedGroupKey_false_for_normal_key()
    {
        Assert.That(AggregationRowCodec.IsReservedGroupKey("group"), Is.False);
    }

    [Test]
    public void EmptyRow_is_recognised_by_IsEmpty()
    {
        Assert.That(AggregationRowCodec.IsEmpty(AggregationRowCodec.EmptyRow()), Is.True);
    }

    [Test]
    public void IsEmpty_false_for_multi_byte_payload()
    {
        Assert.That(AggregationRowCodec.IsEmpty(new byte[] { 0x00, 0x00 }), Is.False);
    }

    [Test]
    public void Key_builders_produce_distinct_reserved_prefixed_keys()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AggregationRowCodec.MembershipKey("s"), Is.EqualTo("\u0000ms"));
            Assert.That(AggregationRowCodec.AccumulatorKey("g", 2), Is.EqualTo("\u0000ag\u00002"));
            Assert.That(AggregationRowCodec.InverseKey("g", 3), Is.EqualTo("\u0000ig\u00003"));
            Assert.That(AggregationRowCodec.FoldInverseKey("g", 4), Is.EqualTo("\u0000fg\u00004"));
            Assert.That(AggregationRowCodec.FirstNonReservedKey, Is.EqualTo("\u0001"));
        });
    }

    [Test]
    public void Slot_is_zero_for_fanout_one_or_less()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AggregationRowCodec.Slot("anything", 1), Is.EqualTo(0));
            Assert.That(AggregationRowCodec.Slot("anything", 0), Is.EqualTo(0));
        });
    }

    [Test]
    public void Slot_is_deterministic_and_in_range()
    {
        var a = AggregationRowCodec.Slot("source-key", 8);
        var b = AggregationRowCodec.Slot("source-key", 8);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.InRange(0, 7));
        });
    }

    [Test]
    public void Membership_round_trips_with_member()
    {
        var row = new AggregationRowCodec.MembershipRow("grp", 12.5, "mbr");

        var decoded = AggregationRowCodec.DecodeMembership(AggregationRowCodec.EncodeMembership(row));

        Assert.That(decoded, Is.EqualTo(row));
    }

    [Test]
    public void Membership_round_trips_without_member()
    {
        var row = new AggregationRowCodec.MembershipRow("grp", 3.0, null);

        var decoded = AggregationRowCodec.DecodeMembership(AggregationRowCodec.EncodeMembership(row));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.GroupKey, Is.EqualTo("grp"));
            Assert.That(decoded.Numeric, Is.EqualTo(3.0));
            Assert.That(decoded.Member, Is.Null);
        });
    }

    [Test]
    public void Accumulator_round_trips_and_is_sixteen_bytes()
    {
        var row = new AggregationRowCodec.AccumulatorRow(7, 42.25);

        var bytes = AggregationRowCodec.EncodeAccumulator(row);
        var decoded = AggregationRowCodec.DecodeAccumulator(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(bytes, Has.Length.EqualTo(16));
            Assert.That(decoded, Is.EqualTo(row));
        });
    }

    [Test]
    public void Inverse_round_trips_mixed_entries()
    {
        var entries = new Dictionary<string, AggregationRowCodec.MemberEntry>
        {
            ["s1"] = new(1.5, "m1"),
            ["s2"] = new(2.5, null),
        };

        var decoded = AggregationRowCodec.DecodeInverse(AggregationRowCodec.EncodeInverse(entries));

        Assert.Multiple(() =>
        {
            Assert.That(decoded["s1"], Is.EqualTo(new AggregationRowCodec.MemberEntry(1.5, "m1")));
            Assert.That(decoded["s2"], Is.EqualTo(new AggregationRowCodec.MemberEntry(2.5, null)));
        });
    }

    [Test]
    public void FoldInverse_round_trips_value_and_timestamp()
    {
        var ts = new HybridLogicalClock { WallClockTicks = 123, Counter = 4 };
        var entries = new Dictionary<string, AggregationRowCodec.FoldMember>
        {
            ["s1"] = new(new byte[] { 9, 8, 7 }, ts),
        };

        var decoded = AggregationRowCodec.DecodeFoldInverse(AggregationRowCodec.EncodeFoldInverse(entries));

        Assert.Multiple(() =>
        {
            Assert.That(decoded["s1"].Value, Is.EqualTo(new byte[] { 9, 8, 7 }));
            Assert.That(decoded["s1"].Timestamp, Is.EqualTo(ts));
        });
    }

    // The row payloads are persisted opaquely in the view tree, so the encoders
    // must stay byte-for-byte compatible with the BinaryWriter layout they were
    // written with (7-bit length-prefixed UTF-8 strings, one bool byte,
    // little-endian numerics, raw value bytes). These tests pin that wire format
    // against a reference BinaryWriter across empty, unicode, null-member,
    // long-string (multi-byte length prefix) and empty-collection inputs.

    private static readonly string LongAscii = new('x', 200);
    private static readonly string Unicode = "grp-\u00e9\u00fc-\u4e2d\u6587-\U0001F600";

    private static byte[] ReferenceMembership(in AggregationRowCodec.MembershipRow row)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8);
        writer.Write(row.GroupKey);
        writer.Write(row.Member is not null);
        writer.Write(row.Numeric);
        if (row.Member is not null)
        {
            writer.Write(row.Member);
        }

        writer.Flush();
        return stream.ToArray();
    }

    private static byte[] ReferenceInverse(IReadOnlyDictionary<string, AggregationRowCodec.MemberEntry> entries)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8);
        writer.Write(entries.Count);
        foreach (var (sourceKey, entry) in entries)
        {
            writer.Write(sourceKey);
            writer.Write(entry.Member is not null);
            writer.Write(entry.Numeric);
            if (entry.Member is not null)
            {
                writer.Write(entry.Member);
            }
        }

        writer.Flush();
        return stream.ToArray();
    }

    private static byte[] ReferenceFoldInverse(IReadOnlyDictionary<string, AggregationRowCodec.FoldMember> entries)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8);
        writer.Write(entries.Count);
        foreach (var (sourceKey, entry) in entries)
        {
            writer.Write(sourceKey);
            writer.Write(entry.Timestamp.WallClockTicks);
            writer.Write(entry.Timestamp.Counter);
            writer.Write(entry.Value.Length);
            writer.Write(entry.Value);
        }

        writer.Flush();
        return stream.ToArray();
    }

    private static IEnumerable<AggregationRowCodec.MembershipRow> MembershipCases()
    {
        yield return new AggregationRowCodec.MembershipRow("grp", 12.5, "mbr");
        yield return new AggregationRowCodec.MembershipRow("grp", 3.0, null);
        yield return new AggregationRowCodec.MembershipRow(string.Empty, 0.0, string.Empty);
        yield return new AggregationRowCodec.MembershipRow(Unicode, -1.5, Unicode);
        yield return new AggregationRowCodec.MembershipRow(LongAscii, double.MaxValue, LongAscii);
    }

    [Test]
    public void EncodeMembership_is_byte_identical_to_reference()
    {
        foreach (var row in MembershipCases())
        {
            Assert.That(AggregationRowCodec.EncodeMembership(row), Is.EqualTo(ReferenceMembership(row)));
        }
    }

    [Test]
    public void EncodeInverse_is_byte_identical_to_reference()
    {
        foreach (var entries in new[]
        {
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal),
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
            {
                ["s1"] = new(1.5, "m1"),
                ["s2"] = new(2.5, null),
                [Unicode] = new(double.MinValue, Unicode),
                [LongAscii] = new(0.0, LongAscii),
            },
        })
        {
            Assert.That(AggregationRowCodec.EncodeInverse(entries), Is.EqualTo(ReferenceInverse(entries)));
        }
    }

    [Test]
    public void EncodeFoldInverse_is_byte_identical_to_reference()
    {
        var ts = new HybridLogicalClock { WallClockTicks = 123, Counter = 4 };
        foreach (var entries in new[]
        {
            new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal),
            new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
            {
                ["s1"] = new(new byte[] { 9, 8, 7 }, ts),
                ["s2"] = new(Array.Empty<byte>(), HybridLogicalClock.Zero),
                [Unicode] = new(new byte[300], new HybridLogicalClock { WallClockTicks = long.MaxValue, Counter = int.MaxValue }),
            },
        })
        {
            Assert.That(AggregationRowCodec.EncodeFoldInverse(entries), Is.EqualTo(ReferenceFoldInverse(entries)));
        }
    }
}
