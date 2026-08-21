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
}
