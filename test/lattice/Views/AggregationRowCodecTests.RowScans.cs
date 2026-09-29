using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for the allocation-free row cursors the group re-materialise paths
/// walk instead of decoding a keyed map.
/// <para>
/// The property that matters is that a cursor is a strict substitute for its
/// decoder: over the same row it must yield the same entries in the same order,
/// and it must reject the same malformed rows at the same byte. A cursor that
/// merely skipped validation would be faster and wrong, so the hostile-row cases
/// below are the load-bearing half of this fixture.
/// </para>
/// </summary>
public partial class AggregationRowCodecTests
{
    private const string Unicode2 = "\u00e9\u4e2d\ud83d\ude00";

    private static Dictionary<string, AggregationRowCodec.MemberEntry> InverseCorpus() => new(StringComparer.Ordinal)
    {
        ["src-a"] = new AggregationRowCodec.MemberEntry(1.5, "m-a"),
        ["src-b"] = new AggregationRowCodec.MemberEntry(-2.25, null),
        [Unicode2] = new AggregationRowCodec.MemberEntry(double.MaxValue, Unicode2),
        [string.Empty] = new AggregationRowCodec.MemberEntry(0.0, string.Empty),
    };

    private static Dictionary<string, AggregationRowCodec.FoldMember> FoldCorpus() => new(StringComparer.Ordinal)
    {
        ["src-a"] = new AggregationRowCodec.FoldMember([1, 2, 3], new HybridLogicalClock { WallClockTicks = 10, Counter = 1 }),
        ["src-b"] = new AggregationRowCodec.FoldMember([], new HybridLogicalClock { WallClockTicks = 20, Counter = 0 }),
        [Unicode2] = new AggregationRowCodec.FoldMember([9], new HybridLogicalClock { WallClockTicks = long.MaxValue, Counter = int.MaxValue }),
    };

    [Test]
    public void InverseRowScan_MoveNext_yields_the_same_entries_as_DecodeInverse()
    {
        var corpus = InverseCorpus();
        var bytes = AggregationRowCodec.EncodeInverse(corpus);
        var expected = AggregationRowCodec.DecodeInverse(bytes);

        var walked = new List<(double Numeric, string? Member)>();
        var scan = new AggregationRowCodec.InverseRowScan(bytes);
        Assert.That(scan.Remaining, Is.EqualTo(corpus.Count));
        while (scan.MoveNext())
        {
            walked.Add((scan.Numeric, scan.Member));
        }

        // The cursor drops the source key, so compare against the decoded values
        // in the decoder's own enumeration order - which is the encode order.
        var decoded = expected.Values.Select(v => (v.Numeric, v.Member)).ToList();
        var remaining = scan.Remaining;
        Assert.Multiple(() =>
        {
            Assert.That(walked, Has.Count.EqualTo(decoded.Count));
            Assert.That(walked, Is.EquivalentTo(decoded));
            Assert.That(remaining, Is.Zero);
        });
    }

    [Test]
    public void InverseRowScan_MoveNextNumeric_yields_every_numeric_and_no_member()
    {
        var corpus = InverseCorpus();
        var bytes = AggregationRowCodec.EncodeInverse(corpus);

        var numerics = new List<double>();
        var scan = new AggregationRowCodec.InverseRowScan(bytes);
        while (scan.MoveNextNumeric())
        {
            numerics.Add(scan.Numeric);
            // The min/max walk steps over the member rather than transcoding it;
            // reading one would mean the skip had not happened.
            Assert.That(scan.Member, Is.Null);
        }

        Assert.That(numerics, Is.EquivalentTo(corpus.Values.Select(v => v.Numeric)));
    }

    [Test]
    public void InverseRowScan_MoveNextNumeric_and_MoveNext_agree_on_the_numerics()
    {
        var bytes = AggregationRowCodec.EncodeInverse(InverseCorpus());

        var viaSkip = new List<double>();
        var skipping = new AggregationRowCodec.InverseRowScan(bytes);
        while (skipping.MoveNextNumeric())
        {
            viaSkip.Add(skipping.Numeric);
        }

        var viaRead = new List<double>();
        var reading = new AggregationRowCodec.InverseRowScan(bytes);
        while (reading.MoveNext())
        {
            viaRead.Add(reading.Numeric);
        }

        Assert.That(viaSkip, Is.EqualTo(viaRead));
    }

    [Test]
    public void InverseRowScan_over_an_empty_map_yields_nothing()
    {
        var bytes = AggregationRowCodec.EncodeInverse(new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal));

        var scan = new AggregationRowCodec.InverseRowScan(bytes);
        var remaining = scan.Remaining;
        var movedNext = scan.MoveNext();
        var movedNumeric = new AggregationRowCodec.InverseRowScan(bytes).MoveNextNumeric();
        Assert.Multiple(() =>
        {
            Assert.That(remaining, Is.Zero);
            Assert.That(movedNext, Is.False);
            Assert.That(movedNumeric, Is.False);
        });
    }

    [Test]
    public void FoldInverseRowScan_yields_the_same_entries_as_DecodeFoldInverse()
    {
        var corpus = FoldCorpus();
        var bytes = AggregationRowCodec.EncodeFoldInverse(corpus);
        var expected = AggregationRowCodec.DecodeFoldInverse(bytes);

        var walked = new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal);
        var scan = new AggregationRowCodec.FoldInverseRowScan(bytes);
        Assert.That(scan.Remaining, Is.EqualTo(corpus.Count));
        while (scan.MoveNext())
        {
            walked[scan.SourceKey] = scan.Member;
        }

        var remaining = scan.Remaining;
        Assert.Multiple(() =>
        {
            Assert.That(walked, Has.Count.EqualTo(expected.Count));
            Assert.That(remaining, Is.Zero);
        });

        foreach (var (key, member) in expected)
        {
            Assert.That(walked.ContainsKey(key), Is.True, $"missing source key '{key}'");
            Assert.Multiple(() =>
            {
                Assert.That(walked[key].Value, Is.EqualTo(member.Value));
                Assert.That(walked[key].Timestamp, Is.EqualTo(member.Timestamp));
            });
        }
    }

    [Test]
    public void FoldInverseRowScan_over_an_empty_map_yields_nothing()
    {
        var bytes = AggregationRowCodec.EncodeFoldInverse(new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal));

        var scan = new AggregationRowCodec.FoldInverseRowScan(bytes);
        var remaining = scan.Remaining;
        var moved = scan.MoveNext();
        Assert.Multiple(() =>
        {
            Assert.That(remaining, Is.Zero);
            Assert.That(moved, Is.False);
        });
    }

    [Test]
    public void InverseRowScan_rejects_a_truncated_row_like_the_decoder()
    {
        var bytes = AggregationRowCodec.EncodeInverse(InverseCorpus());
        var truncated = bytes[..(bytes.Length - 3)];

        // Both walks must reject it, not just the one that materialises strings:
        // a skip is a gate, not a shortcut past one.
        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidDataException>(() => AggregationRowCodec.DecodeInverse(truncated));
            Assert.Throws<InvalidDataException>(() =>
            {
                var scan = new AggregationRowCodec.InverseRowScan(truncated);
                while (scan.MoveNext())
                {
                }
            });
            Assert.Throws<InvalidDataException>(() =>
            {
                var scan = new AggregationRowCodec.InverseRowScan(truncated);
                while (scan.MoveNextNumeric())
                {
                }
            });
        });
    }

    [Test]
    public void InverseRowScan_rejects_an_overstated_entry_count_like_the_decoder()
    {
        // A four-byte count of int.MaxValue over a near-empty row: the bounded
        // count gate must refuse it before anything is sized from it.
        var hostile = new byte[] { 0xFF, 0xFF, 0xFF, 0x7F, 0x00 };

        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidDataException>(() => AggregationRowCodec.DecodeInverse(hostile));
            Assert.Throws<InvalidDataException>(() => _ = new AggregationRowCodec.InverseRowScan(hostile));
        });
    }

    [Test]
    public void FoldInverseRowScan_rejects_a_truncated_row_like_the_decoder()
    {
        var bytes = AggregationRowCodec.EncodeFoldInverse(FoldCorpus());
        var truncated = bytes[..(bytes.Length - 2)];

        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidDataException>(() => AggregationRowCodec.DecodeFoldInverse(truncated));
            Assert.Throws<InvalidDataException>(() =>
            {
                var scan = new AggregationRowCodec.FoldInverseRowScan(truncated);
                while (scan.MoveNext())
                {
                }
            });
        });
    }

    [Test]
    public void FoldInverseRowScan_rejects_an_overstated_entry_count_like_the_decoder()
    {
        var hostile = new byte[] { 0xFF, 0xFF, 0xFF, 0x7F, 0x00 };

        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidDataException>(() => AggregationRowCodec.DecodeFoldInverse(hostile));
            Assert.Throws<InvalidDataException>(() => _ = new AggregationRowCodec.FoldInverseRowScan(hostile));
        });
    }
}
