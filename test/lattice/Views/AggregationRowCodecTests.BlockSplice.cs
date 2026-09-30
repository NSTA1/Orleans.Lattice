using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Holds the block-copy pass two of <c>SpliceInverse</c> / <c>SpliceFoldInverse</c>
/// to the entry-by-entry walk it replaced.
/// <para>
/// Pass one already measures where the matched entry starts and ends, so for a
/// row that carries a key at most once - every row this codec produces - the
/// surviving entries are at most two contiguous runs and can be copied as
/// blocks. The walk is retained for the one input that breaks that premise: a
/// row arriving from a replication peer under <c>ShipView</c> that repeats a
/// key. These tests pin both sides of that fork, because the fast path is only
/// a safe substitution if the guard admits exactly the rows whose surviving
/// entries really are two runs.
/// </para>
/// <para>
/// The parity claim is <b>byte</b> identity rather than decode equivalence:
/// these rows go on the wire, so an entry-order drift would change what a peer
/// receives even though every map round-trips.
/// </para>
/// </summary>
public partial class AggregationRowCodecTests
{
    /// <summary>
    /// Builds a row that carries <paramref name="duplicateKey"/> twice - once in
    /// its natural position and once appended at the end - by concatenating a
    /// one-entry row's payload onto a full row and rewriting the count prefix.
    /// No public encode can produce this, because a <c>Dictionary</c> cannot
    /// hold a key twice; only a peer can send it.
    /// </summary>
    private static byte[] DuplicateKeyInverseRow(
        Dictionary<string, AggregationRowCodec.MemberEntry> map,
        string duplicateKey,
        AggregationRowCodec.MemberEntry duplicateEntry)
    {
        var baseRow = AggregationRowCodec.EncodeInverse(map);
        var tailRow = AggregationRowCodec.EncodeInverse(
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
            {
                [duplicateKey] = duplicateEntry,
            });

        var row = new byte[baseRow.Length + tailRow.Length - sizeof(int)];
        baseRow.CopyTo(row, 0);
        tailRow.AsSpan(sizeof(int)).CopyTo(row.AsSpan(baseRow.Length));
        BitConverter.TryWriteBytes(row.AsSpan(0, sizeof(int)), map.Count + 1);
        return row;
    }

    [Test]
    public void SpliceInverse_collapses_a_repeated_key_into_the_first_match()
    {
        var map = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
        {
            ["src-000"] = new(1.0, "member-0"),
            ["src-001"] = new(2.0, null),
            ["src-002"] = new(3.0, "member-2"),
        };

        var row = DuplicateKeyInverseRow(map, "src-001", new AggregationRowCodec.MemberEntry(99.0, "stale"));
        var replacement = new AggregationRowCodec.MemberEntry(7.5, "member-1");

        // Every copy of the key is removed and the replacement lands in the
        // first match's place, so the result is the row the same map would
        // encode with the replacement applied.
        var expected = AggregationRowCodec.EncodeInverse(
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
            {
                ["src-000"] = map["src-000"],
                ["src-001"] = replacement,
                ["src-002"] = map["src-002"],
            });

        Assert.That(AggregationRowCodec.SpliceInverse(row, "src-001", replacement), Is.EqualTo(expected));
    }

    [Test]
    public void SpliceInverse_removes_every_copy_of_a_repeated_key()
    {
        var map = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
        {
            ["src-000"] = new(1.0, "member-0"),
            ["src-001"] = new(2.0, null),
        };

        var row = DuplicateKeyInverseRow(map, "src-001", new AggregationRowCodec.MemberEntry(99.0, "stale"));
        var expected = AggregationRowCodec.EncodeInverse(
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
            {
                ["src-000"] = map["src-000"],
            });

        Assert.That(AggregationRowCodec.SpliceInverse(row, "src-001", null), Is.EqualTo(expected));
    }

    [Test]
    public void SpliceInverse_moves_a_repeated_key_to_the_end_when_asked()
    {
        var map = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
        {
            ["src-000"] = new(1.0, "member-0"),
            ["src-001"] = new(2.0, null),
            ["src-002"] = new(3.0, "member-2"),
        };

        var row = DuplicateKeyInverseRow(map, "src-001", new AggregationRowCodec.MemberEntry(99.0, "stale"));
        var replacement = new AggregationRowCodec.MemberEntry(7.5, "member-1");

        var expected = AggregationRowCodec.EncodeInverse(
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
            {
                ["src-000"] = map["src-000"],
                ["src-002"] = map["src-002"],
                ["src-001"] = replacement,
            });

        Assert.That(
            AggregationRowCodec.SpliceInverse(row, "src-001", replacement, moveToEnd: true),
            Is.EqualTo(expected));
    }

    [Test]
    public void SpliceInverse_block_copy_matches_a_re_encode_at_every_match_position()
    {
        // The fast path emits a before-run and an after-run. Sweeping the match
        // across the row exercises the empty before-run (first entry), the empty
        // after-run (last entry), and both non-empty.
        const int Count = 6;
        var map = InverseFixture(Count);
        var row = AggregationRowCodec.EncodeInverse(map);
        var replacement = new AggregationRowCodec.MemberEntry(-4.25, "swapped");

        for (var i = 0; i < Count; i++)
        {
            var key = $"src-{i:D3}";
            Assert.Multiple(() =>
            {
                Assert.That(
                    AggregationRowCodec.SpliceInverse(row, key, replacement),
                    Is.EqualTo(ReferenceInverse(row, key, replacement)),
                    $"replace at index {i}");
                Assert.That(
                    AggregationRowCodec.SpliceInverse(row, key, null),
                    Is.EqualTo(ReferenceInverse(row, key, null)),
                    $"remove at index {i}");
            });
        }
    }

    [Test]
    public void SpliceFoldInverse_collapses_a_repeated_key_into_the_first_match()
    {
        var map = new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
        {
            ["src-000"] = new([1, 2], new HybridLogicalClock { WallClockTicks = 10, Counter = 0 }),
            ["src-001"] = new([3], new HybridLogicalClock { WallClockTicks = 11, Counter = 1 }),
            ["src-002"] = new([], new HybridLogicalClock { WallClockTicks = 12, Counter = 2 }),
        };

        var baseRow = AggregationRowCodec.EncodeFoldInverse(map);
        var tailRow = AggregationRowCodec.EncodeFoldInverse(
            new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
            {
                ["src-001"] = new([9, 9, 9], new HybridLogicalClock { WallClockTicks = 99, Counter = 9 }),
            });

        var row = new byte[baseRow.Length + tailRow.Length - sizeof(int)];
        baseRow.CopyTo(row, 0);
        tailRow.AsSpan(sizeof(int)).CopyTo(row.AsSpan(baseRow.Length));
        BitConverter.TryWriteBytes(row.AsSpan(0, sizeof(int)), map.Count + 1);

        var replacement = new AggregationRowCodec.FoldMember(
            [7, 7],
            new HybridLogicalClock { WallClockTicks = 77, Counter = 7 });

        var expected = AggregationRowCodec.EncodeFoldInverse(
            new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
            {
                ["src-000"] = map["src-000"],
                ["src-001"] = replacement,
                ["src-002"] = map["src-002"],
            });

        Assert.That(AggregationRowCodec.SpliceFoldInverse(row, "src-001", replacement), Is.EqualTo(expected));
    }

    [Test]
    public void SpliceFoldInverse_block_copy_matches_a_re_encode_at_every_match_position()
    {
        const int Count = 6;
        var map = FoldFixture(Count);
        var row = AggregationRowCodec.EncodeFoldInverse(map);
        var replacement = new AggregationRowCodec.FoldMember(
            [0xAA, 0xBB],
            new HybridLogicalClock { WallClockTicks = 5_000, Counter = 42 });

        for (var i = 0; i < Count; i++)
        {
            var key = $"src-{i:D3}";
            Assert.Multiple(() =>
            {
                Assert.That(
                    AggregationRowCodec.SpliceFoldInverse(row, key, replacement),
                    Is.EqualTo(ReferenceFold(row, key, replacement)),
                    $"replace at index {i}");
                Assert.That(
                    AggregationRowCodec.SpliceFoldInverse(row, key, null),
                    Is.EqualTo(ReferenceFold(row, key, null)),
                    $"remove at index {i}");
            });
        }
    }

    [Test]
    public void SpliceInverse_still_rejects_a_truncated_row_on_the_block_path()
    {
        // The fast path copies bytes as blocks, so it must not become a way to
        // pass an unvalidated row through: pass one still parses every entry.
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(3));
        for (var cut = 1; cut < row.Length; cut++)
        {
            var truncated = row.AsSpan(0, cut).ToArray();
            Assert.That(
                Rejects(() => AggregationRowCodec.SpliceInverse(truncated, "src-001", new(1.0, null))),
                Is.True,
                $"truncation at {cut} was not rejected");
        }
    }
}
