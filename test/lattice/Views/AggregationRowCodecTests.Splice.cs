using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Parity tests for the in-place row splices and the membership head read.
/// <para>
/// <c>SpliceInverse</c> / <c>SpliceFoldInverse</c> replace a decode into a
/// <c>Dictionary</c>, a single-key mutation, and a full re-encode. That is only a
/// safe substitution because <c>Dictionary</c> enumerates in entry-index order:
/// assigning a key that is present keeps its slot, assigning an absent key
/// appends, and removing one leaves the rest in order. These tests hold that
/// equivalence to the <b>byte</b>, for every mutation shape, rather than merely
/// asserting that the spliced row decodes to the right map - a weaker claim that
/// would let entry order drift and change what a peer receives on the wire.
/// </para>
/// <para>
/// They also hold the validation boundary in place. A row can arrive from a
/// replication peer under <c>ShipView</c>, so the splice must reject exactly the
/// rows the decode rejected; a splice that copied bytes through without bounding
/// them would have removed a security check rather than removed work.
/// </para>
/// </summary>
public partial class AggregationRowCodecTests
{
    private static Dictionary<string, AggregationRowCodec.MemberEntry> InverseFixture(int count) =>
        Enumerable.Range(0, count).ToDictionary(
            i => $"src-{i:D3}",
            i => new AggregationRowCodec.MemberEntry(i * 1.5, i % 2 == 0 ? $"member-{i}" : null),
            StringComparer.Ordinal);

    private static Dictionary<string, AggregationRowCodec.FoldMember> FoldFixture(int count) =>
        Enumerable.Range(0, count).ToDictionary(
            i => $"src-{i:D3}",
            i => new AggregationRowCodec.FoldMember(
                [(byte)i, 0x7F, 0x00],
                new HybridLogicalClock { WallClockTicks = 1_000 + i, Counter = i }),
            StringComparer.Ordinal);

    /// <summary>The decode / mutate / re-encode the inverse splice replaces.</summary>
    private static byte[]? ReferenceInverse(byte[] row, string key, AggregationRowCodec.MemberEntry? add)
    {
        var map = AggregationRowCodec.DecodeInverse(row);
        if (add is { } entry)
        {
            map[key] = entry;
        }
        else
        {
            map.Remove(key);
        }

        return map.Count == 0 ? null : AggregationRowCodec.EncodeInverse(map);
    }

    /// <summary>The decode / mutate / re-encode the fold splice replaces.</summary>
    private static byte[]? ReferenceFold(byte[] row, string key, AggregationRowCodec.FoldMember? add)
    {
        var map = AggregationRowCodec.DecodeFoldInverse(row);
        if (add is { } entry)
        {
            map[key] = entry;
        }
        else
        {
            map.Remove(key);
        }

        return map.Count == 0 ? null : AggregationRowCodec.EncodeFoldInverse(map);
    }

    // --- Inverse rows ---

    [Test]
    public void SpliceInverse_replacing_a_present_key_matches_a_re_encode_byte_for_byte()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(8));
        var add = new AggregationRowCodec.MemberEntry(99.25, "replacement");

        Assert.That(
            AggregationRowCodec.SpliceInverse(row, "src-003", add),
            Is.EqualTo(ReferenceInverse(row, "src-003", add)));
    }

    [Test]
    public void SpliceInverse_appending_an_absent_key_matches_a_re_encode_byte_for_byte()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(8));
        var add = new AggregationRowCodec.MemberEntry(-3.5, null);

        Assert.That(
            AggregationRowCodec.SpliceInverse(row, "src-zzz", add),
            Is.EqualTo(ReferenceInverse(row, "src-zzz", add)));
    }

    [Test]
    public void SpliceInverse_removing_a_present_key_matches_a_re_encode_byte_for_byte()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(8));

        Assert.That(
            AggregationRowCodec.SpliceInverse(row, "src-000", null),
            Is.EqualTo(ReferenceInverse(row, "src-000", null)));
    }

    [Test]
    public void SpliceInverse_removing_an_absent_key_leaves_the_row_unchanged()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(4));

        Assert.That(AggregationRowCodec.SpliceInverse(row, "src-zzz", null), Is.EqualTo(row));
    }

    [Test]
    public void SpliceInverse_returns_null_when_the_last_entry_is_removed()
    {
        // The caller reads null as "delete the row". Returning a zero-entry row
        // instead would leave a husk that a later read resurrects as an empty
        // group rather than as an absent one.
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(1));

        Assert.That(AggregationRowCodec.SpliceInverse(row, "src-000", null), Is.Null);
    }

    [Test]
    public void SpliceInverse_seeds_a_new_shard_from_the_empty_entry_row()
    {
        var add = new AggregationRowCodec.MemberEntry(7.5, "first");
        var expected = AggregationRowCodec.EncodeInverse(
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal) { ["src-000"] = add });

        Assert.That(
            AggregationRowCodec.SpliceInverse(AggregationRowCodec.EmptyEntryRow, "src-000", add),
            Is.EqualTo(expected));
    }

    [Test]
    public void SpliceInverse_preserves_entry_order_across_a_remove_then_append()
    {
        // Two splices in sequence must land where two dictionary mutations would,
        // which is the whole basis for substituting one for the other.
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(5));
        var add = new AggregationRowCodec.MemberEntry(1.25, "re-added");

        var spliced = AggregationRowCodec.SpliceInverse(
            AggregationRowCodec.SpliceInverse(row, "src-002", null)!, "src-002", add);

        var reference = ReferenceInverse(ReferenceInverse(row, "src-002", null)!, "src-002", add);

        Assert.That(spliced, Is.EqualTo(reference));
    }

    [Test]
    public void SpliceInverse_round_trips_through_DecodeInverse()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(6));
        var add = new AggregationRowCodec.MemberEntry(42.0, "m");

        var decoded = AggregationRowCodec.DecodeInverse(AggregationRowCodec.SpliceInverse(row, "src-004", add)!);

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Has.Count.EqualTo(6));
            Assert.That(decoded["src-004"], Is.EqualTo(add));
            Assert.That(decoded["src-000"].Member, Is.EqualTo("member-0"));
            Assert.That(decoded["src-001"].Member, Is.Null);
        });
    }

    // --- Fold-inverse rows ---

    [Test]
    public void SpliceFoldInverse_replacing_a_present_key_matches_a_re_encode_byte_for_byte()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(8));
        var add = new AggregationRowCodec.FoldMember(
            [0xAA, 0xBB],
            new HybridLogicalClock { WallClockTicks = 99_999, Counter = 3 });

        Assert.That(
            AggregationRowCodec.SpliceFoldInverse(row, "src-005", add),
            Is.EqualTo(ReferenceFold(row, "src-005", add)));
    }

    [Test]
    public void SpliceFoldInverse_appending_an_absent_key_matches_a_re_encode_byte_for_byte()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(8));
        var add = new AggregationRowCodec.FoldMember(
            [],
            new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });

        Assert.That(
            AggregationRowCodec.SpliceFoldInverse(row, "src-zzz", add),
            Is.EqualTo(ReferenceFold(row, "src-zzz", add)));
    }

    [Test]
    public void SpliceFoldInverse_removing_a_present_key_matches_a_re_encode_byte_for_byte()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(8));

        Assert.That(
            AggregationRowCodec.SpliceFoldInverse(row, "src-007", null),
            Is.EqualTo(ReferenceFold(row, "src-007", null)));
    }

    [Test]
    public void SpliceFoldInverse_returns_null_when_the_last_entry_is_removed()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(1));

        Assert.That(AggregationRowCodec.SpliceFoldInverse(row, "src-000", null), Is.Null);
    }

    [Test]
    public void SpliceFoldInverse_preserves_an_empty_value_payload()
    {
        // A zero-length value is a legal fold member, and the splice copies value
        // bytes as a raw span - so the length-zero case is exactly the one a
        // bounds check could get wrong without the row becoming unreadable.
        var add = new AggregationRowCodec.FoldMember(
            [],
            new HybridLogicalClock { WallClockTicks = 5, Counter = 1 });

        var spliced = AggregationRowCodec.SpliceFoldInverse(
            AggregationRowCodec.EmptyEntryRow, "src-000", add)!;

        Assert.That(AggregationRowCodec.DecodeFoldInverse(spliced)["src-000"].Value, Is.Empty);
    }

    [Test]
    public void SpliceFoldInverse_round_trips_through_DecodeFoldInverse()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(6));
        var add = new AggregationRowCodec.FoldMember(
            [0x01, 0x02, 0x03],
            new HybridLogicalClock { WallClockTicks = 7, Counter = 2 });

        var decoded = AggregationRowCodec.DecodeFoldInverse(
            AggregationRowCodec.SpliceFoldInverse(row, "src-002", add)!);

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Has.Count.EqualTo(6));
            Assert.That(decoded["src-002"].Value, Is.EqualTo(new byte[] { 0x01, 0x02, 0x03 }));
            Assert.That(decoded["src-002"].Timestamp.Counter, Is.EqualTo(2));
            Assert.That(decoded["src-003"].Value, Is.EqualTo(new byte[] { 0x03, 0x7F, 0x00 }));
        });
    }

    // --- The splices validate exactly as strictly as the decodes they replace ---

    [Test]
    public void SpliceInverse_rejects_every_truncation_the_decode_rejects()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(4));

        for (var cut = 1; cut < row.Length; cut++)
        {
            var truncated = row[..cut];
            var decodeRejected = Rejects(() => AggregationRowCodec.DecodeInverse(truncated));
            var spliceRejected = Rejects(() => AggregationRowCodec.SpliceInverse(truncated, "src-001", null));

            Assert.That(
                spliceRejected,
                Is.EqualTo(decodeRejected),
                $"row truncated to {cut} byte(s): decode rejected={decodeRejected}, splice rejected={spliceRejected}");
        }
    }

    [Test]
    public void SpliceFoldInverse_rejects_every_truncation_the_decode_rejects()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(4));

        for (var cut = 1; cut < row.Length; cut++)
        {
            var truncated = row[..cut];
            var decodeRejected = Rejects(() => AggregationRowCodec.DecodeFoldInverse(truncated));
            var spliceRejected = Rejects(() => AggregationRowCodec.SpliceFoldInverse(truncated, "src-001", null));

            Assert.That(
                spliceRejected,
                Is.EqualTo(decodeRejected),
                $"row truncated to {cut} byte(s): decode rejected={decodeRejected}, splice rejected={spliceRejected}");
        }
    }

    [Test]
    public void SpliceInverse_rejects_an_entry_count_the_row_cannot_hold()
    {
        var hostile = RowDeclaring(declaredCount: 100, trailingBytes: 20);

        Assert.That(
            () => AggregationRowCodec.SpliceInverse(hostile, "src-000", null),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void SpliceFoldInverse_rejects_an_entry_count_the_row_cannot_hold()
    {
        var hostile = RowDeclaring(declaredCount: 100, trailingBytes: 20);

        Assert.That(
            () => AggregationRowCodec.SpliceFoldInverse(hostile, "src-000", null),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void SpliceFoldInverse_rejects_a_value_length_the_row_cannot_hold()
    {
        // One entry, an empty key, a clock, then a declared value length far past
        // the end of the row. Sizing anything from that length without bounding it
        // is the allocation-amplification bug the bound exists to stop.
        var hostile = new byte[4 + 1 + 8 + 4 + 4];
        hostile[0] = 1;
        hostile[4] = 0;
        hostile[^4] = 0xFF;
        hostile[^3] = 0xFF;
        hostile[^2] = 0xFF;
        hostile[^1] = 0x7F;

        Assert.That(
            () => AggregationRowCodec.SpliceFoldInverse(hostile, "src-000", null),
            Throws.InstanceOf<InvalidDataException>());
    }

    private static bool Rejects(Action action)
    {
        try
        {
            action();
            return false;
        }
        catch (InvalidDataException)
        {
            return true;
        }
    }

    // --- Membership head read ---

    [Test]
    public void DecodeMembershipHead_agrees_with_DecodeMembership_on_a_set_union_row()
    {
        var row = AggregationRowCodec.EncodeMembership(
            new AggregationRowCodec.MembershipRow("group/eu", 12.5, "member-eu"));

        var full = AggregationRowCodec.DecodeMembership(row);
        var head = AggregationRowCodec.DecodeMembershipHead(row);

        Assert.Multiple(() =>
        {
            Assert.That(head.GroupKey, Is.EqualTo(full.GroupKey));
            Assert.That(head.Numeric, Is.EqualTo(full.Numeric));
        });
    }

    [Test]
    public void DecodeMembershipHead_agrees_with_DecodeMembership_on_a_member_free_row()
    {
        var row = AggregationRowCodec.EncodeMembership(
            new AggregationRowCodec.MembershipRow("group/eu", -0.5, null));

        var full = AggregationRowCodec.DecodeMembership(row);
        var head = AggregationRowCodec.DecodeMembershipHead(row);

        Assert.Multiple(() =>
        {
            Assert.That(head.GroupKey, Is.EqualTo(full.GroupKey));
            Assert.That(head.Numeric, Is.EqualTo(full.Numeric));
        });
    }

    [Test]
    public void DecodeMembershipHead_rejects_every_truncation_DecodeMembership_rejects()
    {
        // The member is skipped, not decoded - but its length prefix is still read
        // and still bounded, so a row that lies about it must still be rejected.
        var row = AggregationRowCodec.EncodeMembership(
            new AggregationRowCodec.MembershipRow("group/eu", 12.5, "member-eu"));

        for (var cut = 1; cut < row.Length; cut++)
        {
            var truncated = row[..cut];
            var fullRejected = Rejects(() => AggregationRowCodec.DecodeMembership(truncated));
            var headRejected = Rejects(() => AggregationRowCodec.DecodeMembershipHead(truncated));

            Assert.That(
                headRejected,
                Is.EqualTo(fullRejected),
                $"row truncated to {cut} byte(s): decode rejected={fullRejected}, head rejected={headRejected}");
        }
    }

    [Test]
    public void DecodeMembershipHead_rejects_a_member_length_the_row_cannot_hold()
    {
        var row = AggregationRowCodec.EncodeMembership(
            new AggregationRowCodec.MembershipRow("g", 1.0, "m"));

        // Re-declare the member's 7-bit length prefix as 0x7F (127 bytes) when one
        // byte follows.
        var hostile = (byte[])row.Clone();
        hostile[^2] = 0x7F;

        Assert.That(
            () => AggregationRowCodec.DecodeMembershipHead(hostile),
            Throws.InstanceOf<InvalidDataException>());
    }
}
