using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Parity tests for the fused same-group re-contribution splice.
/// <para>
/// A min / max / set-union or custom-fold contribution that keeps its group -
/// the steady state, since a source key's group only changes when the grouped
/// column does - sends its retraction and its addition to the <b>same</b> shard
/// row. The applier used to read, splice and write that row twice.
/// <c>moveToEnd</c> fuses the pair into one pass by eliding the old entry and
/// appending the new one at the end, which is precisely what the pair produced:
/// the removal elides the entry, and the re-add then finds the key absent and
/// appends it.
/// </para>
/// <para>
/// These tests hold that to the <b>byte</b>, not merely to the decoded map. The
/// rows go out on the wire under <c>ShipView</c>, so a fusion that agreed only
/// semantically would change what a replication peer receives. Each case is
/// asserted against the unfused pair it replaces <i>and</i> against the
/// dictionary round trip that pair is itself defined against, so neither oracle
/// can drift unnoticed.
/// </para>
/// </summary>
public partial class AggregationRowCodecTests
{
    /// <summary>The two-splice pair the fused splice replaces, including the applier's drain-to-delete step.</summary>
    private static byte[]? UnfusedInversePair(byte[] row, string key, AggregationRowCodec.MemberEntry add)
    {
        var retracted = AggregationRowCodec.SpliceInverse(row, key, add: null);
        return AggregationRowCodec.SpliceInverse(
            retracted is null ? AggregationRowCodec.EmptyEntryRow : retracted.AsSpan(),
            key,
            add);
    }

    /// <summary>The fold counterpart of <see cref="UnfusedInversePair"/>.</summary>
    private static byte[]? UnfusedFoldPair(byte[] row, string key, AggregationRowCodec.FoldMember add)
    {
        var retracted = AggregationRowCodec.SpliceFoldInverse(row, key, add: null);
        return AggregationRowCodec.SpliceFoldInverse(
            retracted is null ? AggregationRowCodec.EmptyEntryRow : retracted.AsSpan(),
            key,
            add);
    }

    /// <summary>The dictionary round trip the unfused pair is itself defined against.</summary>
    private static byte[]? ReferenceInverseRemoveThenAdd(byte[] row, string key, AggregationRowCodec.MemberEntry add)
    {
        var map = AggregationRowCodec.DecodeInverse(row);
        map.Remove(key);

        // Re-encode between the two halves, exactly as the applier's two store
        // round trips did. That matters: a Dictionary reuses the freed slot when
        // a removed key is re-added within the same instance, so mutating one map
        // twice would NOT reproduce the order two round trips produce.
        var intermediate = map.Count == 0 ? null : AggregationRowCodec.EncodeInverse(map);
        var reread = intermediate is null
            ? new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
            : AggregationRowCodec.DecodeInverse(intermediate);
        reread[key] = add;
        return AggregationRowCodec.EncodeInverse(reread);
    }

    private static byte[]? ReferenceFoldRemoveThenAdd(byte[] row, string key, AggregationRowCodec.FoldMember add)
    {
        var map = AggregationRowCodec.DecodeFoldInverse(row);
        map.Remove(key);
        var intermediate = map.Count == 0 ? null : AggregationRowCodec.EncodeFoldInverse(map);
        var reread = intermediate is null
            ? new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
            : AggregationRowCodec.DecodeFoldInverse(intermediate);
        reread[key] = add;
        return AggregationRowCodec.EncodeFoldInverse(reread);
    }

    private static readonly AggregationRowCodec.MemberEntry FusedEntry = new(99.25, "member-fused");

    private static readonly AggregationRowCodec.FoldMember FusedFoldEntry = new(
        [0xAA, 0xBB, 0xCC],
        new HybridLogicalClock { WallClockTicks = 9_999, Counter = 7 });

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(8)]
    [TestCase(64)]
    public void SpliceInverse_moveToEnd_matches_the_retract_then_add_pair_for_a_present_key(int count)
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(count));
        const string Key = "src-000";

        var fused = AggregationRowCodec.SpliceInverse(row, Key, FusedEntry, moveToEnd: true);

        Assert.Multiple(() =>
        {
            Assert.That(fused, Is.EqualTo(UnfusedInversePair(row, Key, FusedEntry)), "unfused pair");
            Assert.That(fused, Is.EqualTo(ReferenceInverseRemoveThenAdd(row, Key, FusedEntry)), "dictionary round trip");
        });
    }

    [TestCase(1)]
    [TestCase(8)]
    public void SpliceInverse_moveToEnd_appends_a_key_absent_from_the_row(int count)
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(count));
        const string Key = "src-absent";

        var fused = AggregationRowCodec.SpliceInverse(row, Key, FusedEntry, moveToEnd: true);

        Assert.Multiple(() =>
        {
            Assert.That(fused, Is.EqualTo(UnfusedInversePair(row, Key, FusedEntry)), "unfused pair");
            Assert.That(fused, Is.EqualTo(AggregationRowCodec.SpliceInverse(row, Key, FusedEntry)), "plain splice appends too");
        });
    }

    /// <summary>
    /// The fused splice must move the entry to the end, not leave it in place -
    /// that is the whole difference from the plain splice, and it is what makes
    /// the fusion byte-identical to the pair it replaces.
    /// </summary>
    [Test]
    public void SpliceInverse_moveToEnd_reorders_a_present_key_to_the_end_unlike_the_plain_splice()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(4));
        const string Key = "src-000";

        var inPlace = AggregationRowCodec.SpliceInverse(row, Key, FusedEntry);
        var moved = AggregationRowCodec.SpliceInverse(row, Key, FusedEntry, moveToEnd: true);

        Assert.Multiple(() =>
        {
            Assert.That(moved, Is.Not.EqualTo(inPlace), "the two modes must differ for a present, non-last key");
            Assert.That(AggregationRowCodec.DecodeInverse(moved!).Keys, Is.EqualTo(new[] { "src-001", "src-002", "src-003", Key }));
            Assert.That(AggregationRowCodec.DecodeInverse(inPlace!).Keys, Is.EqualTo(new[] { Key, "src-001", "src-002", "src-003" }));
        });
    }

    /// <summary>
    /// The entry already sitting last is the case where moving it to the end is a
    /// no-op, so both modes must agree byte-for-byte.
    /// </summary>
    [Test]
    public void SpliceInverse_moveToEnd_matches_the_plain_splice_when_the_key_is_already_last()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(4));
        const string Key = "src-003";

        Assert.That(
            AggregationRowCodec.SpliceInverse(row, Key, FusedEntry, moveToEnd: true),
            Is.EqualTo(AggregationRowCodec.SpliceInverse(row, Key, FusedEntry)));
    }

    /// <summary>
    /// A single-entry shard is the case the unfused pair drained to a delete and
    /// then re-created. The fused splice never deletes it, and must still land on
    /// the identical row.
    /// </summary>
    [Test]
    public void SpliceInverse_moveToEnd_reseeds_a_single_entry_shard_without_draining_it()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(1));
        const string Key = "src-000";

        var fused = AggregationRowCodec.SpliceInverse(row, Key, FusedEntry, moveToEnd: true);

        Assert.Multiple(() =>
        {
            Assert.That(AggregationRowCodec.SpliceInverse(row, Key, add: null), Is.Null, "the retract half drained the shard");
            Assert.That(fused, Is.Not.Null);
            Assert.That(fused, Is.EqualTo(UnfusedInversePair(row, Key, FusedEntry)));
        });
    }

    [Test]
    public void SpliceInverse_moveToEnd_seeds_an_empty_row()
    {
        var seeded = AggregationRowCodec.SpliceInverse(AggregationRowCodec.EmptyEntryRow, "src-000", FusedEntry, moveToEnd: true);

        Assert.That(
            seeded,
            Is.EqualTo(AggregationRowCodec.EncodeInverse(
                new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal) { ["src-000"] = FusedEntry })));
    }

    /// <summary>A removal has no entry to position, so the mode must not change it.</summary>
    [Test]
    public void SpliceInverse_moveToEnd_is_ignored_for_a_removal()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(8));

        Assert.That(
            AggregationRowCodec.SpliceInverse(row, "src-003", add: null, moveToEnd: true),
            Is.EqualTo(AggregationRowCodec.SpliceInverse(row, "src-003", add: null)));
    }

    /// <summary>
    /// The row can arrive from a replication peer, so the fused mode must reject
    /// exactly the rows the unfused splice rejects - at every cut length, not just
    /// a sampled one.
    /// </summary>
    [Test]
    public void SpliceInverse_moveToEnd_rejects_exactly_the_truncations_the_plain_splice_rejects()
    {
        var row = AggregationRowCodec.EncodeInverse(InverseFixture(8));

        for (var cut = 1; cut < row.Length; cut++)
        {
            var truncated = row[..cut];
            var plain = Rejects(() => AggregationRowCodec.SpliceInverse(truncated, "src-003", FusedEntry));
            var fused = Rejects(() => AggregationRowCodec.SpliceInverse(truncated, "src-003", FusedEntry, moveToEnd: true));
            Assert.That(fused, Is.EqualTo(plain), $"truncated to {cut} byte(s)");
        }
    }

    // ---- fold-inverse counterpart ----

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(8)]
    [TestCase(64)]
    public void SpliceFoldInverse_moveToEnd_matches_the_retract_then_add_pair_for_a_present_key(int count)
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(count));
        const string Key = "src-000";

        var fused = AggregationRowCodec.SpliceFoldInverse(row, Key, FusedFoldEntry, moveToEnd: true);

        Assert.Multiple(() =>
        {
            Assert.That(fused, Is.EqualTo(UnfusedFoldPair(row, Key, FusedFoldEntry)), "unfused pair");
            Assert.That(fused, Is.EqualTo(ReferenceFoldRemoveThenAdd(row, Key, FusedFoldEntry)), "dictionary round trip");
        });
    }

    [TestCase(1)]
    [TestCase(8)]
    public void SpliceFoldInverse_moveToEnd_appends_a_key_absent_from_the_row(int count)
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(count));
        const string Key = "src-absent";

        Assert.That(
            AggregationRowCodec.SpliceFoldInverse(row, Key, FusedFoldEntry, moveToEnd: true),
            Is.EqualTo(UnfusedFoldPair(row, Key, FusedFoldEntry)));
    }

    [Test]
    public void SpliceFoldInverse_moveToEnd_reorders_a_present_key_to_the_end_unlike_the_plain_splice()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(4));
        const string Key = "src-000";

        var moved = AggregationRowCodec.SpliceFoldInverse(row, Key, FusedFoldEntry, moveToEnd: true);

        Assert.Multiple(() =>
        {
            Assert.That(moved, Is.Not.EqualTo(AggregationRowCodec.SpliceFoldInverse(row, Key, FusedFoldEntry)));
            Assert.That(AggregationRowCodec.DecodeFoldInverse(moved!).Keys, Is.EqualTo(new[] { "src-001", "src-002", "src-003", Key }));
        });
    }

    [Test]
    public void SpliceFoldInverse_moveToEnd_reseeds_a_single_entry_shard_without_draining_it()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(1));
        const string Key = "src-000";

        Assert.Multiple(() =>
        {
            Assert.That(AggregationRowCodec.SpliceFoldInverse(row, Key, add: null), Is.Null, "the retract half drained the shard");
            Assert.That(
                AggregationRowCodec.SpliceFoldInverse(row, Key, FusedFoldEntry, moveToEnd: true),
                Is.EqualTo(UnfusedFoldPair(row, Key, FusedFoldEntry)));
        });
    }

    [Test]
    public void SpliceFoldInverse_moveToEnd_is_ignored_for_a_removal()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(8));

        Assert.That(
            AggregationRowCodec.SpliceFoldInverse(row, "src-003", add: null, moveToEnd: true),
            Is.EqualTo(AggregationRowCodec.SpliceFoldInverse(row, "src-003", add: null)));
    }

    [Test]
    public void SpliceFoldInverse_moveToEnd_rejects_exactly_the_truncations_the_plain_splice_rejects()
    {
        var row = AggregationRowCodec.EncodeFoldInverse(FoldFixture(8));

        for (var cut = 1; cut < row.Length; cut++)
        {
            var truncated = row[..cut];
            var plain = Rejects(() => AggregationRowCodec.SpliceFoldInverse(truncated, "src-003", FusedFoldEntry));
            var fused = Rejects(() => AggregationRowCodec.SpliceFoldInverse(truncated, "src-003", FusedFoldEntry, moveToEnd: true));
            Assert.That(fused, Is.EqualTo(plain), $"truncated to {cut} byte(s)");
        }
    }
}
