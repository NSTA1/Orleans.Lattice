using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="LeafSnapshotHydrationSource"/>, the seekable
/// backing a bounded leaf hydration reads through.
/// <para>
/// The load-bearing property here is the refusal: a frame whose rows are not
/// strictly ascending by ordinal key cannot back a binary search, so a seek
/// over it would fail to find rows that are present. That is data invisibility,
/// not a slow path, so the source must decline such a frame outright and leave
/// the caller on the full-decode path.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotHydrationSourceTests
{
    private static LeafSnapshotRow Row(string key, string value, bool tombstone = false)
        => new(
            key,
            new LwwValue<byte[]>
            {
                Value = tombstone ? null : Encoding.UTF8.GetBytes(value),
                Timestamp = new HybridLogicalClock { WallClockTicks = 1L },
                IsTombstone = tombstone,
            });

    private static LeafSnapshotHydrationSource Create(params LeafSnapshotRow[] rows)
    {
        Assert.That(LeafSnapshotHydrationSource.TryCreate(LeafSnapshotCodec.Encode(rows), out var source), Is.True);
        return source;
    }

    [Test]
    public void TryCreate_rejects_a_frame_whose_rows_are_not_ascending()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("b", "1"), Row("a", "2") });

        Assert.That(LeafSnapshotHydrationSource.TryCreate(frame, out _), Is.False,
            "a seek over an unsorted frame would silently miss rows that are present");
    }

    [Test]
    public void TryCreate_rejects_a_frame_carrying_duplicate_keys()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1"), Row("a", "2") });

        Assert.That(LeafSnapshotHydrationSource.TryCreate(frame, out _), Is.False);
    }

    [Test]
    public void TryCreate_rejects_a_truncated_frame()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1") });

        Assert.That(LeafSnapshotHydrationSource.TryCreate(frame.AsSpan(0, frame.Length - 4).ToArray(), out _), Is.False);
    }

    [Test]
    public void TryCreate_accepts_an_empty_frame()
    {
        var source = Create();

        Assert.That(source.RowCount, Is.Zero);
        Assert.That(source.BlockCount, Is.Zero);
        Assert.That(source.IsFullyHydrated, Is.True, "a frame with no blocks is trivially fully hydrated");
        Assert.That(source.TotalStateBytes, Is.Zero);
        Assert.That(source.TotalLiveRows, Is.Zero);
    }

    [Test]
    public void Aggregates_match_the_rows_without_decoding_any_of_them()
    {
        var source = Create(Row("a", "12345"), Row("b", "x"), Row("c", string.Empty, tombstone: true));

        Assert.That(source.RowCount, Is.EqualTo(3));
        Assert.That(source.TotalLiveRows, Is.EqualTo(2), "a tombstone is not a live row");
        Assert.That(source.TotalStateBytes, Is.EqualTo(1 + 5 + 1 + 1 + 1),
            "utf8 key length plus stored value length, with a tombstone contributing its key only");
        Assert.That(source.RowsMaterialised, Is.Zero, "computing the aggregates must not decode a row");
        Assert.That(source.BytesRead, Is.Zero);
    }

    [Test]
    public void Blocks_partition_the_rows_contiguously()
    {
        var rows = new LeafSnapshotRow[LeafSnapshotHydrationSource.BlockRows + 5];
        for (var i = 0; i < rows.Length; i++)
        {
            rows[i] = Row($"k{i:D4}", "v");
        }

        var source = Create(rows);

        Assert.That(source.BlockCount, Is.EqualTo(2));
        Assert.That(LeafSnapshotHydrationSource.BlockOf(0), Is.Zero);
        Assert.That(LeafSnapshotHydrationSource.BlockOf(LeafSnapshotHydrationSource.BlockRows), Is.EqualTo(1));
        Assert.That(source.BlockEndExclusive(0), Is.EqualTo(LeafSnapshotHydrationSource.BlockRows));
        Assert.That(source.BlockEndExclusive(1), Is.EqualTo(rows.Length),
            "the trailing block is clamped to the row count");
    }

    [Test]
    public void TryFindLowerBound_and_RowKeyEquals_locate_a_key_without_reading_payload()
    {
        var source = Create(Row("alpha", "1"), Row("beta", "2"), Row("gamma", "3"));

        Assert.That(source.TryFindLowerBound("beta"u8, out var index), Is.True);
        Assert.That(index, Is.EqualTo(1));
        Assert.That(source.RowKeyEquals(index, "beta"u8), Is.True);
        Assert.That(source.RowKeyEquals(index, "gamma"u8), Is.False);
        Assert.That(source.BytesRead, Is.Zero, "a seek must not touch the payload");
        Assert.That(source.Seeks, Is.EqualTo(1));
    }

    [Test]
    public void TryFindLowerBound_returns_the_row_count_for_a_key_past_the_last_row()
    {
        var source = Create(Row("a", "1"), Row("b", "2"));

        Assert.That(source.TryFindLowerBound("z"u8, out var index), Is.True);
        Assert.That(index, Is.EqualTo(source.RowCount));
    }

    [Test]
    public void TryReadRowAt_decodes_the_row_and_accounts_the_bytes_it_consumed()
    {
        var source = Create(Row("a", "1"), Row("b", "22"));

        Assert.That(source.TryReadRowAt(1, out var row), Is.True);
        Assert.That(row.Key, Is.EqualTo("b"));
        Assert.That(row.Value.Value, Is.EqualTo(Encoding.UTF8.GetBytes("22")));
        Assert.That(source.RowsMaterialised, Is.EqualTo(1));
        Assert.That(source.BytesRead, Is.GreaterThan(0));
    }

    [Test]
    public void TryReadRowKeyAt_decodes_the_key_without_accounting_payload_bytes()
    {
        var source = Create(Row("a", "1"), Row("b", "22"));

        Assert.That(source.TryReadRowKeyAt(1, out var key), Is.True);
        Assert.That(key, Is.EqualTo("b"));
        Assert.That(source.BytesRead, Is.Zero);
        Assert.That(source.RowsMaterialised, Is.Zero);
    }

    [Test]
    public void TryReadRowAt_rejects_an_out_of_range_index()
    {
        var source = Create(Row("a", "1"));

        Assert.That(source.TryReadRowAt(1, out _), Is.False);
        Assert.That(source.TryReadRowAt(-1, out _), Is.False);
        Assert.That(source.TryReadRowKeyAt(7, out _), Is.False);
    }

    [Test]
    public void Hydrate_commit_and_evict_move_a_block_between_states()
    {
        var source = Create(Row("a", "1"), Row("b", "2"));

        Assert.That(source.IsHydrated(0), Is.False);
        var keys = source.BeginHydrate(0);
        keys[0] = "a";
        keys[1] = "b";
        source.CommitHydrated(0);

        Assert.That(source.IsHydrated(0), Is.True);
        Assert.That(source.HydratedBlockCount, Is.EqualTo(1));
        Assert.That(source.IsFullyHydrated, Is.True);
        Assert.That(source.HydratedKeys(0).ToArray(), Is.EqualTo(new[] { "a", "b" }).AsCollection);

        source.MarkEvicted(0);
        Assert.That(source.IsHydrated(0), Is.False);
        Assert.That(source.HydratedBlockCount, Is.Zero);
    }

    [Test]
    public void AbandonHydrate_releases_the_buffer_without_committing()
    {
        var source = Create(Row("a", "1"));
        source.BeginHydrate(0);

        source.AbandonHydrate(0);

        Assert.That(source.IsHydrated(0), Is.False);
        Assert.That(source.HydratedKeys(0).Length, Is.Zero);
    }

    [Test]
    public void A_pinned_block_is_never_selected_for_eviction()
    {
        var source = Create(Row("a", "1"));
        source.BeginHydrate(0);
        source.CommitHydrated(0);

        Assert.That(source.TrySelectEvictionCandidate(-1, -1, out _), Is.True);

        source.Pin(0);

        Assert.That(source.IsPinned(0), Is.True);
        Assert.That(source.TrySelectEvictionCandidate(-1, -1, out _), Is.False,
            "re-reading a mutated block would resurrect the snapshot value the mutation replaced");
    }

    [Test]
    public void The_protected_range_is_never_selected_for_eviction()
    {
        var rows = new LeafSnapshotRow[LeafSnapshotHydrationSource.BlockRows * 2];
        for (var i = 0; i < rows.Length; i++)
        {
            rows[i] = Row($"k{i:D4}", "v");
        }

        var source = Create(rows);
        for (var block = 0; block < source.BlockCount; block++)
        {
            source.BeginHydrate(block);
            source.CommitHydrated(block);
        }

        Assert.That(source.TrySelectEvictionCandidate(0, 1, out _), Is.False,
            "the blocks the current operation is using must survive so it can return the rows it was asked for");
        Assert.That(source.TrySelectEvictionCandidate(0, 0, out var candidate), Is.True);
        Assert.That(candidate, Is.EqualTo(1));
    }

    [Test]
    public void Eviction_selects_the_least_recently_touched_block()
    {
        var rows = new LeafSnapshotRow[LeafSnapshotHydrationSource.BlockRows * 3];
        for (var i = 0; i < rows.Length; i++)
        {
            rows[i] = Row($"k{i:D4}", "v");
        }

        var source = Create(rows);
        for (var block = 0; block < 3; block++)
        {
            source.BeginHydrate(block);
            source.CommitHydrated(block);
        }

        // Commit order made block 0 the oldest; touching it makes block 1 the oldest.
        source.Touch(0);

        Assert.That(source.TrySelectEvictionCandidate(-1, -1, out var candidate), Is.True);
        Assert.That(candidate, Is.EqualTo(1));
    }

    [Test]
    public void TrySelectEvictionCandidate_reports_nothing_when_no_block_is_hydrated()
    {
        var source = Create(Row("a", "1"));

        Assert.That(source.TrySelectEvictionCandidate(-1, -1, out _), Is.False);
    }

    [Test]
    public void Pin_and_Touch_ignore_a_block_index_outside_the_frame()
    {
        var source = Create(Row("a", "1"));

        Assert.DoesNotThrow(() => source.Pin(99));
        Assert.DoesNotThrow(() => source.Touch(99));
        Assert.That(source.IsPinned(99), Is.False);
        Assert.That(source.IsHydrated(99), Is.False);
    }

    [Test]
    public void Release_returns_every_pooled_key_buffer()
    {
        var source = Create(Row("a", "1"));
        source.BeginHydrate(0);
        source.CommitHydrated(0);

        source.Release();

        Assert.That(source.HydratedKeys(0).Length, Is.Zero);
    }

    [Test]
    public void Frame_exposes_the_bytes_the_source_reads_through()
    {
        var frame = LeafSnapshotCodec.Encode(new[] { Row("a", "1") });
        Assert.That(LeafSnapshotHydrationSource.TryCreate(frame, out var source), Is.True);

        Assert.That(source.Frame, Is.SameAs(frame),
            "the fallback to a full decode must be able to stream the very frame that was installed");
    }

    [Test]
    public void An_astral_key_seek_agrees_with_ordinal_string_order()
    {
        // The trap the codec's CompareKeysUtf8 exists to avoid: a surrogate
        // pair sorts BELOW U+E000..U+FFFF under ordinal comparison, while its
        // UTF-8 bytes sort above them. A hand-rolled byte compare would place
        // the emoji last and the seek would skip it.
        var keys = new[] { "a", "\ue000private", "\U0001F600emoji" };
        Array.Sort(keys, StringComparer.Ordinal);
        var source = Create(keys.Select(k => Row(k, "v")).ToArray());

        foreach (var key in keys)
        {
            var utf8 = Encoding.UTF8.GetBytes(key);
            Assert.That(source.TryFindLowerBound(utf8, out var index), Is.True);
            Assert.That(index, Is.LessThan(source.RowCount), $"the seek must find {key}");
            Assert.That(source.RowKeyEquals(index, utf8), Is.True, $"the seek must land on {key}");
        }
    }
}
