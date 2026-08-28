using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="EntryHistoryReader"/>: the pure scan-window, bounds,
/// key-match, and row/mutation mapping logic behind the per-key history read path.
/// </summary>
[TestFixture]
public sealed class EntryHistoryReaderTests
{
    private static HybridLogicalClock Clock(long wall, int counter) =>
        new() { WallClockTicks = wall, Counter = counter };

    // -- ResolveViewScanWindow ------------------------------------------------

    [Test]
    public void ResolveViewScanWindow_no_bounds_scans_the_full_key_prefix()
    {
        var (start, end) = EntryHistoryReader.ResolveViewScanWindow("k", null, null);

        Assert.That(start, Is.EqualTo("k/"));
        Assert.That(end, Is.EqualTo("k0"));
    }

    [Test]
    public void ResolveViewScanWindow_upper_bound_excludes_all_rows_and_sibling_keys()
    {
        var (_, end) = EntryHistoryReader.ResolveViewScanWindow("k", null, null);

        // Every row sorts under "k/...."; the exclusive end "k0" is strictly greater.
        var lastRow = HistoryKey.Encode("k", Clock(long.MaxValue, int.MaxValue));
        Assert.That(string.CompareOrdinal(lastRow, end), Is.LessThan(0));

        // A sibling key's rows sort at or above the end, so they are excluded.
        var siblingRow = HistoryKey.Encode("k1", Clock(0, 0));
        Assert.That(string.CompareOrdinal(siblingRow, end), Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public void ResolveViewScanWindow_from_hlc_sets_inclusive_lower_bound()
    {
        var from = Clock(42, 1);
        var (start, _) = EntryHistoryReader.ResolveViewScanWindow("k", from, null);

        Assert.That(start, Is.EqualTo(HistoryKey.Encode("k", from)));
    }

    [Test]
    public void ResolveViewScanWindow_continuation_resumes_strictly_after_the_last_key()
    {
        var lastKey = HistoryKey.Encode("k", Clock(10, 0));
        var (start, _) = EntryHistoryReader.ResolveViewScanWindow("k", null, lastKey);

        Assert.That(start, Is.EqualTo(lastKey + '\u0000'));
        // Strictly greater than the already-returned key.
        Assert.That(string.CompareOrdinal(lastKey, start), Is.LessThan(0));
        // Strictly less than the next fixed-width row, so nothing is skipped.
        var nextKey = HistoryKey.Encode("k", Clock(10, 1));
        Assert.That(string.CompareOrdinal(start, nextKey), Is.LessThan(0));
    }

    [Test]
    public void ResolveViewScanWindow_continuation_takes_precedence_over_from_hlc()
    {
        var lastKey = HistoryKey.Encode("k", Clock(10, 0));
        var (start, _) = EntryHistoryReader.ResolveViewScanWindow("k", Clock(1, 0), lastKey);

        Assert.That(start, Is.EqualTo(lastKey + '\u0000'));
    }

    // -- WithinBounds ---------------------------------------------------------

    [Test]
    public void WithinBounds_null_bounds_accepts_anything()
    {
        Assert.That(EntryHistoryReader.WithinBounds(Clock(5, 0), null, null), Is.True);
    }

    [Test]
    public void WithinBounds_is_inclusive_at_both_ends()
    {
        var from = Clock(5, 0);
        var to = Clock(9, 0);

        Assert.Multiple(() =>
        {
            Assert.That(EntryHistoryReader.WithinBounds(from, from, to), Is.True);
            Assert.That(EntryHistoryReader.WithinBounds(to, from, to), Is.True);
            Assert.That(EntryHistoryReader.WithinBounds(Clock(4, 9), from, to), Is.False);
            Assert.That(EntryHistoryReader.WithinBounds(Clock(9, 1), from, to), Is.False);
        });
    }

    // -- MapViewRow -----------------------------------------------------------

    [Test]
    public void MapViewRow_set_full_value_under_budget_is_not_truncated()
    {
        var row = new HistoryRow
        {
            Timestamp = Clock(7, 2),
            Kind = HistoryRowKind.Set,
            SourceKey = "k",
            OriginClusterId = "cluster-a",
            Value = new byte[] { 1, 2, 3 },
            ValueHash = 99,
            ValueLength = 3,
            Mode = LatticeMergeMode.LwwRegister,
            RetentionShape = HistoryRetentionMode.FullValue,
        };

        var rev = EntryHistoryReader.MapViewRow(row, previewBudget: 256);

        Assert.Multiple(() =>
        {
            Assert.That(rev.Hlc, Is.EqualTo(row.Timestamp));
            Assert.That(rev.Kind, Is.EqualTo(HistoryRowKind.Set));
            Assert.That(rev.SourceKey, Is.EqualTo("k"));
            Assert.That(rev.OriginClusterId, Is.EqualTo("cluster-a"));
            Assert.That(rev.ValuePreview, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(rev.ValueLength, Is.EqualTo(3));
            Assert.That(rev.ValueTruncated, Is.False);
            Assert.That(rev.ValueHash, Is.EqualTo(99));
            Assert.That(rev.Delta, Is.Null);
            Assert.That(rev.RetentionShape, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(rev.VectorClock, Is.Null);
        });
    }

    [Test]
    public void MapViewRow_clips_value_preview_to_budget_and_reports_full_length()
    {
        var row = new HistoryRow
        {
            Timestamp = Clock(1, 0),
            Kind = HistoryRowKind.Set,
            SourceKey = "k",
            Value = new byte[500],
            ValueLength = 500,
        };

        var rev = EntryHistoryReader.MapViewRow(row, previewBudget: 64);

        Assert.That(rev.ValuePreview, Has.Length.EqualTo(64));
        Assert.That(rev.ValueLength, Is.EqualTo(500));
        Assert.That(rev.ValueTruncated, Is.True);
    }

    [Test]
    public void MapViewRow_metadata_only_row_has_null_preview_but_keeps_fingerprint()
    {
        var row = new HistoryRow
        {
            Timestamp = Clock(1, 0),
            Kind = HistoryRowKind.Set,
            SourceKey = "k",
            Value = null,
            ValueHash = 1234,
            ValueLength = 8,
            RetentionShape = HistoryRetentionMode.MetadataOnly,
        };

        var rev = EntryHistoryReader.MapViewRow(row, previewBudget: 256);

        Assert.Multiple(() =>
        {
            Assert.That(rev.ValuePreview, Is.Null);
            Assert.That(rev.ValueTruncated, Is.False);
            Assert.That(rev.ValueHash, Is.EqualTo(1234));
            Assert.That(rev.ValueLength, Is.EqualTo(8));
        });
    }

    [Test]
    public void MapViewRow_crdt_delta_row_carries_delta()
    {
        var row = new HistoryRow
        {
            Timestamp = Clock(3, 0),
            Kind = HistoryRowKind.CrdtDelta,
            SourceKey = "k",
            Delta = new byte[] { 9, 8, 7 },
            Mode = LatticeMergeMode.PnCounter,
        };

        var rev = EntryHistoryReader.MapViewRow(row, previewBudget: 256);

        Assert.Multiple(() =>
        {
            Assert.That(rev.Kind, Is.EqualTo(HistoryRowKind.CrdtDelta));
            Assert.That(rev.Delta, Is.EqualTo(new byte[] { 9, 8, 7 }));
            Assert.That(rev.Mode, Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(rev.ValuePreview, Is.Null);
        });
    }

    [Test]
    public void MapViewRow_range_tombstone_row_carries_end_key()
    {
        var row = new HistoryRow
        {
            Timestamp = Clock(4, 0),
            Kind = HistoryRowKind.RangeTombstone,
            SourceKey = "a",
            EndKey = "z",
        };

        var rev = EntryHistoryReader.MapViewRow(row, previewBudget: 256);

        Assert.Multiple(() =>
        {
            Assert.That(rev.Kind, Is.EqualTo(HistoryRowKind.RangeTombstone));
            Assert.That(rev.SourceKey, Is.EqualTo("a"));
            Assert.That(rev.EndKey, Is.EqualTo("z"));
        });
    }

    // -- MapWalMutation -------------------------------------------------------

    [Test]
    public void MapWalMutation_set_maps_to_set_with_hash_length_and_full_value_shape()
    {
        var vc = new VersionVector();
        var mutation = new LatticeMutation
        {
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3, 4 },
            Timestamp = Clock(6, 1),
            OriginClusterId = "cluster-c",
            VectorClock = vc,
            Mode = LatticeMergeMode.LwwRegister,
        };

        var rev = EntryHistoryReader.MapWalMutation(mutation, previewBudget: 256);

        Assert.Multiple(() =>
        {
            Assert.That(rev.Kind, Is.EqualTo(HistoryRowKind.Set));
            Assert.That(rev.ValuePreview, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
            Assert.That(rev.ValueLength, Is.EqualTo(4));
            Assert.That(rev.ValueHash, Is.Not.Zero);
            Assert.That(rev.RetentionShape, Is.EqualTo(HistoryRetentionMode.FullValue));
            VectorClockAssert.SameFrontier(rev.VectorClock, vc);
        });
    }

    [Test]
    public void MapWalMutation_set_with_delta_maps_to_crdt_delta()
    {
        var mutation = new LatticeMutation
        {
            Kind = MutationKind.Set,
            Key = "k",
            Delta = new byte[] { 5, 5 },
            Timestamp = Clock(6, 2),
            Mode = LatticeMergeMode.OrSet,
        };

        var rev = EntryHistoryReader.MapWalMutation(mutation, previewBudget: 256);

        Assert.Multiple(() =>
        {
            Assert.That(rev.Kind, Is.EqualTo(HistoryRowKind.CrdtDelta));
            Assert.That(rev.Delta, Is.EqualTo(new byte[] { 5, 5 }));
            Assert.That(rev.ValuePreview, Is.Null);
            Assert.That(rev.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    [Test]
    public void MapWalMutation_delete_and_tombstone_map_to_delete()
    {
        var delete = new LatticeMutation { Kind = MutationKind.Delete, Key = "k", Timestamp = Clock(1, 0) };
        var tombstone = new LatticeMutation { Kind = MutationKind.Tombstone, Key = "k", Timestamp = Clock(2, 0) };

        Assert.That(EntryHistoryReader.MapWalMutation(delete, 256).Kind, Is.EqualTo(HistoryRowKind.Delete));
        Assert.That(EntryHistoryReader.MapWalMutation(tombstone, 256).Kind, Is.EqualTo(HistoryRowKind.Delete));
    }

    [Test]
    public void MapWalMutation_delete_range_maps_to_range_tombstone_with_end_key()
    {
        var mutation = new LatticeMutation
        {
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "m",
            Timestamp = Clock(3, 0),
        };

        var rev = EntryHistoryReader.MapWalMutation(mutation, 256);

        Assert.Multiple(() =>
        {
            Assert.That(rev.Kind, Is.EqualTo(HistoryRowKind.RangeTombstone));
            Assert.That(rev.SourceKey, Is.EqualTo("a"));
            Assert.That(rev.EndKey, Is.EqualTo("m"));
        });
    }

    [Test]
    public void MapWalMutation_clips_large_value_to_budget()
    {
        var mutation = new LatticeMutation
        {
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[1000],
            Timestamp = Clock(1, 0),
        };

        var rev = EntryHistoryReader.MapWalMutation(mutation, previewBudget: 128);

        Assert.That(rev.ValuePreview, Has.Length.EqualTo(128));
        Assert.That(rev.ValueLength, Is.EqualTo(1000));
        Assert.That(rev.ValueTruncated, Is.True);
    }

    // -- WalMutationMatchesKey ------------------------------------------------

    [Test]
    public void WalMutationMatchesKey_exact_point_writes_match_only_their_key()
    {
        var set = new LatticeMutation { Kind = MutationKind.Set, Key = "k" };

        Assert.That(EntryHistoryReader.WalMutationMatchesKey(set, "k"), Is.True);
        Assert.That(EntryHistoryReader.WalMutationMatchesKey(set, "other"), Is.False);
    }

    [Test]
    public void WalMutationMatchesKey_unconstrained_range_covers_keys_in_its_half_open_range()
    {
        var range = new LatticeMutation { Kind = MutationKind.DeleteRange, Key = "a", EndExclusiveKey = "m" };

        Assert.Multiple(() =>
        {
            Assert.That(EntryHistoryReader.WalMutationMatchesKey(range, "a"), Is.True, "inclusive start");
            Assert.That(EntryHistoryReader.WalMutationMatchesKey(range, "f"), Is.True);
            Assert.That(EntryHistoryReader.WalMutationMatchesKey(range, "m"), Is.False, "exclusive end");
            Assert.That(EntryHistoryReader.WalMutationMatchesKey(range, "z"), Is.False);
        });
    }

    [Test]
    public void WalMutationMatchesKey_predicate_filtered_range_uses_matched_keys()
    {
        var range = new LatticeMutation
        {
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            MatchedKeys = new[] { "b", "d" },
        };

        Assert.Multiple(() =>
        {
            Assert.That(EntryHistoryReader.WalMutationMatchesKey(range, "b"), Is.True);
            Assert.That(EntryHistoryReader.WalMutationMatchesKey(range, "d"), Is.True);
            Assert.That(EntryHistoryReader.WalMutationMatchesKey(range, "c"), Is.False,
                "in range but not in the matched set");
        });
    }

    [Test]
    public void WalMutationMatchesKey_transaction_terminal_never_matches()
    {
        var commit = new LatticeMutation { Kind = MutationKind.TxCommit, Key = "k" };

        Assert.That(EntryHistoryReader.WalMutationMatchesKey(commit, "k"), Is.False);
    }
}
