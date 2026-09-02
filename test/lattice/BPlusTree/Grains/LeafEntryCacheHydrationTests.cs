using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Bounded-hydration coverage for <see cref="LeafEntryCache"/> (issue #1839).
/// <para>
/// The contract under test is that a cache backed by a lazily hydrated snapshot
/// is <em>indistinguishable</em> from one that decoded every row, except in the
/// work it does. Every aggregate, every lookup, every walk returns the same
/// answer; only the number of frame bytes actually read differs, and that number
/// tracks what was asked for rather than what the leaf holds.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafEntryCacheHydrationTests
{
    private const int PayloadBytes = 512;

    private static LeafEntryCache NewCache()
        => new(new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal));

    private static string Key(int i) => $"k{i:D5}";

    private static LwwValue<byte[]> Value(int i, int payloadBytes = PayloadBytes)
    {
        var bytes = new byte[payloadBytes];
        for (var b = 0; b < bytes.Length; b++)
        {
            bytes[b] = (byte)((i + b) & 0xFF);
        }

        return new LwwValue<byte[]>
        {
            Value = bytes,
            Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
        };
    }

    private static LeafSnapshotRow[] Corpus(int rowCount, int payloadBytes = PayloadBytes)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = i % 7 == 6
                ? new LeafSnapshotRow(
                    Key(i),
                    new LwwValue<byte[]>
                    {
                        Value = null,
                        IsTombstone = true,
                        Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i },
                    })
                : new LeafSnapshotRow(Key(i), Value(i, payloadBytes), i % 11 == 3 ? LatticeMergeMode.GCounter : null);
        }

        return rows;
    }

    private static LeafEntryCache Attached(LeafSnapshotRow[] rows, long budgetBytes = 0L)
    {
        var cache = NewCache();
        Assert.That(cache.TryAttachSnapshot(LeafSnapshotCodec.Encode(rows), budgetBytes), Is.True);
        return cache;
    }

    private static LeafEntryCache FullyDecoded(LeafSnapshotRow[] rows)
    {
        var cache = NewCache();
        foreach (var row in rows)
        {
            cache.StoreRow(row.Key, row.Value);
            if (row.MergeMode is { } mode)
            {
                cache.SetMergeMode(row.Key, mode);
            }
        }

        return cache;
    }

    [Test]
    public void TryAttachSnapshot_reports_the_whole_projection_before_anything_is_materialised()
    {
        var rows = Corpus(100);
        var reference = FullyDecoded(rows);

        var cache = Attached(rows);

        Assert.That(cache.HasPendingHydration, Is.True);
        Assert.That(cache.HydratedRowCount, Is.Zero, "nothing may be resident before a read asks for it");
        Assert.That(cache.Count, Is.EqualTo(reference.Count));
        Assert.That(cache.StateBytes, Is.EqualTo(reference.StateBytes));
        Assert.That(cache.LiveCount, Is.EqualTo(reference.LiveCount));
        Assert.That(cache.SnapshotBytesRead, Is.Zero);
    }

    [Test]
    public void TryAttachSnapshot_declines_a_frame_that_cannot_back_a_seek()
    {
        var cache = NewCache();
        var unsorted = LeafSnapshotCodec.Encode(new[]
        {
            new LeafSnapshotRow("b", Value(1)),
            new LeafSnapshotRow("a", Value(2)),
        });

        Assert.That(cache.TryAttachSnapshot(unsorted, 0L), Is.False);
        Assert.That(cache.HasPendingHydration, Is.False);
        Assert.That(cache.Count, Is.Zero, "a declined attach leaves the caller to decode the frame in full");
    }

    [Test]
    public void TryAttachSnapshot_rejects_a_null_frame()
        => Assert.Throws<ArgumentNullException>(() => NewCache().TryAttachSnapshot(null!, 0L));

    [Test]
    public void A_point_read_materialises_one_block_and_reads_a_fraction_of_the_frame()
    {
        var rows = Corpus(320);
        var frameLength = LeafSnapshotCodec.Encode(rows).Length;
        var cache = Attached(rows);

        Assert.That(cache.TryGetRow(Key(159), out var row), Is.True);
        Assert.That(row.Value, Is.EqualTo(rows[159].Value.Value));

        Assert.That(cache.HydratedRowCount, Is.EqualTo(LeafSnapshotHydrationSource.BlockRows));
        Assert.That(cache.SnapshotRowsMaterialised, Is.EqualTo(LeafSnapshotHydrationSource.BlockRows));
        Assert.That(cache.SnapshotBytesRead, Is.LessThan(frameLength / 4),
            "one point read must not pay for the whole leaf");
        Assert.That(cache.HasPendingHydration, Is.True);
    }

    [Test]
    public void A_point_read_for_a_key_the_snapshot_does_not_carry_materialises_nothing()
    {
        var cache = Attached(Corpus(320));

        Assert.That(cache.TryGetRow("absent-key", out _), Is.False);

        Assert.That(cache.SnapshotRowsMaterialised, Is.Zero);
        Assert.That(cache.SnapshotBytesRead, Is.Zero, "a miss is answered by the index table alone");
        Assert.That(cache.SnapshotSeeks, Is.EqualTo(1));
    }

    [Test]
    public void ContainsKey_answers_from_the_index_table_without_materialising()
    {
        var cache = Attached(Corpus(320));

        Assert.That(cache.ContainsKey(Key(7)), Is.True);
        Assert.That(cache.ContainsKey("nope"), Is.False);
        Assert.That(cache.SnapshotRowsMaterialised, Is.Zero);
        Assert.That(cache.SnapshotBytesRead, Is.Zero);
    }

    [Test]
    public void Bytes_read_scales_with_the_requested_key_range_and_not_with_leaf_size()
    {
        // The acceptance property, expressed deterministically: hold the range
        // fixed and grow the leaf around it. The work must not follow the leaf.
        var small = Attached(Corpus(128));
        var large = Attached(Corpus(1024));

        small.HydrateRange(Key(64), Key(80));
        large.HydrateRange(Key(64), Key(80));

        Assert.That(large.SnapshotRowsMaterialised, Is.EqualTo(small.SnapshotRowsMaterialised),
            "an eight-times-larger leaf must materialise the same rows for the same key range");
        Assert.That(large.SnapshotBytesRead, Is.EqualTo(small.SnapshotBytesRead));
        Assert.That(large.PendingHydrationRowCount, Is.GreaterThan(small.PendingHydrationRowCount));
    }

    [Test]
    public void Bytes_read_grows_with_the_range_width_on_a_fixed_leaf()
    {
        var rows = Corpus(1024);
        var narrow = Attached(rows);
        var wide = Attached(rows);

        narrow.HydrateRange(Key(0), Key(32));
        wide.HydrateRange(Key(0), Key(512));

        Assert.That(wide.SnapshotBytesRead, Is.GreaterThan(narrow.SnapshotBytesRead * 8),
            "a sixteen-times-wider range reads materially more of the same leaf");
        Assert.That(wide.SnapshotRowsMaterialised, Is.GreaterThan(narrow.SnapshotRowsMaterialised));
    }

    [Test]
    public void EnumerateRange_returns_exactly_the_rows_a_full_walk_would_return_for_that_range()
    {
        var rows = Corpus(256);
        var reference = FullyDecoded(rows);
        var cache = Attached(rows);

        var expected = reference.EnumerateRows()
            .Where(kv => string.CompareOrdinal(kv.Key, Key(40)) >= 0
                && string.CompareOrdinal(kv.Key, Key(90)) < 0)
            .Select(kv => kv.Key)
            .ToArray();

        var actual = new List<string>();
        foreach (var kv in cache.EnumerateRange(Key(40), Key(90)))
        {
            actual.Add(kv.Key);
        }

        Assert.That(actual, Is.EqualTo(expected).AsCollection);
        Assert.That(cache.HasPendingHydration, Is.True, "a bounded scan must leave the rest of the leaf unread");
    }

    [Test]
    public void EnumerateRange_with_open_bounds_walks_the_whole_cache()
    {
        var rows = Corpus(70);
        var cache = Attached(rows);

        var seen = new List<string>();
        foreach (var kv in cache.EnumerateRange(null, null))
        {
            seen.Add(kv.Key);
        }

        Assert.That(seen, Is.EqualTo(rows.Select(r => r.Key).ToArray()).AsCollection);
        Assert.That(cache.HasPendingHydration, Is.False, "an unbounded scan is a full walk");
    }

    [Test]
    public void EnumerateRange_over_an_empty_intersection_materialises_nothing()
    {
        var cache = Attached(Corpus(128));

        var seen = 0;
        foreach (var _ in cache.EnumerateRange(Key(90), Key(10)))
        {
            seen++;
        }

        Assert.That(seen, Is.Zero);
        Assert.That(cache.SnapshotRowsMaterialised, Is.Zero);
    }

    [Test]
    public void Every_whole_cache_walk_materialises_the_snapshot_first()
    {
        var rows = Corpus(100);

        var viaRows = Attached(rows);
        _ = viaRows.EnumerateRows().Count();
        Assert.That(viaRows.HasPendingHydration, Is.False);

        var viaKeys = Attached(rows);
        _ = viaKeys.Keys.Count();
        Assert.That(viaKeys.HasPendingHydration, Is.False);

        var viaUnderlying = Attached(rows);
        Assert.That(viaUnderlying.UnderlyingRows, Has.Count.EqualTo(rows.Length));
        Assert.That(viaUnderlying.HasPendingHydration, Is.False);

        var viaBackfill = Attached(rows);
        viaBackfill.OverwriteStateBytesForBackfill(123L);
        Assert.That(viaBackfill.HasPendingHydration, Is.False);
        Assert.That(viaBackfill.StateBytes, Is.EqualTo(123L),
            "the backfill figure describes the whole projection, so no residual may be added to it");
    }

    [Test]
    public void A_full_walk_returns_byte_identical_rows_to_a_fully_decoded_cache()
    {
        var rows = Corpus(200);
        var reference = FullyDecoded(rows);
        var cache = Attached(rows);

        // Touch a couple of ranges first so the walk runs over a genuinely
        // half-hydrated cache rather than a pristine one.
        cache.HydrateRange(Key(10), Key(20));
        Assert.That(cache.TryGetRow(Key(150), out _), Is.True);

        var expected = reference.EnumerateRows().ToArray();
        var actual = cache.EnumerateRows().ToArray();

        Assert.That(actual.Length, Is.EqualTo(expected.Length));
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.That(actual[i].Key, Is.EqualTo(expected[i].Key));
            Assert.That(actual[i].Value.Value, Is.EqualTo(expected[i].Value.Value));
            Assert.That(actual[i].Value.IsTombstone, Is.EqualTo(expected[i].Value.IsTombstone));
            Assert.That(actual[i].Value.Timestamp, Is.EqualTo(expected[i].Value.Timestamp));
        }

        Assert.That(cache.Count, Is.EqualTo(reference.Count));
        Assert.That(cache.StateBytes, Is.EqualTo(reference.StateBytes));
        Assert.That(cache.LiveCount, Is.EqualTo(reference.LiveCount));
    }

    [Test]
    public void Aggregates_stay_exact_across_a_partial_hydration()
    {
        var rows = Corpus(256);
        var reference = FullyDecoded(rows);
        var cache = Attached(rows);

        foreach (var probe in new[] { 3, 40, 41, 200, 255 })
        {
            Assert.That(cache.TryGetRow(Key(probe), out _), Is.True);
            Assert.That(cache.Count, Is.EqualTo(reference.Count));
            Assert.That(cache.StateBytes, Is.EqualTo(reference.StateBytes));
            Assert.That(cache.LiveCount, Is.EqualTo(reference.LiveCount));
        }
    }

    [Test]
    public void The_per_key_merge_mode_survives_a_partial_hydration()
    {
        var rows = Corpus(64);
        var cache = Attached(rows);
        var expected = rows.First(r => r.MergeMode is not null);

        Assert.That(cache.GetMergeMode(expected.Key), Is.EqualTo(expected.MergeMode));
        Assert.That(cache.GetMergeMode(Key(0)), Is.Null, "a plain last-writer-wins key carries no mode");
    }

    [Test]
    public void A_write_over_a_snapshot_row_wins_and_survives_a_later_walk()
    {
        var rows = Corpus(128);
        var cache = Attached(rows);
        var replacement = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("overwritten"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 9_000L },
        };

        cache.StoreRow(Key(64), replacement);

        Assert.That(cache.TryGetRow(Key(64), out var read), Is.True);
        Assert.That(read.Value, Is.EqualTo(replacement.Value));

        var walked = cache.EnumerateRows().Single(kv => kv.Key == Key(64));
        Assert.That(walked.Value.Value, Is.EqualTo(replacement.Value),
            "a full walk must never resurrect the snapshot value a write replaced");
        Assert.That(cache.Count, Is.EqualTo(rows.Length));
    }

    [Test]
    public void A_removal_of_a_snapshot_row_is_not_resurrected_by_a_later_read_or_walk()
    {
        var rows = Corpus(128);
        var cache = Attached(rows);

        Assert.That(cache.Remove(Key(64)), Is.True);

        Assert.That(cache.ContainsKey(Key(64)), Is.False);
        Assert.That(cache.TryGetRow(Key(64), out _), Is.False);
        Assert.That(cache.Count, Is.EqualTo(rows.Length - 1));
        Assert.That(cache.EnumerateRows().Any(kv => kv.Key == Key(64)), Is.False);
    }

    [Test]
    public void A_key_inserted_alongside_the_snapshot_is_visible_everywhere()
    {
        var rows = Corpus(128);
        var cache = Attached(rows);
        var inserted = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("new"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 9_000L },
        };

        cache.StoreRow("k00064a", inserted);

        Assert.That(cache.Count, Is.EqualTo(rows.Length + 1));
        Assert.That(cache.ContainsKey("k00064a"), Is.True);
        var keys = new List<string>();
        foreach (var kv in cache.EnumerateRange(Key(64), Key(65)))
        {
            keys.Add(kv.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { Key(64), "k00064a" }).AsCollection);
    }

    [Test]
    public void EnumerateKeysUnordered_yields_every_key_exactly_once_without_reading_payload()
    {
        var rows = Corpus(200);
        var cache = Attached(rows);
        cache.HydrateRange(Key(10), Key(20));
        cache.StoreRow("zzz-extra", Value(1, 8));
        Assert.That(cache.Remove(Key(15)), Is.True);
        var bytesReadBefore = cache.SnapshotBytesRead;

        var seen = new List<string>();
        foreach (var key in cache.EnumerateKeysUnordered())
        {
            seen.Add(key);
        }

        var expected = rows.Select(r => r.Key).Where(k => k != Key(15)).Append("zzz-extra").ToArray();
        Assert.That(seen, Has.Count.EqualTo(expected.Length));
        Assert.That(seen.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(seen.Count),
            "the resident and snapshot halves are disjoint, so no key may be yielded twice");
        Assert.That(seen.Order(StringComparer.Ordinal).ToArray(),
            Is.EqualTo(expected.Order(StringComparer.Ordinal).ToArray()).AsCollection);
        Assert.That(cache.SnapshotBytesRead, Is.EqualTo(bytesReadBefore),
            "a key-only walk must not decode a payload");
        Assert.That(cache.HasPendingHydration, Is.True);
    }

    [Test]
    public void EnumerateKeysUnordered_on_a_cache_with_no_snapshot_yields_the_resident_keys()
    {
        var cache = FullyDecoded(Corpus(5));

        var seen = new List<string>();
        foreach (var key in cache.EnumerateKeysUnordered())
        {
            seen.Add(key);
        }

        Assert.That(seen, Has.Count.EqualTo(5));
    }

    [Test]
    public void The_resident_budget_evicts_clean_ranges_and_re_materialises_them_correctly()
    {
        var rows = Corpus(256, payloadBytes: 256);
        var reference = FullyDecoded(rows);
        // One block of this corpus is roughly 32 * 256 bytes, so a budget of two
        // blocks forces eviction once a third block lands.
        var cache = Attached(rows, budgetBytes: LeafSnapshotHydrationSource.BlockRows * 256L * 2);

        for (var i = 0; i < 256; i += LeafSnapshotHydrationSource.BlockRows)
        {
            Assert.That(cache.TryGetRow(Key(i), out _), Is.True);
        }

        Assert.That(cache.EvictedBlockCount, Is.GreaterThan(0), "the budget must actually bite");
        Assert.That(cache.HydratedRowCount, Is.LessThan(rows.Length), "the leaf must not be wholly resident");
        Assert.That(cache.Count, Is.EqualTo(reference.Count), "eviction never changes the logical row count");
        Assert.That(cache.StateBytes, Is.EqualTo(reference.StateBytes));
        Assert.That(cache.LiveCount, Is.EqualTo(reference.LiveCount));

        // An evicted range must come back byte-identical.
        Assert.That(cache.TryGetRow(Key(0), out var reRead), Is.True);
        Assert.That(reRead.Value, Is.EqualTo(Value(0, 256).Value));

        var walked = cache.EnumerateRows().ToArray();
        Assert.That(walked.Length, Is.EqualTo(rows.Length));
        for (var i = 0; i < rows.Length; i++)
        {
            Assert.That(walked[i].Key, Is.EqualTo(rows[i].Key));
            Assert.That(walked[i].Value.Value, Is.EqualTo(rows[i].Value.Value));
        }
    }

    [Test]
    public void A_mutated_range_is_pinned_and_survives_sustained_budget_pressure()
    {
        var rows = Corpus(256, payloadBytes: 256);
        var cache = Attached(rows, budgetBytes: LeafSnapshotHydrationSource.BlockRows * 256L);
        var replacement = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("pinned-write"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 9_000L },
        };

        cache.StoreRow(Key(0), replacement);
        Assert.That(cache.Remove(Key(1)), Is.True);

        // Drive every other block through the budget, repeatedly.
        for (var pass = 0; pass < 2; pass++)
        {
            for (var i = LeafSnapshotHydrationSource.BlockRows; i < 256; i += LeafSnapshotHydrationSource.BlockRows)
            {
                Assert.That(cache.TryGetRow(Key(i), out _), Is.True);
            }
        }

        Assert.That(cache.EvictedBlockCount, Is.GreaterThan(0));
        Assert.That(cache.TryGetRow(Key(0), out var read), Is.True);
        Assert.That(read.Value, Is.EqualTo(replacement.Value), "a write must never be evicted and re-read from the frame");
        Assert.That(cache.ContainsKey(Key(1)), Is.False, "a removal must never be resurrected by eviction");
        Assert.That(cache.Count, Is.EqualTo(rows.Length - 1));
    }

    [Test]
    public void A_zero_budget_hydrates_on_demand_and_never_evicts()
    {
        var cache = Attached(Corpus(256, payloadBytes: 256), budgetBytes: 0L);

        for (var i = 0; i < 256; i += LeafSnapshotHydrationSource.BlockRows)
        {
            Assert.That(cache.TryGetRow(Key(i), out _), Is.True);
        }

        Assert.That(cache.EvictedBlockCount, Is.Zero);
    }

    [Test]
    public void A_range_wider_than_the_budget_still_returns_every_row_it_was_asked_for()
    {
        var rows = Corpus(256, payloadBytes: 256);
        var cache = Attached(rows, budgetBytes: 1024L);

        var seen = new List<string>();
        foreach (var kv in cache.EnumerateRange(Key(0), Key(200)))
        {
            seen.Add(kv.Key);
        }

        Assert.That(seen, Has.Count.EqualTo(200),
            "the blocks the current operation is using are protected from its own trim");
    }

    [Test]
    public void Clear_drops_the_snapshot_backing_entirely()
    {
        var cache = Attached(Corpus(128));

        cache.Clear();

        Assert.That(cache.HasPendingHydration, Is.False);
        Assert.That(cache.Count, Is.Zero);
        Assert.That(cache.StateBytes, Is.Zero);
        Assert.That(cache.LiveCount, Is.Zero);
        Assert.That(cache.PendingHydrationRowCount, Is.Zero);
    }

    [Test]
    public void An_empty_snapshot_attaches_as_a_fully_hydrated_empty_cache()
    {
        var cache = NewCache();

        Assert.That(cache.TryAttachSnapshot(LeafSnapshotCodec.Encode([]), 0L), Is.True);

        Assert.That(cache.Count, Is.Zero);
        Assert.That(cache.HasPendingHydration, Is.True, "an empty frame has no block to hydrate");
        Assert.That(cache.EnumerateRows(), Is.Empty);
        Assert.That(cache.HasPendingHydration, Is.False);
    }

    [Test]
    public void TryPeekRow_materialises_the_row_without_draining_a_deferred_payload()
    {
        var rows = Corpus(64);
        var cache = Attached(rows);

        Assert.That(cache.TryPeekRow(Key(32), out var row, out var isDeferred), Is.True);
        Assert.That(isDeferred, Is.False);
        Assert.That(row.Value, Is.EqualTo(Value(32).Value));
    }

    [Test]
    public void StoreDeferredRow_over_a_snapshot_row_pins_the_range_and_materialises_lazily()
    {
        var rows = Corpus(128, payloadBytes: 256);
        var cache = Attached(rows, budgetBytes: LeafSnapshotHydrationSource.BlockRows * 256L);
        var payload = Encoding.UTF8.GetBytes("deferred-value");
        var metadata = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = new HybridLogicalClock { WallClockTicks = 9_000L },
        };

        cache.StoreDeferredRow(Key(0), metadata, () => payload, payload.Length);

        for (var i = LeafSnapshotHydrationSource.BlockRows; i < 128; i += LeafSnapshotHydrationSource.BlockRows)
        {
            Assert.That(cache.TryGetRow(Key(i), out _), Is.True);
        }

        Assert.That(cache.TryGetRow(Key(0), out var read), Is.True);
        Assert.That(read.Value, Is.EqualTo(payload));
    }

    [Test]
    public void Hydration_allocates_no_more_per_row_than_the_rows_themselves()
    {
        // Differential, never absolute, and sampled repeatedly with the minimum
        // kept: see AllocationProbe for why each of those is load-bearing. The
        // corpus and the frame are built in `prepare`, outside the measured
        // window, so only the hydration itself is charged.
        var growth = AllocationProbe.Growth(
            static rowCount =>
            {
                var cache = NewCache();
                cache.TryAttachSnapshot(LeafSnapshotCodec.Encode(Corpus(rowCount, payloadBytes: 64)), 0L);
                return cache;
            },
            static (cache, _) => cache.HydrateAll(),
            smallSize: 128,
            largeSize: 256);

        var perRow = growth / 128.0;
        var rowFootprint = 64 + (2 * IntPtr.Size);

        Assert.That(perRow, Is.LessThan(rowFootprint * 3),
            "the marginal cost of hydrating a row must be the row itself (key string, value array, node) "
            + "and not a per-row scratch buffer");
        Assert.That(perRow, Is.GreaterThan(0),
            "a hydrated row must cost something; a zero here would mean the probe is not measuring the work");
    }

    [Test]
    public void A_key_seek_allocates_nothing_beyond_a_fixed_cost()
    {
        var cache = Attached(Corpus(1024, payloadBytes: 32));

        var growth = AllocationProbe.Growth(
            _ => cache,
            static (probe, iterations) =>
            {
                var hits = 0;
                for (var i = 0; i < iterations; i++)
                {
                    if (probe.ContainsKey("absent-key"))
                    {
                        hits++;
                    }
                }

                AllocationProbe.ScalarSink = hits;
            },
            smallSize: 1_000,
            largeSize: 2_000);

        Assert.That(growth, Is.Zero,
            "a seek encodes its key into a stack buffer and probes the index table, so doubling the number "
            + "of seeks must not allocate a byte more");
        Assert.That(cache.SnapshotRowsMaterialised, Is.Zero, "and a miss must never materialise a row");
    }

    [Test]
    public void A_key_only_walk_after_eviction_still_yields_every_key_exactly_once()
    {
        var rows = Corpus(256, payloadBytes: 256);
        var cache = Attached(rows, budgetBytes: LeafSnapshotHydrationSource.BlockRows * 256L * 2);

        for (var i = 0; i < 256; i += LeafSnapshotHydrationSource.BlockRows)
        {
            Assert.That(cache.TryGetRow(Key(i), out _), Is.True);
        }

        Assert.That(cache.EvictedBlockCount, Is.GreaterThan(0));

        var seen = new List<string>();
        foreach (var key in cache.EnumerateKeysUnordered())
        {
            seen.Add(key);
        }

        Assert.That(seen.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(seen.Count),
            "an evicted block returns its keys to the snapshot half, so the two halves must stay disjoint");
        Assert.That(seen.Order(StringComparer.Ordinal).ToArray(),
            Is.EqualTo(rows.Select(r => r.Key).ToArray()).AsCollection);
    }

    [Test]
    public void A_bounded_range_walk_adds_no_allocation_over_a_bare_dictionary_walk()
    {
        // A differential of differentials, because the floor is not zero:
        // SortedDictionary's own struct enumerator allocates an internal node
        // stack per foreach, which today's whole-cache walk pays too (and pays
        // an interface box on top of). What must be zero is the marginal cost
        // the bounded wrapper adds over that floor.
        var cache = Attached(Corpus(512, payloadBytes: 32));
        var dictionary = cache.UnderlyingRows;

        var rangeGrowth = AllocationProbe.Growth(
            _ => cache,
            static (probe, iterations) =>
            {
                var count = 0;
                for (var i = 0; i < iterations; i++)
                {
                    foreach (var kv in probe.EnumerateRange(Key(0), Key(8)))
                    {
                        count += kv.Key.Length;
                    }
                }

                AllocationProbe.ScalarSink = count;
            },
            smallSize: 500,
            largeSize: 1_000);

        var dictionaryGrowth = AllocationProbe.Growth(
            _ => dictionary,
            static (rows, iterations) =>
            {
                var count = 0;
                for (var i = 0; i < iterations; i++)
                {
                    foreach (var kv in rows)
                    {
                        count += kv.Key.Length;
                        if (string.CompareOrdinal(kv.Key, Key(8)) >= 0)
                        {
                            break;
                        }
                    }
                }

                AllocationProbe.ScalarSink = count;
            },
            smallSize: 500,
            largeSize: 1_000);

        Assert.That(dictionaryGrowth, Is.GreaterThan(0L),
            "the comparison is only meaningful if the bare walk really does allocate a node stack per foreach; "
            + "a zero here would make the assertion below vacuous");
        Assert.That(rangeGrowth, Is.LessThanOrEqualTo(dictionaryGrowth),
            "RangeRows and its enumerator are structs resolved by pattern, so bounding a walk must cost "
            + "nothing beyond walking the dictionary at all");
    }

    [Test]
    public void A_key_only_walk_allocates_nothing_per_iteration_beyond_the_keys()
    {
        var cache = Attached(Corpus(256, payloadBytes: 32));
        cache.HydrateRange(Key(0), Key(64));
        var bytesReadBeforeWalks = cache.SnapshotBytesRead;

        var growth = AllocationProbe.Growth(
            _ => cache,
            static (probe, iterations) =>
            {
                var count = 0;
                for (var i = 0; i < iterations; i++)
                {
                    foreach (var key in probe.EnumerateKeysUnordered())
                    {
                        count += key.Length;
                    }
                }

                AllocationProbe.ScalarSink = count;
            },
            smallSize: 20,
            largeSize: 40);

        var perWalk = growth / 20.0;

        Assert.That(perWalk, Is.LessThan(256 * 64),
            "an unhydrated row contributes its key string and nothing else - no value array, no boxing");
        Assert.That(cache.SnapshotBytesRead, Is.EqualTo(bytesReadBeforeWalks),
            "and the walks themselves must not have decoded a payload; only the range hydration above reads bytes");
    }
}
