using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Attribution for a forfeited leaf-division fast path (issue #2787).
/// <para>
/// A leaf divides cheaply only while its lazily hydrated snapshot frame is
/// still attached. Detaching is irreversible for the life of the activation, so
/// the first whole-cache operation to run forfeits the fast path for every
/// later one - including a division, which then has to materialise the whole
/// leaf to place a cut. The split seam cannot see any of this: it observes a
/// bare <see langword="false"/> from the bisect, identical whether the leaf
/// never had a frame or an unrelated operation consumed it.
/// </para>
/// <para>
/// Those two cases have opposite costs. A leaf replayed from the write-ahead log
/// never attaches a frame and its rows are already resident, so the fallback is
/// free; a leaf whose frame was consumed pays for the whole leaf, resident and
/// unsheddable. The fixtures below pin that the pair
/// (refusal reason, detaching seam) separates them, because neither value does
/// so alone.
/// </para>
/// </summary>
public sealed class LeafSnapshotDetachAttributionTests
{
    private static LeafEntryCache NewCache()
        => new(new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal));

    private static string Key(int i) => $"k{i:D5}";

    private static LeafSnapshotRow[] Corpus(int rowCount)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = new LeafSnapshotRow(
                Key(i),
                new LwwValue<byte[]>
                {
                    Value = new byte[64],
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
                });
        }

        return rows;
    }

    private static LeafEntryCache CacheWithFrame(int rowCount, long budgetBytes = 0L)
    {
        var cache = NewCache();
        Assert.That(
            cache.TryAttachSnapshot(LeafSnapshotCodec.Encode(Corpus(rowCount)), budgetBytes),
            Is.True,
            "the frame must attach, or the fixture measures a cache that never had a fast path");
        return cache;
    }

    // ---------------------------------------------------------------
    // The benign case: no frame was ever attached.
    // ---------------------------------------------------------------

    [Test]
    public void A_cache_that_never_attached_a_frame_reports_no_detaching_seam()
    {
        var cache = NewCache();

        // Runs the accessor that detaches on a frame-backed cache. Nothing is
        // forfeited here because there was nothing to forfeit.
        _ = cache.EnumerateRows().ToList();

        Assert.That(
            cache.LastDetachSeam,
            Is.EqualTo(LeafSnapshotDetachSeam.None),
            "a cache with no frame must not report a detach - otherwise a leaf replayed from the "
            + "write-ahead log is indistinguishable from one whose frame was consumed, and only "
            + "the second is a defect");
    }

    [Test]
    public void A_refusal_with_no_frame_ever_attached_pairs_the_reason_with_no_seam()
    {
        var cache = NewCache();

        Assert.Multiple(() =>
        {
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.False);
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.NoSnapshotAttached));
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "reason alone cannot classify this refusal; the seam is what makes it benign");
        });
    }

    // ---------------------------------------------------------------
    // The harmful case: a frame existed and something else consumed it.
    // ---------------------------------------------------------------

    [Test]
    public void A_refusal_after_a_whole_cache_accessor_pairs_the_same_reason_with_the_guilty_seam()
    {
        var cache = CacheWithFrame(rowCount: 64);

        Assert.That(
            cache.TryGetBisectingKeyWithoutHydrating(out _, out var beforeReason),
            Is.True,
            "the frame is attached, so the fast path must be available before anything consumes it");
        Assert.That(beforeReason, Is.EqualTo(LeafBisectRefusalReason.None));

        _ = cache.Keys.ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out var afterReason),
                Is.False);
            Assert.That(
                afterReason,
                Is.EqualTo(LeafBisectRefusalReason.NoSnapshotAttached),
                "the refusal reason is identical to the never-attached case, which is exactly why "
                + "the reason on its own is not a usable instrument");
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.KeysAccessor),
                "the seam is the only signal that separates this expensive refusal from the free one");
        });
    }

    [Test]
    public void Each_whole_cache_accessor_records_itself_as_the_detaching_seam()
    {
        var viaKeys = CacheWithFrame(rowCount: 16);
        _ = viaKeys.Keys.ToList();

        var viaRows = CacheWithFrame(rowCount: 16);
        _ = viaRows.EnumerateRows().ToList();

        var viaUnderlying = CacheWithFrame(rowCount: 16);
        _ = viaUnderlying.UnderlyingRows;

        Assert.Multiple(() =>
        {
            Assert.That(viaKeys.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.KeysAccessor));
            Assert.That(viaRows.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.EnumerateRowsAccessor));
            Assert.That(viaUnderlying.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.UnderlyingRowsAccessor));
        });
    }

    [Test]
    public void Clearing_a_frame_backed_cache_records_the_clear_seam()
    {
        var cache = CacheWithFrame(rowCount: 16);

        cache.Clear();

        Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.Clear));
    }

    // ---------------------------------------------------------------
    // The bounded route must NOT detach, which is the property the fix relies on.
    // ---------------------------------------------------------------

    [Test]
    public void A_bounded_window_fold_leaves_the_frame_attached_and_the_fast_path_available()
    {
        var cache = CacheWithFrame(rowCount: 512, budgetBytes: 4L * 1024);

        var visited = 0;
        foreach (var (startInclusive, endExclusive) in cache.GetFullScanWindowsWithoutHydrating())
        {
            foreach (var _ in cache.EnumerateRange(startInclusive, endExclusive))
            {
                visited++;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(512), "the fold must still visit every row exactly once");
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "a bounded fold must leave the frame attached; if this reddens, every caller "
                + "migrated to the fold is silently still forfeiting the division fast path");
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.True,
                "the leaf must still be able to place a cut after a full bounded scan");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
        });
    }

    [Test]
    public void A_whole_cache_scan_of_the_same_corpus_forfeits_what_the_fold_preserves()
    {
        // The mirror of the arm above, on an identical corpus. Its value is not
        // that it reddens - it is that it stays green and shows the difference
        // is the accessor, not the corpus.
        var cache = CacheWithFrame(rowCount: 512, budgetBytes: 4L * 1024);

        var visited = cache.EnumerateRows().Count();

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(512), "both routes must visit the same rows");
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.EnumerateRowsAccessor));
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out _),
                Is.False,
                "the whole-cache route forfeits the fast path - this is the behaviour the "
                + "migrated callers exist to stop");
        });
    }

    // ---------------------------------------------------------------
    // Frame-shape refusals, which are not size related.
    // ---------------------------------------------------------------

    [Test]
    public void A_single_row_frame_refuses_because_it_has_no_interior_pivot()
    {
        var cache = CacheWithFrame(rowCount: 1);

        Assert.Multiple(() =>
        {
            Assert.That(cache.TryGetBisectingKeyWithoutHydrating(out _, out var reason), Is.False);
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.TooFewRows));
        });
    }

    [Test]
    public void A_successful_bisect_reports_no_refusal_reason_and_leaves_the_frame_attached()
    {
        var cache = CacheWithFrame(rowCount: 64);

        Assert.Multiple(() =>
        {
            Assert.That(cache.TryGetBisectingKeyWithoutHydrating(out var key, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
            Assert.That(key, Is.EqualTo(Key(32)));
            Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None));
        });
    }

    [Test]
    public void The_reason_reporting_overload_agrees_with_the_bare_one()
    {
        // The bare overload is the one production called before the reason
        // existed, and several fixtures still use it. It must stay a pure
        // delegation, or the instrument measures a different decision from the
        // one the split path actually takes.
        var attached = CacheWithFrame(rowCount: 64);
        var detached = NewCache();

        Assert.Multiple(() =>
        {
            Assert.That(
                attached.TryGetBisectingKeyWithoutHydrating(out var bareKey),
                Is.EqualTo(attached.TryGetBisectingKeyWithoutHydrating(out var reasonKey, out _)));
            Assert.That(bareKey, Is.EqualTo(reasonKey));
            Assert.That(
                detached.TryGetBisectingKeyWithoutHydrating(out _),
                Is.EqualTo(detached.TryGetBisectingKeyWithoutHydrating(out _, out _)));
        });
    }

    // ---------------------------------------------------------------
    // Two-sided proof at the real split seam.
    //
    // Everything above runs against the cache in isolation. These two arms run
    // against an actual grain, and they exist because an instrument that can
    // only ever report one answer is exactly as vacuous as the tests it was
    // built to replace - and would not look it. A constant proves nothing
    // unless the assertion references something other than that constant, so
    // the same instrument is asserted to report BOTH answers on two leaves that
    // differ only in whether their frame survived. In each case the frame's
    // state is established WITHOUT the instrument, from the resident row count,
    // so neither arm is satisfiable by an instrument stuck on one value.
    // ---------------------------------------------------------------

    private static async Task<BPlusLeafGrain> RehydratedLeafAsync(int rowCount)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 25L,
                EncodedRows = LeafSnapshotCodec.Encode(Corpus(rowCount)),
                SnapshotOffsetsByPartition = [25L],
            }));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-attribution";
        state.State.ShardIndex = 0;

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            TestOptionsResolver.Create(
                baseOptions: new LatticeOptions
                {
                    WalPartitions = 1,
                    LeafPartialHydrationEnabled = true,
                    LeafHydrationResidentBytes = 4L * 1024,
                },
                maxLeafKeys: 1_000_000,
                shardCount: 1,
                factory: grainFactory),
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        Assert.That(
            await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None),
            Is.True,
            "the leaf must come online from its snapshot without decoding it");
        return grain;
    }

    [Test]
    public async Task A_leaf_whose_frame_survived_reports_the_fast_path_was_available()
    {
        var grain = await RehydratedLeafAsync(512);

        // The positive side. The frame is provably attached, established
        // without the instrument: no row has been materialised, which only an
        // attached frame permits.
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "precondition: nothing may be resident, or the frame has already gone");

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out var key, out var reason),
                Is.True,
                "a leaf with an attached frame must place its cut from the frame alone");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
            Assert.That(key, Is.Not.Null.And.Not.Empty);
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None));
        });
    }

    [Test]
    public async Task A_leaf_whose_frame_was_consumed_reports_the_fast_path_was_forfeited()
    {
        var grain = await RehydratedLeafAsync(512);

        // The negative side, seeded deliberately through EntriesForTest.
        //
        // That accessor is the trap this instrument exists to close: it routes
        // through UnderlyingRows -> HydrateAll -> DetachSnapshot, so a test that
        // touches it has forfeited the frame before asserting anything. Which is
        // precisely what makes it the most reliable way to construct a
        // known-detached leaf - the detach is guaranteed by source, not by the
        // instrument under test. Do not "clean this up" to a gentler accessor:
        // the guarantee is the entire point of using it here.
        Assert.That(grain.EntriesForTest, Is.Not.Empty, "the accessor must materialise the leaf");

        // Again established independently of the instrument: every row is now
        // resident, which only a full hydration can achieve.
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.EqualTo(512),
            "precondition: the whole leaf must be resident, proving the frame was released");

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.False,
                "with the frame gone the leaf cannot pivot without materialising itself");
            Assert.That(
                reason,
                Is.EqualTo(LeafBisectRefusalReason.NoSnapshotAttached),
                "this is refusal reason 1 - the one an unconverted seam produces");
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.UnderlyingRowsAccessor),
                "and the seam must name EntriesForTest's route, not the division itself");
        });
    }
}
