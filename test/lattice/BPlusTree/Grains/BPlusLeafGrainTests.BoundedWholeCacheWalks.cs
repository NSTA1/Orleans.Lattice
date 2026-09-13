using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Bounded whole-cache walks on the baseline-freeze and replayed range-delete
/// seams (issue #2771, epic #2368).
/// <para>
/// Both seams read every row the leaf holds, and both reached for a whole-cache
/// accessor to do it. That accessor calls <c>HydrateAll</c>, which ends in
/// <c>DetachSnapshot</c> and is irreversible: the lazily hydrated frame is gone
/// for the life of the activation and every row is resident with no later
/// eviction able to recover the footprint. The cost is not the transient
/// memory - it is that the frame is what lets an oversized leaf be divided
/// from frame keys alone, so the first whole-cache accessor to run forfeits the
/// cheap division permanently and the leaf can no longer be made smaller.
/// </para>
/// <para>
/// The fixtures below assert the repaired shape as two properties. First,
/// equivalence: a bounded window walk returns exactly what the one-pass walk
/// returned, element for element, because the windows are disjoint, exhaustive
/// and ascending. Second, and the property the epic is actually about, the
/// frame survives the walk - so a leaf that could divide cheaply before a
/// freeze or a range delete can still divide cheaply after one.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainBoundedWholeCacheWalkTests
{
    private const string TreeId = "tree-bounded-whole-cache-walk";
    private const int PayloadBytes = 256;

    /// <summary>Rows are a fixed width, so the corpus size is predictable.</summary>
    private static byte[] Payload(int i)
    {
        var bytes = new byte[PayloadBytes];
        for (var b = 0; b < bytes.Length; b++)
        {
            bytes[b] = (byte)((i + b) & 0xFF);
        }

        return bytes;
    }

    private static string Key(int i) => $"k{i:D6}";

    /// <summary>
    /// A corpus that mixes live rows, tombstones, and rows carrying a non-null
    /// merge mode, so an equivalence assertion covers all three discriminators
    /// rather than only the common one.
    /// </summary>
    private static LeafSnapshotRow[] Corpus(int rowCount)
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
                : new LeafSnapshotRow(
                    Key(i),
                    LwwValue<byte[]>.Create(
                        Payload(i),
                        new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i }),
                    i % 11 == 3 ? LatticeMergeMode.GCounter : null);
        }

        return rows;
    }

    private static BPlusLeafGrain CreateLeaf(long walHead = 0L)
    {
        var services = new ServiceCollection().BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = TreeId } };

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(walHead));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 },
            maxLeafKeys: 1_000_000,
            shardCount: 1,
            factory: grainFactory);

        return new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
    }

    /// <summary>
    /// A leaf whose cache is backed by a lazily hydrated snapshot with a
    /// resident budget well under the corpus, so the walk under test really is
    /// forced across several windows and really does evict between them.
    /// </summary>
    private static BPlusLeafGrain LeafWithAttachedSnapshot(
        LeafSnapshotRow[] rows,
        long residentBudgetBytes = 16L * 1024)
    {
        var grain = CreateLeaf();
        Assert.That(
            grain.CacheForTest.TryAttachSnapshot(LeafSnapshotCodec.Encode(rows), residentBudgetBytes),
            Is.True,
            "the corpus must back a bounded read, or the fixture proves nothing");
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
        return grain;
    }

    /// <summary>A leaf holding the same rows with nothing left lazily hydrated.</summary>
    private static BPlusLeafGrain LeafWithDecodedRows(LeafSnapshotRow[] rows)
    {
        var grain = CreateLeaf();
        foreach (var row in rows)
        {
            grain.CacheForTest.StoreRow(row.Key, row.Value);
            if (row.MergeMode is { } mode)
            {
                grain.CacheForTest.SetMergeMode(row.Key, mode);
            }
        }

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.False);
        return grain;
    }

    private static LatticeMutation DeleteRange(string startInclusive, string endExclusive)
        => new()
        {
            TreeId = TreeId,
            Kind = MutationKind.DeleteRange,
            Key = startInclusive,
            EndExclusiveKey = endExclusive,
            Timestamp = new HybridLogicalClock { WallClockTicks = 900_000L },
            IsTombstone = true,
        };

    private static IReadOnlyList<string> TombstonedKeys(BPlusLeafGrain grain, LeafSnapshotRow[] corpus)
    {
        var tombstoned = new List<string>();
        foreach (var row in corpus)
        {
            if (grain.CacheForTest.TryGetRow(row.Key, out var current) && current.IsTombstone)
            {
                tombstoned.Add(row.Key);
            }
        }

        return tombstoned;
    }

    // ---------------------------------------------------------------------
    // FreezeProjectionAsync (BPlusLeafGrain.FrozenBaseline.cs)
    // ---------------------------------------------------------------------

    [Test]
    public async Task A_baseline_freeze_leaves_the_lazily_hydrated_frame_attached()
    {
        var corpus = Corpus(512);
        var grain = LeafWithAttachedSnapshot(corpus);

        // The property that matters, established before the freeze so the
        // assertion after it is a change rather than a coincidence: an
        // oversized leaf can choose its division pivot from frame keys alone.
        Assert.That(grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _), Is.True);

        await grain.FreezeProjectionAsync(CancellationToken.None);

        Assert.That(
            grain.CacheForTest.HasPendingHydration,
            Is.True,
            "the freeze must not detach the frame - a shard baseline capture drives it across "
            + "every leaf in the chain, so detaching here strands the whole shard");
        Assert.That(
            grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _),
            Is.True,
            "a leaf that could divide cheaply before the freeze must still divide cheaply after it");
    }

    [Test]
    public async Task A_baseline_freeze_returns_exactly_the_rows_the_whole_cache_walk_returned()
    {
        var corpus = Corpus(512);

        var bounded = await LeafWithAttachedSnapshot(corpus).FreezeProjectionAsync(CancellationToken.None);
        var reference = await LeafWithDecodedRows(corpus).FreezeProjectionAsync(CancellationToken.None);

        Assert.That(bounded.Rows.Select(r => r.Key), Is.EqualTo(reference.Rows.Select(r => r.Key)).AsCollection);
        Assert.That(
            bounded.Rows.Select(r => r.Value.Value),
            Is.EqualTo(reference.Rows.Select(r => r.Value.Value)).AsCollection);
        Assert.That(
            bounded.Rows.Select(r => r.Value.IsTombstone),
            Is.EqualTo(reference.Rows.Select(r => r.Value.IsTombstone)).AsCollection);
    }

    [Test]
    public async Task A_baseline_freeze_stamps_the_per_key_merge_mode_on_every_window()
    {
        var corpus = Corpus(512);
        var expected = corpus.ToDictionary(r => r.Key, r => r.MergeMode, StringComparer.Ordinal);

        var freeze = await LeafWithAttachedSnapshot(corpus).FreezeProjectionAsync(CancellationToken.None);

        Assert.That(freeze.Rows, Has.Count.EqualTo(corpus.Length));

        // The merge mode is read while the row is resident. Reading it through
        // the hydrating accessor instead would end in a trim, evicting rows
        // from the dictionary the window walk's enumerator is positioned on -
        // a hazard that only exists once the walk stops detaching the frame,
        // which is why it has to be pinned here rather than assumed.
        foreach (var row in freeze.Rows)
        {
            Assert.That(row.MergeMode, Is.EqualTo(expected[row.Key]), $"merge mode drifted for {row.Key}");
        }

        Assert.That(
            freeze.Rows.Count(r => r.MergeMode is not null),
            Is.EqualTo(corpus.Count(r => r.MergeMode is not null)),
            "a corpus with no merge modes at all would make the assertion above vacuous");
    }

    [Test]
    public async Task A_baseline_freeze_is_identical_whether_or_not_a_budget_forces_several_windows()
    {
        var corpus = Corpus(512);

        // An unbounded budget degenerates the window list to a single
        // unbounded window, so this arm walks in one pass while the other
        // walks in many. Both must produce the same freeze.
        var onePass = await LeafWithAttachedSnapshot(corpus, residentBudgetBytes: 0L)
            .FreezeProjectionAsync(CancellationToken.None);
        var manyWindows = await LeafWithAttachedSnapshot(corpus, residentBudgetBytes: 4L * 1024)
            .FreezeProjectionAsync(CancellationToken.None);

        Assert.That(
            manyWindows.Rows.Select(r => r.Key),
            Is.EqualTo(onePass.Rows.Select(r => r.Key)).AsCollection);
        Assert.That(
            manyWindows.Rows.Select(r => r.MergeMode),
            Is.EqualTo(onePass.Rows.Select(r => r.MergeMode)).AsCollection);
    }

    // ---------------------------------------------------------------------
    // ApplyDeleteRange (BPlusLeafGrain.Projection.cs)
    // ---------------------------------------------------------------------

    [Test]
    public void A_replayed_range_delete_leaves_the_lazily_hydrated_frame_attached()
    {
        var corpus = Corpus(512);
        var grain = LeafWithAttachedSnapshot(corpus);

        ((ILeafProjection)grain).Apply(DeleteRange(Key(64), Key(128)));

        Assert.That(
            grain.CacheForTest.HasPendingHydration,
            Is.True,
            "a range delete over part of the leaf must not materialise - and strand - the rest of it");
        Assert.That(grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _), Is.True);
    }

    [Test]
    public void A_replayed_range_delete_tombstones_exactly_the_keys_the_whole_cache_walk_selected()
    {
        var corpus = Corpus(512);
        var mutation = DeleteRange(Key(64), Key(128));

        var bounded = LeafWithAttachedSnapshot(corpus);
        var reference = LeafWithDecodedRows(corpus);
        ((ILeafProjection)bounded).Apply(mutation);
        ((ILeafProjection)reference).Apply(mutation);

        var expected = corpus
            .Where(r => string.CompareOrdinal(r.Key, Key(64)) >= 0
                && string.CompareOrdinal(r.Key, Key(128)) < 0)
            .Select(r => r.Key)
            .ToArray();

        Assert.That(expected, Is.Not.Empty, "the range must actually select rows");
        Assert.That(TombstonedKeys(bounded, corpus), Is.EqualTo(TombstonedKeys(reference, corpus)).AsCollection);
        Assert.That(TombstonedKeys(bounded, corpus), Is.SupersetOf(expected));
    }

    [Test]
    public void A_replayed_range_delete_spanning_many_windows_misses_no_key_at_a_window_boundary()
    {
        // A tight budget puts a window boundary every few blocks, so the range
        // below straddles several of them. Stitching the windows back together
        // is the one thing a clipped walk can get wrong, and a dropped or
        // duplicated key at a boundary is exactly how it would show.
        var corpus = Corpus(512);
        var grain = LeafWithAttachedSnapshot(corpus, residentBudgetBytes: 4L * 1024);

        ((ILeafProjection)grain).Apply(DeleteRange(Key(5), Key(500)));

        var expected = corpus
            .Where(r => string.CompareOrdinal(r.Key, Key(5)) >= 0
                && string.CompareOrdinal(r.Key, Key(500)) < 0)
            .Select(r => r.Key)
            .ToArray();

        foreach (var key in expected)
        {
            Assert.That(grain.CacheForTest.TryGetRow(key, out var row), Is.True, $"{key} vanished");
            Assert.That(row.IsTombstone, Is.True, $"{key} was not tombstoned");
        }

        // Rows outside the range are untouched on both sides of it, which is
        // what rules out a window whose clip ran wide.
        Assert.That(grain.CacheForTest.TryGetRow(Key(4), out var below), Is.True);
        Assert.That(below.IsTombstone, Is.False);
        Assert.That(grain.CacheForTest.TryGetRow(Key(500), out var above), Is.True);
        Assert.That(above.IsTombstone, Is.False);
    }

    [Test]
    public void A_replayed_range_delete_selecting_nothing_leaves_the_leaf_untouched()
    {
        var corpus = Corpus(128);
        var grain = LeafWithAttachedSnapshot(corpus);

        // Ordinally above every key in the corpus, so every window clips empty.
        ((ILeafProjection)grain).Apply(DeleteRange("z0", "z9"));

        Assert.That(TombstonedKeys(grain, corpus), Is.EqualTo(
            corpus.Where(r => r.Value.IsTombstone).Select(r => r.Key).ToArray()).AsCollection);
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
    }

    // ---------------------------------------------------------------------
    // LeafEntryCache.GetMergeModeWithoutHydrating
    // ---------------------------------------------------------------------

    [Test]
    public void GetMergeModeWithoutHydrating_rejects_a_null_key()
    {
        var grain = LeafWithAttachedSnapshot(Corpus(64));
        Assert.Throws<ArgumentNullException>(() => grain.CacheForTest.GetMergeModeWithoutHydrating(null!));
    }

    [Test]
    public void GetMergeModeWithoutHydrating_agrees_with_the_hydrating_accessor_for_a_resident_row()
    {
        var corpus = Corpus(64);
        var cache = LeafWithDecodedRows(corpus).CacheForTest;

        foreach (var row in corpus)
        {
            Assert.That(
                cache.GetMergeModeWithoutHydrating(row.Key),
                Is.EqualTo(cache.GetMergeMode(row.Key)),
                $"merge mode disagreed for {row.Key}");
        }
    }

    [Test]
    public void GetMergeModeWithoutHydrating_reads_nothing_out_of_the_frame()
    {
        var corpus = Corpus(512);
        var cache = LeafWithAttachedSnapshot(corpus).CacheForTest;

        // Deliberately asked for a key whose merge mode IS recorded in the
        // frame, so a null answer here is the absence of hydration rather than
        // the absence of a mode - the accessor is only correct where residency
        // has already been established, and this pins that it does not sneak
        // one in.
        var keyWithMode = corpus.First(r => r.MergeMode is not null).Key;

        Assert.That(cache.GetMergeModeWithoutHydrating(keyWithMode), Is.Null);
        Assert.That(cache.SnapshotBytesRead, Is.Zero);
        Assert.That(cache.HydratedRowCount, Is.Zero);
    }
}
