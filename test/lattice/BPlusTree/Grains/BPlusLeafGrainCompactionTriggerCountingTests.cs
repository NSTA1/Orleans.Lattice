using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The compaction-policy scans must not forfeit the leaf-division fast path
/// (issue #2787).
/// <para>
/// Both scans only count live rows against tombstones - they retain nothing -
/// yet both reached for the whole-cache row view, which materialises every row
/// and then detaches the snapshot frame irreversibly. That mattered here more
/// than at any other seam for two reasons. The trigger runs on <em>every</em>
/// successful foreground commit once either threshold is configured, so on a
/// write-heavy tree it was reliably the first operation to consume a leaf's
/// frame; and the sampler has no threshold guard at all, so it ran
/// unconditionally at every compaction pass entry. An observability sample was
/// therefore enough to leave a leaf able to divide only by materialising itself
/// whole - the allocation an oversized leaf can least afford, and the one the
/// division exists to make unnecessary.
/// </para>
/// <para>
/// Both quantities were already available in O(1) and residual-aware, so the
/// fix is a substitution rather than a bounded re-walk: it is exact by the
/// identical tombstone predicate, and it materialises nothing whatsoever. The
/// figures below are asserted unchanged, and the counters are pinned against a
/// full recount, because an incrementally maintained counter is the one thing
/// the substitution gives up against a per-call recomputation.
/// </para>
/// </summary>
public sealed class BPlusLeafGrainCompactionTriggerCountingTests
{
    private const int RowCount = 512;

    /// <summary>Every seventh row is a tombstone, matching the shared corpus shape.</summary>
    private static bool IsTombstoneRow(int i) => i % 7 == 6;

    private static int ExpectedTombstones
    {
        get
        {
            var count = 0;
            for (var i = 0; i < RowCount; i++)
            {
                if (IsTombstoneRow(i)) count++;
            }

            return count;
        }
    }

    private static string Key(int i) => $"k{i:D6}";

    private static LeafSnapshotRow[] Rows()
    {
        var rows = new LeafSnapshotRow[RowCount];
        for (var i = 0; i < RowCount; i++)
        {
            rows[i] = IsTombstoneRow(i)
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
                    new LwwValue<byte[]>
                    {
                        Value = new byte[256],
                        Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
                    });
        }

        return rows;
    }

    private static async Task<(BPlusLeafGrain Grain, ITombstoneCompactionGrain Compactor)> RehydratedLeafAsync()
    {
        var compactor = Substitute.For<ITombstoneCompactionGrain>();
        compactor.RequestCompactionAsync(Arg.Any<int>(), Arg.Any<string>()).Returns(Task.FromResult(true));

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 25L,
                EncodedRows = LeafSnapshotCodec.Encode(Rows()),
                SnapshotOffsetsByPartition = [25L],
            }));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ITombstoneCompactionGrain>(Arg.Any<string>()).Returns(compactor);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-compaction-fold";
        state.State.ShardIndex = 0;

        var resolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaxLeafBytes = 64L * 1024,
                LeafPartialHydrationEnabled = true,
                LeafHydrationResidentBytes = 8L * 1024,

                // Both thresholds enabled, as the repository-context churn trees
                // configure them. With both at their defaults the trigger returns
                // before it scans anything and the arm below would be vacuous.
                MinTombstoneRatioForCompaction = 0.01,
                MaxLeafEntriesBeforeForcedCompaction = 1,
            },
            maxLeafKeys: 1_000_000,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        Assert.That(
            await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None),
            Is.True,
            "the leaf must come online from its snapshot without decoding it");
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "activation must not materialise a row - otherwise these arms measure nothing");

        return (grain, compactor);
    }

    /// <summary>
    /// Long enough that no tombstone in the corpus is old enough to reap, so a
    /// pass sampled at entry reaps nothing and the arm measures the sampler
    /// rather than the reaper.
    /// </summary>
    private static readonly TimeSpan ReapNothing = TimeSpan.FromDays(365);

    [Test]
    public async Task Sampling_the_tombstone_ratio_leaves_the_division_fast_path_available()
    {
        var (grain, _) = await RehydratedLeafAsync();

        var measurements = new List<double>();
        using (MeterListening.StartForInstrument(
            LatticeMetrics.LeafTombstoneRatio,
            l => l.SetMeasurementEventCallback<double>((_, value, _, _) =>
            {
                lock (measurements) measurements.Add(value);
            })))
        {
            // Driven through the public compaction seam, which samples at pass
            // entry. Reaching the sampler by reflection instead would prove it
            // folds correctly when called and nothing about whether the pass
            // calls it - the precise defect this epic exists to fix, and the one
            // ReflectionSeamAuditTests guards (issue #2735).
            var reaped = await grain.CompactTombstonesAsync(ReapNothing);
            Assert.That(reaped, Is.Zero, "the grace period must hold every tombstone back");
        }

        var expectedRatio = (double)ExpectedTombstones / RowCount;

        Assert.Multiple(() =>
        {
            // Non-vacuity: a sampler that recorded nothing would satisfy every
            // other assertion here trivially.
            Assert.That(
                measurements, Has.Count.EqualTo(1),
                "the sampler must still record exactly one ratio");
            Assert.That(
                measurements[0], Is.EqualTo(expectedRatio).Within(1e-9),
                "the bounded fold must produce the same ratio as the whole-cache walk");

            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "sampling a ratio must not consume the leaf's snapshot frame");
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.True,
                "after sampling, the leaf must still be able to place a cut from the frame alone");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
        });
    }

    [Test]
    public async Task Evaluating_the_compaction_trigger_leaves_the_division_fast_path_available()
    {
        var (grain, compactor) = await RehydratedLeafAsync();

        // Driven through the public delete seam. EvaluateCompactionTrigger has
        // exactly two production call sites, DeleteAsync and DeleteRangeAsync,
        // and the call is unconditional in both - so the public seam is not
        // merely preferable to reflection here, it is cheap and available.
        Assert.That(
            await grain.DeleteAsync(Key(0)),
            Is.True,
            "precondition: the delete must land, or the trigger is never reached");

        // Non-vacuity, and it is load-bearing: the trigger returns early when
        // neither threshold is configured and when the leaf holds no
        // tombstones. Either would make the frame assertions below pass without
        // the scan ever running - green, and identical against unconverted
        // code. The dispatch is the only signal here that is non-zero solely
        // because the fold ran to completion, so it is asserted first.
        await compactor.Received(1).RequestCompactionAsync(Arg.Any<int>(), Arg.Any<string>());

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.Not.EqualTo(LeafSnapshotDetachSeam.EnumerateRowsAccessor),
                "evaluating the trigger must not consume the leaf's snapshot frame; this runs on "
                + "every foreground commit, so a detach here forfeits the division fast path "
                + "almost immediately on a write-heavy tree");
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.True,
                "after a delete, the leaf must still place a cut from the frame alone");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
        });
    }

    [Test]
    public async Task The_scans_materialise_nothing_beyond_the_written_block()
    {
        var (grain, _) = await RehydratedLeafAsync();

        await grain.CompactTombstonesAsync(ReapNothing);
        var afterSample = grain.CacheForTest.HydratedRowCount;

        Assert.That(
            afterSample,
            Is.Zero,
            "the ratio sample is read in O(1), so a compaction pass that reaps nothing may not "
            + "materialise a single row; a bounded fold would leave a window's worth resident, "
            + "and this is stronger than that");
    }

    [Test]
    public async Task The_substituted_counters_agree_with_a_full_recount()
    {
        var (grain, _) = await RehydratedLeafAsync();
        var cache = grain.CacheForTest;

        // The O(1) counters are maintained incrementally, where the enumeration
        // they replaced recomputed from the rows themselves on every call. That
        // is the one thing the substitution gives up, so it is pinned here: a
        // drift in the accounting would otherwise be inherited silently by the
        // trigger instead of being corrected at the next commit.
        var recountTotal = 0;
        var recountTombstones = 0;
        foreach (var (_, lww) in cache.EnumerateRows())
        {
            recountTotal++;
            if (lww.IsTombstone) recountTombstones++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(recountTotal, Is.EqualTo(RowCount), "the corpus must be fully visible");
            Assert.That(recountTombstones, Is.EqualTo(ExpectedTombstones));
            Assert.That(
                cache.Count, Is.EqualTo(recountTotal),
                "Count must equal the enumerated row count");
            Assert.That(
                cache.Count - cache.LiveCount, Is.EqualTo((long)recountTombstones),
                "Count - LiveCount must equal the tombstone count the enumeration produced");
        });
    }
}
