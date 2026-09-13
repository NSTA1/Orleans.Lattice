using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The digest paths must not forfeit the leaf-division fast path
/// (issue #2852, epic #2368).
/// <para>
/// <c>ComputeStructuralLeafCounts</c> counted tombstones by streaming the
/// whole-cache row view, which routes through <c>HydrateAll</c> and ends in
/// <c>DetachSnapshot</c>. That method sits on the digest path of <em>every</em>
/// mutation and on the adoption path an internal node drives at child seeding
/// (<c>SetParentAsync</c> followed immediately by
/// <c>GetChildDigestSnapshotAsync</c>), so a leaf's snapshot frame was consumed
/// before it had taken a single write - and a leaf whose frame is gone can only
/// divide by materialising itself whole. The same accessor backed both arms of
/// <c>GetProjectionDigestForRangeAsync</c>, which additionally made a
/// <em>bounded</em> digest read cost the whole leaf.
/// </para>
/// <para>
/// No fixture caught any of it, and the reason is structural rather than
/// accidental: every existing digest fixture constructs the grain with a
/// <see cref="FakePersistentState{T}"/> that never sets <c>ParentId</c>, and
/// <c>PublishDigestUpwardAsync</c> returns early on a null parent. The digest
/// path had therefore never been driven with a parent present at all, let alone
/// with a frame attached. Closing that hole is the first arm below.
/// </para>
/// <para>
/// Frame state is established with the resident row count rather than with the
/// instrument under test, for the reason
/// <c>LeafSnapshotDetachAttributionTests</c> sets out: a test that configures a
/// scenario through the same accessor whose behaviour it is asserting is the
/// instrument agreeing with itself.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainDigestFrameTests
{
    private const string TreeId = "tree-digest-frame";
    private const int RowCount = 512;
    private const int PayloadBytes = 256;

    /// <summary>Comfortably under the corpus, so the walk is forced across windows.</summary>
    private const long SmallBudgetBytes = 8L * 1024;

    /// <summary>
    /// Comfortably over the corpus. This is the adversarial ratio: a conversion
    /// that is form-only - converted in shape, unchanged in cost - still passes
    /// every row-level equivalence assertion here, and is caught only by the
    /// resident-row-count assertions on this arm.
    /// </summary>
    private const long LargeBudgetBytes = 8L * 1024 * 1024;

    private static readonly GrainId ParentId = GrainId.Create("internal", "digest-frame-parent");

    private static string Key(int i) => $"k{i:D6}";

    /// <summary>Every seventh row is a tombstone, so the live/tombstone split is non-trivial.</summary>
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

    private static byte[] Payload(int i)
    {
        var bytes = new byte[PayloadBytes];
        for (var b = 0; b < bytes.Length; b++)
        {
            bytes[b] = (byte)((i + b) & 0xFF);
        }

        return bytes;
    }

    private static LeafSnapshotRow[] Corpus()
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
                    LwwValue<byte[]>.Create(
                        Payload(i),
                        new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i }),
                    i % 11 == 3 ? LatticeMergeMode.GCounter : null);
        }

        return rows;
    }

    private sealed record Harness(
        BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        IBPlusInternalGrain Parent);

    private static Harness CreateLeaf(GrainId? parentId, long residentBudgetBytes)
    {
        var h = CreateLeafWithoutFrame(parentId);

        Assert.That(
            h.Grain.CacheForTest.TryAttachSnapshot(LeafSnapshotCodec.Encode(Corpus()), residentBudgetBytes),
            Is.True,
            "the corpus must back a bounded read, or every frame assertion here is vacuous");

        Assert.That(
            h.Grain.CacheForTest.HasPendingHydration,
            Is.True,
            "seeding must leave the frame attached");
        Assert.That(
            h.Grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "seeding must not materialise a row - otherwise these arms measure the seeding");

        return h;
    }

    private static Harness CreateLeafWithoutFrame(GrainId? parentId)
    {
        var services = new ServiceCollection().BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var parent = Substitute.For<IBPlusInternalGrain>();
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(parent);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var state = new FakePersistentState<LeafNodeState>
        {
            State =
            {
                TreeId = TreeId,
                ShardIndex = 0,
                ParentId = parentId,

                // The running projection hash is seeded rather than left null
                // so the lazy backfill (a full, if already-bounded, walk)
                // cannot run inside the seam under test and supply the
                // residency this fixture measures. Only the digest paths' own
                // reads are measured. Its value is never asserted: the range
                // digest folds the rows themselves and does not read it.
                ProjectionHash = new byte[16],
            },
        };

        var resolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaxLeafBytes = 64L * 1024 * 1024,

                // Coalescing off so a foreground write publishes inline. With
                // the default 5 ms window the publish would be deferred to a
                // timer tick, and the arm would assert against a digest path
                // that had not yet run.
                DigestCoalescingWindowMs = 0,
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

        return new Harness(grain, state, parent);
    }

    /// <summary>
    /// Asserts that no whole-cache accessor released the frame.
    /// </summary>
    /// <remarks>
    /// Deliberately an exclusion of the four accessor seams rather than an
    /// equality against <see cref="LeafSnapshotDetachSeam.None"/>.
    /// <see cref="LeafSnapshotDetachSeam.RangeHydrationCompleted"/> is a
    /// legitimate outcome for a bounded walk whose windows happened to cover
    /// every block inside the resident budget, so pinning to <c>None</c> would
    /// make the verdict depend on a budget-versus-leaf ratio the caller does
    /// not control, and would fail by construction on the small-leaf arms. The
    /// four accessor seams are the defect; the other members are not.
    /// </remarks>
    private static void AssertNoWholeCacheSeam(BPlusLeafGrain grain) =>
        Assert.That(
            grain.CacheForTest.LastDetachSeam,
            Is.Not.EqualTo(LeafSnapshotDetachSeam.KeysAccessor)
                .And.Not.EqualTo(LeafSnapshotDetachSeam.EnumerateRowsAccessor)
                .And.Not.EqualTo(LeafSnapshotDetachSeam.UnderlyingRowsAccessor)
                .And.Not.EqualTo(LeafSnapshotDetachSeam.StateBytesBackfill),
            "a whole-cache accessor consumed the frame - the forfeiture this conversion "
            + "exists to prevent, and the one thing row-level assertions cannot see");

    // ---------------------------------------------------------------------
    // Part 1 - ComputeStructuralLeafCounts on the publication and adoption paths
    // ---------------------------------------------------------------------

    [Test]
    public async Task A_foreground_write_on_a_leaf_with_a_parent_leaves_the_frame_attached()
    {
        // The coverage hole itself. Every pre-existing digest fixture leaves
        // ParentId null, and PublishDigestUpwardAsync returns early on a null
        // parent, so this is the first arm on the branch to drive the digest
        // path with a parent present.
        var h = CreateLeaf(ParentId, SmallBudgetBytes);

        Assert.That(
            await h.Grain.SetAsync("zzz-new-key", Encoding.UTF8.GetBytes("v")),
            Is.Null,
            "precondition: the write must land without splitting");

        // Non-vacuity, and load-bearing: without a parent the publish path
        // returns before it ever reaches the structural counts, and every
        // frame assertion below would pass against unconverted code. The
        // received publish is the only signal here that is non-zero solely
        // because the digest path ran to completion.
        await h.Parent.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());

        Assert.Multiple(() =>
        {
            Assert.That(
                h.Grain.CacheForTest.HasPendingHydration,
                Is.True,
                "a single foreground write must not consume the leaf's snapshot frame");
            AssertNoWholeCacheSeam(h.Grain);
            Assert.That(
                h.Grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.True,
                "after one write the leaf must still place a division cut from frame keys alone");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
        });
    }

    [Test]
    public async Task Adoption_under_an_internal_node_leaves_the_frame_attached()
    {
        // BPlusInternalGrain seeds a child by calling SetParentAsync and then
        // immediately GetChildDigestSnapshotAsync, so this pair ran before the
        // leaf had taken any write at all.
        var h = CreateLeaf(parentId: null, SmallBudgetBytes);

        await h.Grain.SetParentAsync(ParentId);
        var snapshot = await h.Grain.GetChildDigestSnapshotAsync();

        Assert.Multiple(() =>
        {
            // Non-vacuity: a snapshot that described nothing would satisfy the
            // frame assertions trivially.
            Assert.That(
                snapshot.EntryCount, Is.EqualTo(RowCount),
                "the snapshot must describe the whole leaf");
            Assert.That(snapshot.LiveCount, Is.EqualTo(RowCount - ExpectedTombstones));
            Assert.That(snapshot.TombstoneCount, Is.EqualTo(ExpectedTombstones));

            Assert.That(
                h.Grain.CacheForTest.HasPendingHydration,
                Is.True,
                "adoption must not consume the frame - the leaf has not been written to yet");
            Assert.That(
                h.Grain.CacheForTest.HydratedRowCount,
                Is.Zero,
                "the structural split is read from O(1) counters, so adoption must materialise "
                + "no row whatsoever");
            AssertNoWholeCacheSeam(h.Grain);
        });
    }

    [Test]
    public async Task Adoption_of_a_leaf_smaller_than_the_resident_budget_materialises_nothing()
    {
        // The adversarial ratio. Here the leaf fits entirely inside the
        // resident budget, so a bounded re-walk would be free to materialise
        // every row and report the benign RangeHydrationCompleted seam while
        // every count below still matched. Only the residency assertion
        // separates a counter read from a walk, and the ratio is pinned by
        // construction on this arm, so the seam equality can be asserted
        // exactly rather than by exclusion.
        var h = CreateLeaf(parentId: null, LargeBudgetBytes);

        await h.Grain.SetParentAsync(ParentId);
        var snapshot = await h.Grain.GetChildDigestSnapshotAsync();
        var topology = await h.Grain.GetTopologyNodeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TombstoneCount, Is.EqualTo(ExpectedTombstones));
            Assert.That(topology.TombstoneCount, Is.EqualTo(ExpectedTombstones));
            Assert.That(topology.LiveCount, Is.EqualTo(RowCount - ExpectedTombstones));

            Assert.That(
                h.Grain.CacheForTest.HydratedRowCount,
                Is.Zero,
                "with the whole leaf inside the budget, a walk would materialise all of it and "
                + "nothing else here would notice; the counters must materialise none of it");
            Assert.That(
                h.Grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "no read on this arm touches a row, so nothing may release the frame");
            Assert.That(h.Grain.CacheForTest.HasPendingHydration, Is.True);
        });
    }

    [Test]
    public async Task The_published_structural_split_matches_a_full_recount()
    {
        // The counters are maintained incrementally where the enumeration they
        // replaced recomputed from the rows on every call. That is the one
        // thing the substitution gives up, so it is pinned against an explicit
        // recount - including after a write, which moves both counters.
        var h = CreateLeaf(ParentId, SmallBudgetBytes);

        Assert.That(await h.Grain.DeleteAsync(Key(0)), Is.True, "precondition: the delete must land");
        Assert.That(
            await h.Grain.SetAsync(Key(6), Encoding.UTF8.GetBytes("resurrected")),
            Is.Null,
            "precondition: a tombstoned key is rewritten live, so the split must move both ways");

        var snapshot = await h.Grain.GetChildDigestSnapshotAsync();

        // The recount runs last: it is the whole-cache accessor under
        // indictment, so it would destroy the frame state the arms above
        // measure if it ran before them.
        long recountTotal = 0;
        long recountTombstones = 0;
        foreach (var (_, lww) in h.Grain.CacheForTest.EnumerateRows())
        {
            recountTotal++;
            if (lww.IsTombstone) recountTombstones++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(recountTotal, Is.EqualTo(RowCount), "the corpus must be fully visible");
            Assert.That(
                recountTombstones, Is.EqualTo(ExpectedTombstones),
                "one tombstone was created and one resurrected, so the total is unchanged");
            Assert.That(
                snapshot.TombstoneCount, Is.EqualTo(recountTombstones),
                "the published tombstone count must equal the count the scan produced");
            Assert.That(
                snapshot.LiveCount, Is.EqualTo(recountTotal - recountTombstones),
                "the published live count must equal the count the scan produced");
            Assert.That(
                snapshot.LiveCount + snapshot.TombstoneCount, Is.EqualTo(snapshot.EntryCount),
                "the split must still sum to the published entry count");
        });
    }

    // ---------------------------------------------------------------------
    // Part 2 - GetProjectionDigestForRangeAsync (BPlusLeafGrain.Digest.cs)
    // ---------------------------------------------------------------------

    /// <summary>
    /// The same corpus held with nothing lazily hydrated, so a digest computed
    /// from it is the pre-change one-pass answer by construction.
    /// </summary>
    private static BPlusLeafGrain LeafWithDecodedRows()
    {
        var h = CreateLeafWithoutFrame(parentId: null);
        foreach (var row in Corpus())
        {
            h.Grain.CacheForTest.StoreRow(row.Key, row.Value);
            if (row.MergeMode is { } mode)
            {
                h.Grain.CacheForTest.SetMergeMode(row.Key, mode);
            }
        }

        Assert.That(
            h.Grain.CacheForTest.HasPendingHydration,
            Is.False,
            "the oracle leaf must hold every row outright, or it is not the one-pass answer");
        return h.Grain;
    }

    [TestCase(null, null, RowCount, TestName = "Unbounded")]
    [TestCase("k000100", "k000200", 100, TestName = "Bounded")]
    [TestCase("k000100", null, RowCount - 100, TestName = "OpenAbove")]
    [TestCase(null, "k000064", 64, TestName = "OpenBelow")]
    [TestCase("k000900", "k000999", 0, TestName = "DisjointFromTheCorpus")]
    public async Task A_range_digest_is_identical_to_the_one_pass_answer(
        string? startInclusive, string? endExclusive, int expectedCount)
    {
        var attached = CreateLeaf(parentId: null, SmallBudgetBytes).Grain;
        var decoded = LeafWithDecodedRows();

        var windowed = await attached.GetProjectionDigestForRangeAsync(startInclusive, endExclusive);
        var onePass = await decoded.GetProjectionDigestForRangeAsync(startInclusive, endExclusive);

        Assert.Multiple(() =>
        {
            Assert.That(
                windowed.EntryCount, Is.EqualTo(expectedCount),
                "the windowed fold must visit exactly the in-range rows");
            Assert.That(onePass.EntryCount, Is.EqualTo(expectedCount));
            Assert.That(
                windowed.Hash, Is.EqualTo(onePass.Hash),
                "the windows are disjoint and exhaustive and the fold is an XOR, so clipping "
                + "them to the requested range must reproduce the one-pass hash byte for byte");
            Assert.That(windowed.CheckpointOffset, Is.EqualTo(onePass.CheckpointOffset));
        });
    }

    [Test]
    public async Task An_unbounded_range_digest_leaves_the_frame_attached()
    {
        var h = CreateLeaf(parentId: null, SmallBudgetBytes);

        var digest = await h.Grain.GetProjectionDigestForRangeAsync(null, null);

        Assert.Multiple(() =>
        {
            Assert.That(
                digest.EntryCount, Is.EqualTo(RowCount),
                "non-vacuity: the fold must actually have visited every row");
            Assert.That(
                h.Grain.CacheForTest.HasPendingHydration,
                Is.True,
                "a whole-leaf digest read must stream rather than materialise the leaf whole");
            AssertNoWholeCacheSeam(h.Grain);
            Assert.That(
                h.Grain.CacheForTest.HydratedRowCount,
                Is.LessThan(RowCount),
                "windows already visited must be evictable, so the walk's peak residency must "
                + "stay under the corpus");
        });
    }

    [Test]
    public async Task A_bounded_range_digest_costs_the_range_rather_than_the_leaf()
    {
        // The adversarial ratio again, now for the fold: the leaf fits inside
        // the resident budget, so nothing evicts and residency is decided
        // purely by how much the read asks for. A whole-cache walk that
        // filtered per row - the shape this replaced - would materialise all
        // RowCount rows and still return the correct digest.
        var h = CreateLeaf(parentId: null, LargeBudgetBytes);

        var digest = await h.Grain.GetProjectionDigestForRangeAsync(Key(100), Key(200));

        Assert.Multiple(() =>
        {
            Assert.That(
                digest.EntryCount, Is.EqualTo(100),
                "non-vacuity: the fold must have visited the hundred in-range rows");
            Assert.That(
                h.Grain.CacheForTest.HydratedRowCount,
                Is.LessThan(RowCount),
                "a bounded digest read must hydrate only the blocks spanning its range - this is "
                + "the assertion a per-row filter over the whole-cache view cannot pass");
            Assert.That(
                h.Grain.CacheForTest.HasPendingHydration,
                Is.True,
                "the blocks outside the range are untouched, so the frame must survive");
            AssertNoWholeCacheSeam(h.Grain);
        });
    }

    [Test]
    public async Task A_range_digest_disjoint_from_the_corpus_materialises_nothing()
    {
        var h = CreateLeaf(parentId: null, LargeBudgetBytes);

        var digest = await h.Grain.GetProjectionDigestForRangeAsync(Key(900), Key(999));

        Assert.Multiple(() =>
        {
            Assert.That(digest.EntryCount, Is.Zero);
            Assert.That(
                h.Grain.CacheForTest.HydratedRowCount,
                Is.Zero,
                "a window wholly outside the requested range is skipped on two ordinal "
                + "comparisons and must hydrate nothing");
            Assert.That(
                h.Grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "nothing was read, so nothing may release the frame");
        });
    }
}
