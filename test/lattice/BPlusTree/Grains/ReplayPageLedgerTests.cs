using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the process-wide replay page ledger (issue #5877): eviction
/// across rings, pages reclaimed before any leaf is shed, the demand table's
/// lease and bound, and the snapshot a serve projects from surviving an
/// eviction.
/// </summary>
[TestFixture]
public sealed class ReplayPageLedgerTests
{
    private const string Tree = "ledger-tree";

    private static ReplayPage Page(long fromExclusive, int count, long incarnation = 0, int valueBytes = 100)
    {
        var entries = new CommitLogSliceEntry[count];
        for (var i = 0; i < count; i++)
        {
            var offset = fromExclusive + 1 + i;
            entries[i] = new CommitLogSliceEntry(offset, new LatticeMutation
            {
                TreeId = Tree,
                Kind = MutationKind.Set,
                Key = $"k{offset:D6}",
                Value = new byte[valueBytes],
            });
        }

        return new ReplayPage(fromExclusive, entries, incarnation);
    }

    private static ReplayPageRing Ring(LeafResidentWorkingSet workingSet)
    {
        var ring = new ReplayPageRing(workingSet.Pages);
        ring.Reset(0);
        return ring;
    }

    [Test]
    public void A_partition_filling_after_another_filled_the_budget_still_caches_its_pages()
    {
        var size = ReplayPageRing.EstimateBytes(Page(-1, 10));
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        workingSet.Pages.Configure(4 * size);
        var first = Ring(workingSet);
        var second = Ring(workingSet);

        for (var i = 0; i < 4; i++)
            Assert.That(first.TryInsert(Page(i * 10 - 1, 10), first.Epoch), Is.True);
        for (var i = 0; i < 4; i++)
        {
            Assert.That(second.TryInsert(Page(i * 10 - 1, 10), second.Epoch), Is.True,
                "a full budget must evict across partitions, not refuse the partition that came second");
        }

        Assert.Multiple(() =>
        {
            Assert.That(second.Pages, Has.Count.GreaterThan(0));
            Assert.That(workingSet.Pages.Bytes, Is.LessThanOrEqualTo(4 * size));
        });
    }

    [Test]
    public void Pages_are_reclaimed_before_any_leaf_is_shed()
    {
        var size = ReplayPageRing.EstimateBytes(Page(-1, 10));
        var workingSet = new LeafResidentWorkingSet(10 * size);
        var ring = Ring(workingSet);
        for (var i = 0; i < 8; i++)
            Assert.That(ring.TryInsert(Page(i * 10 - 1, 10), ring.Epoch), Is.True);

        var shed = 0;
        using var older = workingSet.Register(Tree, 2 * size, snapshotBanked: true, () => shed++);
        using var newer = workingSet.Register(Tree, 4 * size, snapshotBanked: true, () => shed++);

        Assert.Multiple(() =>
        {
            Assert.That(shed, Is.Zero, "a leaf must not be shed while cached pages remain to give way");
            Assert.That(workingSet.Pages.Bytes, Is.LessThanOrEqualTo(4 * size));
            Assert.That(workingSet.ResidentBytes, Is.EqualTo(6 * size));
        });

        using var third = workingSet.Register(Tree, 6 * size, snapshotBanked: true, () => shed++);
        Assert.Multiple(() =>
        {
            Assert.That(workingSet.Pages.Bytes, Is.Zero, "every page goes before the first leaf");
            Assert.That(shed, Is.GreaterThan(0), "with no pages left, an older leaf is shed");
        });
    }

    [Test]
    public void A_page_never_displaces_a_resident_leaf()
    {
        var size = ReplayPageRing.EstimateBytes(Page(-1, 10));
        var workingSet = new LeafResidentWorkingSet(4 * size);
        var shed = 0;
        using var leaf = workingSet.Register(Tree, 4 * size, snapshotBanked: true, () => shed++);
        var ring = Ring(workingSet);

        Assert.Multiple(() =>
        {
            Assert.That(ring.TryInsert(Page(-1, 10), ring.Epoch), Is.False);
            Assert.That(shed, Is.Zero);
        });
    }

    [Test]
    public void The_page_being_inserted_is_never_the_one_evicted()
    {
        var size = ReplayPageRing.EstimateBytes(Page(-1, 10));
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        workingSet.Pages.Configure(size);
        var ring = Ring(workingSet);
        Assert.That(ring.TryInsert(Page(-1, 10), ring.Epoch), Is.True);

        var newer = Page(9, 10);
        Assert.Multiple(() =>
        {
            Assert.That(ring.TryInsert(newer, ring.Epoch), Is.True);
            Assert.That(ring.Pages, Is.EqualTo(new[] { newer }));
        });
    }

    [Test]
    public void A_serve_projects_its_snapshot_even_when_the_pages_are_evicted_under_it()
    {
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        var ring = Ring(workingSet);
        Assert.That(ring.TryInsert(Page(-1, 10), ring.Epoch), Is.True);
        Assert.That(ring.TryInsert(Page(9, 10), ring.Epoch), Is.True);

        var run = ring.RunFrom(-1);
        lock (workingSet.Gate)
            workingSet.Pages.ReclaimLocked(long.MaxValue);

        var (slice, complete, _) = ReplaySliceProjection.Project(run, -1, 19, 100, default);
        Assert.Multiple(() =>
        {
            Assert.That(ring.Pages, Is.Empty, "instrument validation: the pages were evicted");
            Assert.That(complete, Is.True);
            Assert.That(slice.Select(e => e.Offset), Is.EqualTo(Enumerable.Range(0, 20).Select(i => (long)i)));
        });
    }

    [Test]
    public void A_declared_reader_that_never_reads_stops_protecting_pages_when_its_lease_lapses()
    {
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        var ring = Ring(workingSet);
        const long declaredAt = 1_000;
        const long lease = 500;
        ring.NoteDemand("leaf-a", 4, lease, now: declaredAt);

        Assert.That(ring.LiveDemand(declaredAt + lease - 1), Is.EqualTo(new[] { 4L }), "live inside its lease");
        Assert.That(ring.LiveDemand(declaredAt + lease), Is.Empty, "lapsed at the end of its lease");
        Assert.That(ring.DemandCount, Is.Zero, "a lapsed declaration is dropped");
    }

    [Test]
    public void The_demand_table_is_bounded_and_drops_the_oldest_lease_first()
    {
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        var ring = Ring(workingSet);
        ring.NoteDemand("oldest", -100, leaseTicks: long.MaxValue / 8);
        for (var i = 0; i < ReplayPageRing.MaxDemandEntries; i++)
            ring.NoteDemand($"leaf-{i}", i, leaseTicks: long.MaxValue / 4);

        Assert.Multiple(() =>
        {
            Assert.That(ring.DemandCount, Is.EqualTo(ReplayPageRing.MaxDemandEntries));
            Assert.That(ring.LiveDemand(System.Diagnostics.Stopwatch.GetTimestamp()), Does.Not.Contain(-100L), "the oldest lease is the one dropped");
        });
    }

    [Test]
    public void A_fill_that_completes_after_the_ring_is_cleared_charges_nothing()
    {
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        var ring = Ring(workingSet);
        var epochAtFillStart = ring.Epoch;

        ring.Clear();

        Assert.Multiple(() =>
        {
            Assert.That(ring.TryInsert(Page(-1, 10), epochAtFillStart), Is.False,
                "an orphan fill finishing after deactivation must not be cached");
            Assert.That(workingSet.Pages.Bytes, Is.Zero,
                "nothing is charged to the working set for a page no ring can evict");
        });
    }

    [Test]
    public void With_no_declared_readers_the_highest_pages_are_evicted_first_even_when_the_head_was_refilled_last()
    {
        var size = ReplayPageRing.EstimateBytes(Page(-1, 10));
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        workingSet.Pages.Configure(2 * size);
        var ring = Ring(workingSet);
        Assert.That(ring.TryInsert(Page(9, 10), ring.Epoch), Is.True);
        Assert.That(ring.TryInsert(Page(-1, 10), ring.Epoch), Is.True, "the head, cached last");

        Assert.That(ring.TryInsert(Page(19, 10), ring.Epoch), Is.True);

        Assert.That(ring.Pages.Select(p => p.FromExclusive), Does.Contain(-1L),
            "with no declared readers the head is what the next wave reads first, so it stays");
    }

    [Test]
    public void The_demand_lease_is_a_named_ten_minute_constant()
        => Assert.That(LeafReplayCoordinatorGrain.ReplayDemandLease, Is.EqualTo(TimeSpan.FromMinutes(10)));

    [Test]
    public async Task A_replay_slice_read_carries_the_reader_id_on_that_call_only()
    {
        var coordinator = NSubstitute.Substitute.For<ILeafReplayCoordinatorGrain>();
        string? seen = "unset";
        NSubstitute.SubstituteExtensions.Returns(
            coordinator.ReadSliceAsync(
                NSubstitute.Arg.Any<long>(), NSubstitute.Arg.Any<long>(), NSubstitute.Arg.Any<int>(), NSubstitute.Arg.Any<CancellationToken>()),
            _ =>
            {
                seen = LeafReplayReaderContext.Current;
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([]);
            });
        var reader = new ReplaySliceReader(coordinator, Tree, 0, reader: "leaf-x");

        await reader.ReadSliceAsync(-1, 10, onNarrowed: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo("leaf-x"), "the read call carries the reader");
            Assert.That(LeafReplayReaderContext.Current, Is.Null, "and nothing after it does");
        });
    }

    [Test]
    public void With_queued_readers_at_the_head_the_pages_needed_latest_are_evicted_first()
    {
        var size = ReplayPageRing.EstimateBytes(Page(-1, 10));
        var workingSet = new LeafResidentWorkingSet(1L << 30);
        workingSet.Pages.Configure(3 * size);
        var ring = Ring(workingSet);
        ring.NoteDemand("queued", -1, leaseTicks: long.MaxValue / 4);
        ring.NoteDemand("reading", 25, leaseTicks: long.MaxValue / 4);
        for (var i = 0; i < 4; i++)
            Assert.That(ring.TryInsert(Page(i * 10 - 1, 10), ring.Epoch), Is.True);

        Assert.That(ring.Pages.Select(p => p.FromExclusive), Does.Contain(-1L),
            "the head page, which the queued reader reads first, is kept");
    }
}
