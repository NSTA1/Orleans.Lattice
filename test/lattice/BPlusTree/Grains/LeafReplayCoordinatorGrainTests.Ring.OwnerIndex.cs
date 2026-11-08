using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the ring page owner index and the wide ring-served slice
/// (issue #5997). The indexed projection must equal the linear reference byte
/// for byte, and a wide slice must equal the concatenation of the narrow
/// slices it spans with their intermediate routing-only markers elided.
/// </summary>
public partial class LeafReplayCoordinatorGrainTests
{
    private static readonly ShardMap OwnerIndexMap16 = ShardMap.CreateDefault(16, 4);
    private static readonly ShardMap OwnerIndexMap64 = ShardMap.CreateDefault(64, 8);

    /// <summary>A log with every kind, null keys on key-scoped kinds, and holes.</summary>
    private static List<CommitLogSliceEntry> OwnerIndexLog(Random rng, int count)
    {
        var log = RandomLog(rng, count);
        for (var i = 0; i < log.Count; i++)
        {
            // A real entry never carries the default timestamp a routing-only
            // marker does, including the one at offset 0.
            log[i] = log[i] with { Mutation = log[i].Mutation with { Timestamp = new HybridLogicalClock { WallClockTicks = log[i].Offset + 1 } } };
            if (rng.Next(12) == 0 && WalKeyFilter.IsKeyScoped(log[i].Mutation.Kind))
                log[i] = log[i] with { Mutation = log[i].Mutation with { Key = null } };
        }

        return log;
    }

    private static WalKeyFilter OwnerIndexFilter(Random rng)
    {
        string? Bound() => rng.Next(3) == 0 ? null : "k" + rng.Next(40).ToString("D2");
        var low = Bound();
        var high = Bound();
        return rng.Next(5) switch
        {
            0 => default,
            1 => new WalKeyFilter(low, high),
            2 => new WalKeyFilter(low, high, OwnerIndexMap16, rng.Next(4)),
            3 => new WalKeyFilter(low, high, OwnerIndexMap64, rng.Next(8)),
            _ => new WalKeyFilter(null, null, OwnerIndexMap16, rng.Next(4)),
        };
    }

    /// <summary>Random runs: contiguous, overlapping, gapped and out of order.</summary>
    private static List<ReplayPage> OwnerIndexRun(Random rng, IReadOnlyList<CommitLogSliceEntry> log, long from)
    {
        var pages = Paginate(rng, log, from);
        if (pages.Count > 1 && rng.Next(4) == 0)
            pages.RemoveAt(rng.Next(1, pages.Count));
        if (pages.Count > 1 && rng.Next(4) == 0)
        {
            // An overlapping page: re-read from inside an earlier one.
            var at = rng.Next(pages.Count);
            var inside = pages[at].Entries;
            var cut = rng.Next(inside.Length);
            var startAt = cut == 0 ? pages[at].FromExclusive : inside[cut - 1].Offset;
            var tail = log.Where(e => e.Offset > startAt).Take(rng.Next(1, 20)).ToArray();
            if (tail.Length > 0)
                pages.Add(new ReplayPage(startAt, tail, 1));
        }

        if (rng.Next(3) == 0)
            pages = [.. pages.OrderBy(_ => rng.Next())];
        return pages;
    }

    private static string Signature((CommitLogSliceEntry[] Slice, bool Complete, long Covered) p) =>
        $"{p.Complete}|{p.Covered}|" + string.Join(";", p.Slice.Select(Comparable));

    [Test]
    public void Indexed_projection_equals_the_linear_reference_byte_for_byte_for_random_runs_filters_budgets_and_cuts()
    {
        var rng = new Random(5997);
        var markers = 0;
        for (var trial = 0; trial < 6000; trial++)
        {
            var log = OwnerIndexLog(rng, rng.Next(1, 160));
            var high = log[^1].Offset;
            var from = rng.Next(4) == 0 ? -1 : rng.NextInt64(-1, high + 1);
            var to = rng.NextInt64(from, high + 6);
            var budget = rng.Next(3) == 0 ? rng.Next(1, 4) : rng.Next(1, 200);
            var filter = OwnerIndexFilter(rng);
            var run = OwnerIndexRun(rng, log, from);

            var indexed = ReplaySliceProjection.Project(run, from, to, budget, filter);
            var linear = ReplaySliceProjection.ProjectLinear(run, from, to, budget, filter);
            if (linear.Slice.Length > 0 && IsRoutingOnly(linear.Slice[^1]))
                markers++;

            Assert.That(Signature(indexed), Is.EqualTo(Signature(linear)),
                $"trial {trial}: from={from} to={to} budget={budget} filter={filter} pages={run.Count}");
        }

        Assert.That(markers, Is.GreaterThan(100), "instrument validation: the trials exercise the trailing marker");
    }

    [Test]
    public void Indexed_projection_matches_the_linear_reference_for_two_virtual_shard_counts_on_one_page()
    {
        var rng = new Random(16_64);
        var log = OwnerIndexLog(rng, 150);
        var page = new ReplayPage(-1, [.. log], 1);
        for (var round = 0; round < 40; round++)
        {
            // Alternate the slot count on the same page, so its slot cache is
            // rebuilt for one count and then asked for the other.
            var map = round % 2 == 0 ? OwnerIndexMap16 : OwnerIndexMap64;
            var filter = new WalKeyFilter(null, null, map, round % 4);
            var indexed = ReplaySliceProjection.Project([page], -1, log[^1].Offset, 1000, filter);
            var linear = ReplaySliceProjection.ProjectLinear([page], -1, log[^1].Offset, 1000, filter);
            Assert.That(Signature(indexed), Is.EqualTo(Signature(linear)), $"round {round}, V={map.VirtualShardCount}");
        }
    }

    [Test]
    public void A_wide_ring_slice_equals_its_narrow_slices_concatenated_with_intermediate_markers_elided()
    {
        var rng = new Random(59_97);
        for (var trial = 0; trial < 3000; trial++)
        {
            var log = OwnerIndexLog(rng, rng.Next(1, 400));
            var high = log[^1].Offset;
            var from = rng.Next(4) == 0 ? -1 : rng.NextInt64(-1, high + 1);
            var to = rng.NextInt64(from, high + 6);
            var budget = rng.Next(1, 8);
            var filter = OwnerIndexFilter(rng);
            var run = Paginate(rng, log, from);

            var wide = ReplaySliceProjection.ProjectWide(run, from, to, budget, filter, incarnation: 1);
            var expected = NarrowConcatenation(run, from, wide.CoveredThroughInclusive, budget, filter);

            Assert.That(wide.Slice.Select(Comparable), Is.EqualTo(expected.Select(Comparable)),
                $"trial {trial}: from={from} to={to} budget={budget} filter={filter} covered={wide.CoveredThroughInclusive} complete={wide.Complete}"
                + $" wide=[{string.Join(",", wide.Slice.Select(e => e.Offset + (IsRoutingOnly(e) ? "m" : "")))}]"
                + $" narrow=[{string.Join(",", expected.Select(e => e.Offset + (IsRoutingOnly(e) ? "m" : "")))}]");
            Assert.That(wide.Slice.Count(e => !IsRoutingOnly(e)), Is.LessThanOrEqualTo(budget), $"trial {trial}: the return bound");
            Assert.That(wide.Examined, Is.LessThanOrEqualTo((long)budget * ReplaySliceProjection.RingExamineFactor), $"trial {trial}: the examine cap");
            Assert.That(wide.Slice.Take(Math.Max(0, wide.Slice.Length - 1)).Any(IsRoutingOnly), Is.False, $"trial {trial}: no intermediate marker");
            if (wide.Slice.Length > 0)
            {
                Assert.That(wide.Slice[^1].Offset, Is.LessThanOrEqualTo(wide.CoveredThroughInclusive),
                    $"trial {trial}: the trailing entry never passes the covered run or its last examined offset");
            }
        }
    }

    [Test]
    public void Replaying_through_wide_ring_slices_leaves_a_leaf_in_the_state_narrow_slices_leave_it()
    {
        var rng = new Random(5_997);
        for (var trial = 0; trial < 500; trial++)
        {
            var log = OwnerIndexLog(rng, rng.Next(1, 600));
            var head = log[^1].Offset;
            var budget = rng.Next(1, 6);
            var filter = OwnerIndexFilter(rng);
            var run = Paginate(rng, log, -1);

            var (narrowState, narrowNext) = Replay(run, head, budget, filter, wide: false);
            var (wideState, wideNext) = Replay(run, head, budget, filter, wide: true);

            Assert.That(wideState, Is.EqualTo(narrowState), $"trial {trial}: the applied state");
            Assert.That(wideNext, Is.EqualTo(narrowNext), $"trial {trial}: the reader's next offset");
        }
    }

    [Test]
    public void A_wide_ring_slice_never_widens_across_a_page_read_at_another_incarnation()
    {
        var rng = new Random(3);
        var first = new ReplayPage(-1, [.. Enumerable.Range(0, 4).Select(i => Entry(i, MutationKind.Set, "k01", rng))], 1);
        var later = new ReplayPage(3, [.. Enumerable.Range(4, 4).Select(i => Entry(i, MutationKind.Set, "k01", rng))], 2);

        var wide = ReplaySliceProjection.ProjectWide([first, later], -1, 7, 100, default, incarnation: 1);

        Assert.That(wide.Complete, Is.False, "the run's coverage at the serve's incarnation ends at offset 3");
        Assert.That(wide.CoveredThroughInclusive, Is.EqualTo(3));
        Assert.That(wide.Slice.Select(e => e.Offset), Is.EqualTo(new long[] { 0, 1, 2, 3 }));
    }

    [Test]
    public void A_page_that_closes_a_gap_is_used_whatever_its_position_in_the_run_at_the_wide_budget()
    {
        var rng = new Random(2);
        var first = new ReplayPage(-1, [Entry(0, MutationKind.Set, "k01", rng), Entry(1, MutationKind.Set, "k01", rng)], 1);
        var beyondGap = new ReplayPage(5, [Entry(6, MutationKind.Set, "k01", rng)], 1);
        var closesGap = new ReplayPage(1, [Entry(3, MutationKind.Set, "k01", rng), Entry(5, MutationKind.Set, "k01", rng)], 1);

        var wide = ReplaySliceProjection.ProjectWide([first, beyondGap, closesGap], -1, 6, 1, default, incarnation: 1);

        Assert.That(wide.Complete, Is.True, "a budget of one returns one entry: the return bound completes the slice");
        var all = new List<long>();
        var from = -1L;
        while (from < 6)
        {
            var next = ReplaySliceProjection.ProjectWide([first, beyondGap, closesGap], from, 6, 1, default, incarnation: 1);
            Assert.That(next.Slice, Is.Not.Empty);
            all.AddRange(next.Slice.Select(e => e.Offset));
            from = next.CoveredThroughInclusive;
        }

        Assert.That(all, Is.EqualTo(new long[] { 0, 1, 3, 5, 6 }));
    }

    [Test]
    public void Indexed_projection_allocates_only_for_the_entries_it_returns()
    {
        var rng = new Random(77);
        var log = Enumerable.Range(0, 64 * 256).Select(i => Entry(i, MutationKind.Set, "k" + rng.Next(40).ToString("D2"), rng)).ToList();
        var run = Paginate(rng, log, -1);
        var filter = new WalKeyFilter("k17", "k18", OwnerIndexMap64, 3);
        var head = log[^1].Offset;

        // Warm up: the slot table and any lazily-built state are built once per page.
        ReplaySliceProjection.Project(run, -1, head, int.MaxValue, filter);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var (slice, _, _) = ReplaySliceProjection.Project(run, -1, head, int.MaxValue, filter);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        var perReturned = System.Runtime.CompilerServices.Unsafe.SizeOf<CommitLogSliceEntry>() + 64L;
        Assert.That(allocated, Is.LessThanOrEqualTo(4096 + (slice.Length * perReturned)),
            $"examined {log.Count} entries across {run.Count} pages, returned {slice.Length}");
    }

    [Test]
    public void Indexed_projection_never_visits_an_entry_outside_the_filters_key_range_or_never_excludable_set()
    {
        var rng = new Random(78);
        var log = Enumerable.Range(0, 20_000)
            .Select(i => Entry(i, i % 97 == 0 ? MutationKind.TxCommit : MutationKind.Set, "k" + rng.Next(40).ToString("D2"), rng))
            .ToList();
        var run = Paginate(rng, log, -1);
        var filter = new WalKeyFilter("k17", "k18", OwnerIndexMap64, 3);

        ReplayPageIndex.VisitedForTest = 0;
        ReplaySliceProjection.Project(run, -1, log[^1].Offset, int.MaxValue, filter);
        var visited = ReplayPageIndex.VisitedForTest;

        var candidates = log.Count(e => !WalKeyFilter.IsKeyScoped(e.Mutation.Kind)
            || string.CompareOrdinal(e.Mutation.Key, "k17") >= 0 && string.CompareOrdinal(e.Mutation.Key, "k18") < 0);
        Assert.That(visited, Is.EqualTo(candidates), "the index visits the key range and the never-excludable set, nothing else");
        Assert.That(visited, Is.LessThan(log.Count / 10), "instrument validation: the filter excludes most of the log");
    }

    /// <summary>
    /// The narrow slices a reader reads from <paramref name="from"/> through
    /// <paramref name="through"/>, concatenated, with every routing-only marker
    /// elided except one for the last entry examined, when it was excluded.
    /// </summary>
    private static List<CommitLogSliceEntry> NarrowConcatenation(
        IReadOnlyList<ReplayPage> run, long from, long through, int budget, WalKeyFilter filter)
    {
        var result = new List<CommitLogSliceEntry>();
        var at = from;
        CommitLogSliceEntry? lastMarker = null;
        while (at < through)
        {
            var (slice, _, covered) = ReplaySliceProjection.ProjectLinear(run, at, through, budget, filter);
            // A slice that examined nothing leaves the last examined entry, and so
            // its marker, where the previous slice left it.
            if (slice.Length > 0)
                lastMarker = null;
            foreach (var e in slice)
            {
                if (IsRoutingOnly(e))
                    lastMarker = e;
                else
                    result.Add(e);
            }

            if (covered <= at)
                break;
            at = covered;
        }

        if (lastMarker is { } marker && (result.Count == 0 || result[^1].Offset < marker.Offset))
            result.Add(marker);
        return result;
    }

    /// <summary>A leaf's replay through ring slices: last-writer state per key, and where it stops.</summary>
    private static (Dictionary<string, string> State, long Next) Replay(
        IReadOnlyList<ReplayPage> run, long head, int budget, WalKeyFilter filter, bool wide)
    {
        var state = new Dictionary<string, string>();
        var from = -1L;
        while (from < head)
        {
            var slice = wide
                ? ReplaySliceProjection.ProjectWide(run, from, head, budget, filter, incarnation: 1).Slice
                : ReplaySliceProjection.ProjectLinear(run, from, head, budget, filter).Slice;
            if (slice.Length == 0)
                break;

            foreach (var e in slice)
            {
                if (IsRoutingOnly(e))
                    continue;
                var key = e.Mutation.Key ?? "(none)";
                state[key] = $"{e.Offset}:{e.Mutation.Kind}:{(e.Mutation.Value is null ? "" : Convert.ToHexString(e.Mutation.Value))}";
            }

            from = slice[^1].Offset;
        }

        return (state, from);
    }
}
