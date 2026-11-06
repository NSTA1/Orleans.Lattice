using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the replay page ring's projection (issue #5896, conditions A-a
/// and A-b): a slice projected from cached, unfiltered pages must equal what a
/// direct filtered read of the same window returns, and coverage must never be
/// assumed past the last entry a fill actually returned.
/// </summary>
[TestFixture]
public partial class LeafReplayCoordinatorGrainTests
{
    private static readonly MutationKind[] Kinds =
        [MutationKind.Set, MutationKind.Delete, MutationKind.DeleteRange, MutationKind.TxCommit, MutationKind.TxAbort, MutationKind.Tombstone];

    private static CommitLogSliceEntry Entry(long offset, MutationKind kind, string key, Random rng) =>
        new(offset, new LatticeMutation
        {
            TreeId = "t",
            Kind = kind,
            Key = key,
            EndExclusiveKey = kind == MutationKind.DeleteRange ? key + "~" : null,
            Value = kind == MutationKind.Set ? [(byte)rng.Next(256)] : null,
            Timestamp = new HybridLogicalClock { WallClockTicks = offset, Counter = 0 },
        });

    private static List<CommitLogSliceEntry> RandomLog(Random rng, int count)
    {
        var entries = new List<CommitLogSliceEntry>(count);
        var offset = -1L;
        for (var i = 0; i < count; i++)
        {
            // Random holes: offsets that were never readable.
            offset += 1 + (rng.Next(4) == 0 ? rng.Next(1, 5) : 0);
            entries.Add(Entry(offset, Kinds[rng.Next(Kinds.Length)], "k" + rng.Next(40).ToString("D2"), rng));
        }

        return entries;
    }

    private static List<ReplayPage> Paginate(Random rng, IReadOnlyList<CommitLogSliceEntry> log, long fromExclusive)
    {
        var pages = new List<ReplayPage>();
        var start = fromExclusive;
        var remaining = log.Where(e => e.Offset > fromExclusive).ToList();
        while (remaining.Count > 0)
        {
            var take = Math.Min(remaining.Count, rng.Next(1, 12));
            pages.Add(new ReplayPage(start, remaining.Take(take).ToArray(), incarnation: 1));
            start = remaining[take - 1].Offset;
            remaining.RemoveRange(0, take);
        }

        return pages;
    }

    private static async Task<CommitLogSliceEntry[]> DirectFilteredReadAsync(
        IReadOnlyList<CommitLogSliceEntry> log, long fromExclusive, long toInclusive, int budget, WalKeyFilter filter)
    {
        var result = new List<CommitLogSliceEntry>();
        await foreach (var (offset, mutation) in WalFilteredRead.ApplyAsync(
            Source(log.Where(e => e.Offset > fromExclusive)), toInclusive, budget, filter))
        {
            result.Add(new CommitLogSliceEntry(offset, mutation));
        }

        return result.ToArray();

        static async IAsyncEnumerable<(long, LatticeMutation)> Source(IEnumerable<CommitLogSliceEntry> entries)
        {
            foreach (var e in entries)
                yield return (e.Offset, e.Mutation);
            await Task.CompletedTask;
        }
    }

    private static WalKeyFilter RandomFilter(Random rng) => rng.Next(4) switch
    {
        0 => default,
        1 => new WalKeyFilter("k10", "k25"),
        2 => new WalKeyFilter(null, "k05"),
        _ => new WalKeyFilter("k" + rng.Next(40).ToString("D2"), null),
    };

    [Test]
    public async Task Ring_projection_equals_provider_filtered_read_for_random_windows_filters_budgets_holes_and_tails()
    {
        var rng = new Random(5896);
        for (var trial = 0; trial < 2000; trial++)
        {
            var log = RandomLog(rng, rng.Next(1, 120));
            var high = log[^1].Offset;
            var from = rng.Next(4) == 0 ? -1 : rng.NextInt64(-1, high + 1);
            var to = rng.NextInt64(from, high + 6);
            var budget = rng.Next(4) == 0 ? 1 : rng.Next(1, 300);
            var filter = RandomFilter(rng);

            // The ring holds every entry readable now, cut into random pages.
            var pages = Paginate(rng, log, from);
            var (slice, complete, _) = ReplaySliceProjection.Project(pages, from, to, budget, filter);
            var direct = await DirectFilteredReadAsync(log, from, to, budget, filter);

            Assert.That(complete || pages.Count == 0 || pages[^1].CoveredThroughInclusive < to, Is.True,
                $"trial {trial}: a run that reaches the window's end projects a complete slice");
            if (complete)
            {
                Assert.That(slice, Is.EqualTo(direct),
                    $"trial {trial}: from={from} to={to} budget={budget} filter={filter}");
            }
            else
            {
                // An incomplete projection is the run's covered prefix; the rest is a fill.
                Assert.That(pages.Count == 0 || pages[^1].CoveredThroughInclusive < to, Is.True, $"trial {trial}");
                Assert.That(slice.Where(e => !IsRoutingOnly(e)).Select(e => e.Offset),
                    Is.SubsetOf(direct.Select(e => e.Offset)), $"trial {trial}");
            }
        }
    }

    private static bool IsRoutingOnly(CommitLogSliceEntry e) => e.Mutation.Value is null && e.Mutation.Timestamp == default;

    [Test]
    public void A_page_filled_at_tail_t_never_serves_entries_appended_after_t_as_absent()
    {
        var rng = new Random(1);
        // Filled while the tail was 5: the page covers through 5, not through the planned 20.
        var page = new ReplayPage(-1, [.. Enumerable.Range(0, 6).Select(i => Entry(i, MutationKind.Set, "k01", rng))], 1);
        Assert.That(page.CoveredThroughInclusive, Is.EqualTo(5));

        // The tail rises to 9; a request for (2, 9] must not be complete from the ring.
        var (slice, complete, covered) = ReplaySliceProjection.Project([page], 2, 9, 100, default);
        Assert.That(complete, Is.False, "offsets 6..9 were never read and must come from storage");
        Assert.That(covered, Is.EqualTo(5));
        Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new long[] { 3, 4, 5 }));

        // Two pages with an uncovered gap between them are never stitched.
        var later = new ReplayPage(7, [Entry(8, MutationKind.Set, "k01", rng), Entry(9, MutationKind.Set, "k01", rng)], 1);
        (slice, complete, covered) = ReplaySliceProjection.Project([page, later], 2, 9, 100, default);
        Assert.That(complete, Is.False, "the gap (5, 7] was never read");
        Assert.That(covered, Is.EqualTo(5));
        Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new long[] { 3, 4, 5 }));
    }

    [Test]
    public void A_page_that_closes_a_gap_is_used_whatever_its_position_in_the_run()
    {
        var rng = new Random(2);
        var first = new ReplayPage(-1, [Entry(0, MutationKind.Set, "k01", rng), Entry(1, MutationKind.Set, "k01", rng)], 1);
        var beyondGap = new ReplayPage(5, [Entry(6, MutationKind.Set, "k01", rng)], 1);
        var closesGap = new ReplayPage(1, [Entry(3, MutationKind.Set, "k01", rng), Entry(5, MutationKind.Set, "k01", rng)], 1);

        // The fill that closes the gap arrives after the page beyond it.
        var (slice, complete, covered) = ReplaySliceProjection.Project([first, beyondGap, closesGap], -1, 6, 100, default);

        Assert.That(complete, Is.True);
        Assert.That(covered, Is.EqualTo(6));
        Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new long[] { 0, 1, 3, 5, 6 }));
    }

    [Test]
    public void A_fill_that_returned_nothing_creates_no_page()
    {
        Assert.That(() => new ReplayPage(3, [], 1), Throws.ArgumentException);
    }
}
