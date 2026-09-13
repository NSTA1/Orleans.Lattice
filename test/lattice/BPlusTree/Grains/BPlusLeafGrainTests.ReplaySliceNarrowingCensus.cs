using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <c>orleans.lattice.wal.replay.slice_narrowings</c>, the
/// instrument issue #2867 adds so that the second of the two factors setting
/// peak WAL replay memory is observable at all.
/// <para>
/// <b>Why the instrument exists.</b> Peak replay memory is the <i>product</i>
/// of two terms: how many replays run concurrently, and how much each one
/// buffers. The first term is an option
/// (<c>WalMaterialiserMaxConcurrentReplays</c>), is surfaced on the container
/// tuning overlay, and is already measured from both sides by
/// <c>permit_adaptations</c> and <c>permit_queue_wait</c>. The second term is
/// the per-replay slice width, and it is a <c>private const</c> - no option, no
/// environment variable, no metric. The deploy surface therefore exposes
/// exactly one of the two factors, which is the defect #2867 reports.
/// </para>
/// <para>
/// <b>What the counter buys that a log did not.</b> The #2742 narrowing is the
/// only thing in the process that ever moves the second term, and until now it
/// announced itself through a warning log alone. A log is not a series: it
/// cannot be differenced, it cannot be compared across trees, and it is
/// throttled by the sink rather than by the event. So after an out-of-memory
/// activation failure an operator could not distinguish the two stories whose
/// remedies point in opposite directions - the narrowing engaged and was not
/// enough, or it never engaged because the allocation that failed was not the
/// slice read at all.
/// </para>
/// <para>
/// Every arm here is deterministic, reusing the scripted
/// <c>PressuredReplayCoordinator</c> from the #2742 fixture rather than
/// exhausting a real heap. Both arms are
/// <see cref="NonParallelizableAttribute"/> because a
/// <see cref="MeterListener"/> is process-wide and a sibling test replaying the
/// same tree id concurrently would land its measurements in this one's
/// collection.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>One recorded measurement, with its tags copied off the span.</summary>
    private readonly record struct NarrowingMeasurement(
        long Value,
        IReadOnlyList<KeyValuePair<string, object?>> Tags);

    /// <summary>
    /// Runs <paramref name="activate"/> with a listener attached to the
    /// slice-narrowing counter and returns everything it recorded.
    /// <para>
    /// The instrument is passed to <see cref="MeterListening.StartForInstrument"/>
    /// as a parameter rather than matched inside the callback, so reading it at
    /// this call site forces <c>LatticeMetrics</c>' type initialiser to complete
    /// before the listener exists - the re-entrant-publication hazard the
    /// repository's meter-ordering convention is about.
    /// </para>
    /// </summary>
    private static async Task<List<NarrowingMeasurement>> RecordNarrowingsAsync(Func<Task> activate)
    {
        var recorded = new List<NarrowingMeasurement>();

        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalReplaySliceNarrowings,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var copied = new KeyValuePair<string, object?>[tags.Length];
                for (var i = 0; i < tags.Length; i++)
                {
                    copied[i] = tags[i];
                }

                lock (recorded)
                {
                    recorded.Add(new NarrowingMeasurement(value, copied));
                }
            }));

        await activate();

        lock (recorded)
        {
            return recorded.ToList();
        }
    }

    private static IEnumerable<NarrowingMeasurement> ForResumableTree(
        IEnumerable<NarrowingMeasurement> measurements) =>
        measurements.Where(m => m.Tags.Any(
            t => string.Equals(t.Key, LatticeMetrics.TagTree, StringComparison.Ordinal)
                && string.Equals(t.Value as string, ResumableTreeId, StringComparison.Ordinal)));

    [Test]
    [NonParallelizable]
    public async Task Every_slice_narrowing_is_counted_exactly_once()
    {
        // The widest read is unaffordable and a narrower one is not, which is
        // the field condition. 256 is refused, 64 is refused, 16 is served -
        // so the loop narrows precisely as often as the coordinator refuses.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 16, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();
        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);

        var measurements = await RecordNarrowingsAsync(
            () => ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        var mine = ForResumableTree(measurements).ToList();

        Assert.That(coord.Refusals, Is.GreaterThan(0),
            "precondition: the coordinator must actually have refused a read, or this arm proves "
            + "nothing about counting narrowings");

        // The load-bearing assertion, and it is an EQUALITY deliberately. A
        // `GreaterThan(0)` here would survive an increment moved out to once
        // per activation, or one placed on the terminal give-up branch that
        // does not narrow - both of which would make the series a different
        // quantity than its name and documentation claim.
        Assert.That(
            mine.Sum(m => m.Value),
            Is.EqualTo((long)coord.Refusals),
            "the counter must be an exact census of narrowings. Every refused read below the "
            + "single-entry floor narrows exactly once, so the total recorded must equal the number "
            + "of refusals the coordinator scripted - no more, and no fewer.");
    }

    [Test]
    [NonParallelizable]
    public async Task A_narrowing_is_attributed_to_the_tree_and_partition_that_narrowed()
    {
        // The tree tag is the whole operational point. #2691 established that
        // one repo-context tree generated 139x the WAL of its siblings under
        // identical cycling; a process-wide narrowing count could never have
        // shown which tree was buffering hardest, because every tree in the
        // silo would have summed into one series.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 16, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();
        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);

        var measurements = await RecordNarrowingsAsync(
            () => ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        var narrowings = ForResumableTree(measurements).Where(m => m.Value > 0).ToList();

        Assert.That(narrowings, Is.Not.Empty,
            "precondition: at least one narrowing must have been recorded against this tree");

        Assert.Multiple(() =>
        {
            foreach (var m in narrowings)
            {
                Assert.That(
                    m.Tags.Any(t => string.Equals(t.Key, LatticeMetrics.TagPartition, StringComparison.Ordinal)),
                    Is.True,
                    "a narrowing must carry the partition it happened on. Width is a per-replay "
                    + "property and a replay is per partition, so a tree-only attribution would "
                    + "average away the one partition whose window is unaffordable");

                Assert.That(
                    m.Tags.Any(t => string.Equals(t.Key, LatticeTenantLabel.TagTenant, StringComparison.Ordinal)),
                    Is.True,
                    "every tree-tagged instrument in this repository carries the derived tenant "
                    + "label alongside it, so a tenant-scoped view is not silently blind to this "
                    + "series");
            }
        });
    }

    [Test]
    [NonParallelizable]
    public async Task A_healthy_replay_publishes_the_series_at_zero_rather_than_publishing_nothing()
    {
        // The priming arm, and the reason it is not ceremony: the HEALTHY
        // steady state for a partition is never to narrow. A Counter exports no
        // series at all until its first Add, so without the prime the common
        // case is byte-identical on the scrape to a build where the instrument
        // was never wired - and telling those two apart after an out-of-memory
        // failure is the entire purpose of the instrument. Priming a counter is
        // safe in a way priming a histogram is not: adding zero mints the
        // series with the exact tag set a later increment carries and cannot
        // perturb the value, whereas a primed histogram would record a
        // fabricated sample that reads as a real observation.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(
            head: 12, sliceSize: 4, affordableBudget: int.MaxValue, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();
        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);

        var measurements = await RecordNarrowingsAsync(
            () => ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        var mine = ForResumableTree(measurements).ToList();

        Assert.That(coord.Refusals, Is.Zero,
            "precondition: nothing may have been refused, or this is not the healthy arm");

        Assert.That(mine, Is.Not.Empty,
            "an unpressured replay must still mint the series. With no prime the counter is silent "
            + "on every healthy partition, so a future zero on the scrape would be an ABSENCE rather "
            + "than a measurement - indistinguishable from a build that cannot narrow at all.");

        Assert.That(mine.Sum(m => m.Value), Is.Zero,
            "the prime must not perturb the value it mints, or a healthy partition would report "
            + "narrowings it never performed");

        Assert.That(
            mine.Any(m => m.Tags.Any(
                t => string.Equals(t.Key, LatticeMetrics.TagPartition, StringComparison.Ordinal))),
            Is.True,
            "the prime must mint the series at the FINEST dimension the counter is read at. A prime "
            + "carrying only the tree tag would leave every per-partition series still absent, which "
            + "is the dimension an operator actually reads.");
    }
}
