using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the SURVIVAL of a leaf non-convergence stall run across
/// an overflow of the detector's observation map (issue #2285).
/// <para>
/// Issue #2285 was filed because the stall line asserted permanence on its first
/// repeat, so a 70-second burst and a permanent freeze were textually identical.
/// The remedy shipped two things: a counter the line advertises as "the exact
/// census of the condition", and a repeat count plus a span the line instructs an
/// operator to judge by - "a short run of repeats over a few seconds is commonly
/// transient ... a leaf that cannot converge keeps reporting with a rising count
/// over a widening span".
/// </para>
/// <para>
/// Both of those live in one process-wide map keyed by
/// <c>(tree, leaf, partition)</c>, which had no eviction other than a WHOLESALE
/// <c>Clear()</c> at its capacity. That clear discarded the stalling entries
/// along with the converging ones, and it is the only state the two shipped
/// signals have:
/// </para>
/// <list type="bullet">
/// <item>the stuck leaf's <c>Repeats</c> restarts at 1 and its span at zero, so
/// the freeze re-presents as a fresh burst - the exact reading #2285 exists to
/// prevent, reintroduced by a capacity event that has nothing to do with
/// convergence;</item>
/// <item>the observation immediately after the clear compares against nothing,
/// so it returns <c>IsStall=false</c> and the counter is NOT incremented, which
/// falsifies the census sentence the message tells the operator to rely on.</item>
/// </list>
/// <para>
/// The clear is reachable rather than theoretical. Entries are only ever added,
/// never individually removed, so the map accumulates one key per
/// <c>(tree, leaf, partition)</c> a silo has ever replayed; the pass-1 sweep
/// visits EVERY partition, and a cold leaf starts every partition at the "-1"
/// sentinel, so each cold activation contributes one key per non-empty
/// partition. On the deployment #2285 was measured on that is at least the
/// ~1,350 sibling leaves recorded against a single WAL partition of
/// <c>repo-context-vector-metadata</c> multiplied by the 8 partitions the issue
/// observed - about 10,800 keys for one tree, against a capacity of 8,192,
/// before any other tree is counted.
/// </para>
/// <para>
/// The remedy is to evict the entries that carry no stall run in preference to
/// the ones that do, since a converging entry is reconstructed by its next
/// observation and a stall run is not reconstructible at all. These tests are
/// the discriminators: both fail before that change, and they fail in the two
/// distinct ways above rather than by one shared assertion.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string StallEvictionTreeId = "tree-stall-eviction";
    private const string StallEvictionLeafId = "leaf-stall-eviction";

    /// <summary>
    /// Drives the observation map past its capacity with converging leaf
    /// partitions that are nothing to do with the leaf under test: every filler
    /// reports a checkpoint it has never reported before, so each is a fresh
    /// insert carrying no stall run. Filling with CONVERGING leaves is the point
    /// - it is the ordinary healthy traffic of a large tree, not a pathological
    /// case, and it must not be able to erase the evidence about a leaf that is
    /// not converging.
    /// </summary>
    private static void OverflowReplayObservations()
    {
        for (var i = 0; i <= BPlusLeafGrain.ReplayCheckpointObservationCapacity; i++)
        {
            BPlusLeafGrain.NoteReplayCheckpointObservation(
                "tree-stall-eviction-filler", $"leaf-filler-{i}", partition: 0, checkpoint: i);
        }
    }

    /// <summary>
    /// DISCRIMINATOR for the "asserts permanence" half of issue #2285.
    /// <para>
    /// A leaf builds a stall run of two repeats over a measurable span - the
    /// evidence the shipped message tells an operator to judge by - and unrelated
    /// healthy traffic then overflows the map. The leaf is still stuck at the
    /// identical checkpoint, so its next observation must continue the run at
    /// repeat 3 over a span that has not gone backwards.
    /// </para>
    /// <para>
    /// Before the fix this fails on <c>IsStall</c>: the wholesale clear removed
    /// the only record that this leaf had ever been seen, so the observation is
    /// treated as a first sighting, reports <c>Repeats=0</c>, and a permanent
    /// freeze is handed back to the operator wearing the signature of a burst.
    /// </para>
    /// </summary>
    [Test]
    public void Replay_stall_run_survives_an_overflow_of_the_observation_map()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();

        const long frozenCheckpoint = 100;
        var seed = BPlusLeafGrain.NoteReplayCheckpointObservation(
            StallEvictionTreeId, StallEvictionLeafId, partition: 0, checkpoint: frozenCheckpoint);
        var firstRepeat = BPlusLeafGrain.NoteReplayCheckpointObservation(
            StallEvictionTreeId, StallEvictionLeafId, partition: 0, checkpoint: frozenCheckpoint);
        var secondRepeat = BPlusLeafGrain.NoteReplayCheckpointObservation(
            StallEvictionTreeId, StallEvictionLeafId, partition: 0, checkpoint: frozenCheckpoint);

        OverflowReplayObservations();

        var afterOverflow = BPlusLeafGrain.NoteReplayCheckpointObservation(
            StallEvictionTreeId, StallEvictionLeafId, partition: 0, checkpoint: frozenCheckpoint);

        Assert.Multiple(() =>
        {
            Assert.That(seed.IsStall, Is.False,
                "A first observation has nothing to compare against and is not yet evidence of anything.");
            Assert.That(firstRepeat.Repeats, Is.EqualTo(1));
            Assert.That(secondRepeat.Repeats, Is.EqualTo(2));

            Assert.That(afterOverflow.IsStall, Is.True,
                "The leaf is stuck at the identical checkpoint, so whether its non-convergence is detected at "
                + "all must not depend on how many OTHER leaf partitions this silo happens to have replayed.");
            Assert.That(afterOverflow.Repeats, Is.EqualTo(3),
                "The shipped line tells the operator to judge a freeze by a rising repeat count. A count reset "
                + "to 1 by an unrelated capacity event re-presents the freeze as the burst that #2285 was "
                + "filed over, and it is silent about having done so.");
            Assert.That(afterOverflow.Span, Is.GreaterThanOrEqualTo(secondRepeat.Span),
                "The span is the other half of that instruction: it must keep widening across the run, so a "
                + "freeze cannot be made to look freshly started.");
        });
    }

    /// <summary>
    /// DISCRIMINATOR for the CENSUS half of issue #2285, driven through the grain
    /// so it holds the counter as an operator actually meets it.
    /// <para>
    /// The stall line names
    /// <c>orleans.lattice.leaf.activation_stalled_replays</c> as "the exact
    /// census of the condition", because the line itself is throttled to a
    /// sample. Two stalls occur here on the same frozen checkpoint, separated by
    /// an overflow. Both are occurrences of the condition, so the census is 2.
    /// </para>
    /// <para>
    /// Before the fix this reports 1. The overflow discarded the observation the
    /// second stall would have been detected against, so no stall is recognised
    /// and nothing is counted - an undercount produced by map pressure, invisible
    /// in the series, in a counter that documents itself as exact.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_stall_census_counts_through_an_overflow_of_the_observation_map()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        using var metrics = new LeafReplayMetricRecorder(LatticeMetrics.LeafActivationStalledReplays.Name);
        var logs = new RecordingLoggerFactory();

        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);
        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);

        OverflowReplayObservations();

        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);

        Assert.That(metrics.Count(BudgetUnitsTreeId), Is.EqualTo(2),
            "Both re-entries are occurrences of the condition. Counting one of them makes the census a "
            + "function of unrelated observation-map pressure, and the shipped message directs the operator "
            + "to this counter precisely because the log line is only a sample.");
    }

    /// <summary>
    /// GUARD on the memory bound the capacity exists to enforce. Preferring
    /// stalling entries when evicting must not turn the soft cap into an
    /// unbounded map: a silo on which EVERY tracked leaf partition is stalling
    /// still has to shed state, and it sheds it wholesale, which is no worse than
    /// the behaviour this change replaces.
    /// </summary>
    [Test]
    public void Replay_observation_map_stays_bounded_when_every_entry_is_stalling()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();

        // Two observations per key at the same checkpoint, so every entry in the
        // map carries a stall run and none is eligible for preferential
        // eviction.
        for (var i = 0; i <= BPlusLeafGrain.ReplayCheckpointObservationCapacity * 2; i++)
        {
            BPlusLeafGrain.NoteReplayCheckpointObservation("tree-stall-bound", $"leaf-{i}", partition: 0, checkpoint: 7);
            BPlusLeafGrain.NoteReplayCheckpointObservation("tree-stall-bound", $"leaf-{i}", partition: 0, checkpoint: 7);
        }

        Assert.That(BPlusLeafGrain.ReplayCheckpointObservationCountForTests,
            Is.LessThanOrEqualTo(BPlusLeafGrain.ReplayCheckpointObservationCapacity),
            "The cap is what keeps this map from growing with the leaf population for the life of the silo, "
            + "and retaining stall runs must not be able to defeat it.");
    }
}
