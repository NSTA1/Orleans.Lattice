using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the SENSITIVITY of the leaf non-convergence fault
/// detector (issue #2291).
/// <para>
/// The fault criterion is per-leaf: a leaf re-enters replay with its persisted
/// checkpoint unchanged, so the previous activation banked no durable forward
/// progress. The gate that decided whether to report it also required the
/// partition-wide offset gap to exceed
/// <see cref="LatticeOptions.MaxLeafReplayEntries"/>. Those are different
/// quantities: the gap is shared with every sibling leaf pinned to the
/// partition (~1,350 of them on the measured deployment) and the budget is
/// advisory and per-leaf, so the conjunction made a permanent stall SILENT
/// wherever the partition happened to be shallow.
/// </para>
/// <para>
/// The blind spot was arithmetic rather than statistical, which is why raising
/// the budget could not have closed it. The gap can never exceed the readable
/// WAL depth of its partition, so on any partition holding at most
/// <c>MaxLeafReplayEntries</c> entries the conjunction is unsatisfiable and no
/// leaf pinned to it could be reported at all, however completely it was
/// stuck. Field measurement on the deployed container found the visible
/// population sitting between 2.64x and 8.18x the configured cap of 10,000,
/// with not one observation inside 164% of the threshold, so the whole
/// under-cap region was unlit.
/// </para>
/// <para>
/// The gate's stated justification was that "a frozen checkpoint whose
/// partition gap fits inside the budget is an idle leaf, not a livelock". That
/// is refuted by the code itself: an idle leaf returns earlier, on the
/// <c>head &lt;= checkpoint</c> check, and never reaches this gate. Past that
/// point there is unreplayed work by construction, so a frozen checkpoint is a
/// livelock at any gap.
/// <see cref="Replay_idle_leaf_with_nothing_to_replay_is_never_reported_as_stalled"/>
/// holds that refutation to the code so it cannot silently stop being true.
/// </para>
/// <para>
/// The correction is a level split rather than a straight promotion. The size
/// of the under-cap population is unknown, and it cannot be estimated from the
/// visible one without reading a population off the very filter that defines
/// it. So over-cap keeps <see cref="LogLevel.Warning"/> and warning volume is
/// unchanged, while under-cap emits at <see cref="LogLevel.Information"/> and
/// becomes countable for the first time. These tests therefore look for fault
/// lines at ANY level and assert the level separately, so neither arm can
/// regress into the other unnoticed.
/// </para>
/// <para>
/// The same gate also enclosed the stall COUNTER added by issue #2285, whose
/// own log line advertises it as "the exact census of the condition". Gated, it
/// was an exact census of the over-cap subset instead, undercounting by an
/// amount nothing in the system could observe while documenting itself as
/// authoritative. Counting on the convergence predicate alone is what makes
/// that shipped sentence true, and
/// <see cref="Replay_stall_on_an_under_cap_partition_is_metered_as_part_of_the_census"/>
/// is the discriminator for it.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Drives one activation of a fresh leaf that re-enters replay at
    /// <paramref name="persistedCheckpoint"/>, sharing a partition whose head
    /// is <paramref name="partitionHead"/>, against
    /// <paramref name="maxLeafReplayEntries"/>. The leaf's own post-filter work
    /// is deliberately tiny (3 entries), so it is never over its OWN budget and
    /// any line it produces is the fault line rather than the cost line.
    /// </summary>
    private static async Task ActivateStallCandidateAsync(
        RecordingLoggerFactory logs,
        long partitionHead,
        long persistedCheckpoint,
        int maxLeafReplayEntries)
    {
        var entries = BuildSharedPartitionEntries(siblingEntries: 40, ownEntries: 3);
        var coord = BuildBudgetUnitsCoordinator(partitionHead, entries);
        var (grain, _) = CreateBudgetUnitsLeaf(
            coord,
            logs,
            partitionHead: partitionHead,
            persistedCheckpoint: persistedCheckpoint,
            maxLeafReplayEntries: maxLeafReplayEntries);
        await ActivateAsync(grain);
    }

    /// <summary>
    /// Every non-convergence fault line captured, at ANY level. The detector
    /// reports one fault at two levels (issue #2291), so a filter fixed to
    /// warnings could not see the under-cap arm at all - and a test that cannot
    /// see a line cannot tell "never emitted" from "emitted quietly", which is
    /// the exact confusion this item exists to remove.
    /// </summary>
    private static IReadOnlyList<RecordedLogEntry> StalledFaultLines(RecordingLoggerFactory logs) =>
        logs.Entries.Where(e => e.Message.Contains("WITHOUT its persisted checkpoint having advanced", StringComparison.Ordinal)).ToArray();

    /// <summary>
    /// DISCRIMINATOR for issue #2291, and the test the item exists for.
    /// <para>
    /// Exactly the stall of
    /// <c>Replay_leaf_whose_checkpoint_does_not_advance_still_warns_as_a_fault</c>
    /// - the same leaf, the same unchanging checkpoint, the same banked-nothing
    /// previous activation - with one quantity changed: the partition gap
    /// (4,900) now fits inside the budget (10,000, the value configured on the
    /// deployed container). The leaf is stalled by precisely the same criterion,
    /// and the operator's evidence of it disappears.
    /// </para>
    /// <para>
    /// Before the fix this test fails, and it fails by finding NOTHING: the
    /// conjunction is false, so the fault is not reported at any log level and
    /// the stall is invisible. That silence is the defect, and a bound
    /// expressed on the partition gap cannot express its absence.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_leaf_stalled_on_an_under_cap_partition_is_reported_as_a_fault()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        var logs = new RecordingLoggerFactory();

        // Gap is 4,900 against a budget of 10,000: comfortably under the cap,
        // which is the region the deployed detector never lit.
        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);

        Assert.That(StalledFaultLines(logs), Is.Empty,
            "A single activation is not evidence of a stall: the criterion needs a repeat.");

        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);

        var stalled = StalledFaultLines(logs).Single();
        Assert.Multiple(() =>
        {
            Assert.That(stalled.Int64("Checkpoint"), Is.EqualTo(100),
                "The line must report the checkpoint that failed to advance.");
            Assert.That(stalled.Int64("Gap"), Is.EqualTo(4_900));
            Assert.That(stalled.Int64("Gap"), Is.LessThan(10_000),
                "This is the whole point of the item: the fault is reported even though the gap is INSIDE the budget the old gate required it to EXCEED. The budget itself is deliberately no longer printed (issue #2285), so this compares against the configured value passed in.");
            Assert.That(stalled.Level, Is.EqualTo(LogLevel.Information),
                "Under-cap is the region that was never observable, so it is reported at Information: countable at last, without disturbing today's warning volume.");
            Assert.That(StalledWarnings(logs), Is.Empty,
                "Bounding the worst case is the point of the level split: while the under-cap population is unknown it must not be added to the warning stream.");
            Assert.That(OverBudgetWarnings(logs), Is.Empty,
                "The leaf's own work is under its own budget, so this must be reported as a fault and not as a slow replay.");
        });
    }

    /// <summary>
    /// DISCRIMINATOR showing the defect is a property of the gate rather than of
    /// any particular budget value: the same stall must be reported whatever
    /// the cap is set to, and only its LEVEL may depend on the cap. The 10,000
    /// case is the deployed configuration and reported nothing at all before the
    /// fix; the 5 case is the pre-existing over-cap shape and must keep warning
    /// exactly as it does today. Running both in one method makes the asymmetry
    /// the subject of the test, so "raise the budget" is visibly not a remedy -
    /// the gap can never exceed its partition's WAL depth, so on a shallow
    /// partition NO cap makes the old conjunction satisfiable.
    /// </summary>
    [TestCase(5, LogLevel.Warning)]
    [TestCase(10_000, LogLevel.Information)]
    public async Task Replay_stall_is_reported_independently_of_the_partition_gap_budget(
        int maxLeafReplayEntries,
        LogLevel expectedLevel)
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        var logs = new RecordingLoggerFactory();

        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: maxLeafReplayEntries);
        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: maxLeafReplayEntries);

        var stalled = StalledFaultLines(logs);
        Assert.Multiple(() =>
        {
            Assert.That(stalled, Has.Count.EqualTo(1),
                "The leaf is not converging, and whether an operator learns that must not depend on a quantity measured on its siblings.");
            Assert.That(stalled[0].Level, Is.EqualTo(expectedLevel),
                "The budget selects the level of the fault line, never whether the fault is reported at all.");
        });
    }

    /// <summary>
    /// GUARD, and the executable refutation of the gate's stated justification.
    /// <para>
    /// The gate was defended on the ground that "a frozen checkpoint whose
    /// partition gap fits inside the budget is an idle leaf, not a livelock".
    /// If that were true, removing the budget term would turn every idle leaf
    /// into a fault line. It is not true: an idle leaf is one with nothing to
    /// replay, <c>head &lt;= checkpoint</c>, and that returns before the gate is
    /// ever evaluated. This leaf re-activates repeatedly at a checkpoint that
    /// never moves - the literal frozen checkpoint - and must stay silent,
    /// because its checkpoint is frozen at the head rather than behind it.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_idle_leaf_with_nothing_to_replay_is_never_reported_as_stalled()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        var logs = new RecordingLoggerFactory();

        // head == checkpoint: fully caught up, so there is no work outstanding
        // and no forward progress to fail to make.
        await ActivateStallCandidateAsync(logs, partitionHead: 100, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);
        await ActivateStallCandidateAsync(logs, partitionHead: 100, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);
        await ActivateStallCandidateAsync(logs, partitionHead: 100, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);

        Assert.That(StalledFaultLines(logs), Is.Empty,
            "An idle leaf returns on the head <= checkpoint check and never reaches the fault gate, so widening that gate cannot make idleness noisy at any level.");
    }

    /// <summary>
    /// GUARD against the fix becoming a new source of noise: a leaf whose
    /// checkpoint DOES advance between activations is converging, however far
    /// behind the head it is, and must stay silent under the widened gate.
    /// </summary>
    [Test]
    public async Task Replay_leaf_whose_checkpoint_advances_stays_silent_under_the_widened_gate()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        var logs = new RecordingLoggerFactory();

        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);
        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 220, maxLeafReplayEntries: 10_000);
        await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 340, maxLeafReplayEntries: 10_000);

        Assert.That(StalledFaultLines(logs), Is.Empty,
            "Slow forward progress is not a stall, and the fault line must keep distinguishing the two at every level.");
    }

    /// <summary>
    /// DISCRIMINATOR for the CENSUS claim, where issues #2285 and #2291 meet,
    /// and the most consequential half of this change.
    /// <para>
    /// The stall line tells an operator, in as many words, that "the exact
    /// census of the condition is the counter
    /// orleans.lattice.leaf.activation_stalled_replays". While that counter sat
    /// inside the over-budget conjunction the sentence was false: the counter
    /// was an exact census of the over-cap SUBSET, undercounting by an amount
    /// nothing in the system could observe, and it was advertised as a census in
    /// the very message an operator would use to check it. A wrong number that
    /// documents itself as authoritative is worse than no number.
    /// </para>
    /// <para>
    /// <c>Replay_stall_fault_is_metered_on_every_occurrence_even_when_the_warning_is_throttled</c>
    /// establishes the census against the log THROTTLE, but it runs at
    /// <c>maxLeafReplayEntries: 5</c>, so every occurrence it counts is
    /// over-cap and the budget gate is satisfied throughout. It therefore
    /// cannot see this. This test is the same census claim at
    /// <c>10_000</c>, the deployed cap, where the gate used to zero it.
    /// </para>
    /// <para>
    /// Three activations from one unchanged checkpoint produce two stalls (the
    /// first has nothing to compare against), both inside one throttle
    /// interval. So the census is 2 while the log emits once - and that single
    /// line must be Information, proving the counter was fixed without adding
    /// anything at all to the warning stream.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_stall_on_an_under_cap_partition_is_metered_as_part_of_the_census()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        using var metrics = new LeafReplayMetricRecorder(LatticeMetrics.LeafActivationStalledReplays.Name);
        var logs = new RecordingLoggerFactory();

        for (var activation = 0; activation < 3; activation++)
        {
            await ActivateStallCandidateAsync(logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 10_000);
        }

        Assert.Multiple(() =>
        {
            Assert.That(metrics.Count(BudgetUnitsTreeId), Is.EqualTo(2),
                "The counter must census every occurrence of the condition, not every occurrence that also happened to be over a budget measured on this leaf's siblings.");
            Assert.That(StalledFaultLines(logs), Has.Count.EqualTo(1),
                "The line stays a throttled SAMPLE of the condition, which is exactly why the counter has to be the census.");
            Assert.That(StalledWarnings(logs), Is.Empty,
                "The census is repaired without adding one line to the warning stream: that is what makes the worst case of this change bounded.");
        });
    }
}
