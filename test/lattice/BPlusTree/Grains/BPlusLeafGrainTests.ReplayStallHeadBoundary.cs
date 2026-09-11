using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the OFF-BY-ONE at the partition head boundary that made
/// the leaf non-convergence detector report every converged leaf as stalled
/// (issue #2668).
/// <para>
/// The two quantities the early return compares are measured from different
/// origins. <c>head</c> is
/// <see cref="ILeafReplayCoordinatorGrain.GetHeadOffsetAsync"/>, documented as
/// "the next sequence number that will be assigned to a future append", so it
/// is EXCLUSIVE and is one past the newest entry. <c>checkpoint</c> is
/// <c>ProjectionCheckpointOffset</c>, the highest offset this leaf has SCANNED,
/// so it is INCLUSIVE and equals the offset of an entry that really exists.
/// A leaf that has read the whole partition therefore sits at
/// <c>checkpoint == head - 1</c>, not at <c>checkpoint == head</c>.
/// </para>
/// <para>
/// <c>head &lt;= checkpoint</c> compares the exclusive bound against the
/// inclusive one, so it is false for a fully caught-up leaf. Every converged
/// leaf on a partition that has ever been written therefore skips the early
/// return, enters replay, reads the empty range <c>(head - 1, head]</c>, applies
/// nothing, correctly does not advance its checkpoint, and is reported on its
/// next activation as a leaf that "re-entered replay WITHOUT its persisted
/// checkpoint having advanced" - a message that goes on to tell the operator
/// that "writes routed to it are being lost".
/// </para>
/// <para>
/// This is self-sustaining rather than transient, which is what distinguishes it
/// from the sibling-entry explanation the issue was filed on. A leaf that
/// entered replay because a SIBLING wrote does read that sibling's entry, and
/// the checkpoint advance at the bottom of the scan loop sits OUTSIDE the
/// <c>ShouldApplyDuringReplay</c> filter precisely so that "an entry skipped as
/// another leaf's work still moves the checkpoint". Such a leaf advances and
/// stops reporting, so it cannot produce a persistent stall. The boundary case
/// can, because there is no entry to read at all.
/// </para>
/// <para>
/// It also explains the field signature exactly: the reported partition gap is
/// <c>head - checkpoint</c>, which at this boundary is ALWAYS exactly 1, and
/// distinct leaves pinned to one partition report identical checkpoints because
/// being caught up is a partition-wide steady state rather than a per-leaf
/// accident.
/// </para>
/// <para>
/// The guard that should have caught this,
/// <c>Replay_idle_leaf_with_nothing_to_replay_is_never_reported_as_stalled</c>,
/// encodes "fully caught up" as <c>partitionHead: 100, persistedCheckpoint:
/// 100</c>. That state is UNREACHABLE in production: the checkpoint is assigned
/// from <c>entry.Offset</c> of an entry the scan actually read, so it can never
/// reach the exclusive head. The guard passes against a state the system cannot
/// occupy, which is why the reachable one next to it shipped unnoticed. These
/// tests pin the reachable boundary instead.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The offset of the newest entry <see cref="BuildSharedPartitionEntries"/>
    /// produces for (40 sibling, 3 own): it numbers entries from 1, so 43 is the
    /// last one that exists.
    /// </summary>
    private const long HeadBoundaryNewestOffset = 43;

    /// <summary>
    /// The head a WAL reports once the entry at
    /// <see cref="HeadBoundaryNewestOffset"/> has been appended. The head is the
    /// NEXT sequence to be assigned, so it is one past the newest entry.
    /// </summary>
    private const long HeadBoundaryHead = HeadBoundaryNewestOffset + 1;

    /// <summary>
    /// DISCRIMINATOR for issue #2668, and the false-positive arm of its
    /// acceptance criteria.
    /// <para>
    /// A leaf that has scanned the whole partition sits at
    /// <c>checkpoint == head - 1</c>. It has nothing outstanding, so it must not
    /// be reported as failing to converge and must not be counted by
    /// <c>orleans.lattice.leaf.activation_stalled_replays</c>, which the fault
    /// line advertises as "the exact census of the condition".
    /// </para>
    /// <para>
    /// The counter is asserted as well as the log because the counter is the
    /// surface operators are directed to trust: a fix that quietened the line
    /// and left the census inflated would not have fixed what the dashboard
    /// reads.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_caught_up_leaf_one_behind_the_exclusive_head_is_never_reported_as_stalled()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        using var census = new LeafReplayMetricRecorder(LatticeMetrics.LeafActivationStalledReplays.Name);
        var logs = new RecordingLoggerFactory();

        // The reachable caught-up state: the newest entry is at
        // HeadBoundaryNewestOffset and this leaf has scanned it, so its
        // checkpoint IS that offset while the head sits one beyond.
        for (var activation = 0; activation < 3; activation++)
        {
            await ActivateStallCandidateAsync(
                logs,
                partitionHead: HeadBoundaryHead,
                persistedCheckpoint: HeadBoundaryNewestOffset,
                maxLeafReplayEntries: 10_000);
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                StalledFaultLines(logs),
                Is.Empty,
                "A leaf whose checkpoint is the newest entry on the partition has read everything there is to "
                + "read. Its checkpoint does not advance because there is nothing to advance over, which is "
                + "convergence and not a livelock. Reporting it tells the operator that writes routed to it are "
                + "being lost, which is false.");

            Assert.That(
                census.Count(BudgetUnitsTreeId),
                Is.Zero,
                "orleans.lattice.leaf.activation_stalled_replays is documented in the fault line, in the source, "
                + "and in the Grafana panel as the EXACT census of the condition. Counting a converged leaf "
                + "makes that claim false on every deployment whose WAL has ever been written.");
        });
    }

    /// <summary>
    /// TRUE-POSITIVE arm, and the guard that stops the fix being a silencer.
    /// <para>
    /// A leaf whose checkpoint is genuinely BEHIND the newest entry has work
    /// outstanding. If it re-enters replay at that same checkpoint it banked no
    /// durable progress, which is the livelock of issue #1819, and it must still
    /// be reported and still be counted.
    /// </para>
    /// <para>
    /// This arm is what a fix that merely deleted the detector, or that gated it
    /// on an unconditionally-zero applied count, would fail.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_leaf_frozen_behind_the_newest_entry_is_still_reported_and_still_counted()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        using var census = new LeafReplayMetricRecorder(LatticeMetrics.LeafActivationStalledReplays.Name);
        var logs = new RecordingLoggerFactory();

        // Frozen well short of the newest entry: there is real unreplayed work
        // for this leaf's partition and no forward progress is being banked.
        const long frozenCheckpoint = 10;
        for (var activation = 0; activation < 3; activation++)
        {
            await ActivateStallCandidateAsync(
                logs,
                partitionHead: HeadBoundaryHead,
                persistedCheckpoint: frozenCheckpoint,
                maxLeafReplayEntries: 10_000);
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                StalledFaultLines(logs),
                Is.Not.Empty,
                "A checkpoint frozen strictly behind the newest entry is the fault this detector exists for. "
                + "Narrowing the head comparison must not cost the true positive.");

            Assert.That(
                census.Count(BudgetUnitsTreeId),
                Is.GreaterThan(0),
                "The census must still count a genuine non-convergence, or the fix has traded a false positive "
                + "for a false negative.");
        });
    }

    /// <summary>
    /// Pins the ARITHMETIC that makes the false positive self-sustaining, so the
    /// reported gap cannot drift back to being read as a workload measurement.
    /// <para>
    /// At this boundary the gap the fault line prints is always exactly 1,
    /// whatever the partition depth or the fan-out, because it is
    /// <c>head - (head - 1)</c>. The deployed container reported <c>partition
    /// gap 1 entries</c> on every one of 6,632 samples, and a gap that is
    /// invariant under load is a boundary condition rather than a backlog.
    /// </para>
    /// </summary>
    [Test]
    public void Replay_gap_at_the_head_boundary_is_always_exactly_one_entry()
    {
        foreach (var head in new long[] { 1, 2, 44, 30_802, 31_054 })
        {
            var caughtUpCheckpoint = head - 1;
            Assert.That(
                head - caughtUpCheckpoint,
                Is.EqualTo(1),
                "The gap a converged leaf reports is head - (head - 1) and so carries no information about the "
                + "partition's depth. A constant gap of 1 across every sample is the signature of this boundary.");
        }
    }
}
