using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3477. A leaf partition latched as stale by #3450/#3451 was later
/// observed replaying from a HIGHER persisted checkpoint, with no error in
/// between. The question was whether that higher checkpoint was earned (a
/// snapshot covered the gap, so the latch was a false alarm) or not (a
/// checkpoint was persisted past a range the leaf never applied).
/// <para>
/// <b>The verdict: silent loss.</b> The projection checkpoint hint seam,
/// <c>ApplyCheckpointHintAsync</c>, stamps a WAL head captured elsewhere - by a
/// split, for the donor and for the new sibling - straight into the persisted
/// checkpoint. It never asked whether the partition was latched. A stale leaf
/// that is still live keeps taking writes and keeps splitting, so its donor-side
/// hint persisted the split-time WAL head over a trimmed range the leaf had
/// never applied. That moved the persisted checkpoint signature, which silently
/// cleared the latch; the next replay started from the new checkpoint and the
/// trimmed range was never reported again.
/// </para>
/// <para>
/// <b>The fix.</b> The seam refuses a hint for a partition the latch holds as
/// stale. A direct <c>SetCheckpointOffsetAsync</c> models a real read and still
/// re-arms the latch (<c>A_persisted_checkpoint_change_rearms_the_stale_drive_latch</c>).
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// THE regression. A hint on a stale-latched partition must not move the
    /// persisted checkpoint and must not clear the latch.
    /// <para>
    /// RED pre-fix: the hint persists 50 over a checkpoint of 4 whose
    /// (4, 50] range the leaf never applied, and the latch reads cleared.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_checkpoint_hint_does_not_advance_a_stale_latched_partition()
    {
        var decision = FallOffLogDecision.TailReplay;
        var staleClassifications = 0;

        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: UniqueCoverageRepairTreeId("latched-hint-refused"),
            detectorDecision: () =>
            {
                if (decision == FallOffLogDecision.SnapshotThenWal)
                {
                    staleClassifications++;
                }

                return decision;
            });

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        decision = FallOffLogDecision.SnapshotThenWal;

        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
        Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True, "precondition: latched");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L), "precondition: persisted at 4");

        await grain.SetCheckpointOffsetHintsAsync(new long[] { 50 });

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L),
                "the hint is a WAL head captured elsewhere, not evidence that this leaf applied "
                + "(4, 50]. The WAL was trimmed past 4 and no snapshot covers the gap, so "
                + "persisting 50 records as applied a range this leaf never held");

            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(4L),
                "nor may the hint park as a pending advance, which the next checkpoint flush "
                + "would persist on the interval path");

            Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True,
                "a refused hint changes no persisted checkpoint, so the verdict still stands. A "
                + "latch that a hint can clear is how the loss went unreported");
        });

        Assert.ThrowsAsync<LeafProjectionStaleException>(
            () => grain.DriveStarvedCheckpointAsync(),
            "the WAL GC sweep must keep receiving the stale fault for this leaf");

        Assert.That(staleClassifications, Is.EqualTo(1),
            "the second drive answers from the latch, which the hint left intact");
    }

    /// <summary>
    /// The positive control for the regression above: the identical hint on
    /// the identical leaf, never latched, must advance. Without it the
    /// regression could pass because hints simply do nothing in this harness.
    /// </summary>
    [Test]
    public async Task A_checkpoint_hint_advances_a_partition_that_is_not_latched()
    {
        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: UniqueCoverageRepairTreeId("unlatched-hint-advances"));

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);

        Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.False, "precondition: not latched");

        await grain.SetCheckpointOffsetHintsAsync(new long[] { 50 });

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(50L),
            "an un-latched leaf takes the hint exactly as before #3477");
    }

    /// <summary>
    /// The refusal is scoped to the partition the latch names. The stale
    /// partition here is partition 1, not the default partition 0, so a fix
    /// that fell back to partition 0 - or refused every partition - fails one
    /// of the two assertions. It also pins the ordering hazard: hints are
    /// applied partition by partition, so partition 0's legitimate advance
    /// lands first and changes the summed checkpoint signature. A refusal keyed
    /// on that sum would release partition 1's hint one iteration later.
    /// </summary>
    [Test]
    public async Task A_checkpoint_hint_is_refused_only_for_the_stale_partition()
    {
        var staleFromNow = false;

        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            walPartitions: 2,
            treeId: UniqueCoverageRepairTreeId("latched-hint-per-partition"),
            detectorDecisionForPartition: partition =>
                staleFromNow && partition == 1
                    ? FallOffLogDecision.SnapshotThenWal
                    : FallOffLogDecision.TailReplay);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        using (LatticeApplyOffsetContext.BeginScope(1, 4L))
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        }

        staleFromNow = true;

        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
        Assert.That(grain.IsProjectionStaleDriveLatched(2), Is.True, "precondition: latched");

        await grain.SetCheckpointOffsetHintsAsync(new long[] { 50, 60 });

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffsetsByPartition![1], Is.EqualTo(4L),
                "partition 1 is the one whose WAL was trimmed past its checkpoint, so its hint "
                + "must be refused");

            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(50L),
                "partition 0 replayed cleanly; refusing its hint too would hold back a partition "
                + "the verdict says nothing about");
        });
    }

    /// <summary>
    /// The refusal must not outlive the latch. Once a genuine checkpoint change
    /// clears it, hints advance again; otherwise a leaf that was repaired would
    /// stay unable to take a split frontier for the rest of its activation.
    /// </summary>
    [Test]
    public async Task A_checkpoint_hint_advances_again_once_the_latch_is_cleared()
    {
        var decision = FallOffLogDecision.TailReplay;

        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: UniqueCoverageRepairTreeId("latched-hint-rearmed"),
            detectorDecision: () => decision);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        decision = FallOffLogDecision.SnapshotThenWal;

        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());

        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(9, CancellationToken.None);
        Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.False, "precondition: re-armed");

        await grain.SetCheckpointOffsetHintsAsync(new long[] { 50 });

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(50L),
            "the refusal belongs to the verdict, and the verdict no longer holds");
    }
}
