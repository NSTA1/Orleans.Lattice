using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3450. A leaf whose projection is stale (the WAL was trimmed past
/// an offset its persisted checkpoint still needs, with no covering snapshot)
/// cannot be repaired by the starvation drive, and must not be re-driven for the
/// life of its activation.
/// <para>
/// <b>The defect.</b> The coverage-lag timer routes a leaf whose checkpoint has
/// frozen to the starvation drive (#3389), and re-arms every
/// <c>CheckpointStallTicksBeforeDrive</c> ticks. A stale leaf's checkpoint is
/// frozen precisely BECAUSE replay cannot advance it, so the drive throws
/// <see cref="LeafProjectionStaleException"/> on every window, forever. Each
/// attempt took a replay permit, and each fault escaped the timer callback and
/// was logged twice by the runtime. A live deployment logged about 10,000 such
/// faults in four hours from roughly 200 leaf partitions.
/// </para>
/// <para>
/// <b>The fix.</b> The verdict is latched against the persisted checkpoint
/// signature. The timer skips a latched leaf. The WAL GC sweep's direct call
/// still receives the typed fault without a replay, so its classification is
/// unchanged. Any persisted checkpoint change clears the latch.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The defect fixture. Several stall windows of ticks on a stale leaf must
    /// not throw, and must classify the projection only once.
    /// <para>
    /// RED pre-fix: the first stalled drive throws the stale fault out of
    /// <c>OnCoverageLagTimerTickAsync</c>.
    /// </para>
    /// </summary>
    [Test]
    public async Task Coverage_lag_ticks_classify_a_stale_leaf_once_and_do_not_throw()
    {
        var treeId = UniqueCoverageRepairTreeId("stale-latch-ticks");
        var decision = FallOffLogDecision.TailReplay;
        var staleClassifications = 0;

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: null,
            treeId: treeId,
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

        // From here on the WAL is "trimmed past" the checkpoint.
        decision = FallOffLogDecision.SnapshotThenWal;

        const int windows = 3;
        var reasons = new List<string>();
        using (ListenForDriverDeclinesOnTree(treeId, reasons))
        {
            for (var tick = 0; tick < TicksToReachStalledDrive * windows; tick++)
            {
                Assert.DoesNotThrowAsync(
                    () => grain.OnCoverageLagTimerTickAsync(CancellationToken.None),
                    "a stale projection is an operator condition, not a timer fault. Letting it "
                    + "escape the timer callback is what logged it twice per drive, forever");
            }
        }

        var stalledReason = (string?)LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value;
        Assert.Multiple(() =>
        {
            Assert.That(
                reasons.Count(r => string.Equals(r, stalledReason, StringComparison.Ordinal)),
                Is.GreaterThan(1),
                "input count: the ticks must have reached the stalled arm in more than one "
                + "window, or the single classification below would prove nothing about the latch");

            Assert.That(staleClassifications, Is.EqualTo(1),
                "the drive must reach the stale verdict once and then stop replaying. Replaying "
                + "the same checkpoint against a WAL whose tail only moves forward cannot reach a "
                + "different verdict, so every further attempt only spends a replay permit");
        });
    }

    /// <summary>
    /// The WAL GC sweep calls the drive directly and classifies the leaf by the
    /// fault it receives. A latched leaf must still hand it the typed fault, so
    /// the sweep's accounting is unchanged, but without running another replay.
    /// </summary>
    [Test]
    public async Task A_latched_stale_leaf_still_faults_a_direct_drive_without_replaying()
    {
        var treeId = UniqueCoverageRepairTreeId("stale-latch-direct");
        var decision = FallOffLogDecision.TailReplay;
        var staleClassifications = 0;

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: treeId,
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

        Assert.ThrowsAsync<LeafProjectionStaleException>(
            () => grain.DriveStarvedCheckpointAsync(),
            "the first drive runs the replay and reaches the stale verdict");

        Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True,
            "the verdict must be latched once reached");

        var second = Assert.ThrowsAsync<LeafProjectionStaleException>(
            () => grain.DriveStarvedCheckpointAsync(),
            "a latched leaf must still fault the sweep's direct call, so the sweep keeps "
            + "classifying it as a leaf activation alone cannot clear");

        Assert.Multiple(() =>
        {
            Assert.That(staleClassifications, Is.EqualTo(1),
                "the second drive must answer from the latch, not by replaying again");

            Assert.That(second!.InnerException, Is.InstanceOf<LeafProjectionStaleException>(),
                "the latched fault carries the original verdict so the operator still sees the "
                + "partition and checkpoint it was reached at");
        });
    }

    /// <summary>
    /// The re-arm control. The latch is a statement about specific persisted
    /// checkpoints, so any change to them must clear it and let the next drive
    /// classify again. Without this the latch could outlive the state it
    /// describes, which is the non-convergence the #3389 re-arm exists to
    /// prevent.
    /// </summary>
    [Test]
    public async Task A_persisted_checkpoint_change_rearms_the_stale_drive_latch()
    {
        var treeId = UniqueCoverageRepairTreeId("stale-latch-rearm");
        var decision = FallOffLogDecision.TailReplay;
        var staleClassifications = 0;

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: treeId,
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

        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(9, CancellationToken.None);

        Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.False,
            "a moved persisted checkpoint is new state the verdict was never reached on");

        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
        Assert.That(staleClassifications, Is.EqualTo(2),
            "the drive after the move must replay and classify again rather than answer from "
            + "the old verdict");
    }
}
