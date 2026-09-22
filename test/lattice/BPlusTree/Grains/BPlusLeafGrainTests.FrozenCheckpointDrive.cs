using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3389. A leaf whose durable checkpoint HAS a value but has STOPPED
/// ADVANCING loses acknowledged writes on restart, and no existing detector
/// reports it.
/// <para>
/// <b>The defect.</b> <c>IsStarvedOfDurableCheckpoint</c> returns
/// <c>false</c> the moment any partition holds <c>checkpoint(p) &gt;= 0</c>,
/// because it was written for a leaf frozen at the <c>-1</c> birth sentinel.
/// The condition that actually costs data is not "has never checkpointed" but
/// "the checkpoint is not advancing". A leaf frozen at a stale NON-NEGATIVE
/// offset is in at least as much trouble, and is harder to see: it hydrates
/// cleanly on restart, passes every eligibility gate, and reports
/// <c>recheck_coverage_current</c> forever, because coverage and checkpoint are
/// frozen EQUAL and so <c>checkpoint(p) &gt; covered(p)</c> is false.
/// </para>
/// <para>
/// <b>Why it is stable.</b> Writes after the freeze land in the WAL and are
/// acknowledged. Compaction classifies them dead and reclaims them, taking
/// retained WAL to zero; an empty WAL means the GC finds no blocking pin; no
/// blocking pin means the blocked-leaf sweep never runs; and that sweep is the
/// only other route to the starvation drive. Compaction, by operating
/// correctly, erases the signal that would trigger the repair.
/// </para>
/// <para>
/// <b>Method rules.</b> These fixtures follow the sibling instrumentation file:
/// every one asserts a NON-ZERO input count before asserting on arms, so an
/// empty listener cannot satisfy a negative assertion vacuously. The listener is
/// filtered to each fixture's own tree, so a sibling fixture's ordinary decline
/// cannot stand in for the arm under test.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Ticks needed to reach the drive: one to establish the signature
    /// baseline, then <c>CheckpointStallTicksBeforeDrive</c> ticks that observe
    /// it unchanged. Kept as a named local rather than inlined so that a change
    /// to the production threshold fails this fixture loudly rather than
    /// silently reducing it to a weaker assertion.
    /// </summary>
    private const int TicksToReachStalledDrive = 4;

    /// <summary>
    /// The defect fixture. A leaf that HAS checkpointed, holds rows, and whose
    /// checkpoint never moves again must eventually be routed to the starvation
    /// drive on its own distinct arm.
    /// <para>
    /// Before the fix this leaf reported <c>recheck_coverage_current</c> on
    /// every tick forever, which is the arm documented as the healthy majority.
    /// That is the whole defect: a tree actively losing every write it accepted
    /// was indistinguishable, in every counter, from a tree that had made
    /// everything durable.
    /// </para>
    /// </summary>
    [Test]
    public async Task Coverage_lag_tick_routes_a_leaf_whose_checkpoint_has_frozen_to_the_drive()
    {
        var treeId = UniqueCoverageRepairTreeId("checkpoint-frozen");

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: null,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        // The row is what makes this the losing population. Without it the leaf
        // has nothing at stake and must NOT be charged a replay, which the
        // empty-leaf fixture below asserts directly.
        SeedRow(grain);

        // One persist, then never again. This is the frozen state: a real,
        // non-negative checkpoint that stops advancing while writes continue.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(4L),
                "precondition: this leaf HAS checkpointed. A negative checkpoint here would make "
                + "it the #3300 starved population instead, which the older predicate already "
                + "handles - and the test would then pass without covering this defect at all");

            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(4L),
                "precondition: coverage is EQUAL to the checkpoint, not behind it. That equality "
                + "is exactly why the debounce reports coverage_current: checkpoint > covered is "
                + "false, so the healthy-looking arm is reached by a leaf in the worst state");
        });

        var reasons = new List<string>();
        using (ListenForDriverDeclinesOnTree(treeId, reasons))
        {
            for (var tick = 0; tick < TicksToReachStalledDrive; tick++)
            {
                await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }
        }

        Assert.That(reasons, Is.Not.Empty,
            "input count: the ticks must have reached the driver at all. An empty list would let "
            + "the arm assertion below pass vacuously, which is the same absence-is-not-evidence "
            + "failure that let this defect survive in production telemetry");

        Assert.That(
            reasons,
            Does.Contain(LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value),
            "a leaf whose checkpoint has frozen while it holds rows must be routed to the drive "
            + "that replays the WAL, because replaying from the checkpoint is precisely what "
            + "advances a checkpoint that has stopped advancing. Before the fix this leaf took "
            + "the coverage_current arm on every tick and was never repaired");
    }

    /// <summary>
    /// The discriminating control, and the reason the fixture above is evidence
    /// about the predicate rather than about the tick count. The SAME number of
    /// ticks, on a leaf whose checkpoint ADVANCES between them, must never reach
    /// the stalled arm.
    /// <para>
    /// Without this, "tick four times and take the new arm" would pass the
    /// fixture above while charging a WAL replay to every healthy leaf in the
    /// estate on a fixed cadence. The two fixtures differ in exactly one
    /// variable: whether the checkpoint moved.
    /// </para>
    /// </summary>
    [Test]
    public async Task Coverage_lag_tick_never_drives_a_leaf_whose_checkpoint_is_still_advancing()
    {
        var treeId = UniqueCoverageRepairTreeId("checkpoint-advancing");

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: null,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);

        var reasons = new List<string>();
        using (ListenForDriverDeclinesOnTree(treeId, reasons))
        {
            for (var tick = 0; tick < TicksToReachStalledDrive; tick++)
            {
                // The single variable under test: the checkpoint moves between
                // every tick, exactly as a healthy leaf taking writes does.
                await ((ILeafProjection)grain).SetCheckpointOffsetAsync(
                    5 + tick,
                    CancellationToken.None);

                await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }
        }

        Assert.That(reasons, Is.Not.Empty,
            "input count: the ticks must have reached the driver, or the negative assertion below "
            + "is satisfied by an unarmed listener rather than by the predicate declining");

        Assert.That(
            reasons,
            Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value),
            "an advancing checkpoint must never be classified as stalled. This is what keeps the "
            + "predicate a detector of non-advancement rather than a timer that fires on every "
            + "leaf once enough ticks have elapsed");
    }

    /// <summary>
    /// The second control. An EMPTY leaf whose checkpoint is equally frozen must
    /// not be driven either.
    /// <para>
    /// A frozen checkpoint is only a fault when there are rows whose durability
    /// depends on it advancing. A genuinely empty leaf has nothing to lose, and
    /// charging it a WAL replay on a fixed cadence would convert a silent stall
    /// into estate-wide replay load - the cost the older predicate's live-data
    /// requirement was careful to avoid, and which this fix must not reintroduce.
    /// </para>
    /// </summary>
    [Test]
    public async Task Coverage_lag_tick_never_drives_an_empty_leaf_whose_checkpoint_has_frozen()
    {
        var treeId = UniqueCoverageRepairTreeId("checkpoint-frozen-empty");

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: null,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        // Deliberately NO SeedRow. This is the only difference from the defect
        // fixture, so a pass here is evidence about the live-data requirement.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);

        var reasons = new List<string>();
        using (ListenForDriverDeclinesOnTree(treeId, reasons))
        {
            for (var tick = 0; tick < TicksToReachStalledDrive; tick++)
            {
                await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }
        }

        Assert.That(reasons, Is.Not.Empty,
            "input count: the ticks must have reached the driver, or the negative assertion below "
            + "proves nothing about the empty-leaf carve-out");

        Assert.That(
            reasons,
            Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value),
            "an empty leaf has no rows whose durability depends on the checkpoint, so it must not "
            + "be charged a replay however long its checkpoint has been still");
    }

    /// <summary>
    /// The re-arm fixture. A leaf that stays frozen must be driven at a bounded
    /// rate, not on every tick.
    /// <para>
    /// This is the coordinator's caveat from #3340 applied here: a fix that
    /// converts a self-reinforcing loop into a latched one has traded one
    /// failure for another. Driving on every tick would make a permanently
    /// frozen leaf replay its WAL continuously, so the stall counter resets when
    /// the drive is requested and the next drive costs a further threshold of
    /// ticks.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_permanently_frozen_leaf_is_driven_at_a_bounded_rate_rather_than_every_tick()
    {
        var treeId = UniqueCoverageRepairTreeId("checkpoint-frozen-rearm");

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: null,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);

        // Three full threshold windows' worth of ticks.
        const int windows = 3;
        var reasons = new List<string>();
        using (ListenForDriverDeclinesOnTree(treeId, reasons))
        {
            for (var tick = 0; tick < TicksToReachStalledDrive * windows; tick++)
            {
                await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }
        }

        var drives = reasons.Count(r => r == LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value);

        Assert.Multiple(() =>
        {
            Assert.That(drives, Is.GreaterThan(0),
                "the leaf is still frozen, so it must still be driven. A zero here would mean the "
                + "re-arm had latched the repair OFF after its first attempt, which trades silent "
                + "data loss for silent non-convergence - the same defect class, one level up");

            Assert.That(drives, Is.LessThan(TicksToReachStalledDrive * windows),
                "but it must NOT be driven on every tick. Each drive costs a WAL replay, so an "
                + "unbounded rate on a permanently frozen leaf would replace a silent stall with "
                + "continuous replay load");
        });
    }
}
