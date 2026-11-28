using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6226. Discarding an undone resize's destination deregisters its leaves'
/// cursors and durable pins once. A leaf of that tree activated afterwards (by a
/// WAL GC drive, a sweep, or a stray call) used to re-register its cursors and
/// re-publish its durable pins on its first checkpoint flush - which nothing
/// cleared again until a purge, so the discarded tree held the shared WAL. Each
/// activation now probes the tree's retention once, before its replay can flush;
/// a discarded tree's leaf unregisters its own consumers on its first report and publishes nothing
/// afterwards, while a live or soft-deleted (recoverable) tree keeps publishing
/// with no probe on its checkpoint path.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static ILeafCursorReporter RecordingCursorReporter()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(), Arg.Any<IReadOnlyList<MaterialiserPinReport>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(true));
        return reporter;
    }

    private static int PublishCalls(ILeafCursorReporter reporter) =>
        reporter.ReceivedCalls().Count(c => c.GetMethodInfo().Name is
            nameof(ILeafCursorReporter.ReportAsync)
            or nameof(ILeafCursorReporter.ReportCheckpointAsync)
            or nameof(ILeafCursorReporter.ClearCheckpointAsync)
            or nameof(ILeafCursorReporter.NoteDurableMaterialiserFrontier)
            or nameof(ILeafCursorReporter.FlushDurableMaterialiserFrontierAsync));

    private static int UnregisterCalls(ILeafCursorReporter reporter) =>
        reporter.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILeafCursorReporter.UnregisterAsync));

    private static int ProbeCalls(ITreeDeletionGrain deletion) =>
        deletion.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITreeDeletionGrain.GetPhysicalRetentionAsync));

    private sealed record CheckpointFlushObservation(
        int ActivationUnregisters, int FlushUnregisters, int FlushPublishes, int FlushProbes, int ActivationProbes);

    private async Task<CheckpointFlushObservation> ObserveCheckpointFlushAsync(ITreeDeletionGrain deletion, string treeId)
    {
        var reporter = RecordingCursorReporter();
        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: treeId,
            cursorReporter: reporter,
            treeDeletion: deletion);
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 };
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        var activationUnregisters = UnregisterCalls(reporter);
        var activationProbes = ProbeCalls(deletion);
        SeedRow(grain);
        reporter.ClearReceivedCalls();
        deletion.ClearReceivedCalls();

        using (LatticeApplyOffsetContext.BeginScope(0, 7))
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);
        }

        Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(7L), "precondition: the checkpoint was persisted");
        return new CheckpointFlushObservation(activationUnregisters, UnregisterCalls(reporter), PublishCalls(reporter), ProbeCalls(deletion), activationProbes);
    }

    /// <summary>
    /// A leaf activated after its tree was discarded unregisters its own consumers
    /// and re-publishes no cursor or pin on a later checkpoint flush.
    /// <para>RED pre-fix: the flush tail reports every partition and publishes the pin.</para>
    /// </summary>
    [Test]
    public async Task A_leaf_activated_after_its_tree_was_discarded_unregisters_and_republishes_no_cursor_or_pin()
    {
        var observed = await ObserveCheckpointFlushAsync(
            TreeDeletionReporting(PhysicalTreeRetention.Discarded), ResizeCopyTreeId("discarded-flush-report"));

        Assert.Multiple(() =>
        {
            Assert.That(observed.ActivationUnregisters + observed.FlushUnregisters, Is.GreaterThan(0),
                "the first report after activation must clear anything an earlier activation re-published after the discard");
            Assert.That(observed.FlushPublishes, Is.Zero,
                "the discard deregistered this tree's cursors and pins; a re-publish would hold the WAL until a purge");
        });
    }

    /// <summary>
    /// Control: a live tree keeps reporting and pinning on every checkpoint flush,
    /// and its checkpoint path makes no retention probe.
    /// </summary>
    [Test]
    public async Task A_checkpoint_flush_on_a_leaf_of_a_live_tree_reports_its_cursor_and_pin_without_a_probe()
    {
        var observed = await ObserveCheckpointFlushAsync(
            TreeDeletionReporting(PhysicalTreeRetention.Live), UniqueCoverageRepairTreeId("live-flush-report"));

        Assert.Multiple(() =>
        {
            Assert.That(observed.ActivationUnregisters + observed.FlushUnregisters, Is.Zero, "a live tree's pins must not be retired");
            Assert.That(observed.FlushPublishes, Is.GreaterThan(0),
                "a live tree's pins hold the WAL its leaves still need, so the flush must keep publishing them");
            Assert.That(observed.FlushProbes, Is.Zero,
                "the retention probe runs once per activation, never on the checkpoint path");
        });
    }

    /// <summary>
    /// Only a resize copy can be discarded, so an ordinary tree's leaf activation
    /// makes no retention probe: a cold start or bulk load of a large tree must
    /// not fan every leaf activation into its tree deletion grain.
    /// </summary>
    [Test]
    public async Task A_leaf_activation_of_an_ordinary_tree_makes_no_retention_probe()
    {
        var observed = await ObserveCheckpointFlushAsync(
            TreeDeletionReporting(PhysicalTreeRetention.Discarded), UniqueCoverageRepairTreeId("ordinary-activation"));

        Assert.Multiple(() =>
        {
            Assert.That(observed.ActivationProbes, Is.Zero,
                "an ordinary tree cannot be discarded; its activation must not call the deletion grain");
            Assert.That(observed.ActivationUnregisters + observed.FlushUnregisters, Is.Zero,
                "an ordinary tree's pins must not be retired");
            Assert.That(observed.FlushPublishes, Is.GreaterThan(0), "an ordinary tree keeps publishing its pins");
        });
    }

    /// <summary>
    /// A soft-deleted tree can be recovered, so its leaves must keep pinning the
    /// WAL they would replay after recovery.
    /// </summary>
    [Test]
    public async Task A_checkpoint_flush_on_a_leaf_of_a_deleted_tree_still_reports_its_cursor_and_pin()
    {
        var observed = await ObserveCheckpointFlushAsync(
            TreeDeletionReporting(PhysicalTreeRetention.Deleted), ResizeCopyTreeId("deleted-flush-report"));

        Assert.Multiple(() =>
        {
            Assert.That(observed.ActivationUnregisters + observed.FlushUnregisters, Is.Zero, "a deleted tree is recoverable");
            Assert.That(observed.FlushPublishes, Is.GreaterThan(0),
                "a deleted tree is recoverable, so its pins must survive a checkpoint flush");
        });
    }

    private sealed record CoverageLagTickObservation(int Unregisters, int Probes);

    private async Task<CoverageLagTickObservation> ObserveCoverageLagTickAsync(
        string treeId, PhysicalTreeRetention verdictAfterActivation, ITreeResizeGrain? treeResize = null, int ticks = 1)
    {
        var verdict = PhysicalTreeRetention.Live;
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.GetPhysicalRetentionAsync().Returns(_ => Task.FromResult(verdict));
        var reporter = RecordingCursorReporter();
        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: treeId,
            cursorReporter: reporter,
            treeDeletion: deletion,
            treeResize: treeResize);
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 };
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        using (LatticeApplyOffsetContext.BeginScope(0, 7))
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);
        }

        // The leaf is resident and checkpointed; the discard lands afterwards.
        verdict = verdictAfterActivation;
        reporter.ClearReceivedCalls();
        deletion.ClearReceivedCalls();

        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        var firstTickProbes = ProbeCalls(deletion);
        deletion.ClearReceivedCalls();
        for (var tick = 1; tick < ticks; tick++)
        {
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        }

        return new CoverageLagTickObservation(
            UnregisterCalls(reporter), ticks == 1 ? firstTickProbes : ProbeCalls(deletion));
    }

    private static ITreeResizeGrain ResizeCoordinator(bool namesCopy, bool holdsMigrations)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.ReferencesPhysicalTreeAsync(Arg.Any<string>()).Returns(Task.FromResult(namesCopy));
        resize.HoldsShardMigrationsAsync().Returns(Task.FromResult(holdsMigrations));
        return resize;
    }

    /// <summary>
    /// Once the copy's resize names it and no longer holds the replaced copy's
    /// migrations, no undo can discard it, so later full passes stop probing.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_on_a_resize_copy_beyond_undo_stops_probing_its_retention()
    {
        var observed = await ObserveCoverageLagTickAsync(
            ResizeCopyTreeId("settled-tick"), PhysicalTreeRetention.Live,
            ResizeCoordinator(namesCopy: true, holdsMigrations: false), ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(observed.Probes, Is.Zero, "a copy no undo can reach must not pay a probe on every later pass");
            Assert.That(observed.Unregisters, Is.Zero, "a live tree's pins must not be retired");
        });
    }

    /// <summary>
    /// While the replaced copy is still mirrored, an undo can still discard the
    /// copy, so every full pass keeps probing.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_on_a_resize_copy_an_undo_can_still_discard_keeps_probing()
    {
        var observed = await ObserveCoverageLagTickAsync(
            ResizeCopyTreeId("undoable-tick"), PhysicalTreeRetention.Live,
            ResizeCoordinator(namesCopy: true, holdsMigrations: true), ticks: 3);

        Assert.That(observed.Probes, Is.EqualTo(2), "each later pass must re-check a copy an undo can still discard");
    }

    private static string ResizeCopyTreeId(string label) =>
        $"{UniqueCoverageRepairTreeId(label)}/resized/{Guid.NewGuid():N}";

    /// <summary>
    /// A resident, already-checkpointed leaf of a resize copy that is discarded
    /// after its activation retires its own pins on the next full coverage-lag
    /// pass, rather than banking or re-publishing them.
    /// <para>RED pre-fix: the tick advances and re-publishes; nothing unregisters.</para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_on_a_resident_leaf_of_a_discarded_resize_copy_retires_its_pins()
    {
        var observed = await ObserveCoverageLagTickAsync(
            ResizeCopyTreeId("discarded-tick"), PhysicalTreeRetention.Discarded);

        Assert.Multiple(() =>
        {
            Assert.That(observed.Probes, Is.GreaterThan(0), "a resize copy's full pass must check whether it was discarded");
            Assert.That(observed.Unregisters, Is.GreaterThan(0),
                "the discard deregistered this tree's pins once; the resident leaf must clear what it re-published");
        });
    }

    /// <summary>
    /// A soft-deleted resize copy is recoverable, so the tick keeps its pins.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_on_a_leaf_of_a_deleted_resize_copy_keeps_its_pins()
    {
        var observed = await ObserveCoverageLagTickAsync(
            ResizeCopyTreeId("deleted-tick"), PhysicalTreeRetention.Deleted);

        Assert.That(observed.Unregisters, Is.Zero, "a deleted tree is recoverable");
    }

    /// <summary>
    /// Only a resize copy can be discarded, so an ordinary tree's coverage-lag
    /// pass makes no retention probe.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_on_a_leaf_of_an_ordinary_tree_makes_no_retention_probe()
    {
        var observed = await ObserveCoverageLagTickAsync(
            UniqueCoverageRepairTreeId("ordinary-tick"), PhysicalTreeRetention.Live);

        Assert.Multiple(() =>
        {
            Assert.That(observed.Probes, Is.Zero, "an ordinary tree cannot be discarded; its tick must not call the deletion grain");
            Assert.That(observed.Unregisters, Is.Zero, "a live tree's pins must not be retired");
        });
    }

    /// <summary>
    /// A faulted retention probe fails open: the leaf behaves as a live tree's.
    /// </summary>
    [Test]
    public async Task A_faulted_retention_probe_at_activation_fails_open_and_keeps_publishing()
    {
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.GetPhysicalRetentionAsync().ThrowsAsync(new InvalidOperationException("deletion grain unreachable"));

        var observed = await ObserveCheckpointFlushAsync(deletion, ResizeCopyTreeId("faulted-probe-flush-report"));

        Assert.Multiple(() =>
        {
            Assert.That(observed.ActivationUnregisters + observed.FlushUnregisters, Is.Zero, "an unknown verdict must never retire pins");
            Assert.That(observed.FlushPublishes, Is.GreaterThan(0),
                "an unknown verdict must leave a live tree's pins published");
        });
    }
}
