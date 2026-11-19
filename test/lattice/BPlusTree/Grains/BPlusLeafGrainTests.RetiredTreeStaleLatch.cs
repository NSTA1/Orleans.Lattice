using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6058. A leaf of a retired physical tree - superseded by a resize, or
/// discarded by an undone resize - has had its WAL released with the tree, so a
/// replay of its frozen checkpoint reaches the stale-projection verdict. That
/// verdict is expected for a retired tree and must not raise the
/// backup-before-restart Error the latch raises for a live one.
/// <para>
/// R1: the coverage-lag timer consults the same liveness verdict the WAL GC
/// sweep uses (<see cref="ITreeDeletionGrain.GetPhysicalRetentionAsync"/>) and
/// does not drive a leaf of a retired tree. R2: a drive that still reaches the
/// latch on a retired tree logs it at Information. A live tree, and a probe that
/// faults, keep the original behaviour.
/// </para>
/// <para>
/// The stalled arm yields while any replay waits on the process-wide gate, so a
/// parallel fixture could withhold a drive; each test is
/// <see cref="NonParallelizableAttribute"/>, as the sibling timer fixtures are.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string TrimmedPastMessage = "WAL has been trimmed past an offset";

    private static ITreeDeletionGrain TreeDeletionReporting(PhysicalTreeRetention retention)
    {
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.GetPhysicalRetentionAsync().Returns(Task.FromResult(retention));
        return deletion;
    }

    private async Task<(BPlusLeafGrain Grain, Func<int> StaleClassifications)> CreateStalledStaleLeafAsync(
        string treeId,
        ITreeDeletionGrain treeDeletion,
        ILoggerFactory? loggerFactory = null)
    {
        var wal = new GrowingWal();
        var decision = FallOffLogDecision.TailReplay;
        var staleClassifications = 0;

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: null,
            treeId: treeId,
            replayCoordinator: wal.Coordinator,
            treeDeletion: treeDeletion,
            loggerFactory: loggerFactory,
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
        wal.GrowTo(5);
        decision = FallOffLogDecision.SnapshotThenWal;
        return (grain, () => staleClassifications);
    }

    private static async Task TickThroughStallWindowsAsync(BPlusLeafGrain grain, int windows)
    {
        for (var tick = 0; tick < TicksToReachStalledDrive * windows; tick++)
        {
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// R1 for an undone resize's destination. The timer must not replay the
    /// leaf, so it never reaches the stale verdict, and the discard is cached so
    /// further stall windows cost no further call to the deletion grain.
    /// <para>RED pre-fix: the drive replays and latches the stale verdict.</para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_stall_timer_does_not_drive_a_leaf_of_a_discarded_tree()
    {
        var deletion = TreeDeletionReporting(PhysicalTreeRetention.Discarded);
        var (grain, staleClassifications) = await CreateStalledStaleLeafAsync(
            UniqueCoverageRepairTreeId("retired-discarded"), deletion);

        await TickThroughStallWindowsAsync(grain, windows: 3);

        Assert.Multiple(() =>
        {
            Assert.That(staleClassifications(), Is.Zero,
                "a discarded tree's WAL was released with it: replaying its checkpoint can only "
                + "reach a stale verdict that is not data loss");
            Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.False,
                "no drive ran, so nothing may be latched");
            Assert.That(deletion.ReceivedCalls().Count(), Is.EqualTo(1),
                "a discard is final, so it is read once per activation and then served from the cache");
        });
    }

    /// <summary>
    /// R1 for a soft-deleted tree. The drive is not run, but the verdict is not
    /// cached, because a deleted tree can be recovered: once it is, a sweep
    /// drive that latches the stale verdict must alarm at Error again.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_stall_timer_does_not_drive_a_leaf_of_a_deleted_tree()
    {
        var retention = PhysicalTreeRetention.Deleted;
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.GetPhysicalRetentionAsync().Returns(_ => Task.FromResult(retention));
        var logs = new RecordingLoggerFactory();
        var (grain, staleClassifications) = await CreateStalledStaleLeafAsync(
            UniqueCoverageRepairTreeId("retired-deleted"), deletion, logs);

        await TickThroughStallWindowsAsync(grain, windows: 2);

        Assert.Multiple(() =>
        {
            Assert.That(staleClassifications(), Is.Zero);
            Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.False);
            Assert.That(deletion.ReceivedCalls().Count(), Is.GreaterThanOrEqualTo(1),
                "the timer drive must consult the tree's retention before replaying");
        });

        retention = PhysicalTreeRetention.Live;
        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());

        Assert.That(
            logs.Entries.Count(e => e.Level == LogLevel.Error && e.Message.Contains(TrimmedPastMessage, StringComparison.Ordinal)),
            Is.EqualTo(1),
            "a deletion is recoverable, so it must be re-read rather than cached");
    }

    /// <summary>
    /// The control. A live tree is driven, latched and alarmed exactly as before
    /// the retention check existed.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_stall_timer_still_latches_and_alarms_a_stale_leaf_of_a_live_tree()
    {
        var logs = new RecordingLoggerFactory();
        var (grain, staleClassifications) = await CreateStalledStaleLeafAsync(
            UniqueCoverageRepairTreeId("retired-live"),
            TreeDeletionReporting(PhysicalTreeRetention.Live),
            logs);

        await TickThroughStallWindowsAsync(grain, windows: 2);

        Assert.Multiple(() =>
        {
            Assert.That(staleClassifications(), Is.EqualTo(1));
            Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True);
            Assert.That(
                logs.Entries.Count(e => e.Level == LogLevel.Error && e.Message.Contains(TrimmedPastMessage, StringComparison.Ordinal)),
                Is.EqualTo(1),
                "a live tree's stale projection may hold the only copy of trimmed writes and must alarm");
        });
    }

    /// <summary>
    /// Fail-open. A deletion grain that cannot answer must leave behaviour as it
    /// was before the check, never suppress a drive or an alarm on a live tree.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_faulting_retention_probe_fails_open_and_still_latches_the_stale_leaf()
    {
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.GetPhysicalRetentionAsync().ThrowsAsync(new TimeoutException("deletion grain unreachable"));
        var logs = new RecordingLoggerFactory();
        var (grain, staleClassifications) = await CreateStalledStaleLeafAsync(
            UniqueCoverageRepairTreeId("retired-probe-fault"), deletion, logs);

        await TickThroughStallWindowsAsync(grain, windows: 2);

        Assert.Multiple(() =>
        {
            Assert.That(staleClassifications(), Is.EqualTo(1));
            Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True);
            Assert.That(logs.Entries.Any(e => e.Level == LogLevel.Error && e.Message.Contains(TrimmedPastMessage, StringComparison.Ordinal)),
                Is.True);
        });
    }

    /// <summary>
    /// R2. The sweep can still call the drive directly on a leaf whose tree has
    /// just been retired. The latch and the typed fault are unchanged, so the
    /// sweep's classification is too, but the log is Information, not the
    /// backup-before-restart Error.
    /// <para>RED pre-fix: the latch logs at Error.</para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_stale_latch_on_a_retired_tree_logs_information_not_an_error()
    {
        var logs = new RecordingLoggerFactory();
        var (grain, _) = await CreateStalledStaleLeafAsync(
            UniqueCoverageRepairTreeId("retired-latch-log"),
            TreeDeletionReporting(PhysicalTreeRetention.Deleted),
            logs);

        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());

        Assert.Multiple(() =>
        {
            Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True,
                "the latch is a statement about the checkpoint and is kept regardless of liveness");
            Assert.That(logs.Entries.Where(e => e.Level >= LogLevel.Error), Is.Empty,
                "a retired tree's released WAL is not data loss and must not page an operator");
            Assert.That(
                logs.Entries.Count(e => e.Level == LogLevel.Information && e.Message.Contains("not data loss", StringComparison.Ordinal)),
                Is.EqualTo(1));
        });
    }
}
