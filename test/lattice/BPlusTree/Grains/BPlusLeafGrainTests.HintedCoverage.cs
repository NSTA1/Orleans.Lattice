using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5877 (a5e2e2ef's H1-H5): a split sibling born with a hinted checkpoint holds it pending,
/// and never persists a checkpoint above <c>-1</c> at a partition until a capture of its own
/// cache covering that partition has landed. Every flush path - the graceful-deactivation
/// flush and the coalescing due-flush - captures first, then persists; a capture that fails
/// leaves the partition at <c>-1</c> persisted, its advance pending. A partition already
/// covered pays nothing new.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>A leaf born with a hinted checkpoint at partition 0, seeded with one row.</summary>
    private static async Task<(BPlusLeafGrain Grain, Orleans.Lattice.Tests.Fakes.FakePersistentState<Orleans.Lattice.BPlusTree.State.LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot, List<long> PersistedAtCapture)>
        HintedSiblingAsync(TimeSpan interval, Exception? captureFailure = null, long[]? existingCoverage = null, long persisted = -1L)
    {
        var (grain, state, snapshot, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: persisted,
            reClassifyEveryN: 1000,
            existingCoverage: existingCoverage,
            treeId: UniqueCoverageRepairTreeId("hinted-sibling"),
            materialiserCheckpointInterval: interval);

        // Each capture records the persisted checkpoint at the moment it is taken.
        var persistedAtCapture = new List<long>();
        if (captureFailure is null)
        {
            snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .Returns(_ =>
                {
                    persistedAtCapture.Add(PersistedPartitionZero(state.State));
                    return Task.FromResult(LeafSnapshotSaveOutcome.Kept);
                });
        }
        else
        {
            snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .Throws(_ =>
                {
                    persistedAtCapture.Add(PersistedPartitionZero(state.State));
                    return captureFailure;
                });
        }

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        return (grain, state, snapshot, persistedAtCapture);
    }

    private static long PersistedPartitionZero(Orleans.Lattice.BPlusTree.State.LeafNodeState state) =>
        state.ProjectionCheckpointOffsetAssigned == true ? state.ProjectionCheckpointOffset : -1L;

    private static Task DeactivateAsync(BPlusLeafGrain grain) =>
        ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

    [Test]
    public async Task A_deactivating_hinted_sibling_captures_before_it_persists_its_hint()
    {
        var (grain, state, _, persistedAtCapture) = await HintedSiblingAsync(TimeSpan.FromHours(1));
        await grain.SetCheckpointOffsetHintsAsync([7]);
        var persistedBefore = PersistedPartitionZero(state.State);

        await DeactivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(persistedBefore, Is.EqualTo(-1L), "precondition: the hint is held pending under coalescing");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(7L), "precondition: the hint is the leaf's current checkpoint");
            Assert.That(persistedAtCapture, Is.Not.Empty, "the deactivation never captured the hinted partition");
            Assert.That(persistedAtCapture[0], Is.EqualTo(-1L),
                "the hint was persisted before the capture covering it landed (H2): a kill between the two leaves a checkpoint above coverage");
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(7L), "the covered hint is persisted after the capture");
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(7L),
                "the persisted checkpoint is above the capture's coverage");
        });
    }

    [Test]
    public async Task A_hinted_sibling_whose_capture_fails_persists_its_checkpoint_at_minus_one()
    {
        var (grain, state, _, persistedAtCapture) = await HintedSiblingAsync(
            TimeSpan.FromHours(1), captureFailure: new InvalidOperationException("snapshot store is down"));
        await grain.SetCheckpointOffsetHintsAsync([7]);

        await DeactivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(persistedAtCapture, Is.Not.Empty, "precondition: the deactivation attempted a capture");
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L), "precondition: no capture landed");
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(-1L),
                "a hint was persisted above coverage after its capture failed (H2): the next activation must repair or replay cold");
        });
    }

    [Test]
    public async Task A_due_flush_of_a_hinted_sibling_captures_before_it_persists_its_hint()
    {
        // Every-entry mode: the hint's own stamp is a due flush.
        var (grain, state, _, persistedAtCapture) = await HintedSiblingAsync(TimeSpan.Zero);

        await grain.SetCheckpointOffsetHintsAsync([7]);

        Assert.Multiple(() =>
        {
            Assert.That(persistedAtCapture, Is.Not.Empty, "the due flush never captured the hinted partition");
            Assert.That(persistedAtCapture[0], Is.EqualTo(-1L),
                "the due flush persisted the hint before the capture covering it landed (H2)");
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(7L), "the covered hint is persisted after the capture");
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(7L));
        });
    }

    [Test]
    public async Task A_cold_restart_after_a_failed_deactivation_capture_needs_no_repair_for_a_hinted_sibling()
    {
        // H2, H3: the deactivation's capture fails, so the hint is not persisted; the next
        // activation finds no checkpointed partition without coverage, and needs no repair
        // capture before it can publish a usable pin.
        var treeId = UniqueCoverageRepairTreeId("hinted-restart");
        var (grain, state, snapshot, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            treeId: treeId,
            materialiserCheckpointInterval: TimeSpan.FromHours(1));
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("snapshot store is down"));
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await grain.SetCheckpointOffsetHintsAsync([7]);
        await DeactivateAsync(grain);

        // The store recovers; the leaf reactivates over the same state and snapshot store.
        var capturesAtRestart = 0;
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                capturesAtRestart++;
                return Task.FromResult(LeafSnapshotSaveOutcome.Kept);
            });
        var (restarted, _, _, _) = CreateLeafForCoverageRepair(
            treeId: treeId,
            materialiserCheckpointInterval: TimeSpan.FromHours(1),
            reactivateState: state,
            reactivateSnapshot: snapshot);
        await LeafActivationHarness.ActivateAsync(restarted, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(-1L), "the hint was persisted above coverage");
            Assert.That(capturesAtRestart, Is.Zero, "the restart needed an activation-time repair capture for a hint persisted above coverage");
            Assert.That(restarted.HasCheckpointedPartitionWithoutCoverage(1), Is.False);
        });
    }

    [Test]
    public async Task A_failing_store_is_asked_for_a_hint_capture_once_per_tick_never_once_per_flush()
    {
        // H2's retry is bounded and never charged to the zero-coverage repair budget: after a
        // failed hint capture, the next one waits for the coverage-lag tick.
        var (grain, state, _, persistedAtCapture) = await HintedSiblingAsync(
            TimeSpan.Zero, captureFailure: new InvalidOperationException("snapshot store is down"));
        for (var hint = 7L; hint < 47L; hint++)
        {
            await grain.SetCheckpointOffsetHintsAsync([hint]);
        }

        var beforeTick = persistedAtCapture.Count;
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        await grain.SetCheckpointOffsetHintsAsync([47]);

        Assert.Multiple(() =>
        {
            Assert.That(beforeTick, Is.EqualTo(1), "40 due flushes against a failing store each asked it for a capture");
            Assert.That(persistedAtCapture.Count, Is.GreaterThan(beforeTick), "the coverage-lag tick did not re-arm the hint capture");
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(-1L), "a hint was persisted above coverage");
        });
    }

    [Test]
    public async Task A_hint_at_a_covered_partition_persists_with_no_new_capture()
    {
        // C4: a partition a snapshot already covers pays nothing new.
        var (grain, state, _, persistedAtCapture) = await HintedSiblingAsync(TimeSpan.Zero, existingCoverage: [3], persisted: 3L);

        await grain.SetCheckpointOffsetHintsAsync([7]);

        Assert.Multiple(() =>
        {
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(0L), "precondition: the partition is covered");
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(7L), "the hint is persisted");
            Assert.That(persistedAtCapture, Is.Empty, "a covered partition's hint paid for a capture");
        });
    }
}
