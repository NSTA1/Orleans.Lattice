using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

public sealed partial class LatticeBackupIncrementalSagaConsistencyTests
{
    /// <summary>
    /// Detector for issue #4686. A base written before captures recorded their
    /// undecided sagas carries <see cref="BackupConsistencyCut.UndecidedSagaIds"/> as
    /// <see langword="null"/>, so it cannot hand those sagas on. The scenario is the
    /// one <see cref="A_saga_undecided_at_the_base_and_committed_with_no_record_in_the_window_is_restored_whole"/>
    /// pins, over a base rewritten as such a legacy manifest: an increment layered on
    /// it would never look the saga up and would restore the committed batch as
    /// absent, so the capture must fall back to a full backup, which resolves it
    /// against its own decision snapshot and restores it whole.
    /// </summary>
    [Test]
    public async Task An_increment_on_a_legacy_base_that_recorded_no_undecided_sagas_falls_back_to_a_full_backup_holding_the_batch_whole()
    {
        var treeId = TreeName("legacy");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (_, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));

        var decision = SagaCallGate.HoldDecision();
        var saga = tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]);
        await WithTimeout(decision.Entered.Task, "the saga never reached its commit decision");
        var baseBackup = await Capture.CaptureAsync(new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(treeId)));
        Assert.That(baseBackup.Manifest.ConsistencyCut.UndecidedSagaIds, Has.Count.EqualTo(1),
            "the base must have held the saga pre-saga because it was undecided");

        // Rewrite the base as a manifest captured before the set was recorded.
        var sink = SiloServices.GetRequiredService<ILatticeBackupSink>();
        var legacyBase = baseBackup.Manifest with
        {
            ConsistencyCut = baseBackup.Manifest.ConsistencyCut with { UndecidedSagaIds = null },
        };
        await sink.WriteManifestAsync(legacyBase);
        Assert.That(
            (await sink.ReadManifestAsync(baseBackup.BackupId))!.ConsistencyCut.UndecidedSagaIds,
            Is.Null,
            "the base must now read back as a legacy manifest");

        // The saga commits, and none of its records reaches the increment's window.
        var terminals = SagaCallGate.HoldTerminals();
        decision.Release.TrySetResult();
        await WithTimeout(terminals.Entered.Task, "the saga never committed");

        var wal = await ReadWalAsync(treeId);
        AssertPreparedBeforeWindow(wal, legacyBase, k0);
        AssertPreparedBeforeWindow(wal, legacyBase, k1);

        var capture = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(treeId), baseBackup.BackupId));
        terminals.Release.TrySetResult();
        await WithTimeout(saga, "the saga never completed once its terminals were released");

        var restored = await RestoreAsync(capture, k0, k1);
        Assert.Multiple(() =>
        {
            Assert.That(restored[k0], Is.EqualTo("post"), "the saga committed before the capture, so it must be restored");
            Assert.That(restored[k1], Is.EqualTo("post"), "the saga committed before the capture, so it must be restored");
            Assert.That(capture.Manifest.Kind, Is.EqualTo(BackupKind.Full),
                "an increment cannot be layered on a legacy base, so the capture must fall back to a full backup");
            Assert.That(capture.Manifest.BaseBackupId, Is.Null, "the fallback starts a new chain");
            Assert.That(capture.Manifest.ConsistencyCut.UndecidedSagaIds, Is.Not.Null,
                "the new chain's base records its undecided sagas");
        });
    }
}
