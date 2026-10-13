using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

public sealed partial class LatticeBackupRestoreIntegrationTests
{
    [Test]
    public async Task Reverting_a_superseded_restore_preserves_the_later_row_and_takes_no_reservation()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>(Source).SetAsync("key", Bytes("backup"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("superseded-revert", BackupScopeSelector.WholeTree(Source)));
        const string target = "superseded-revert-live";
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("key", Bytes("original"));
        var restore = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        const string later = "superseded-revert-later";
        await _fixture.GrainFactory.GetGrain<ILattice>(later).SetAsync("key", Bytes("later"));
        var registry = _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(target, later);
        var before = (await registry.GetEntryAsync(target))!;

        Assert.ThrowsAsync<InvalidOperationException>(() => _fixture.Restore.RevertRestoreAsync(restore));

        var after = (await registry.GetEntryAsync(target))!;
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.EqualTo(later));
            Assert.That(after.Lineage, Is.EqualTo(before.Lineage));
            Assert.That(after.ShardMap!.Version, Is.EqualTo(before.ShardMap!.Version));
        });
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        var deletion = _fixture.GrainFactory.GetGrain<ITreeDeletionGrain>(target);
        await deletion.BeginAliasChangeAsync("next-operation");
        await deletion.EndAliasChangeAsync("next-operation");
    }
}
