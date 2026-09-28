using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

public sealed partial class LatticeBackupRestoreIntegrationTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Shadow_restore_lifecycle_targets_live_data_and_revert_refuses_deleted_tree(bool revert)
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("key", Bytes("saved"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("lifecycle", BackupScopeSelector.WholeTree(Source)));
        var target = _fixture.GrainFactory.GetGrain<ILattice>("lifecycle-target");
        await target.SetAsync("key", Bytes("before"));
        var result = await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
            backup.BackupId, "lifecycle-target", mode: LatticeRestoreMode.ShadowCutover));
        await target.DeleteTreeAsync();
        Assert.ThrowsAsync<InvalidOperationException>(async () => await target.GetAsync("key"));
        Assert.ThrowsAsync<InvalidOperationException>(() => target.SetAsync("other", Bytes("blocked")));
        Assert.ThrowsAsync<InvalidOperationException>(() => _fixture.Restore.RevertRestoreAsync(result));
        await target.RecoverTreeAsync();
        Assert.That(await target.GetAsync("key"), Is.EqualTo(Bytes("saved")));
        if (revert)
        {
            await _fixture.Restore.RevertRestoreAsync(result);
            Assert.That(await target.GetAsync("key"), Is.EqualTo(Bytes("before")));
        }
        var registry = _fixture.GrainFactory.GetLatticeRegistry();
        var physical = await registry.ResolveAsync("lifecycle-target");
        await target.DeleteTreeAsync();
        await target.PurgeTreeAsync();
        Assert.That(await registry.ExistsAsync(physical), Is.False);
        Assert.That(await target.TreeExistsAsync(), Is.False);
    }
}
