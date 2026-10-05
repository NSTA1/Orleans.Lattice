using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A restore that swaps a tree's content for a backup's re-stamps the tree's
/// content lineage, and so does reverting it; an in-place restore merges into the
/// existing content and keeps it (issue #4537). A replication receiver deletes a
/// source-origin key an in-place re-bootstrap no longer carries only while its
/// copy is aligned with the source's lineage, so a swapped-in copy must read as a
/// different lineage.
/// </summary>
public sealed partial class LatticeBackupRestoreIntegrationTests
{
    private async Task<Guid?> LineageAsync(string tree) =>
        (await _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(tree))?.Lineage;

    [Test]
    public async Task A_shadow_cutover_restamps_the_lineage_and_its_revert_restamps_it_again()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("backup-v1"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("lineage-cutover", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-lineage-live";
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("live-key", Bytes("live-value"));
        var before = await LineageAsync(target);

        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        var cutOver = await LineageAsync(target);
        await _fixture.Restore.RevertRestoreAsync(result);
        var reverted = await LineageAsync(target);

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.Not.Null, "PRECONDITION: the live tree has a lineage");
            Assert.That(cutOver, Is.Not.Null.And.Not.EqualTo(before), "the cutover swapped in the backup's content");
            Assert.That(reverted, Is.Not.Null.And.Not.EqualTo(cutOver).And.Not.EqualTo(before),
                "the revert swaps content again, and a receiver aligned before the cutover may have seen writes to the shadow");
        });
    }

    [Test]
    public async Task An_in_place_restore_keeps_the_lineage()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("lineage-in-place", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-lineage-in-place";
        await _fixture.GrainFactory.GetGrain<ILattice>(target).SetAsync("existing", Bytes("value"));
        var before = await LineageAsync(target);

        var result = await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(backup.BackupId, target));

        Assert.Multiple(async () =>
        {
            Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.InPlace));
            Assert.That(await LineageAsync(target), Is.EqualTo(before),
                "an in-place restore merges by last-writer-wins into the existing content");
        });
    }
}
