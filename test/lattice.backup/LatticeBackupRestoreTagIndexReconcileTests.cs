using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression coverage for the tag-index reconcile that a shadow-cutover restore
/// fires. A tag index is maintained inline in a sibling index tree and is not
/// tail-orphaned, but a shadow-cutover restore reverts the subject tree's
/// contents without reprojecting the index, so membership for keys absent from
/// the restored point-in-time would keep answering tag queries until the next
/// scheduled reconcile sweep. The restore must trigger a prompt reconcile so the
/// index converges to the restored subject immediately.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupRestoreTagIndexReconcileTests
{
    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task RestoreAsync_shadow_cutover_reconciles_tag_index_membership_for_the_restored_subject()
    {
        await _fixture.InitializeAsync();

        // The backup source captures a point-in-time that contains only 'keep'.
        const string source = "recon-src";
        var sourceTree = _fixture.GrainFactory.GetGrain<ILattice>(source);
        await sourceTree.SetAsync("keep", Bytes("keep-v"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("recon", BackupScopeSelector.WholeTree(source)));

        // The live target carries 'keep' and 'gone', both tagged 'red', with the
        // index built over the live tree so it records the target as a covered tree.
        const string target = "recon-live";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeTagIndexFactory>();
        var index = factory.Create(_fixture.GrainFactory.GetGrain<ILattice>(target), "recon-colors");
        await index.SetValueWithTags("keep", Bytes("keep-v"), "red").CommitAsync();
        await index.SetValueWithTags("gone", Bytes("gone-v"), "red").CommitAsync();

        var before = await RedKeysAsync(index);
        Assert.That(before, Is.EquivalentTo(new[] { "gone", "keep" }),
            "Both tagged keys must be indexed before the restore.");

        // Shadow-cutover the target back to the captured point-in-time: the target
        // now resolves to a shadow containing only 'keep'; 'gone' is absent.
        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));

        // The restore fired a reconcile, so the orphaned 'gone' membership row is
        // already gone without waiting for the scheduled sweep.
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        var after = await RedKeysAsync(index);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await live.GetAsync("gone"), Is.Null, "The restored subject must not contain 'gone'.");
            Assert.That(after, Is.EquivalentTo(new[] { "keep" }),
                "The reconcile fired by the restore must drop membership for the key the restore removed.");
        });
    }

    private static async Task<List<string>> RedKeysAsync(ILatticeTagIndex index)
    {
        var keys = new List<string>();
        await foreach (var key in index.WithAnyTags("red"))
        {
            keys.Add(key);
        }
        return keys;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
}
