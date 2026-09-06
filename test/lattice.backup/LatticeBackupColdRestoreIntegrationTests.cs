using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end coverage for <see cref="ILatticeBackupColdRestoreService"/>: a
/// disaster restore that depends on the durable sink alone. Every test first
/// captures a backup (populating both the sink and the catalog), then <b>clears
/// the catalog</b> to simulate a cluster that lost its grain storage, and finally
/// cold-restores from the sink. It proves a full backup and a base-plus-increment
/// chain both reconstruct the tree with an empty catalog, that the chain is walked
/// from the sink, that the recovered cluster is left with a correct catalog, and
/// that an unknown backup, a broken chain, and a missing artifact each surface a
/// clear validation error.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupColdRestoreIntegrationTests
{
    private const string Source = "orders";

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Full restore with an empty catalog -----------------------------

    [Test]
    public async Task ColdRestoreAsync_full_backup_with_empty_catalog_reconstructs_the_tree()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));
        await source.SetAsync("k3", Bytes("v3"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        // Simulate a cold cluster: the sink still holds the manifest + artifacts,
        // but the catalog is gone.
        await ClearCatalogAsync();
        Assert.That(await _fixture.Catalog.GetAsync(backup.BackupId), Is.Null, "catalog starts empty");

        const string target = "orders-cold-full";
        var result = await _fixture.ColdRestore.ColdRestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(3));
            Assert.That(result.TargetTreeId, Is.EqualTo(target));
            Assert.That(Str((await restored.GetAsync("k1"))!), Is.EqualTo("v1"));
            Assert.That(Str((await restored.GetAsync("k2"))!), Is.EqualTo("v2"));
            Assert.That(Str((await restored.GetAsync("k3"))!), Is.EqualTo("v3"));
        });

        // The recovered cluster is left with a correct catalog: the cold restore
        // re-projects the sink's manifests back into the reserved catalog tree.
        Assert.That(await _fixture.Catalog.GetAsync(backup.BackupId), Is.Not.Null,
            "cold restore re-projects the catalog from the sink");
    }

    // ---- Chain walked from the sink -------------------------------------

    [Test]
    public async Task ColdRestoreAsync_base_plus_incremental_chain_folds_from_the_sink_with_empty_catalog()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));
        await source.SetAsync("k3", Bytes("v3"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Source)));

        await source.SetAsync("k1", Bytes("v1-updated"));
        await source.SetAsync("k4", Bytes("v4"));
        await source.DeleteAsync("k2");

        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "inc", BackupScopeSelector.WholeTree(Source), baseBackup.BackupId));

        // Cold cluster: clear the catalog so the chain must be walked from the sink.
        await ClearCatalogAsync();

        const string target = "orders-cold-chain";
        var result = await _fixture.ColdRestore.ColdRestoreAsync(
            new LatticeRestoreRequest(increment.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await Assert.MultipleAsync(async () =>
        {
            // The engine walked back to the base (found via the sink) and folded the
            // delta on top: overwrite, delete, and new key all applied.
            Assert.That(result.ManifestChain, Has.Count.EqualTo(2), "base + increment walked from the sink");
            Assert.That(Str((await restored.GetAsync("k1"))!), Is.EqualTo("v1-updated"), "overwrite folded");
            Assert.That(await restored.GetAsync("k2"), Is.Null, "delete folded");
            Assert.That(Str((await restored.GetAsync("k3"))!), Is.EqualTo("v3"), "untouched base entry survives");
            Assert.That(Str((await restored.GetAsync("k4"))!), Is.EqualTo("v4"), "new key folded");
        });

        // Both the base and the increment are re-catalogued from the sink.
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await _fixture.Catalog.GetAsync(baseBackup.BackupId), Is.Not.Null);
            Assert.That(await _fixture.Catalog.GetAsync(increment.BackupId), Is.Not.Null);
        });
    }

    // ---- Idempotency ----------------------------------------------------

    [Test]
    public async Task ColdRestoreAsync_rerun_is_a_no_op()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("rerun", BackupScopeSelector.WholeTree(Source)));
        await ClearCatalogAsync();

        const string target = "orders-cold-rerun";
        var request = new LatticeRestoreRequest(backup.BackupId, target);
        var first = await _fixture.ColdRestore.ColdRestoreAsync(request);
        var second = await _fixture.ColdRestore.ColdRestoreAsync(request);

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(second.EntriesApplied, Is.EqualTo(first.EntriesApplied));
            Assert.That(Str((await restored.GetAsync("k1"))!), Is.EqualTo("v1"));
            Assert.That(Str((await restored.GetAsync("k2"))!), Is.EqualTo("v2"));
        });
    }

    // ---- Clear-error paths ----------------------------------------------

    [Test]
    public async Task ColdRestoreAsync_backup_absent_from_the_sink_throws_validation()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await _fixture.ColdRestore.ColdRestoreAsync(
                new LatticeRestoreRequest(
                    "0000000000000000000000000000000000000000000000000000000000000000", "any-tree")),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    [Test]
    public async Task ColdRestoreAsync_broken_base_chain_throws_validation()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Source)));
        await source.SetAsync("k2", Bytes("v2"));
        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "inc", BackupScopeSelector.WholeTree(Source), baseBackup.BackupId));

        // Break the chain: the base manifest is gone from the sink and the catalog,
        // so the increment's BaseBackupId cannot be resolved.
        await ClearCatalogAsync();
        await _fixture.Sink.DeleteManifestAsync(baseBackup.BackupId);

        Assert.That(
            async () => await _fixture.ColdRestore.ColdRestoreAsync(
                new LatticeRestoreRequest(increment.BackupId, "orders-cold-broken")),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    [Test]
    public async Task ColdRestoreAsync_missing_artifact_throws_validation()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("no-artifact", BackupScopeSelector.WholeTree(Source)));

        // The manifest survives in the sink but its artifact is gone: the sink lost
        // a blob. Cold restore must refuse before installing anything.
        await ClearCatalogAsync();
        var artifactId = backup.Manifest.ContentDescriptors.Single().ArtifactId;
        await _fixture.Sink.DeleteArtifactAsync(artifactId);

        Assert.That(
            async () => await _fixture.ColdRestore.ColdRestoreAsync(
                new LatticeRestoreRequest(backup.BackupId, "orders-cold-noartifact")),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    // ---- Argument guards ------------------------------------------------

    [Test]
    public async Task ColdRestoreAsync_null_request_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.ColdRestore.ColdRestoreAsync(null!),
            Throws.ArgumentNullException);
    }

    // ---- Helpers --------------------------------------------------------

    private async Task ClearCatalogAsync()
    {
        var ids = new List<string>();
        await foreach (var manifest in _fixture.Catalog.ListAsync())
        {
            ids.Add(manifest.Id);
        }

        foreach (var id in ids)
        {
            await _fixture.Catalog.RemoveAsync(id);
        }
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] bytes) => Encoding.UTF8.GetString(bytes);
}
