using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Integration coverage for the capturing-cluster stamp
/// (<see cref="BackupManifest.CapturingClusterId"/>) and incremental chain
/// affinity, driven through the live capture service on a single-silo cluster.
/// The stamp is written on every capture, including this single-cluster
/// deployment, where it is simply the local cluster id. An incremental extends
/// only the chain owned by its base's capturing cluster; an extend request whose
/// base was captured on a different cluster produces a fresh full rather than a
/// forked chain. Content-addressed artifacts remain idempotent across identical
/// captures.
/// </summary>
[Category("Integration")]
public sealed class CapturingClusterStampTests
{
    private const string Tree = "orders";

    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CaptureClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Stamp (single-cluster) ------------------------------------------

    [Test]
    public async Task CaptureAsync_stamps_the_local_cluster_id_in_a_single_cluster_deployment()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Tree)));

        Assert.That(backup.Manifest.CapturingClusterId, Is.EqualTo(_fixture.LocalClusterId));
        Assert.That(backup.Manifest.CapturingClusterId, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public async Task CaptureAsync_stamp_survives_a_read_back_from_the_sink()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Tree)));

        var reloaded = await _fixture.Sink.ReadManifestAsync(backup.BackupId);

        Assert.That(reloaded, Is.Not.Null);
        Assert.That(reloaded!.CapturingClusterId, Is.EqualTo(_fixture.LocalClusterId));
    }

    // ---- Chain affinity: same cluster ------------------------------------

    [Test]
    public async Task ChainAffinity_incremental_on_a_local_base_inherits_the_base_capturing_cluster()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Tree)));

        await tree.SetAsync("k2", Bytes("v1"));

        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(Tree), baseBackup.BackupId));

        Assert.Multiple(() =>
        {
            Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(increment.Manifest.BaseBackupId, Is.EqualTo(baseBackup.BackupId));
            // The whole chain shares one capturing-cluster stamp.
            Assert.That(increment.Manifest.CapturingClusterId, Is.EqualTo(baseBackup.Manifest.CapturingClusterId));
            Assert.That(increment.Manifest.CapturingClusterId, Is.EqualTo(_fixture.LocalClusterId));
        });
    }

    // ---- Chain affinity: different cluster --------------------------------

    [Test]
    public async Task ChainAffinity_extend_of_a_foreign_base_starts_a_fresh_full_not_a_fork()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));

        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Tree)));

        // Simulate a base chain owned by a DIFFERENT cluster: re-stamp the stored
        // base manifest with a foreign capturing cluster id. An extend request now
        // arrives on the local cluster, which does not own the chain.
        var foreignBase = baseBackup.Manifest with { CapturingClusterId = "cluster-foreign" };
        await _fixture.Sink.WriteManifestAsync(foreignBase);

        await tree.SetAsync("k2", Bytes("v1"));

        var result = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("extend", BackupScopeSelector.WholeTree(Tree), baseBackup.BackupId));

        Assert.Multiple(() =>
        {
            // No forked incremental: a fresh full on the local cluster, a new chain.
            Assert.That(result.Manifest.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(result.Manifest.BaseBackupId, Is.Null);
            Assert.That(result.Manifest.CapturingClusterId, Is.EqualTo(_fixture.LocalClusterId));
            Assert.That(result.Manifest.CapturingClusterId, Is.Not.EqualTo("cluster-foreign"));
        });
    }

    // ---- Content-addressed idempotency -----------------------------------

    [Test]
    public async Task CaptureAsync_identical_content_is_idempotent_in_the_sink()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v2"));

        var first = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("first", BackupScopeSelector.WholeTree(Tree)));
        var second = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("second", BackupScopeSelector.WholeTree(Tree)));

        // Content-addressed id: identical live state re-captures the same backup id.
        Assert.That(second.BackupId, Is.EqualTo(first.BackupId));

        // The sink holds exactly one manifest for that content-addressed id: the
        // second write was an idempotent overwrite, not a duplicate.
        var manifestsForId = new List<BackupManifest>();
        await foreach (var manifest in _fixture.Sink.ListManifestsAsync())
        {
            if (manifest.Id == first.BackupId)
            {
                manifestsForId.Add(manifest);
            }
        }

        Assert.That(manifestsForId, Has.Count.EqualTo(1));
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
}
