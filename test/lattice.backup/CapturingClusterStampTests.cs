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
/// forked chain. Identical captures publish distinct backups (issue #5418).
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

    // ---- One backup per capture ------------------------------------------

    [Test]
    public async Task CaptureAsync_identical_content_publishes_a_distinct_backup_per_capture()
    {
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v2"));

        var first = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("first", BackupScopeSelector.WholeTree(Tree)));
        var second = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("second", BackupScopeSelector.WholeTree(Tree)));

        // A backup id is unique to its capture (issue #5418): identical live state
        // yields two backups, each with its own manifest and artifact, so neither
        // capture can overwrite the other.
        //
        // The two artifacts are compared by their decoded entries, not by their
        // content digests (#5472). Before #5524 an own row read back from a
        // snapshot with a null origin before background leaf materialization and
        // with the WAL's stamp ("" on a single cluster) after it, so a capture
        // taken either side of that materialization hashed differently. An own
        // row is now null both ways (LatticeRowOrigin), and that is pinned
        // directly by the core's origin tests rather than by a digest here.
        var firstEntries = await DecodeAsync(first.Manifest);
        var secondEntries = await DecodeAsync(second.Manifest);
        Assert.Multiple(async () =>
        {
            Assert.That(second.BackupId, Is.Not.EqualTo(first.BackupId));
            Assert.That(secondEntries, Is.EqualTo(firstEntries));
            Assert.That(firstEntries.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2" }));
            Assert.That((await _fixture.Sink.ProbeAsync(first.BackupId)).IsResolvable, Is.True);
            Assert.That((await _fixture.Sink.ProbeAsync(second.BackupId)).IsResolvable, Is.True);
        });
    }

    // Each captured entry with its origin normalized, so a null and an empty
    // (both unstamped) origin compare equal.
    private async Task<List<(string Key, string Value, HybridLogicalClock Timestamp, bool Tombstone, long ExpiresAt, string? Origin)>> DecodeAsync(
        BackupManifest manifest)
    {
        var entries = new List<(string, string, HybridLogicalClock, bool, long, string?)>();
        await foreach (var chunk in _fixture.Sink.ReadArtifactAsync(manifest.ContentDescriptors.Single().ArtifactId))
        {
            foreach (var entry in _fixture.Serializer.Deserialize<LwwEntry[]>(chunk))
            {
                entries.Add((
                    entry.Key,
                    Encoding.UTF8.GetString(entry.Value ?? []),
                    entry.Timestamp,
                    entry.IsTombstone,
                    entry.ExpiresAtTicks,
                    BackupChainFrontier.NormalizeOrigin(entry.OriginClusterId)));
            }
        }

        return entries;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
}
