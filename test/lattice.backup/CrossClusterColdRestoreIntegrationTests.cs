using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Cross-cluster disaster-recovery coverage (epic capability F): proves a backup
/// captured on one cluster can be cold-restored into a genuinely fresh, fully
/// independent cluster whose <b>only</b> shared state is the durable sink - no
/// out-of-band copy of grain or silo storage. Cluster A writes a known data set
/// and captures a backup into a sink both clusters reach; a separate cluster B,
/// with empty reserved <c>sys-</c> trees, cold-restores from the sink alone. The
/// tests assert B rebuilds the reserved trees from nothing, achieves byte-identical
/// value parity, preserves the causal envelope (hybrid-logical-clock timestamp and
/// version vector) captured on A, matches a structural leaf-projection digest, and
/// ends with a correctly re-projected catalog that lists the restored backup. A
/// second test extends the proof to a base-plus-incremental chain walked entirely
/// from the sink on the cold cluster.
/// </summary>
[Category("Integration")]
public sealed class CrossClusterColdRestoreIntegrationTests
{
    private const string Source = "orders";

    private CrossClusterRestoreFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CrossClusterRestoreFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Full backup, cold-restored into a fresh cluster ----------------

    [Test]
    public async Task ColdRestore_of_a_full_backup_into_a_fresh_cluster_reproduces_data_causal_metadata_and_catalog()
    {
        await _fixture.InitializeAsync();

        // Cluster A: write a known data set including a tombstone, then capture.
        var sourceA = _fixture.GrainFactoryA.GetGrain<ILattice>(Source);
        await sourceA.SetAsync("k1", Bytes("v1"));
        await sourceA.SetAsync("k2", Bytes("v2"));
        await sourceA.SetAsync("k3", Bytes("v3"));
        await sourceA.SetAsync("gone", Bytes("temp"));
        await sourceA.DeleteAsync("gone");

        var backup = await _fixture.CaptureA.CaptureAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        // The causal envelope captured on A, keyed by key: HLC + version vector +
        // tombstone + value + per-key merge mode.
        var sourceEntries = (await DecodeAsync(_fixture.SinkA, _fixture.SerializerA, backup.Manifest))
            .ToDictionary(e => e.Key, StringComparer.Ordinal);

        // Cluster B is fresh: it shares only the sink. Its reserved catalog tree is
        // empty - the backup is not discoverable on B until the cold restore
        // re-projects it.
        Assert.That(await _fixture.CatalogB.GetAsync(backup.BackupId), Is.Null,
            "cluster B starts with an empty sys-backup-catalog");

        const string target = "orders-dr";
        var result = await _fixture.ColdRestoreB.ColdRestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target));

        // Values restore byte-for-byte into B, and the deleted key stays absent.
        var restoredB = _fixture.GrainFactoryB.GetGrain<ILattice>(target);
        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(3));
            Assert.That(result.TargetTreeId, Is.EqualTo(target));
            Assert.That(restoredB.GetAsync("k1").Result, Is.EqualTo(Bytes("v1")));
            Assert.That(restoredB.GetAsync("k2").Result, Is.EqualTo(Bytes("v2")));
            Assert.That(restoredB.GetAsync("k3").Result, Is.EqualTo(Bytes("v3")));
            Assert.That(restoredB.GetAsync("gone").Result, Is.Null, "the tombstoned key is not resurrected");
        });

        // Re-capture the restored tree on B and decode it: every live key carries
        // the exact causal envelope A captured - same HLC timestamp, same version
        // vector, same origin, same value - so causal history survived the
        // cross-cluster restore, not just the raw bytes.
        var restoredEntries = (await CaptureAndDecodeBAsync(target, "verify"))
            .ToDictionary(e => e.Key, StringComparer.Ordinal);

        foreach (var key in new[] { "k1", "k2", "k3" })
        {
            var expected = sourceEntries[key];
            var actual = restoredEntries[key];
            Assert.Multiple(() =>
            {
                Assert.That(actual.Value, Is.EqualTo(expected.Value), $"value for {key}");
                Assert.That(actual.Timestamp, Is.EqualTo(expected.Timestamp), $"HLC for {key}");
                Assert.That(actual.VectorClock, Is.EqualTo(expected.VectorClock), $"version vector for {key}");
                Assert.That(actual.OriginClusterId, Is.EqualTo(expected.OriginClusterId), $"origin for {key}");
                Assert.That(actual.IsTombstone, Is.EqualTo(expected.IsTombstone), $"tombstone flag for {key}");
                Assert.That(actual.MergeMode, Is.EqualTo(expected.MergeMode), $"merge mode for {key}");
            });
        }

        // Structural consistency: the restored tree's content-derived leaf-projection
        // digest matches the source's, so the recovered tree is internally
        // consistent and structurally equal - not merely key-for-key equal.
        var sourceDigest = await sourceA.GetLeafProjectionDigestAsync(0);
        var restoredDigest = await restoredB.GetLeafProjectionDigestAsync(0);
        Assert.Multiple(() =>
        {
            Assert.That(restoredDigest.EntryCount, Is.EqualTo(sourceDigest.EntryCount), "digest entry count");
            Assert.That(restoredDigest.Hash, Is.EqualTo(sourceDigest.Hash), "content-derived structural digest");
        });

        // The cold restore rebuilt B's reserved catalog from the sink: the backup is
        // now discoverable on B.
        var catalogued = await _fixture.CatalogB.GetAsync(backup.BackupId);
        Assert.That(catalogued, Is.Not.Null,
            "cold restore re-projects B's catalog from the sink, making the backup discoverable");

        var listed = new List<string>();
        await foreach (var manifest in _fixture.CatalogB.ListAsync())
        {
            listed.Add(manifest.Id);
        }

        Assert.That(listed, Does.Contain(backup.BackupId), "the restored backup is listable on B");
    }

    // ---- Base + incremental chain, cold-restored on a fresh cluster -----

    [Test]
    public async Task ColdRestore_of_an_incremental_chain_into_a_fresh_cluster_folds_the_chain_from_the_sink()
    {
        await _fixture.InitializeAsync();

        // Cluster A: a base capture, then mutations, then an incremental capture.
        var sourceA = _fixture.GrainFactoryA.GetGrain<ILattice>(Source);
        await sourceA.SetAsync("k1", Bytes("v1"));
        await sourceA.SetAsync("k2", Bytes("v2"));
        await sourceA.SetAsync("k3", Bytes("v3"));

        var baseBackup = await _fixture.CaptureA.CaptureAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Source)));

        await sourceA.SetAsync("k1", Bytes("v1-updated"));
        await sourceA.SetAsync("k4", Bytes("v4"));
        await sourceA.DeleteAsync("k2");

        var increment = await _fixture.IncrementalA.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "inc", BackupScopeSelector.WholeTree(Source), baseBackup.BackupId));

        Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));

        // Cluster B is fresh: neither the base nor the increment is catalogued there.
        Assert.Multiple(() =>
        {
            Assert.That(_fixture.CatalogB.GetAsync(baseBackup.BackupId).Result, Is.Null);
            Assert.That(_fixture.CatalogB.GetAsync(increment.BackupId).Result, Is.Null);
        });

        const string target = "orders-dr-chain";
        var result = await _fixture.ColdRestoreB.ColdRestoreAsync(
            new LatticeRestoreRequest(increment.BackupId, target));

        // The engine walked back to the base via the sink and folded the delta on
        // top: overwrite, delete, and new key all applied; the untouched base entry
        // survives.
        var restoredB = _fixture.GrainFactoryB.GetGrain<ILattice>(target);
        Assert.Multiple(() =>
        {
            Assert.That(result.ManifestChain, Has.Count.EqualTo(2), "base + increment walked from the sink on the cold cluster");
            Assert.That(restoredB.GetAsync("k1").Result, Is.EqualTo(Bytes("v1-updated")), "overwrite folded");
            Assert.That(restoredB.GetAsync("k2").Result, Is.Null, "delete folded");
            Assert.That(restoredB.GetAsync("k3").Result, Is.EqualTo(Bytes("v3")), "untouched base entry survives");
            Assert.That(restoredB.GetAsync("k4").Result, Is.EqualTo(Bytes("v4")), "new key folded");
        });

        // The merged result on B equals A's final state, structurally: same
        // content-derived digest over the whole tree.
        var sourceDigest = await sourceA.GetLeafProjectionDigestAsync(0);
        var restoredDigest = await restoredB.GetLeafProjectionDigestAsync(0);
        Assert.Multiple(() =>
        {
            Assert.That(restoredDigest.EntryCount, Is.EqualTo(sourceDigest.EntryCount), "digest entry count matches A's final state");
            Assert.That(restoredDigest.Hash, Is.EqualTo(sourceDigest.Hash), "structural digest matches A's final state");
        });

        // Both links of the chain are re-catalogued from the sink on B.
        Assert.Multiple(() =>
        {
            Assert.That(_fixture.CatalogB.GetAsync(baseBackup.BackupId).Result, Is.Not.Null);
            Assert.That(_fixture.CatalogB.GetAsync(increment.BackupId).Result, Is.Not.Null);
        });
    }

    // ---- Helpers --------------------------------------------------------

    private static async Task<List<LwwEntry>> DecodeAsync(
        ILatticeBackupSink sink, Orleans.Serialization.Serializer serializer, BackupManifest manifest)
    {
        var descriptor = manifest.ContentDescriptors.Single();
        var all = new List<LwwEntry>();
        await foreach (var chunk in sink.ReadArtifactAsync(descriptor.ArtifactId))
        {
            all.AddRange(serializer.Deserialize<LwwEntry[]>(chunk));
        }

        return all;
    }

    private async Task<List<LwwEntry>> CaptureAndDecodeBAsync(string treeId, string name)
    {
        var backup = await _fixture.CaptureB.CaptureAsync(
            new LatticeBackupCaptureRequest(name, BackupScopeSelector.WholeTree(treeId)));
        return await DecodeAsync(_fixture.SinkB, _fixture.SerializerB, backup.Manifest);
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
}
