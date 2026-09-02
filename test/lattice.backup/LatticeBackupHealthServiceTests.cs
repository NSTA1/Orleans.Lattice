using System.Runtime.CompilerServices;
using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupHealthService"/>: verification layers
/// content-hash checking on top of the sink's cheap presence probe. Exercises the
/// missing-manifest, fully-healthy, missing-artifact, and hash-mismatch outcomes
/// plus the null-argument guard, using an in-memory sink whose probe verdict and
/// artifact bytes are scripted per backup.
/// </summary>
[TestFixture]
public sealed class LatticeBackupHealthServiceTests
{
    private static BackupManifest ManifestWithArtifact(string backupId, string artifactId, string contentHash)
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        return new BackupManifest(
            id: backupId,
            name: "nightly",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: BackupKind.Full,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(1, 4096, new[] { "d0" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("k", BackupKeyMergeMode.Crdt, "a") },
            contentDescriptors: new[] { new BackupContentDescriptor(artifactId, contentHash, 12, 1, scope) },
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) });
    }

    [Test]
    public void Constructor_null_sink_throws() =>
        Assert.That(() => new LatticeBackupHealthService(null!), Throws.ArgumentNullException);

    [Test]
    public void VerifyAsync_empty_backup_id_throws() =>
        Assert.That(
            async () => await new LatticeBackupHealthService(new FakeSink()).VerifyAsync(string.Empty),
            Throws.ArgumentException);

    [Test]
    public async Task VerifyAsync_absent_manifest_reports_missing()
    {
        var service = new LatticeBackupHealthService(new FakeSink());

        var report = await service.VerifyAsync("gone");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Missing));
            Assert.That(report.ManifestPresent, Is.False);
            Assert.That(report.Explanation, Does.Contain("gone"));
            Assert.That(report.IsHealthy, Is.False);
        });
    }

    [Test]
    public async Task VerifyAsync_present_and_matching_content_reports_healthy()
    {
        var bytes = Encoding.UTF8.GetBytes("payload-bytes");
        var hash = BackupContentHash.Compute(bytes);
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("ok", "artifact-1", hash))
            .WithArtifact("artifact-1", bytes);

        var report = await new LatticeBackupHealthService(sink).VerifyAsync("ok");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Healthy));
            Assert.That(report.ManifestPresent, Is.True);
            Assert.That(report.MissingArtifactIds, Is.Empty);
            Assert.That(report.HashMismatchArtifactIds, Is.Empty);
            Assert.That(report.IsHealthy, Is.True);
        });
    }

    [Test]
    public async Task VerifyAsync_missing_artifact_reports_warning_and_names_it()
    {
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("torn", "artifact-1", "unused"))
            .WithMissingArtifacts("torn", "artifact-1");

        var report = await new LatticeBackupHealthService(sink).VerifyAsync("torn");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Warning));
            Assert.That(report.MissingArtifactIds, Is.EqualTo(new[] { "artifact-1" }));
            Assert.That(report.HashMismatchArtifactIds, Is.Empty);
            Assert.That(report.Explanation, Does.Contain("artifact-1"));
        });
    }

    [Test]
    public async Task VerifyAsync_hash_mismatch_reports_warning_and_names_it()
    {
        var bytes = Encoding.UTF8.GetBytes("actual-bytes");
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("rot", "artifact-1", "0000deadbeef"))
            .WithArtifact("artifact-1", bytes);

        var report = await new LatticeBackupHealthService(sink).VerifyAsync("rot");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Warning));
            Assert.That(report.MissingArtifactIds, Is.Empty);
            Assert.That(report.HashMismatchArtifactIds, Is.EqualTo(new[] { "artifact-1" }));
            Assert.That(report.Explanation, Does.Contain("artifact-1"));
        });
    }

    [Test]
    public async Task VerifyAsync_replicated_tree_on_a_non_shared_sink_reports_warning_and_names_the_peer()
    {
        // The backup is locally perfect; only the cross-cluster verdict makes it
        // unusable, and that must surface in the health column with a reason.
        var bytes = Encoding.UTF8.GetBytes("payload-bytes");
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("ok", "artifact-1", BackupContentHash.Compute(bytes)))
            .WithArtifact("artifact-1", bytes);
        var service = new LatticeBackupHealthService(
            sink,
            new FakeSharingProbe(Sharing(BackupSinkSharingStatus.NotShared, "region-b")),
            new FakeReplicatedTreeMembership("orders"));

        var report = await service.VerifyAsync("ok");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Warning));
            Assert.That(report.PeerVisibility, Is.EqualTo(BackupSinkSharingStatus.NotShared));
            Assert.That(report.PeerUnconfirmedClusterIds, Is.EqualTo(new[] { "region-b" }));
            Assert.That(report.Explanation, Does.Contain("region-b"));
            Assert.That(report.Explanation, Does.Contain("NOT shared"));
            Assert.That(report.IsHealthy, Is.False);
        });
    }

    [Test]
    public async Task VerifyAsync_non_replicated_tree_ignores_a_non_shared_sink_verdict()
    {
        // A tree nobody replicates is restorable from this cluster alone, so a
        // cross-cluster verdict must not degrade it.
        var bytes = Encoding.UTF8.GetBytes("payload-bytes");
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("ok", "artifact-1", BackupContentHash.Compute(bytes)))
            .WithArtifact("artifact-1", bytes);
        var service = new LatticeBackupHealthService(
            sink,
            new FakeSharingProbe(Sharing(BackupSinkSharingStatus.NotShared, "region-b")),
            new NoReplicatedTreeMembership());

        var report = await service.VerifyAsync("ok");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Healthy));
            Assert.That(report.PeerVisibility, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(report.PeerUnconfirmedClusterIds, Is.Empty);
        });
    }

    [Test]
    public async Task VerifyAsync_shared_sink_stays_healthy_and_records_the_verdict()
    {
        var bytes = Encoding.UTF8.GetBytes("payload-bytes");
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("ok", "artifact-1", BackupContentHash.Compute(bytes)))
            .WithArtifact("artifact-1", bytes);
        var service = new LatticeBackupHealthService(
            sink,
            new FakeSharingProbe(Sharing(BackupSinkSharingStatus.Shared)),
            new FakeReplicatedTreeMembership("orders"));

        var report = await service.VerifyAsync("ok");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Healthy));
            Assert.That(report.PeerVisibility, Is.EqualTo(BackupSinkSharingStatus.Shared));
            Assert.That(report.Explanation, Does.Not.Contain("NOT shared"));
        });
    }

    [Test]
    public async Task VerifyAsync_unverified_sink_stays_healthy_but_explains_why_it_is_unconfirmed()
    {
        // An offline peer is not a fault: the backup stays Healthy, and the reason
        // records that sharing is merely unconfirmed.
        var bytes = Encoding.UTF8.GetBytes("payload-bytes");
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("ok", "artifact-1", BackupContentHash.Compute(bytes)))
            .WithArtifact("artifact-1", bytes);
        var service = new LatticeBackupHealthService(
            sink,
            new FakeSharingProbe(Sharing(BackupSinkSharingStatus.Unverified, "region-b")),
            new FakeReplicatedTreeMembership("orders"));

        var report = await service.VerifyAsync("ok");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Healthy));
            Assert.That(report.PeerVisibility, Is.EqualTo(BackupSinkSharingStatus.Unverified));
            Assert.That(report.Explanation, Does.Contain("unconfirmed"));
        });
    }

    [Test]
    public async Task VerifyAsync_locally_faulted_backup_on_a_non_shared_sink_reports_both_faults()
    {
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("torn", "artifact-1", "unused"))
            .WithMissingArtifacts("torn", "artifact-1");
        var service = new LatticeBackupHealthService(
            sink,
            new FakeSharingProbe(Sharing(BackupSinkSharingStatus.NotShared, "region-b")),
            new FakeReplicatedTreeMembership("orders"));

        var report = await service.VerifyAsync("torn");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Warning));
            Assert.That(report.Explanation, Does.Contain("artifact-1"));
            Assert.That(report.Explanation, Does.Contain("region-b"));
        });
    }

    [Test]
    public async Task VerifyAsync_probe_that_never_ran_leaves_peer_visibility_not_applicable()
    {
        var bytes = Encoding.UTF8.GetBytes("payload-bytes");
        var sink = new FakeSink()
            .WithManifest(ManifestWithArtifact("ok", "artifact-1", BackupContentHash.Compute(bytes)))
            .WithArtifact("artifact-1", bytes);
        var service = new LatticeBackupHealthService(
            sink,
            new NoBackupSinkSharingProbe(),
            new FakeReplicatedTreeMembership("orders"));

        var report = await service.VerifyAsync("ok");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Healthy));
            Assert.That(report.PeerVisibility, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
        });
    }

    private static BackupSinkSharingReport Sharing(BackupSinkSharingStatus status, params string[] unconfirmed) =>
        new(status, "region-a", unconfirmed.Length, unconfirmed, DateTimeOffset.UnixEpoch, $"probe says {status}.");

    /// <summary>A probe that reports a canned verdict as its last result.</summary>
    private sealed class FakeSharingProbe(BackupSinkSharingReport report) : IBackupSinkSharingProbe
    {
        public BackupSinkSharingReport? LastReport => report;

        public Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(report);
    }

    /// <summary>A membership seam that reports a fixed set of trees as replicated.</summary>
    private sealed class FakeReplicatedTreeMembership(params string[] trees) : IReplicatedTreeMembership
    {
        private readonly HashSet<string> _trees = new(trees, StringComparer.Ordinal);

        public IReadOnlyCollection<string> ReplicatedTrees => _trees;

        public bool IsReplicated(string treeId)
        {
            ArgumentNullException.ThrowIfNull(treeId);
            return _trees.Contains(treeId);
        }
    }

    /// <summary>An in-memory sink whose probe verdict, manifest, and artifact bytes are scripted.</summary>
    private sealed class FakeSink : ILatticeBackupSink
    {
        private readonly Dictionary<string, BackupManifest> _manifests = new(StringComparer.Ordinal);
        private readonly Dictionary<string, byte[]> _artifacts = new(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<string>> _missing = new(StringComparer.Ordinal);

        public bool IsDurable => true;

        public FakeSink WithManifest(BackupManifest manifest)
        {
            _manifests[manifest.Id] = manifest;
            return this;
        }

        public FakeSink WithArtifact(string artifactId, byte[] bytes)
        {
            _artifacts[artifactId] = bytes;
            return this;
        }

        public FakeSink WithMissingArtifacts(string backupId, params string[] artifactIds)
        {
            _missing[backupId] = new HashSet<string>(artifactIds, StringComparer.Ordinal);
            return this;
        }

        public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default)
        {
            if (!_manifests.ContainsKey(backupId))
            {
                return Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: false, Array.Empty<string>()));
            }

            var missing = _missing.TryGetValue(backupId, out var set) ? set.ToArray() : Array.Empty<string>();
            return Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: true, missing));
        }

        public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_manifests.GetValueOrDefault(backupId));

        public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
            string artifactId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            if (_artifacts.TryGetValue(artifactId, out var bytes))
            {
                yield return bytes;
            }
        }

        public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_manifests.ContainsKey(backupId));

        public Task WriteArtifactAsync(string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
