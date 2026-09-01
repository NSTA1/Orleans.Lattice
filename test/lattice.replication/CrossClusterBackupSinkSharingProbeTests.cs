using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="CrossClusterBackupSinkSharingProbe"/>, the
/// capture-side guard that proves a replicated tree's backup sink is the same
/// physical store every peer reads from instead of assuming it. Pins the four
/// things that matter: the probe is completely inert when it cannot apply (no
/// replicated tree, no peers, no backup sink), a peer marker read back from the
/// local sink proves sharing, a missing marker only accuses the sink when the peer
/// is demonstrably reachable, and a marker that does not attest to the expected
/// peer fails closed rather than counting as proof.
/// </summary>
[TestFixture]
public sealed class CrossClusterBackupSinkSharingProbeTests
{
    private const string Self = "region-a";
    private const string Peer = "region-b";

    private static CrossClusterBackupSinkSharingProbe CreateProbe(
        ILatticeBackupSink? sink,
        IReadOnlyCollection<string> peers,
        bool peerReachable = true,
        string clusterId = Self,
        bool withControlChannel = true,
        params string[] replicatedTrees)
    {
        var membership = Substitute.For<IReplicatedTreeMembership>();
        membership.ReplicatedTrees.Returns(replicatedTrees);

        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(peers);

        ISagaControlChannel? channel = null;
        if (withControlChannel)
        {
            var stub = Substitute.For<ISagaControlChannel>();
            if (peerReachable)
            {
                stub.GetStatusAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
                    .Returns(Task.FromResult(new SagaControlResponse()));
            }
            else
            {
                stub.GetStatusAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
                    .Returns<Task<SagaControlResponse>>(_ => throw new InvalidOperationException("peer down"));
            }

            channel = stub;
        }

        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = clusterId });

        return new CrossClusterBackupSinkSharingProbe(
            membership,
            topology,
            channel,
            options,
            NullLogger<CrossClusterBackupSinkSharingProbe>.Instance,
            sink);
    }

    [Test]
    public async Task ProbeAsync_no_backup_sink_is_inert()
    {
        var probe = CreateProbe(sink: null, peers: [Peer], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(report.Explanation, Does.Contain("no backup sink"));
        });
    }

    [Test]
    public async Task ProbeAsync_nothing_replicated_writes_no_marker()
    {
        // Explicit acceptance criterion: a deployment with no replicated tree must
        // gain no new I/O and no new failure mode.
        var sink = new RecordingSink();
        var probe = CreateProbe(sink, peers: [Peer]);

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(sink.Writes, Is.Empty, "An unreplicated deployment must not touch the sink.");
            Assert.That(sink.Reads, Is.Empty);
        });
    }

    [Test]
    public async Task ProbeAsync_unset_cluster_id_is_inert_and_writes_no_marker()
    {
        // A host whose replication cluster id is the unset sentinel has no identity
        // to attest to, so the probe must stay inert rather than derive a marker id
        // from an empty string.
        var sink = new RecordingSink();
        var probe = CreateProbe(sink, peers: [Peer], clusterId: "  ", replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(report.Explanation, Does.Contain("cluster id"));
            Assert.That(sink.Writes, Is.Empty);
            Assert.That(sink.Reads, Is.Empty);
        });
    }

    [Test]
    public async Task ProbeAsync_without_a_control_channel_never_accuses_the_sink()
    {
        // Only the gRPC transport package registers an ISagaControlChannel, so a
        // host on the no-op or a custom transport has none. Without a liveness
        // signal an absent marker can never be shown to be a fault, so the verdict
        // must degrade to Unverified rather than manufacture an accusation.
        var sink = new RecordingSink();
        var probe = CreateProbe(sink, peers: [Peer], withControlChannel: false, replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.Unverified));
            Assert.That(report.UnconfirmedPeerClusterIds, Is.EqualTo(new[] { Peer }));
        });
    }

    [Test]
    public async Task ProbeAsync_zero_peers_writes_no_marker()
    {
        // Explicit acceptance criterion: no cross-cluster probe at all when there
        // are no peers.
        var sink = new RecordingSink();
        var probe = CreateProbe(sink, peers: [], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(report.PeerCount, Is.Zero);
            Assert.That(sink.Writes, Is.Empty, "A peerless deployment must not touch the sink.");
        });
    }

    [Test]
    public async Task ProbeAsync_peer_set_containing_only_self_writes_no_marker()
    {
        var sink = new RecordingSink();
        var probe = CreateProbe(sink, peers: [Self], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(sink.Writes, Is.Empty);
        });
    }

    [Test]
    public async Task ProbeAsync_replicated_tree_with_peers_publishes_its_own_marker()
    {
        var sink = new RecordingSink();
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        await probe.ProbeAsync();

        Assert.That(sink.Writes, Is.EqualTo(new[] { BackupSinkCanary.ArtifactId(Self) }));
    }

    [Test]
    public async Task ProbeAsync_peer_marker_present_reports_shared()
    {
        var sink = new RecordingSink();
        sink.Seed(BackupSinkCanary.ArtifactId(Peer), BackupSinkCanary.Encode(Peer, DateTimeOffset.UnixEpoch));
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.Shared));
            Assert.That(report.PeerCount, Is.EqualTo(1));
            Assert.That(report.UnconfirmedPeerClusterIds, Is.Empty);
            Assert.That(report.ClusterId, Is.EqualTo(Self));
        });
    }

    [Test]
    public async Task ProbeAsync_marker_absent_and_peer_reachable_reports_not_shared()
    {
        // The fault the issue is about: each cluster is writing to its own isolated
        // sink, and the peer is up, so the absence is proof rather than a guess.
        var probe = CreateProbe(new RecordingSink(), peers: [Peer], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotShared));
            Assert.That(report.IsRefuted, Is.True);
            Assert.That(report.UnconfirmedPeerClusterIds, Is.EqualTo(new[] { Peer }));
            Assert.That(report.Explanation, Does.Contain(Peer));
            Assert.That(report.Explanation, Does.Contain("NOT shared"));
        });
    }

    [Test]
    public async Task ProbeAsync_marker_absent_and_peer_unreachable_reports_unverified()
    {
        // A peer that has not started yet must never be accused: an offline peer
        // leaves the verdict undecided.
        var probe = CreateProbe(new RecordingSink(), peers: [Peer], peerReachable: false, replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.Unverified));
            Assert.That(report.IsRefuted, Is.False);
            Assert.That(report.UnconfirmedPeerClusterIds, Is.EqualTo(new[] { Peer }));
        });
    }

    [Test]
    public async Task ProbeAsync_marker_attesting_to_a_different_cluster_is_not_proof()
    {
        // Fail closed: content read out of the sink can never nominate itself as
        // belonging to a peer whose id it does not carry.
        var sink = new RecordingSink();
        sink.Seed(BackupSinkCanary.ArtifactId(Peer), BackupSinkCanary.Encode("region-z", DateTimeOffset.UnixEpoch));
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotShared));
    }

    [Test]
    public async Task ProbeAsync_marker_without_the_magic_header_is_not_proof()
    {
        var sink = new RecordingSink();
        sink.Seed(BackupSinkCanary.ArtifactId(Peer), Encoding.UTF8.GetBytes($"something-else\n{Peer}\n"));
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotShared));
    }

    [Test]
    public async Task ProbeAsync_oversized_marker_is_not_proof()
    {
        var sink = new RecordingSink();
        sink.Seed(BackupSinkCanary.ArtifactId(Peer), new byte[BackupSinkCanary.MaxBytes + 1]);
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotShared));
    }

    [Test]
    public async Task ProbeAsync_read_fault_is_treated_as_an_absent_marker()
    {
        var sink = new RecordingSink { FailReads = true };
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotShared));
    }

    [Test]
    public async Task ProbeAsync_mixed_peers_reports_not_shared_and_names_only_the_unconfirmed_peer()
    {
        var sink = new RecordingSink();
        sink.Seed(BackupSinkCanary.ArtifactId("region-c"), BackupSinkCanary.Encode("region-c", DateTimeOffset.UnixEpoch));
        var probe = CreateProbe(sink, peers: [Peer, "region-c"], replicatedTrees: "orders");

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotShared));
            Assert.That(report.PeerCount, Is.EqualTo(2));
            Assert.That(report.UnconfirmedPeerClusterIds, Is.EqualTo(new[] { Peer }));
        });
    }

    [Test]
    public async Task ProbeAsync_publishes_the_verdict_for_the_cached_health_read()
    {
        var sink = new RecordingSink();
        sink.Seed(BackupSinkCanary.ArtifactId(Peer), BackupSinkCanary.Encode(Peer, DateTimeOffset.UnixEpoch));
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        Assert.That(probe.LastReport, Is.Null, "Nothing is claimed before the first probe.");

        var report = await probe.ProbeAsync();

        Assert.That(probe.LastReport, Is.SameAs(report));
    }

    [Test]
    public async Task ProbeAsync_rewrites_one_marker_per_run_so_the_sink_never_accumulates_litter()
    {
        var sink = new RecordingSink();
        var probe = CreateProbe(sink, peers: [Peer], replicatedTrees: "orders");

        await probe.ProbeAsync();
        await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(sink.Writes, Has.Count.EqualTo(2), "Each run refreshes the marker.");
            Assert.That(sink.Writes.Distinct(), Has.Exactly(1).Items, "Always the same id, so nothing accumulates.");
        });
    }

    [Test]
    public void ArtifactId_is_deterministic_stable_and_free_of_reserved_separators()
    {
        var id = BackupSinkCanary.ArtifactId(Self);

        Assert.Multiple(() =>
        {
            Assert.That(id, Is.EqualTo(BackupSinkCanary.ArtifactId(Self)));
            Assert.That(id, Is.Not.EqualTo(BackupSinkCanary.ArtifactId(Peer)));
            Assert.That(id, Does.StartWith(BackupSinkCanary.ArtifactIdPrefix));

            // The Azure blob backend forbids '/' in an artifact id and the in-cluster
            // sink uses U+001F as its composite-key separator, so a derived id must
            // carry neither.
            Assert.That(id.Contains('/'), Is.False);
            Assert.That(id.Contains('\u001f'), Is.False);
        });
    }

    [Test]
    public void ArtifactId_survives_a_cluster_id_full_of_reserved_characters()
    {
        // Cluster ids are unconstrained free text; hashing is what keeps the derived
        // artifact id legal for every sink backend.
        var id = BackupSinkCanary.ArtifactId("region/a\u001fweird name");

        Assert.Multiple(() =>
        {
            Assert.That(id, Does.StartWith(BackupSinkCanary.ArtifactIdPrefix));
            Assert.That(id.Contains('/'), Is.False);
            Assert.That(id.Contains('\u001f'), Is.False);
            Assert.That(id.Contains(' '), Is.False);
        });
    }

    [Test]
    public void ArtifactId_empty_cluster_id_throws() =>
        Assert.That(() => BackupSinkCanary.ArtifactId(string.Empty), Throws.ArgumentException);

    [Test]
    public void Encode_empty_cluster_id_throws() =>
        Assert.That(() => BackupSinkCanary.Encode(string.Empty, DateTimeOffset.UnixEpoch), Throws.ArgumentException);

    [Test]
    public void Attests_round_trips_its_own_encoding() =>
        Assert.That(
            BackupSinkCanary.Attests(BackupSinkCanary.Encode(Peer, DateTimeOffset.UnixEpoch), Peer),
            Is.True);

    [Test]
    public void Attests_rejects_an_empty_body() =>
        Assert.That(BackupSinkCanary.Attests(ReadOnlySpan<byte>.Empty, Peer), Is.False);

    [Test]
    public void Attests_empty_expected_cluster_id_throws()
    {
        var body = BackupSinkCanary.Encode(Peer, DateTimeOffset.UnixEpoch);

        Assert.That(() => BackupSinkCanary.Attests(body, string.Empty), Throws.ArgumentException);
    }

    /// <summary>
    /// An in-memory backup sink that records the marker ids written and read, so a
    /// test can assert the probe touched the sink exactly as expected - or, for the
    /// inertness tests, not at all.
    /// </summary>
    private sealed class RecordingSink : ILatticeBackupSink
    {
        private readonly Dictionary<string, byte[]> _artifacts = new(StringComparer.Ordinal);

        public bool IsDurable => true;

        public bool FailReads { get; init; }

        public List<string> Writes { get; } = [];

        public List<string> Reads { get; } = [];

        public void Seed(string artifactId, byte[] bytes) => _artifacts[artifactId] = bytes;

        public async Task WriteArtifactAsync(
            string artifactId,
            IAsyncEnumerable<ReadOnlyMemory<byte>> content,
            CancellationToken cancellationToken = default)
        {
            Writes.Add(artifactId);
            var buffer = new List<byte>();
            await foreach (var chunk in content.WithCancellation(cancellationToken))
            {
                buffer.AddRange(chunk.ToArray());
            }

            _artifacts[artifactId] = [.. buffer];
        }

        public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
            string artifactId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            Reads.Add(artifactId);
            await Task.Yield();
            if (FailReads)
            {
                throw new InvalidOperationException("sink read failed");
            }

            if (_artifacts.TryGetValue(artifactId, out var bytes))
            {
                yield return bytes;
            }
        }

        public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
