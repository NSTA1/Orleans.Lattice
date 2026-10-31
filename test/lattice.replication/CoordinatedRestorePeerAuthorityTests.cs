using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5443 end to end over the real restore engine and the real tree, on a silo
/// that enforces authorization (anonymous callers are denied). A coordinated
/// restore's control calls reach a participant cluster carrying only the
/// coordinator's cluster identity, so the participant has no user to authorize:
/// before the fix it re-authorized its admission probe as <c>anonymous</c>, voted
/// abort, and no replicated tree could be restored once authorization was on in
/// more than one region.
/// <para>
/// Each test drives the participant exactly as the inbound saga service does: a
/// request whose coordinator is the authenticated remote peer, delivered with no
/// ambient identity. The participant must complete the restore on that peer's
/// authority, and must still refuse it - as the ambient caller - to anything that
/// is not a current replication peer coordinating a replicated tree from backups
/// of replicated trees.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoordinatedRestorePeerAuthorityTests
{
    private const string Peer = "site-west";
    private const string Target = "factory-orders";
    private const string ClusterLocal = "factory-ledger";

    private static readonly string[] CutKeys = ["order/1", "order/2", "order/3"];

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new CoordinatedRestoreClusterFixture();
        await _fixture.InitializeAsync(denyAnonymous: true);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_restore_coordinated_by_an_enrolled_peer_completes_with_authorization_enforced()
    {
        const string tree = Target + "-complete";
        var backupId = await SeedAndCaptureAsync(tree, "cut-complete");
        var participant = NewParticipant(replicated: [tree], peers: [Peer]);
        var request = Request("saga-complete", tree, backupId, coordinator: Peer);

        Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the call arrives with no identity and no elevation");

        var vote = await participant.PrepareAsync(request);
        Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit), vote.Detail);

        await participant.CommitAsync(request);

        await AssertHoldsExactlyTheCutAsync(tree);
        Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the peer authority does not outlive the saga step");
    }

    [Test]
    public async Task The_same_restore_as_this_clusters_own_coordinator_is_still_authorized_as_the_ambient_caller()
    {
        const string tree = Target + "-own";
        var backupId = await SeedAndCaptureAsync(tree, "cut-own");
        var participant = NewParticipant(replicated: [tree], peers: [Peer]);

        // A request this cluster coordinates runs as the ambient caller, whose
        // identity flows with the loopback call. Here there is none, so it is denied.
        var vote = await participant.PrepareAsync(
            Request("saga-own", tree, backupId, coordinator: CoordinatedRestoreClusterFixture.ClusterId));

        Assert.Multiple(() =>
        {
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(vote.Detail, Does.StartWith("Admission probe failed"));
        });
    }

    [Test]
    public async Task A_coordinator_that_is_not_a_current_replication_peer_gets_no_authority()
    {
        const string tree = Target + "-stranger";
        var backupId = await SeedAndCaptureAsync(tree, "cut-stranger");
        var participant = NewParticipant(replicated: [tree], peers: [Peer]);

        var vote = await participant.PrepareAsync(Request("saga-stranger", tree, backupId, coordinator: "site-unknown"));

        Assert.Multiple(() =>
        {
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(vote.Detail, Does.StartWith("Admission probe failed"));
        });
    }

    [Test]
    public async Task An_enrolled_peer_cannot_restore_a_tree_this_cluster_does_not_replicate()
    {
        const string tree = Target + "-unenrolled";
        var backupId = await SeedAndCaptureAsync(tree, "cut-unenrolled");
        var participant = NewParticipant(replicated: [], peers: [Peer]);

        var vote = await participant.PrepareAsync(Request("saga-unenrolled", tree, backupId, coordinator: Peer));

        Assert.Multiple(() =>
        {
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(vote.Detail, Does.Contain("is not replicated on this cluster"));
        });
    }

    [Test]
    public async Task An_enrolled_peer_cannot_replay_a_backup_of_a_cluster_local_tree_into_a_replicated_one()
    {
        const string tree = Target + "-exfil";
        var secret = await SeedAndCaptureAsync(ClusterLocal, "cut-secret");
        var participant = NewParticipant(replicated: [tree], peers: [Peer]);

        var vote = await participant.PrepareAsync(Request("saga-exfil", tree, secret, coordinator: Peer));

        Assert.Multiple(() =>
        {
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(vote.Detail, Does.Contain("cannot be restored into tree"));
        });

        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(await _fixture.GrainFactory.GetGrain<ILattice>(tree).CountAsync(), Is.Zero,
                "nothing from the cluster-local backup reached the replicated tree");
        }
    }

    [Test]
    public async Task A_peer_cannot_tell_a_missing_backup_from_one_it_may_not_restore()
    {
        const string tree = Target + "-oracle";
        var secret = await SeedAndCaptureAsync(ClusterLocal + "-oracle", "cut-oracle");
        var participant = NewParticipant(replicated: [tree], peers: [Peer]);

        var forbidden = await participant.PrepareAsync(Request("saga-oracle-1", tree, secret, coordinator: Peer));
        var missing = await participant.PrepareAsync(Request("saga-oracle-2", tree, secret, coordinator: Peer) with
        {
            ManifestId = "no-such-backup",
        });

        Assert.Multiple(() =>
        {
            Assert.That(forbidden.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(missing.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(missing.Detail!.Replace("no-such-backup", "{backup}"),
                Is.EqualTo(forbidden.Detail!.Replace(secret, "{backup}")),
                "the two refusals differ only by the backup id the peer itself supplied");
        });
    }

    [Test]
    public async Task An_aborted_peer_restore_leaves_no_alias_operation_behind()
    {
        const string tree = Target + "-abort";
        var backupId = await SeedAndCaptureAsync(tree, "cut-abort");
        var participant = NewParticipant(replicated: [tree], peers: [Peer]);
        var aborted = Request("saga-abort-1", tree, backupId, coordinator: Peer);

        Assert.That((await participant.PrepareAsync(aborted)).Vote, Is.EqualTo(SagaVote.Commit));
        await participant.AbortAsync(aborted);

        // A compensation that left the build's alias operation open would refuse the
        // next restore's build ("alias operation ... in progress").
        var retried = Request("saga-abort-2", tree, backupId, coordinator: Peer);
        var vote = await participant.PrepareAsync(retried);
        Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit), vote.Detail);
        await participant.CommitAsync(retried);

        await AssertHoldsExactlyTheCutAsync(tree);
    }

    private async Task<string> SeedAndCaptureAsync(string tree, string label)
    {
        using var origin = LatticeSystemOrigin.Enter();
        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        foreach (var key in CutKeys)
        {
            await lattice.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest(label, BackupScopeSelector.WholeTree(tree)));
        await lattice.SetAsync("order/after-cut", Encoding.UTF8.GetBytes("after-cut"));
        return backup.BackupId;
    }

    private async Task AssertHoldsExactlyTheCutAsync(string tree)
    {
        using var origin = LatticeSystemOrigin.Enter();
        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await lattice.GetAsync("order/after-cut"), Is.Null, "the restore reverted to the cut");
            Assert.That(await lattice.GetAsync(CutKeys[0]), Is.Not.Null);
            Assert.That(await lattice.CountAsync(), Is.EqualTo(CutKeys.Length));
        });
    }

    private RestoreParticipant NewParticipant(string[] replicated, string[] peers)
    {
        var membership = Substitute.For<IReplicatedTreeMembership>();
        var set = new HashSet<string>(replicated, StringComparer.Ordinal);
        membership.IsReplicated(Arg.Any<string>()).Returns(ci => set.Contains((string)ci[0]));
        membership.ReplicatedTrees.Returns(set);

        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = CoordinatedRestoreClusterFixture.ClusterId });

        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(peers);

        var services = _fixture.SiloServices;
        return new RestoreParticipant(
            services.GetRequiredService<ILatticeCoordinatedRestoreEngine>(),
            services.GetRequiredService<ILatticeBackupRestoreService>(),
            services.GetRequiredService<IRestoreCapacityProbe>(),
            services.GetRequiredService<IGrainFactory>(),
            NullLogger<RestoreParticipant>.Instance,
            setResolver: null,
            membership,
            options,
            topology);
    }

    private static SagaControlRequest Request(string sagaId, string tree, string backupId, string coordinator) => new()
    {
        SagaId = sagaId,
        TargetTree = tree,
        ManifestId = backupId,
        CoordinatorClusterId = coordinator,
    };
}
