using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Reproduces and fixes the multi-cluster union re-advance defect (#1169) end to
/// end over the <b>real</b> restore engine, the <b>real</b> durable
/// <see cref="ISagaWriteFenceGrain"/>, and the <b>real</b>
/// <see cref="RestoreParticipant"/>.
/// <para>
/// #1169: a shadow-cutover restore reverts the tree on the cluster that runs it
/// but, because cross-cluster replication is a union merge with no retraction, a
/// peer keeps every post-cut entry and re-ships it, so the two clusters converge
/// back to the pre-restore union - silently undoing the restore. The coordinated
/// restore built for this epic fixes it two ways: it reverts <b>every</b>
/// replicating cluster to the cut (so no cluster holds post-cut entries), and it
/// keeps cross-cluster shipping paused until the saga has <b>globally</b>
/// completed (#1173), so an early-flipping cluster cannot re-absorb a laggard's
/// still-advanced post-cut entries during the cutover window.
/// </para>
/// <para>
/// <b>Boundary of the reproduction.</b> The replication test project does not host
/// a cross-cluster shipping transport, so two logical tree ids (<c>@us</c> and
/// <c>@eu</c>) in one silo stand in for two clusters' replicas of the same tree,
/// and an in-process union-ship helper stands in for the bidirectional shipper -
/// gated on the real fence's shipping-resume flag exactly as the production
/// shipper is. Everything else is the real production path: the real backup
/// capture and coordinated restore engine build and atomically swap the real
/// physical trees, and the real <see cref="RestoreParticipant"/> engages the real
/// fence grain. The real cross-cluster coordinator and control channel are covered
/// separately by <see cref="Grains.CoordinatedRestoreSagaModelTests"/> (real
/// orchestration over a fake engine); this test is its complement (real engine and
/// fence over an in-process transport).
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoordinatedRestoreReadvanceTests
{
    private const string TreeUs = "mfg-facts@us";
    private const string TreeEu = "mfg-facts@eu";
    private const string SagaId = "restore-mfg-facts";

    // Six facts present at the backup cut, keyed like the sample's append-only
    // versioned fact entries so they sort ahead of the post-cut additions.
    private static readonly string[] CutFactKeys =
    [
        "fact/00000000000000000001",
        "fact/00000000000000000002",
        "fact/00000000000000000003",
        "fact/00000000000000000004",
        "fact/00000000000000000005",
        "fact/00000000000000000006",
    ];

    // The two facts appended after the cut (echoing #1169's ReworkCompleted and
    // FinalAcceptance HLCs) that a naive union merge would re-introduce.
    private const string ReworkKey = "fact/00639190558548499199";
    private const string FinalKey = "fact/00639190559517801779";

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CoordinatedRestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Coordinated_restore_reverts_every_cluster_and_the_union_does_not_readvance()
    {
        await _fixture.InitializeAsync();

        var us = _fixture.GrainFactory.GetGrain<ILattice>(TreeUs);
        var eu = _fixture.GrainFactory.GetGrain<ILattice>(TreeEu);

        // Both clusters hold the identical six-fact state that will be the cut.
        await SeedCutAsync(us);
        await SeedCutAsync(eu);

        // Capture the backup at the cut (on the US replica).
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cut", BackupScopeSelector.WholeTree(TreeUs)));

        // Both clusters advance past the cut: under active-active union replication
        // each replica accepts the two post-cut facts, so both read eight facts.
        await AdvancePastCutAsync(us);
        await AdvancePastCutAsync(eu);
        Assert.Multiple(() =>
        {
            Assert.That(us.CountAsync().Result, Is.EqualTo(8), "US advanced past the cut");
            Assert.That(eu.CountAsync().Result, Is.EqualTo(8), "EU advanced past the cut");
        });

        // Run the coordinated restore across the two clusters as ONE saga: a real
        // participant per cluster (each with its own prepared-shadow cache, as
        // separate cluster processes have), the real coordinated restore engine, and
        // the real durable write-fence grain keyed by the shared saga id.
        var usParticipant = NewParticipant();
        var euParticipant = NewParticipant();
        var requestUs = ControlRequest(TreeUs, backup.BackupId);
        var requestEu = ControlRequest(TreeEu, backup.BackupId);

        // Prepare builds each cluster's shadow unfenced; both vote to commit.
        var voteUs = await usParticipant.PrepareAsync(requestUs);
        var voteEu = await euParticipant.PrepareAsync(requestEu);
        Assert.Multiple(() =>
        {
            Assert.That(voteUs.Vote, Is.EqualTo(SagaVote.Commit), "US prepared");
            Assert.That(voteEu.Vote, Is.EqualTo(SagaVote.Commit), "EU prepared");
        });

        // Commit flips both clusters under the brief cutover fence.
        await usParticipant.CommitAsync(requestUs);
        await euParticipant.CommitAsync(requestEu);

        // The #1169 fix, asserted directly: EVERY cluster is back at the cut. In the
        // bug only the restoring cluster reverted (US: 6 facts) while the peer kept
        // the advanced state (EU: 8 facts). Here both are reverted.
        Assert.Multiple(() =>
        {
            Assert.That(us.CountAsync().Result, Is.EqualTo(6), "US reverted to the cut");
            Assert.That(eu.CountAsync().Result, Is.EqualTo(6), "EU reverted to the cut");
            Assert.That(us.GetAsync(ReworkKey).Result, Is.Null, "US dropped the post-cut rework fact");
            Assert.That(us.GetAsync(FinalKey).Result, Is.Null, "US dropped the post-cut final fact");
            Assert.That(eu.GetAsync(ReworkKey).Result, Is.Null, "EU dropped the post-cut rework fact");
            Assert.That(eu.GetAsync(FinalKey).Result, Is.Null, "EU dropped the post-cut final fact");
        });

        // Cross-cluster shipping stays paused until the saga globally completes
        // (#1173). While it is paused a laggard cannot re-inject its post-cut
        // entries into an already-flipped peer.
        var pausedSnapshot = await _fixture.Fence(SagaId).GetSnapshotAsync();
        Assert.That(pausedSnapshot.ShippingResumed, Is.False,
            "shipping is globally gated until every participant has flipped");

        // A union ship attempted during the pause is refused by the gate, so no
        // post-cut entry can flow either way even if one existed.
        await UnionShipIfResumedAsync(us, eu);
        await UnionShipIfResumedAsync(eu, us);
        Assert.Multiple(() =>
        {
            Assert.That(us.CountAsync().Result, Is.EqualTo(6), "no re-advance while shipping is paused");
            Assert.That(eu.CountAsync().Result, Is.EqualTo(6), "no re-advance while shipping is paused");
        });

        // Global completion observed (every participant flipped): the fence resumes
        // shipping on the next poll.
        _fixture.Completion.Complete = true;
        var resumedSnapshot = await _fixture.Fence(SagaId).PollResumeAsync();
        Assert.That(resumedSnapshot.ShippingResumed, Is.True,
            "shipping resumes once the saga has globally completed");

        // Post-resume union ship: because BOTH clusters were reverted to the cut,
        // union merge introduces nothing - the clusters converge to the restored
        // state, not back to the pre-restore union. This is the property that fails
        // without the epic.
        await UnionShipIfResumedAsync(us, eu);
        await UnionShipIfResumedAsync(eu, us);
        Assert.Multiple(() =>
        {
            Assert.That(us.CountAsync().Result, Is.EqualTo(6), "US stays restored after shipping resumes");
            Assert.That(eu.CountAsync().Result, Is.EqualTo(6), "EU stays restored after shipping resumes");
            Assert.That(us.GetAsync(ReworkKey).Result, Is.Null, "no post-cut fact re-appears on US");
            Assert.That(eu.GetAsync(FinalKey).Result, Is.Null, "no post-cut fact re-appears on EU");
        });
    }

    private RestoreParticipant NewParticipant() =>
        new(
            _fixture.SiloServices.GetRequiredService<ILatticeCoordinatedRestoreEngine>(),
            _fixture.SiloServices.GetRequiredService<ILatticeBackupRestoreService>(),
            _fixture.SiloServices.GetRequiredService<IRestoreCapacityProbe>(),
            _fixture.SiloServices.GetRequiredService<IGrainFactory>(),
            NullLogger<RestoreParticipant>.Instance);

    private static SagaControlRequest ControlRequest(string targetTree, string backupId) =>
        new()
        {
            SagaId = SagaId,
            TargetTree = targetTree,
            ManifestId = backupId,
            CoordinatorClusterId = CoordinatedRestoreClusterFixture.ClusterId,
        };

    /// <summary>
    /// In-process stand-in for the bidirectional cross-cluster shipper: a union
    /// merge that copies every entry it can see from <paramref name="source"/> into
    /// <paramref name="dest"/> - but only when the saga's write-fence has resumed
    /// shipping. This mirrors the production shipper, which is gated on the same
    /// flag, so a paused fence blocks re-advance exactly as it does in production.
    /// </summary>
    private async Task UnionShipIfResumedAsync(ILattice source, ILattice dest)
    {
        var snapshot = await _fixture.Fence(SagaId).GetSnapshotAsync();
        if (!snapshot.ShippingResumed)
        {
            return;
        }

        await foreach (var entry in source.ScanEntriesAsync())
        {
            await dest.SetAsync(entry.Key, entry.Value);
        }
    }

    private static async Task SeedCutAsync(ILattice tree)
    {
        foreach (var key in CutFactKeys)
        {
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }
    }

    private static async Task AdvancePastCutAsync(ILattice tree)
    {
        await tree.SetAsync(ReworkKey, Encoding.UTF8.GetBytes("ReworkCompleted"));
        await tree.SetAsync(FinalKey, Encoding.UTF8.GetBytes("FinalAcceptance"));
    }
}
