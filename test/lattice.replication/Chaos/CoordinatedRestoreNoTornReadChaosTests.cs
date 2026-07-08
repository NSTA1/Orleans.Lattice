using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage for the core #1169 / #1173 guarantee over the <b>real</b> restore
/// engine, the <b>real</b> durable write fence, and the <b>real</b>
/// <see cref="RestoreParticipant"/>: while a coordinated restore runs across two
/// clusters (two logical tree ids in one silo standing in for two replicas), a
/// concurrent reader hammering the tree must never observe a torn saga (a partial
/// key set) nor a re-advanced tree (the post-cut union re-appearing), even with a
/// laggard participant holding global completion. The single atomic alias swap and
/// the globally-gated shipping-resume must keep every read whole-old or whole-new.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class CoordinatedRestoreNoTornReadChaosTests
{
    private const string TreeUs = "chaos-facts@us";
    private const string TreeEu = "chaos-facts@eu";
    private const string SagaId = "restore-chaos-facts";

    private static readonly string[] CutFactKeys =
    [
        "fact/00000000000000000001",
        "fact/00000000000000000002",
        "fact/00000000000000000003",
        "fact/00000000000000000004",
        "fact/00000000000000000005",
        "fact/00000000000000000006",
    ];

    private const string ReworkKey = "fact/00639190558548499199";
    private const string FinalKey = "fact/00639190559517801779";
    private const int CutCount = 6;
    private const int AdvancedCount = 8;

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CoordinatedRestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Concurrent_reader_never_observes_a_torn_or_readvanced_tree_during_restore()
    {
        await _fixture.InitializeAsync();

        var us = _fixture.GrainFactory.GetGrain<ILattice>(TreeUs);
        var eu = _fixture.GrainFactory.GetGrain<ILattice>(TreeEu);

        await SeedCutAsync(us);
        await SeedCutAsync(eu);

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cut", BackupScopeSelector.WholeTree(TreeUs)));

        await AdvancePastCutAsync(us);
        await AdvancePastCutAsync(eu);

        // A concurrent reader samples the US tree's whole-key visibility throughout
        // the restore. Every sample must show either all six cut facts (restored) or
        // all eight advanced facts (pre-restore) - never a partial subset (torn).
        var observed = new ConcurrentQueue<int>();
        using var stop = new CancellationTokenSource();
        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var present = 0;
                foreach (var key in CutFactKeys)
                {
                    if (await us.GetAsync(key) is not null) present++;
                }

                var rework = await us.GetAsync(ReworkKey) is not null ? 1 : 0;
                var final = await us.GetAsync(FinalKey) is not null ? 1 : 0;
                observed.Enqueue(present + rework + final);
            }
        });

        var usParticipant = NewParticipant();
        var euParticipant = NewParticipant();
        var requestUs = ControlRequest(TreeUs, backup.BackupId);
        var requestEu = ControlRequest(TreeEu, backup.BackupId);

        var voteUs = await usParticipant.PrepareAsync(requestUs);
        var voteEu = await euParticipant.PrepareAsync(requestEu);
        Assert.Multiple(() =>
        {
            Assert.That(voteUs.Vote, Is.EqualTo(SagaVote.Commit));
            Assert.That(voteEu.Vote, Is.EqualTo(SagaVote.Commit));
        });

        // Commit US first, then hold: EU is a laggard that has not yet flipped. The
        // globally-gated shipping resume keeps shipping paused, so no re-advance can
        // occur during the window between the two clusters' flips.
        await usParticipant.CommitAsync(requestUs);

        var pausedSnapshot = await _fixture.Fence(SagaId).GetSnapshotAsync();
        Assert.That(pausedSnapshot.ShippingResumed, Is.False,
            "shipping stays globally gated while the laggard has not flipped");

        // Let the reader observe the half-flipped window under the paused gate.
        await UnionShipIfResumedAsync(us, eu);
        await UnionShipIfResumedAsync(eu, us);

        // The laggard finally flips.
        await euParticipant.CommitAsync(requestEu);

        // Global completion observed: shipping resumes.
        _fixture.Completion.Complete = true;
        var resumed = await _fixture.Fence(SagaId).PollResumeAsync();
        Assert.That(resumed.ShippingResumed, Is.True);

        await UnionShipIfResumedAsync(us, eu);
        await UnionShipIfResumedAsync(eu, us);

        stop.Cancel();
        await reader;

        // Drain a final batch of samples now the workload is quiescent.
        for (var i = 0; i < 8; i++)
        {
            var present = 0;
            foreach (var key in CutFactKeys)
            {
                if (await us.GetAsync(key) is not null) present++;
            }
            observed.Enqueue(present);
        }

        // No torn read: every sampled whole-tree size is one of the two legal states.
        foreach (var sample in observed)
        {
            Assert.That(sample is CutCount or AdvancedCount, Is.True,
                $"reader observed a torn tree size {sample}; expected {CutCount} or {AdvancedCount}");
        }

        // Deterministic end state: both clusters restored to the cut, no re-advance.
        Assert.Multiple(() =>
        {
            Assert.That(us.CountAsync().Result, Is.EqualTo(CutCount), "US restored to the cut");
            Assert.That(eu.CountAsync().Result, Is.EqualTo(CutCount), "EU restored to the cut");
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

    private async Task UnionShipIfResumedAsync(ILattice source, ILattice dest)
    {
        var snapshot = await _fixture.Fence(SagaId).GetSnapshotAsync();
        if (!snapshot.ShippingResumed)
        {
            return;
        }

        // Drain the source in one tight pass before shipping. Holding the source
        // grain's streaming enumerator open across a slow cross-cluster write on
        // every entry lets the server-side enumerator idle out (or its activation
        // be collected) mid-scan, surfacing as a transient
        // EnumerationAbortedException; a real shipper re-scans on that signal.
        // Buffering first removes the idle window, and the bounded re-scan covers
        // a genuine mid-scan activation loss. Neither weakens the torn-read /
        // re-advance assertions, which check tree content, not enumerator liveness.
        var buffer = new List<KeyValuePair<string, byte[]>>();
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                buffer.Clear();
                await foreach (var entry in source.EntriesAsync())
                {
                    buffer.Add(entry);
                }
                break;
            }
            catch (EnumerationAbortedException) when (attempt < 4)
            {
                // Transient enumerator loss; re-scan from the start.
            }
        }

        foreach (var entry in buffer)
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
