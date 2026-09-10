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

        // A concurrent reader samples the US tree's whole-tree size throughout the
        // restore. Every sample must show either the six cut facts (restored) or the
        // eight advanced facts (pre-restore) - never a partial subset (torn).
        //
        // The sample MUST be a single atomic observation. An earlier form of this
        // reader composed the size from eight sequential GetAsync calls, which is
        // not a snapshot: the advanced state is a strict superset of the cut state,
        // so all six cut facts are present in both and only ReworkKey/FinalKey vary.
        // Reading those two adjacently and non-atomically across the single atomic
        // alias swap yields a size of 7 - a torn OBSERVATION, not a torn tree - and
        // the assertion then failed on correct behaviour (issue #2633). CountAsync
        // resolves routing once, stamps one registry snapshot across the fan-out and
        // re-checks the shard-map version afterwards, retrying on an alias or
        // topology change, so it returns a size from one coherent tree version.
        // That makes a 7 here mean what the assertion says it means.
        var observed = new ConcurrentQueue<(string Phase, int Sample)>();
        var phase = "before-prepare";
        using var stop = new CancellationTokenSource();
        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                observed.Enqueue((Volatile.Read(ref phase), await us.CountAsync()));
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
        Volatile.Write(ref phase, "committing-us");
        await usParticipant.CommitAsync(requestUs);

        var pausedSnapshot = await _fixture.Fence(SagaId).GetSnapshotAsync();
        Assert.That(pausedSnapshot.ShippingResumed, Is.False,
            "shipping stays globally gated while the laggard has not flipped");

        // Let the reader observe the half-flipped window under the paused gate.
        Volatile.Write(ref phase, "half-flipped-gate-paused");
        await UnionShipIfResumedAsync(us, eu);
        await UnionShipIfResumedAsync(eu, us);

        // The laggard finally flips.
        Volatile.Write(ref phase, "committing-eu");
        await euParticipant.CommitAsync(requestEu);

        // Global completion observed: shipping resumes.
        _fixture.Completion.Complete = true;
        var resumed = await _fixture.Fence(SagaId).PollResumeAsync();
        Assert.That(resumed.ShippingResumed, Is.True);

        Volatile.Write(ref phase, "shipping-resumed");
        await UnionShipIfResumedAsync(us, eu);
        await UnionShipIfResumedAsync(eu, us);

        stop.Cancel();
        await reader;

        // Drain a final batch of samples now the workload is quiescent.
        Volatile.Write(ref phase, "quiescent");
        for (var i = 0; i < 8; i++)
        {
            observed.Enqueue(("quiescent", await us.CountAsync()));
        }

        // No torn read: every sampled whole-tree size is one of the two legal states.
        // The phase is carried alongside the sample so a failure is attributable to a
        // point in the saga rather than being an unplaceable integer (issue #2633).
        foreach (var (samplePhase, sample) in observed)
        {
            Assert.That(sample is CutCount or AdvancedCount, Is.True,
                $"reader observed a torn tree size {sample} during phase '{samplePhase}'; "
                + $"expected {CutCount} or {AdvancedCount}");
        }

        // Deterministic end state: both clusters restored to the cut, no re-advance.
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await us.CountAsync(), Is.EqualTo(CutCount), "US restored to the cut");
            Assert.That(await eu.CountAsync(), Is.EqualTo(CutCount), "EU restored to the cut");
            Assert.That(await us.GetAsync(ReworkKey), Is.Null, "no post-cut fact re-appears on US");
            Assert.That(await eu.GetAsync(FinalKey), Is.Null, "no post-cut fact re-appears on EU");
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
            catch (EnumerationAbortedException) when (attempt < 19)
            {
                // Transient enumerator loss: the source activation can be swapped
                // or collected by the just-completed restore mid-scan. A tight
                // no-delay retry can land every attempt inside the same
                // reactivation window, so back off with a short, growing delay -
                // the bounded re-scan then spans past the transient window before
                // giving up. A production shipper re-scans on this signal the same
                // way.
                await Task.Delay(TimeSpan.FromMilliseconds(25 * (attempt + 1)));
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