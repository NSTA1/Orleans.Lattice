using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4586, the source-restore contract: a replacement of a replicated tree's
/// contents on one cluster alone diverges it from its peers - it is counted as
/// uncoordinated - and a coordinated restore afterwards converges every cluster
/// to the restored contents, without being counted itself. Runs over the real
/// restore engine, the real durable write fence and the real restore
/// participant; two logical trees stand in for two clusters' copies of the tree,
/// as in <see cref="CoordinatedRestoreReadvanceTests"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoordinatedRestoreAfterDivergenceTests
{
    private const string TreeUs = "diverge-facts@us";
    private const string TreeEu = "diverge-facts@eu";
    private const string SagaId = "restore-diverge-facts";

    private static readonly string[] CutKeys = ["fact/01", "fact/02", "fact/03"];
    private const string PostCutKey = "fact/04";
    private const string EuOnlyKey = "fact/05";

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CoordinatedRestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_coordinated_restore_after_an_uncoordinated_one_converges_every_cluster()
    {
        await _fixture.InitializeAsync();
        var us = _fixture.GrainFactory.GetGrain<ILattice>(TreeUs);
        var eu = _fixture.GrainFactory.GetGrain<ILattice>(TreeEu);
        foreach (var key in CutKeys)
        {
            await us.SetAsync(key, Encoding.UTF8.GetBytes(key));
            await eu.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cut", BackupScopeSelector.WholeTree(TreeUs)));
        await us.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("post-cut"));
        await eu.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("post-cut"));
        await eu.SetAsync(EuOnlyKey, Encoding.UTF8.GetBytes("eu-only"));

        // US alone replaces its copy: the post-cut write is gone there and kept on EU.
        var uncoordinated = await CountUncoordinatedAsync(TreeUs, () =>
            _fixture.RestoreService.RestoreAsync(new LatticeRestoreRequest(backup.BackupId, TreeUs, mode: LatticeRestoreMode.ShadowCutover)));
        var diverged = (Us: await us.CountAsync(), Eu: await eu.CountAsync());

        // Both clusters then restore the cut as one coordinated saga.
        var usParticipant = NewParticipant();
        var euParticipant = NewParticipant();
        var requestUs = ControlRequest(TreeUs, backup.BackupId);
        var requestEu = ControlRequest(TreeEu, backup.BackupId);
        var coordinated = await CountUncoordinatedAsync(TreeEu, async () =>
        {
            Assert.That((await usParticipant.PrepareAsync(requestUs)).Vote, Is.EqualTo(SagaVote.Commit));
            Assert.That((await euParticipant.PrepareAsync(requestEu)).Vote, Is.EqualTo(SagaVote.Commit));
            await usParticipant.CommitAsync(requestUs);
            await euParticipant.CommitAsync(requestEu);
        });
        _fixture.Completion.Complete = true;
        await _fixture.Fence(SagaId).PollResumeAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(uncoordinated, Is.EqualTo(1), "the unilateral replacement is counted");
            Assert.That(diverged, Is.EqualTo((3, 5)), "precondition: the clusters diverged");
            Assert.That(coordinated, Is.Zero, "the coordinated cutover holds the receive fence, so it is not counted");
            Assert.That(await us.CountAsync(), Is.EqualTo(3));
            Assert.That(await eu.CountAsync(), Is.EqualTo(3));
            Assert.That(await eu.GetAsync(PostCutKey), Is.Null, "EU no longer holds what US's unilateral restore dropped");
            Assert.That(await eu.GetAsync(EuOnlyKey), Is.Null);
        });
    }

    private static async Task<long> CountUncoordinatedAsync(string tree, Func<Task> body)
    {
        long count = 0;
        using var listener = MeterListening.StartForInstrument(
            LatticeReplicationMetrics.SourceRestoreUncoordinated,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagTree && Equals(tag.Value, tree))
                    {
                        Interlocked.Add(ref count, value);
                    }
                }
            }));
        await body();
        return Interlocked.Read(ref count);
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
}
